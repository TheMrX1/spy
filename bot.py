from __future__ import annotations

import asyncio
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()


# --- settings ----------------------------------------------------------------
@dataclass
class Settings:
    bot_token: str
    trusted_ids: Set[int]
    db_path: Path
    media_dir: Path
    poll_timeout: int
    forward_media_immediately: bool
    clean_after_send: bool
    forward_only_ephemeral: bool


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def parse_settings() -> Settings:
    bot_token = os.getenv("TG_BOT_TOKEN")
    if not bot_token:
        raise SystemExit("Set TG_BOT_TOKEN (bot from @BotFather with Business access)")

    trusted_raw = os.getenv("TRUSTED_USER_IDS", "")
    trusted_ids: Set[int] = set()
    for part in trusted_raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            trusted_ids.add(int(part))
        except ValueError as exc:
            raise SystemExit(f"Bad TRUSTED_USER_IDS entry: {part}") from exc

    poll_timeout = int(os.getenv("POLL_TIMEOUT", "25"))
    forward_media_immediately = _parse_bool(os.getenv("FORWARD_MEDIA_IMMEDIATELY", "true"), True)
    clean_after_send = _parse_bool(os.getenv("CLEAN_AFTER_SEND", "true"), True)
    forward_only_ephemeral = _parse_bool(os.getenv("FORWARD_ONLY_EPHEMERAL", "true"), True)

    return Settings(
        bot_token=bot_token,
        trusted_ids=trusted_ids,
        db_path=Path(os.getenv("DB_PATH", "data/messages.sqlite3")),
        media_dir=Path(os.getenv("MEDIA_DIR", "data/media")),
        poll_timeout=poll_timeout,
        forward_media_immediately=forward_media_immediately,
        clean_after_send=clean_after_send,
        forward_only_ephemeral=forward_only_ephemeral,
    )


# --- storage -----------------------------------------------------------------
class Store:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS owners (
                owner_id INTEGER PRIMARY KEY,
                chat_id INTEGER,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                connection_id TEXT PRIMARY KEY,
                owner_id INTEGER NOT NULL,
                username TEXT,
                enabled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                connection_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                chat_id INTEGER,
                sender_id INTEGER,
                text TEXT,
                media_path TEXT,
                media_type TEXT,
                sender_label TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (connection_id, message_id)
            )
            """
        )
        # миграция: добавляем колонку sender_label при обновлении
        self._ensure_column("messages", "sender_label", "TEXT")
        self.conn.commit()
        self.lock = asyncio.Lock()

    def _ensure_column(self, table: str, column: str, coltype: str) -> None:
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        cols = [row[1] for row in cur.fetchall()]
        if column not in cols:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")
            self.conn.commit()

    async def save_owner(self, owner_id: int, chat_id: int, username: Optional[str]) -> None:
        async with self.lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO owners (owner_id, chat_id, username)
                VALUES (?, ?, ?)
                """,
                (owner_id, chat_id, username),
            )
            self.conn.commit()

    async def save_connection(self, connection_id: str, owner_id: int, username: Optional[str], enabled: bool) -> None:
        async with self.lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO connections (connection_id, owner_id, username, enabled)
                VALUES (?, ?, ?, ?)
                """,
                (connection_id, owner_id, username, int(enabled)),
            )
            self.conn.commit()

    async def get_owner_for_connection(self, connection_id: str) -> Tuple[Optional[int], Optional[int]]:
        async with self.lock:
            cur = self.conn.execute(
                """
                SELECT c.owner_id, o.chat_id
                FROM connections c
                LEFT JOIN owners o ON c.owner_id = o.owner_id
                WHERE c.connection_id = ?
                """,
                (connection_id,),
            )
            row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]

    async def get_owner_chat(self, owner_id: int) -> Optional[int]:
        async with self.lock:
            cur = self.conn.execute("SELECT chat_id FROM owners WHERE owner_id = ?", (owner_id,))
            row = cur.fetchone()
        return row[0] if row else None

    async def upsert_message(
        self,
        connection_id: str,
        message_id: int,
        chat_id: Optional[int],
        sender_id: Optional[int],
        text: str,
        media_path: Optional[str],
        media_type: Optional[str],
        sender_label: Optional[str],
    ) -> None:
        async with self.lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO messages (connection_id, message_id, chat_id, sender_id, text, media_path, media_type, sender_label)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (connection_id, message_id, chat_id, sender_id, text, media_path, media_type, sender_label),
            )
            self.conn.commit()

    async def get_message(self, connection_id: str, message_id: int) -> Optional[Dict[str, Any]]:
        async with self.lock:
            cur = self.conn.execute(
                """
                SELECT connection_id, message_id, chat_id, sender_id, text, media_path, media_type, sender_label
                FROM messages
                WHERE connection_id = ? AND message_id = ?
                """,
                (connection_id, message_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "connection_id": row[0],
            "message_id": row[1],
            "chat_id": row[2],
            "sender_id": row[3],
            "text": row[4],
            "media_path": row[5],
            "media_type": row[6],
            "sender_label": row[7],
        }

    async def delete_message(self, connection_id: str, message_id: int) -> None:
        async with self.lock:
            self.conn.execute(
                "DELETE FROM messages WHERE connection_id = ? AND message_id = ?", (connection_id, message_id)
            )
            self.conn.commit()


# --- Bot API client ----------------------------------------------------------
class BotAPI:
    def __init__(self, token: str, session: aiohttp.ClientSession):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.file_url = f"https://api.telegram.org/file/bot{token}"
        self.session = session

    async def _request(
        self, method: str, params: Optional[Dict[str, Any]] = None, files: Optional[Dict[str, Path]] = None
    ) -> Any:
        url = f"{self.base_url}/{method}"
        if files:
            form = aiohttp.FormData()
            for key, value in (params or {}).items():
                form.add_field(key, str(value))
            for field, path in files.items():
                form.add_field(field, open(path, "rb"), filename=Path(path).name)  # noqa: P201
            async with self.session.post(url, data=form) as resp:
                data = await resp.json()
        else:
            async with self.session.post(url, json=params or {}) as resp:
                data = await resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"{method} failed: {data}")
        return data["result"]

    async def get_updates(self, offset: Optional[int], timeout: int, allowed_updates: list[str]) -> list[Dict[str, Any]]:
        return await self._request(
            "getUpdates",
            params={
                "offset": offset,
                "timeout": timeout,
                "allowed_updates": allowed_updates,
            },
        )

    async def send_message(self, chat_id: int, text: str) -> None:
        await self._request("sendMessage", params={"chat_id": chat_id, "text": text})

    async def send_message_fmt(self, chat_id: int, text: str, parse_mode: Optional[str] = None) -> None:
        params: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if parse_mode:
            params["parse_mode"] = parse_mode
        await self._request("sendMessage", params=params)

    async def send_document(self, chat_id: int, file_path: Path, caption: Optional[str] = None) -> None:
        await self._request(
            "sendDocument",
            params={"chat_id": chat_id, "caption": caption or ""},
            files={"document": file_path},
        )

    async def send_photo(self, chat_id: int, file_path: Path, caption: Optional[str] = None) -> None:
        await self._request(
            "sendPhoto",
            params={"chat_id": chat_id, "caption": caption or ""},
            files={"photo": file_path},
        )

    async def get_file(self, file_id: str) -> Dict[str, Any]:
        return await self._request("getFile", params={"file_id": file_id})

    async def download_file(self, file_path: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self.file_url}/{file_path}"
        async with self.session.get(url) as resp:
            resp.raise_for_status()
            content = await resp.read()
        dest.write_bytes(content)
        return dest

    async def send_media_by_type(
        self, chat_id: int, media_type: str, file_path: Path, caption: Optional[str] = None
    ) -> None:
        if media_type == "photo":
            await self.send_photo(chat_id, file_path, caption)
            return
        await self.send_document(chat_id, file_path, caption)


# --- helpers -----------------------------------------------------------------
def extract_media(message: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if "photo" in message and message["photo"]:
        return "photo", message["photo"][-1]["file_id"]
    for key in ["document", "video", "animation", "audio", "voice", "video_note", "sticker"]:
        if key in message:
            return key, message[key]["file_id"]
    return None, None


def is_ephemeral_media(message: Dict[str, Any], media_type: Optional[str]) -> bool:
    # Telegram Bot API не даёт явного флага для view-once, но ttl_seconds появляется у self-destruct.
    if message.get("ttl_seconds"):
        return True
    if media_type:
        media_obj = message.get(media_type)
        if isinstance(media_obj, dict) and media_obj.get("ttl_seconds"):
            return True
        if isinstance(media_obj, list):
            for item in media_obj:
                if isinstance(item, dict) and item.get("ttl_seconds"):
                    return True
    return False


def html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


async def save_media(
    api: BotAPI, message: Dict[str, Any], media_dir: Path, connection_id: str
) -> Tuple[Optional[str], Optional[str]]:
    media_type, file_id = extract_media(message)
    if not file_id:
        return None, None
    try:
        file_info = await api.get_file(file_id)
        remote_path = file_info.get("file_path")
        if not remote_path:
            return None, None
        dest_dir = media_dir / connection_id
        dest_path = dest_dir / Path(remote_path).name
        await api.download_file(remote_path, dest_path)
        return str(dest_path), media_type
    except Exception as exc:  # noqa: BLE001
        print(f"Не удалось скачать медиа: {exc}")
        return None, media_type


def sender_label(message: Dict[str, Any]) -> str:
    sender = message.get("from", {})
    username = sender.get("username")
    first = sender.get("first_name")
    last = sender.get("last_name")
    uid = sender.get("id")
    name_parts = [p for p in [first, last] if p]
    base = None
    if name_parts:
        base = " ".join(name_parts)
    elif username:
        base = f"@{username}"
    else:
        base = "user"
    return f"{base} (id: {uid})"


def user_allowed(user_id: int, settings: Settings) -> bool:
    return not settings.trusted_ids or user_id in settings.trusted_ids


# --- handlers ----------------------------------------------------------------
async def handle_start(api: BotAPI, store: Store, settings: Settings, message: Dict[str, Any]) -> None:
    user = message.get("from", {})
    user_id = user.get("id")
    chat_id = message["chat"]["id"]
    username = user.get("username")
    if user_id is None:
        return
    if not user_allowed(user_id, settings):
        await api.send_message(chat_id, "Вы не в списке доверенных пользователей.")
        return
    await store.save_owner(user_id, chat_id, username)
    text = (
        "✅ Готов к работе.\n"
        "1) Включите Telegram Business для своего аккаунта.\n"
        "2) В настройках Business добавьте этого бота и разрешите доступ ко всем личным чатам.\n"
        "3) После подключения я буду фиксировать удаленные/измененные сообщения и одноразовые медиа."
    )
    await api.send_message(chat_id, text)


async def handle_business_connection(api: BotAPI, store: Store, settings: Settings, bc: Dict[str, Any]) -> None:
    connection_id = bc["id"]
    user = bc.get("user", {})
    owner_id = user.get("id")
    if owner_id is None:
        return
    if not user_allowed(owner_id, settings):
        return
    enabled = bool(bc.get("is_enabled", bc.get("can_reply", True)))
    await store.save_connection(connection_id, owner_id, user.get("username"), enabled)
    owner_chat = await store.get_owner_chat(owner_id)
    if owner_chat:
        await api.send_message(
            owner_chat,
            f"Бизнес-подключение {connection_id} для @{user.get('username','user')} "
            f"{'включено' if enabled else 'отключено'}.",
        )


async def handle_business_message(
    api: BotAPI, store: Store, settings: Settings, bm: Dict[str, Any]
) -> None:
    connection_id = bm["business_connection_id"]
    message_id = bm["message_id"]
    chat = bm.get("chat", {})
    chat_id = chat.get("id")
    sender_id = bm.get("from", {}).get("id")
    owner_id, owner_chat = await store.get_owner_for_connection(connection_id)
    if owner_id is None or not user_allowed(owner_id, settings):
        return

    text = bm.get("text") or bm.get("caption") or ""
    label = sender_label(bm)
    media_path, media_type = await save_media(api, bm, settings.media_dir, connection_id)
    is_ephemeral = is_ephemeral_media(bm, media_type)
    if not is_ephemeral and bm.get("has_protected_content") and not media_path:
        is_ephemeral = True
    if not is_ephemeral and not media_path and any(bm.get(k) for k in ["photo", "video", "document", "animation", "video_note"]):
        # есть медиа, но file_id не дали — вероятно view-once
        is_ephemeral = True
    await store.upsert_message(
        connection_id=connection_id,
        message_id=message_id,
        chat_id=chat_id,
        sender_id=sender_id,
        text=text,
        media_path=media_path,
        media_type=media_type,
        sender_label=label,
    )

    if settings.forward_media_immediately and owner_chat:
        should_forward = is_ephemeral or not settings.forward_only_ephemeral
        if should_forward and media_path:
            caption = f"Медиа из чата {chat_id} от {label}"
            await api.send_media_by_type(owner_chat, media_type or "document", Path(media_path), caption=caption)
            if settings.clean_after_send:
                await store.delete_message(connection_id, message_id)
        elif should_forward and media_type and not media_path:
            await api.send_message(
                owner_chat,
                f"{label} отправил(а) медиа, но не удалось скачать файл (тип: {media_type}).",
            )
        elif should_forward and not media_type:
            await api.send_message(
                owner_chat,
                f"{label} отправил(а) одноразовое медиа, но Telegram не дал file_id (view-once/TTL).",
            )


async def handle_edited_business_message(
    api: BotAPI, store: Store, settings: Settings, ebm: Dict[str, Any]
) -> None:
    connection_id = ebm["business_connection_id"]
    message_id = ebm["message_id"]
    owner_id, owner_chat = await store.get_owner_for_connection(connection_id)
    if owner_id is None or not user_allowed(owner_id, settings):
        return
    old = await store.get_message(connection_id, message_id)
    new_text = ebm.get("text") or ebm.get("caption") or ""
    label = (old["sender_label"] if old and old.get("sender_label") else None) or sender_label(ebm)
    if old and old["text"] != new_text and owner_chat:
        before = old["text"] or "<пусто>"
        after = new_text or "<пусто>"
        body = (
            f"{html_escape(label)} изменил(а) сообщение:\n\n"
            f"Старое:<blockquote>{html_escape(before)}</blockquote>\n\n"
            f"Новое:<blockquote>{html_escape(after)}</blockquote>"
        )
        await api.send_message_fmt(owner_chat, body, parse_mode="HTML")
        if settings.clean_after_send:
            await store.delete_message(connection_id, message_id)
            return
    await store.upsert_message(
        connection_id=connection_id,
        message_id=message_id,
        chat_id=old["chat_id"] if old else ebm.get("chat", {}).get("id"),
        sender_id=old["sender_id"] if old else ebm.get("from", {}).get("id"),
        text=new_text,
        media_path=old["media_path"] if old else None,
        media_type=old["media_type"] if old else None,
        sender_label=old["sender_label"] if old else label,
    )


async def handle_deleted_business_messages(
    api: BotAPI, store: Store, settings: Settings, dbm: Dict[str, Any]
) -> None:
    connection_id = dbm["business_connection_id"]
    message_ids = dbm.get("message_ids", [])
    owner_id, owner_chat = await store.get_owner_for_connection(connection_id)
    if owner_id is None or not user_allowed(owner_id, settings) or not owner_chat:
        return
    for mid in message_ids:
        saved = await store.get_message(connection_id, mid)
        if not saved:
            continue
        text = saved["text"] or "<без текста>"
        label = saved.get("sender_label") or f"user {saved.get('sender_id')}"
        body = (
            f"{html_escape(label)} удалил(а) сообщение:\n"
            f"Чат: {saved['chat_id']}\n\n"
            f"Текст:\n<blockquote>{html_escape(text)}</blockquote>"
        )
        await api.send_message_fmt(owner_chat, body, parse_mode="HTML")
        if saved["media_path"]:
            await api.send_media_by_type(
                owner_chat,
                saved["media_type"] or "document",
                Path(saved["media_path"]),
                caption="Медиа из удаленного сообщения",
            )
        await store.delete_message(connection_id, mid)


# --- main loop ---------------------------------------------------------------
async def run() -> None:
    settings = parse_settings()
    settings.media_dir.mkdir(parents=True, exist_ok=True)
    store = Store(settings.db_path)
    async with aiohttp.ClientSession() as session:
        api = BotAPI(settings.bot_token, session)
        offset: Optional[int] = None
        allowed_updates = [
            "message",
            "business_connection",
            "business_message",
            "edited_business_message",
            "deleted_business_messages",
        ]
        print("Business bot started. Waiting for updates...")
        while True:
            try:
                updates = await api.get_updates(offset, settings.poll_timeout, allowed_updates)
                for upd in updates:
                    offset = upd["update_id"] + 1
                    if "message" in upd and upd["message"].get("text") == "/start":
                        await handle_start(api, store, settings, upd["message"])
                    if "business_connection" in upd:
                        await handle_business_connection(api, store, settings, upd["business_connection"])
                    if "business_message" in upd:
                        await handle_business_message(api, store, settings, upd["business_message"])
                    if "edited_business_message" in upd:
                        await handle_edited_business_message(api, store, settings, upd["edited_business_message"])
                    if "deleted_business_messages" in upd:
                        await handle_deleted_business_messages(api, store, settings, upd["deleted_business_messages"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"Ошибка цикла: {exc}")
                await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Stopped by user.")

