"""Telegram-бот «Тайный Санта» (aiogram 3).

Что делает бот (зелёные блоки схемы):
  * /start: получает данные пользователя, предлагает войти в TMA (кнопка «Войти»);
  * /start <groupId> (ссылка-приглашение): определяет группу и пользователя
    и просит бэкенд добавить его в группу;
  * /invite <groupId>: выдаёт ссылку-приглашение https://t.me/<бот>?start=<groupId>;
  * напоминания участникам за 2 недели, за неделю и в день дарения;
  * слушает поток событий сервера (Server-Sent Events) и реагирует на них:
      - member_joined: шлёт админу «В группу ... присоединился пользователь ...»
        с кнопками «Удалить» (с подтверждением) и «Открыть группу»;
      - gift_assigned: после жеребьёвки шлёт каждому участнику, кому он дарит.

Авторизация. Бот сам собирает и подписывает initData (так же, как это делает
Telegram: HMAC-SHA256 от токена бота) и шлёт его в заголовке
`Authorization: tma <initData>`. Для запросов «от имени пользователя» в initData
лежит этот пользователь (photo_url пустой). Для служебных запросов (рассылка
напоминаний) в initData лежит сам бот: id бота = число до ":" в токене.
Пользователя бэкенд берёт ТОЛЬКО из проверенного initData, а не из тела запроса.

Контракт с бэкендом (эти ручки должен реализовать ваш сервер):

  POST /api/users/me                   (тело пустое: upsert пользователя из initData)
  POST /api/groups/join/{group_id}     (тело пустое; ответ бот не разбирает)
  POST /api/groups/delete_user         {"user_id": 123321, "group_id": 12}
       (запрашивающий = пользователь из initData; ответ бот не разбирает)
  GET  /api/groups/upcoming            (все не завершённые группы; только для id бота)
       -> [{"id", "name", "swap_date": "YYYY-MM-DD", "place", "money",
            "member_ids": [123, 456]}]

  GET  /api/bot/events                 (SSE, только для id бота)
       Content-Type: text/event-stream. Каждое событие: поля id / event / data
       (data одной строкой JSON) и пустая строка в конце. Раз в 15-30 секунд
       нужен комментарий-пульс (": ping"): бот рвёт соединение, если 90 секунд
       тишины. Если сервер умеет, он должен переотправлять пропущенные события
       по заголовку Last-Event-ID.
       event: member_joined
         {"exchange_id", "name", "admin_id",
          "user": {"id", "first_name", "last_name", "username"}}
       event: gift_assigned     (по одному событию на каждого дарителя)
         {"exchange_id", "name", "swap_date", "place", "money", "giver_id",
          "receiver": {"id", "first_name", "last_name", "username"}}
"""
import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message,
    User,
    WebAppInfo,
)

log = logging.getLogger("santa-bot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
WEBAPP_URL = os.environ["WEBAPP_URL"]
BACKEND_URL = os.environ["BACKEND_URL"].rstrip("/")
BOT_ID = int(BOT_TOKEN.split(":")[0])
REMINDER_TZ = ZoneInfo(os.environ.get("REMINDER_TZ", "Europe/Moscow"))
REMINDER_HOUR = int(os.environ.get("REMINDER_HOUR", "10"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "/data/sent.json"))
REMINDER_DAYS = (14, 7, 0)
LAST_EVENT_FILE = STATE_FILE.parent / "last_event_id"

# Параметр start в Telegram: только A-Z a-z 0-9 _ - и до 64 символов (UUID влезает).
GROUP_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

bot_username = ""  # заполняется при старте


# ---------------------------------------------------------------- initData
def make_init_data(user: dict) -> str:
    """Собирает подписанный initData для пользователя `user`."""
    fields = {
        "auth_date": str(int(time.time())),
        "user": json.dumps(user, ensure_ascii=False, separators=(",", ":")),
    }
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(fields.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    fields["hash"] = hmac.new(
        secret_key, data_check_string.encode(), hashlib.sha256
    ).hexdigest()
    return urlencode(fields, quote_via=quote)  # пробелы как %20, а не "+"


def tg_user_dict(user: User) -> dict:
    return {
        "id": user.id,
        "first_name": user.first_name or "",
        "last_name": user.last_name or "",
        "username": user.username or "",
        "language_code": user.language_code or "",
        "allows_write_to_pm": True,
        "photo_url": "",
    }


def service_user_dict() -> dict:
    """Служебная личность бота для запросов не от имени пользователя."""
    return {
        "id": -16,
        "first_name": "SantaBot",
        "last_name": "",
        "username": os.environ["BOT_TOKEN"],
        "language_code": "",
        "allows_write_to_pm": True,
        "photo_url": "",
    }


# ---------------------------------------------------------------- бэкенд
class Backend:
    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    async def _call(self, method: str, path: str, user: dict, **kwargs):
        # initData генерируется на каждый запрос, чтобы auth_date всегда был свежим.
        headers = {"Authorization": "tma " + make_init_data(user)}
        async with self.session.request(
            method, BACKEND_URL + path, headers=headers, **kwargs
        ) as resp:
            text = await resp.text()
            print(text)
            try:
                data = json.loads(text)
            except ValueError:
                data = None
            return resp.status, data

    async def upsert_user(self, user: User) -> None:
        # photo_url в initData пустой: на сервере не затирайте им сохранённое фото.
        await self._call("POST", "/api/users/me", tg_user_dict(user))

    async def join(self, group_id: str, user: User):
        status, data = await self._call(
            "POST", f"/api/groups/join/{quote(group_id, safe='')}", tg_user_dict(user),
        )
        return status, (data if isinstance(data, dict) else {})

    async def upcoming(self) -> list[dict]:
        status, data = await self._call(
            "GET", "/api/groups/upcoming", service_user_dict()
        )
        if status != 200 or not isinstance(data, list):
            raise RuntimeError(f"upcoming: HTTP {status}")
        return data

    async def remove_member(self, group_id: int, target_user_id: int, requester: User):
        status, data = await self._call(
            "POST", "/api/groups/delete_user", tg_user_dict(requester),
            json={"user_id": target_user_id, "group_id": group_id},
        )
        return status, (data if isinstance(data, dict) else {})

    async def stream_events(self, last_event_id: str):
        """Читает SSE-поток; отдаёт (id, event, data). Завершается, когда поток закрыт."""
        headers = {
            "Authorization": "tma " + make_init_data(service_user_dict()),
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        }
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        # Без общего таймаута (поток вечный), но 90 секунд тишины = обрыв.
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=90)

        async with self.session.get(
            BACKEND_URL + "/api/bot/events", headers=headers, timeout=timeout
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"events: HTTP {resp.status}")
            event, data, event_id = "message", [], last_event_id
            async for raw in resp.content:  # итерация по строкам
                line = raw.decode("utf-8").rstrip("\r\n")
                if line == "":  # пустая строка = конец события
                    if data:
                        yield event_id, event, "\n".join(data)
                    event, data = "message", []
                elif line.startswith(":"):  # комментарий / пульс
                    continue
                else:
                    field, _, value = line.partition(":")
                    value = value[1:] if value.startswith(" ") else value
                    if field == "event":
                        event = value
                    elif field == "data":
                        data.append(value)
                    elif field == "id":
                        event_id = value


# ---------------------------------------------------------------- хелперы
def invite_link(group_id: str) -> str:
    return f"https://t.me/{bot_username}?start={group_id}"


def open_app_kb(text: str = "Войти") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))]
    ])


# ---------------------------------------------------------------- хендлеры
router = Router()


@router.message(CommandStart(deep_link=True))
async def start_with_link(message: Message, command: CommandObject, backend: Backend):
    group_id = command.args or ""
    if not GROUP_ID_RE.match(group_id):
        return await start_plain(message, backend)

    user = message.from_user
    try:
        await backend.upsert_user(user)
        # ТЕСТОВЫЙ РЕЖИМ: ответ сервера не разбираем, статус и тело только в лог.
        status, data = await backend.join(group_id, user)
        log.info("join: HTTP %s %s", status, data)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        log.exception("backend недоступен")
        return await message.answer("Сервер временно недоступен, попробуйте чуть позже.")

    await message.answer(
        "🎅 Запрос на вступление в группу отправлен.",
        reply_markup=open_app_kb("Открыть группу"),
    )


@router.message(CommandStart())
async def start_plain(message: Message, backend: Backend):
    try:
        await backend.upsert_user(message.from_user)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        log.exception("не удалось сохранить пользователя")

    name = html.escape(message.from_user.first_name)
    kb = open_app_kb("Войти")
    kb.inline_keyboard.append(
        [InlineKeyboardButton(text="Как вступить в группу?", callback_data="how_join")]
    )
    await message.answer(
        f"Привет, {name}! 🎄\n"
        "Это Тайный Санта: создавайте группы обмена подарками и приглашайте друзей.\n\n"
        "Нажмите «Войти», чтобы открыть приложение и создать группу.",
        reply_markup=kb,
    )


@router.callback_query(F.data == "how_join")
async def how_join(call: CallbackQuery):
    await call.message.answer(
        "Чтобы вступить в чужую группу, попросите организатора прислать "
        "ссылку-приглашение и перейдите по ней: бот сам добавит вас в группу."
    )
    await call.answer()


@router.message(Command("invite"))
async def cmd_invite(message: Message, command: CommandObject):
    group_id = command.args or ""
    if not GROUP_ID_RE.match(group_id):
        return await message.answer("Использование: /invite &lt;id группы&gt;")
    await message.answer(f"Ссылка-приглашение:\n{invite_link(group_id)}")


# ---------------------------------------------------------------- кнопки админа
def member_kb(exchange_id, user_id) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"kick:{exchange_id}:{user_id}"),
        InlineKeyboardButton(text="Открыть группу", web_app=WebAppInfo(url=WEBAPP_URL)),
    ]])


@router.callback_query(F.data.startswith("kick:"))
async def kick_ask(call: CallbackQuery):
    _, xid, uid = call.data.split(":")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Да, удалить", callback_data=f"kickok:{xid}:{uid}"),
        InlineKeyboardButton(text="Отмена", callback_data=f"kickno:{xid}:{uid}"),
    ]])
    await call.message.edit_reply_markup(reply_markup=kb)
    await call.answer("Удалить участника из группы?")


@router.callback_query(F.data.startswith("kickno:"))
async def kick_cancel(call: CallbackQuery):
    _, xid, uid = call.data.split(":")
    await call.message.edit_reply_markup(reply_markup=member_kb(xid, uid))
    await call.answer()


@router.callback_query(F.data.startswith("kickok:"))
async def kick_confirm(call: CallbackQuery, backend: Backend):
    _, xid, uid = call.data.split(":")
    try:
        # ТЕСТОВЫЙ РЕЖИМ: ответ сервера не разбираем, статус и тело только в лог.
        status, data = await backend.remove_member(int(xid), int(uid), call.from_user)
        log.info("remove-member: HTTP %s %s", status, data)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        log.exception("remove-member: сервер недоступен")
        return await call.answer("Сервер недоступен, попробуйте позже", show_alert=True)

    await call.message.edit_text(
        call.message.html_text + "\n\n📨 Запрос на удаление отправлен.",
        reply_markup=None,
    )
    await call.answer()


# ---------------------------------------------------------------- напоминания
def load_sent() -> set[str]:
    try:
        return set(json.loads(STATE_FILE.read_text()))
    except (OSError, ValueError):
        return set()


def save_sent(sent: set[str]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(sorted(sent)))


def reminder_text(ex: dict, days: int) -> str:
    name = html.escape(str(ex.get("name", "")))
    when = {
        14: "Через 2 недели",
        7: "Через неделю",
        0: "Сегодня",
    }[days]
    lines = [f"🎁 {when} обмен подарками в группе «{name}»!"]
    if ex.get("place"):
        lines.append(f"Где: {html.escape(str(ex['place']))}")
    if ex.get("money"):
        lines.append(f"Бюджет: {html.escape(str(ex['money']))}")
    return "\n".join(lines)


async def run_reminders(bot: Bot, backend: Backend, sent: set[str]) -> None:
    today = datetime.now(REMINDER_TZ).date()
    for ex in await backend.upcoming():
        try:
            swap = date.fromisoformat(str(ex["swap_date"])[:10])
        except (KeyError, ValueError):
            continue
        days = (swap - today).days
        if days not in REMINDER_DAYS:
            continue

        for uid in ex.get("member_ids", []):
            if uid <= 0:  # тестовые пользователи вроде -9
                continue
            key = f"{ex['id']}:{uid}:{days}:{swap}"
            if key in sent:
                continue
            try:
                await bot.send_message(uid, reminder_text(ex, days),
                                       reply_markup=open_app_kb("Открыть"))
                sent.add(key)
            except TelegramForbiddenError:
                sent.add(key)  # бот заблокирован, повторять нет смысла
            except TelegramAPIError:
                log.exception("не отправлено напоминание %s", key)  # повтор в след. раз
            await asyncio.sleep(0.05)  # не выходим за лимит ~30 сообщений/с
    save_sent(sent)


async def reminder_loop(bot: Bot, backend: Backend) -> None:
    sent = load_sent()
    while True:
        # Запуск каждый час после REMINDER_HOUR; дубликаты отсекает множество sent.
        if datetime.now(REMINDER_TZ).hour >= REMINDER_HOUR:
            try:
                await run_reminders(bot, backend, sent)
            except Exception:
                log.exception("ошибка рассылки напоминаний")
        await asyncio.sleep(3600)


# ---------------------------------------------------------------- события сервера (SSE)
def person(u: dict) -> str:
    name = " ".join(x for x in (u.get("first_name"), u.get("last_name")) if x) or "без имени"
    text = f'<a href="tg://user?id={int(u["id"])}">{html.escape(name)}</a>'
    if u.get("username"):
        text += f" (@{html.escape(u['username'])})"
    return text


def fmt_date(value) -> str:
    try:
        return date.fromisoformat(str(value)[:10]).strftime("%d.%m.%Y")
    except ValueError:
        return str(value)


async def on_member_joined(bot: Bot, ev: dict) -> None:
    user = ev["user"]
    admin_id = int(ev["admin_id"])
    if int(user["id"]) == admin_id:  # админ, добавленный при создании группы
        return
    await bot.send_message(
        admin_id,
        f"👤 В группу «{html.escape(str(ev['name']))}» присоединился пользователь "
        f"{person(user)}",
        reply_markup=member_kb(ev["exchange_id"], user["id"]),
    )


async def on_gift_assigned(bot: Bot, ev: dict) -> None:
    lines = [
        f"🎅 В группе «{html.escape(str(ev['name']))}» прошла жеребьёвка!",
        f"Вы дарите подарок: {person(ev['receiver'])}",
    ]
    if ev.get("swap_date"):
        lines.append(f"Когда: {fmt_date(ev['swap_date'])}")
    if ev.get("place"):
        lines.append(f"Где: {html.escape(str(ev['place']))}")
    if ev.get("money"):
        lines.append(f"Бюджет: {html.escape(str(ev['money']))}")
    lines.append("\nНикому не рассказывайте. Это тайна! 🤫")
    await bot.send_message(int(ev["giver_id"]), "\n".join(lines),
                           reply_markup=open_app_kb("Открыть"))


EVENT_HANDLERS = {
    "member_joined": on_member_joined,
    "gift_assigned": on_gift_assigned,
}


async def handle_event(bot: Bot, name: str, raw: str) -> None:
    handler = EVENT_HANDLERS.get(name)
    if handler is None:
        return
    try:
        await handler(bot, json.loads(raw))
    except TelegramForbiddenError:
        log.warning("событие %s: получатель не запускал бота или заблокировал его", name)
    except (TelegramAPIError, KeyError, ValueError, TypeError):
        log.exception("не удалось обработать событие %s: %s", name, raw)
    await asyncio.sleep(0.05)  # лимит Telegram ~30 сообщений в секунду


def load_last_event_id() -> str:
    try:
        return LAST_EVENT_FILE.read_text().strip()
    except OSError:
        return ""


def save_last_event_id(value: str) -> None:
    LAST_EVENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_EVENT_FILE.write_text(value)


async def events_loop(bot: Bot, backend: Backend) -> None:
    last_id = load_last_event_id()
    delay = 1
    while True:
        try:
            async for event_id, name, raw in backend.stream_events(last_id):
                delay = 1
                await handle_event(bot, name, raw)
                if event_id and event_id != last_id:
                    last_id = event_id
                    save_last_event_id(last_id)
            log.warning("SSE: поток закрыт сервером, переподключаюсь")
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError):
            log.exception("SSE: ошибка соединения")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30)


# ---------------------------------------------------------------- запуск
async def main() -> None:
    global bot_username
    logging.basicConfig(level=logging.INFO)

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    backend = Backend()
    await backend.start()

    bot_username = (await bot.get_me()).username
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(
        text="Войти", web_app=WebAppInfo(url=WEBAPP_URL)))

    dp = Dispatcher()
    dp["backend"] = backend
    dp.include_router(router)

    tasks = [
        asyncio.create_task(reminder_loop(bot, backend)),
        asyncio.create_task(events_loop(bot, backend)),
    ]
    try:
        await dp.start_polling(bot)
    finally:
        for task in tasks:
            task.cancel()
        await backend.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
