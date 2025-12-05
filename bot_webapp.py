# Основные классы и функции библиотеки python-telegram-bot:
#  - Update       — объект "обновление" от Telegram (сообщение, нажатие кнопки и т.п.)
#  - InlineKeyboardMarkup / InlineKeyboardButton — инлайн-кнопки под сообщением
#  - WebAppInfo   — специальный тип кнопки, открывающий WebApp (мини-приложение) в Telegram
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

import hmac, hashlib

# ==============================
# НАСТРОЙКИ БОТА
# ==============================

# Токен Telegram-бота.
# ВАЖНО: в бою лучше брать его из переменной окружения (os.getenv("BOT_TOKEN")),
# но здесь он прописан жёстко для простоты запуска.
BOT_TOKEN = "7735626834:AAG2hoI1ShmH24lk2nk-hlnEZv2c0AK2Qsw"

# Базовый URL WebApp-приложения (адрес backend-а FastAPI).
# Именно сюда будут "вклеиваться" параметры uid и sig.
WEBAPP_BASE = "https://month-discounts-trade-written.trycloudflare.com"


# ==============================
# ФУНКЦИЯ ФОРМИРОВАНИЯ ПОДПИСАННОЙ ССЫЛКИ
# ==============================

def make_signed_url(uid: int) -> str:
    """
    Формирует защищённую ссылку на WebApp для конкретного пользователя.

    uid  — Telegram user_id пользователя (уникальный числовой идентификатор аккаунта).
    sig  — криптографическая подпись (HMAC-SHA256) от uid с ключом BOT_TOKEN.

    Зачем это нужно:
    - Чтобы backend мог проверить, что пользователь действительно пришёл из нашего бота,
      а не просто открыл URL вручную и не подделал uid.
    - На сервере в app.py считается такой же HMAC(uid, BOT_TOKEN) и сверяется с параметром sig.

    Если подпись совпала и uid есть в org.json — сервер создаёт сессию и пускает пользователя в /check.
    Если нет — показывает /denied.
    """
    # sig = HMAC-SHA256( key = BOT_TOKEN, message = str(uid) )
    sig = hmac.new(
        BOT_TOKEN.encode(),          # секретный ключ (никому не передаётся)
        str(uid).encode(),           # сообщение (в данном случае — строка с user_id)
        hashlib.sha256               # алгоритм хеширования
    ).hexdigest()

    # Формируем итоговый URL вида:
    # https://.../?uid=123456789&sig=abcdef123456...
    return f"{WEBAPP_BASE}/?uid={uid}&sig={sig}"


# ==============================
# ОБРАБОТЧИК КОМАНДЫ /start
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Основной вход в систему для пользователя.

    Что делает:
    1. Берёт Telegram user_id текущего пользователя (update.effective_user.id).
    2. Формирует подписанную ссылку на WebApp через make_signed_url(uid).
    3. Отправляет сообщение с инлайн-кнопкой "Открыть приложение".
       При нажатии на эту кнопку внутри Telegram открывается WebApp
       (мини-браузер с адресом WEBAPP_BASE/?uid=...&sig=...).

    Варианты:
    - Если /start пришла в виде обычного сообщения → отвечаем на сообщение.
    - Если /start пришла из чего-то другого (например, из кнопки) → отправляем в чат.
    """
    # 1. Получаем уникальный Telegram ID пользователя
    uid = update.effective_user.id

    # 2. Строим подписанную ссылку
    url = make_signed_url(uid)

    # 3. Формируем инлайн-кнопку с WebAppInfo — это спецтип, открывающий мини-приложение
    kb = [[InlineKeyboardButton("Открыть приложение", web_app=WebAppInfo(url=url))]]

    # Текст в сообщении над кнопкой
    text = "Открой мини-приложение:"

    # 4. Отправляем сообщение:
    #    - если апдейт содержит обычное сообщение (update.message) — отвечаем на него
    #    - иначе (на всякий случай) шлём в текущий чат
    if update.message:
        # Ответ на конкретное сообщение (в чате / в ЛС)
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        # Резервный путь: отправка просто в chat_id
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=InlineKeyboardMarkup(kb)
        )


# ==============================
# ТОЧКА ВХОДА ПРИ ЗАПУСКЕ СКРИПТА
# ==============================

if __name__ == "__main__":
    """
    Запуск бота в режиме polling (без вебхука).
    Это удобно для локальной отладки:
      - скрипт сам подключается к Telegram и начинает слушать обновления.
      - команда /start начинает работать сразу после запуска.
    """
    # Создаём приложение python-telegram-bot через ApplicationBuilder
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Регистрируем обработчик команды /start
    app.add_handler(CommandHandler("start", start))

    # Запускаем бесконечный цикл получения обновлений.
    # drop_pending_updates=True — при рестарте бота старые неотработанные апдейты будут отброшены.
    app.run_polling(drop_pending_updates=True)
