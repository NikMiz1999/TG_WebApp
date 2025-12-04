from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import hmac, hashlib

# Жёстко прописываем токен
BOT_TOKEN = "7735626834:AAG2hoI1ShmH24lk2nk-hlnEZv2c0AK2Qsw"

# Базовый URL мини-приложения
WEBAPP_BASE = "https://month-discounts-trade-written.trycloudflare.com"

def make_signed_url(uid: int) -> str:
    # Подпись = HMAC-SHA256(uid, ключ = BOT_TOKEN)
    sig = hmac.new(BOT_TOKEN.encode(), str(uid).encode(), hashlib.sha256).hexdigest()
    return f"{WEBAPP_BASE}/?uid={uid}&sig={sig}"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    url = make_signed_url(uid)
    kb = [[InlineKeyboardButton("Открыть приложение", web_app=WebAppInfo(url=url))]]
    text = "Открой мини-приложение:"
    if update.message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=InlineKeyboardMarkup(kb))

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.run_polling(drop_pending_updates=True)
