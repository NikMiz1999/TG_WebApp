# --- конфиг окружения для dev ---
$env:BOT_TOKEN="7735626834:AAG2hoI1ShmH24lk2nk-hlnEZv2c0AK2Qsw"
$env:GROUP_CHAT_ID="-1002620513089"
$env:TIMESHEET_ID="1J212D9-n0eS5DnEST7JqObeE2S1umHCSRURjhntq4R8"
$env:SESSION_SECRET="devsecret"

# --- запуск сервера ---
uvicorn app:app --reload --log-level info --access-log

