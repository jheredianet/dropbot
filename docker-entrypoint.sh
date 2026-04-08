#!/bin/bash
set -e

echo "=================================================="
echo "DropBot Startup"
echo "=================================================="
echo "Timestamp: $(date)"
echo ""

# Function to refresh cookies
refresh_cookies() {
    echo "[Startup] Checking YouTube cookies..."

    if [ -f /app/cookies.txt ]; then
        echo "[Startup] Cookies file exists, checking freshness..."
    else
        echo "[Startup] No cookies file found, will generate new ones..."
    fi

    # Run cookie manager
    if python3 /app/cookie_manager.py; then
        echo "[Startup] ✅ Cookies are ready"
    else
        echo "[Startup] ⚠️ Warning: Could not generate cookies, continuing without them..."
    fi
}

# Refresh cookies before starting
refresh_cookies

echo ""
echo "[Startup] Starting DropBot..."
echo "=================================================="

# Start the bot
exec python3 dropbot.py "$@"