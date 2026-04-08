#!/usr/bin/env python3
"""
Cookie Manager for DropBot
Automatically obtains and refreshes YouTube cookies using Playwright.

This script generates cookies for yt-dlp to bypass YouTube's bot detection.
Cookies are saved in Netscape format compatible with yt-dlp.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("ERROR: Playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

# Configuration
COOKIES_FILE = "/app/cookies.txt"
COOKIES_CACHE_DIR = "/tmp/cookies_cache"
YOUTUBE_URL = "https://www.youtube.com"
COOKIES_REFRESH_HOURS = 6  # Refresh cookies every 6 hours


async def get_youtube_cookies(headless: bool = True) -> dict:
    """
    Obtain cookies from YouTube using Playwright.

    Args:
        headless: Run browser in headless mode (no GUI)

    Returns:
        Dictionary of cookies
    """
    print(f"[{datetime.now()}] Starting Playwright to get YouTube cookies...")

    async with async_playwright() as p:
        # Launch browser with anti-detection settings
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-infobars',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--lang=es-ES',
            ]
        )

        # Create context with realistic settings
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='es-ES',
            timezone_id='Europe/Madrid',
        )

        page = await context.new_page()

        try:
            print(f"[{datetime.now()}] Navigating to YouTube...")

            # Navigate to YouTube
            await page.goto(YOUTUBE_URL, wait_until='networkidle', timeout=30000)

            # Wait for page to fully load
            await asyncio.sleep(2)

            # Accept cookies consent if present (EU)
            try:
                accept_button = page.locator('button[aria-label*="Accept"], button[aria-label*="accept"], ytd-button-renderer#accept-button button')
                if await accept_button.count() > 0:
                    await accept_button.first.click(timeout=5000)
                    print(f"[{datetime.now()}] Accepted cookies consent")
                    await asyncio.sleep(1)
            except Exception:
                pass  # No consent dialog, continue

            # Simulate some human behavior
            await page.mouse.move(500, 300)
            await asyncio.sleep(0.5)
            await page.mouse.move(600, 400)
            await asyncio.sleep(0.5)

            # Scroll down a bit to trigger more cookies
            await page.evaluate('window.scrollBy(0, 500)')
            await asyncio.sleep(1)

            # Get all cookies
            cookies = await context.cookies()

            print(f"[{datetime.now()}] Obtained {len(cookies)} cookies")

            return {cookie['name']: cookie for cookie in cookies}

        except Exception as e:
            print(f"[{datetime.now()}] ERROR getting cookies: {e}")
            return {}
        finally:
            await browser.close()


def save_cookies_netscape(cookies: dict, output_file: str = COOKIES_FILE):
    """
    Save cookies in Netscape format for yt-dlp.

    Format: domain\tflag\tpath\tsecure\texpiry\tname\tvalue
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w') as f:
        f.write("# Netscape HTTP Cookie File\n")
        f.write("# https://curl.haxx.se/rfc/cookie_spec.html\n")
        f.write("# This is a generated file! Do not edit.\n\n")

        for name, cookie in cookies.items():
            domain = cookie.get('domain', '.youtube.com')
            path = cookie.get('path', '/')
            secure = cookie.get('secure', False)
            expires = cookie.get('expires', -1)
            value = cookie.get('value', '')

            # Handle expiry time
            if isinstance(expires, (int, float)):
                if expires == -1 or expires == 0:
                    expiry = 0
                else:
                    expiry = int(expires)
            else:
                expiry = 0

            # Domain flag (TRUE if domain starts with .)
            domain_flag = "TRUE" if domain.startswith('.') else "FALSE"

            # Secure flag
            secure_flag = "TRUE" if secure else "FALSE"

            f.write(f"{domain}\t{domain_flag}\t{path}\t{secure_flag}\t{expiry}\t{name}\t{value}\n")

    print(f"[{datetime.now()}] Saved {len(cookies)} cookies to {output_file}")


def load_cookies() -> dict:
    """Load cookies from file."""
    if not os.path.exists(COOKIES_FILE):
        return {}

    cookies = {}
    try:
        with open(COOKIES_FILE, 'r') as f:
            for line in f:
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.strip().split('\t')
                if len(parts) >= 7:
                    cookies[parts[5]] = {
                        'domain': parts[0],
                        'path': parts[2],
                        'secure': parts[3] == 'TRUE',
                        'expires': int(parts[4]) if parts[4].isdigit() else 0,
                        'name': parts[5],
                        'value': parts[6]
                    }
    except Exception as e:
        print(f"[{datetime.now()}] ERROR loading cookies: {e}")

    return cookies


def cookies_need_refresh() -> bool:
    """Check if cookies need to be refreshed."""
    if not os.path.exists(COOKIES_FILE):
        return True

    # Check file modification time
    mtime = os.path.getmtime(COOKIES_FILE)
    age = datetime.now() - datetime.fromtimestamp(mtime)

    if age > timedelta(hours=COOKIES_REFRESH_HOURS):
        return True

    # Check if essential cookies exist
    cookies = load_cookies()
    essential = ['VISITOR_INFO1_LIVE', 'PREF']

    for cookie_name in essential:
        if cookie_name not in cookies:
            return True

    return False


async def refresh_cookies():
    """Main function to refresh cookies."""
    print(f"[{datetime.now()}] Starting cookie refresh...")

    # Get new cookies
    cookies = await get_youtube_cookies(headless=True)

    if not cookies:
        print(f"[{datetime.now()}] ERROR: No cookies obtained")
        return False

    # Save cookies
    save_cookies_netscape(cookies)

    print(f"[{datetime.now()}] Cookie refresh completed successfully")
    return True


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='YouTube Cookie Manager for DropBot')
    parser.add_argument('--force', action='store_true', help='Force refresh even if cookies are fresh')
    parser.add_argument('--check', action='store_true', help='Only check if cookies need refresh')
    parser.add_argument('--no-headless', action='store_true', help='Run browser with GUI (for debugging)')

    args = parser.parse_args()

    if args.check:
        needs_refresh = cookies_need_refresh()
        print(f"Cookies need refresh: {needs_refresh}")
        sys.exit(1 if needs_refresh else 0)

    if not args.force and not cookies_need_refresh():
        print(f"[{datetime.now()}] Cookies are still fresh, skipping refresh")
        sys.exit(0)

    success = await refresh_cookies(headless=not args.no_headless)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())