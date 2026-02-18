"""
bot_all_in_one.py - Harel Agents Automation with Pulseem OTP

Flow:
1. Navigate to login page
2. Check for "לחץ כאן" reconnect link and click if present
3. Fill username and password
4. Click submit button (אישור)
5. If OTP screen appears, use Pulseem API to get verification code
6. Enter OTP and submit

Required .env:
PULSEEM_API_KEY=...
PULSEEM_VIRTUAL_NUMBER=053...
HAREL_USERNAME=...
HAREL_PASSWORD=...

Optional .env:
BASE_DOWNLOAD_DIR=C:\FinanceDownloads
PLAYWRIGHT_HEADLESS=true|false

OTP_LOOKBACK_SECONDS=240
OTP_MAX_WAIT_SECONDS=90
OTP_POLL_SECONDS=2
OTP_INITIAL_DELAY_SECONDS=15

PULSEEM_AUTH_MODE=header|bearer|x-api-key
PULSEEM_APIKEY_HEADER=ApiKey   (only used when auth_mode=header)
"""

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ----------------------------
# Load .env
# ----------------------------
load_dotenv()

PULSEEM_API_KEY = os.getenv("PULSEEM_API_KEY", "").strip()
PULSEEM_VIRTUAL_NUMBER = os.getenv("PULSEEM_VIRTUAL_NUMBER", "").strip()
BASE_DOWNLOAD_DIR = os.getenv("BASE_DOWNLOAD_DIR", r"C:\FinanceDownloads").strip()

PULSEEM_AUTH_MODE = os.getenv("PULSEEM_AUTH_MODE", "header").strip().lower()
PULSEEM_APIKEY_HEADER = os.getenv("PULSEEM_APIKEY_HEADER", "ApiKey").strip()

OTP_LOOKBACK_SECONDS = int(os.getenv("OTP_LOOKBACK_SECONDS", "240"))
OTP_MAX_WAIT_SECONDS = int(os.getenv("OTP_MAX_WAIT_SECONDS", "90"))
OTP_POLL_SECONDS = float(os.getenv("OTP_POLL_SECONDS", "2"))
OTP_INITIAL_DELAY_SECONDS = int(os.getenv("OTP_INITIAL_DELAY_SECONDS", "15"))
MANUAL_OTP_FALLBACK = os.getenv("MANUAL_OTP_FALLBACK", "true").strip().lower() in ("1", "true", "yes", "y")
MANUAL_OTP_MAX_WAIT_SECONDS = int(os.getenv("MANUAL_OTP_MAX_WAIT_SECONDS", "240"))

PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").strip().lower() in ("1", "true", "yes", "y")

HAREL_USERNAME = os.getenv("HAREL_USERNAME", "").strip()
HAREL_PASSWORD = os.getenv("HAREL_PASSWORD", "").strip()

if not PULSEEM_VIRTUAL_NUMBER:
    raise RuntimeError("Missing PULSEEM_VIRTUAL_NUMBER in .env")
if not HAREL_USERNAME or not HAREL_PASSWORD:
    print("[WARN] Missing HAREL_USERNAME/HAREL_PASSWORD in .env")

# ----------------------------
# Pulseem settings
# ----------------------------
PULSEEM_URL = "https://api.pulseem.com/api/v1/SmsApi/GetIncomingSmsReport"
OTP_REGEX = re.compile(r"\b(\d{4,8})\b")

# ----------------------------
# Pulseem helpers
# ----------------------------
def normalize_il_number(num: str) -> str:
    """Normalize Israeli phone number to 972XXXXXXXXX format"""
    s = "".join(ch for ch in num.strip() if ch.isdigit())
    if s.startswith("0") and len(s) == 10:
        return "972" + s[1:]
    if s.startswith("972"):
        return s
    return s

def format_time(dt_obj: datetime) -> str:
    return dt_obj.strftime("%d/%m/%Y %H:%M:%S")

def parse_reply_date(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.min

@dataclass
class PulseemAuth:
    api_key: str
    mode: str = "header"
    header_name: str = "ApiKey"

    def headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.mode == "bearer":
            h["Authorization"] = f"Bearer {self.api_key}"
        elif self.mode == "x-api-key":
            h["X-API-KEY"] = self.api_key
        else:
            h[self.header_name] = self.api_key
        return h

def get_incoming_sms_report(
    auth: PulseemAuth,
    search_txt: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    timeout_seconds: int = 20,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if search_txt:
        payload["SearchTxt"] = search_txt[:31]
    if start_time:
        payload["StartTime"] = format_time(start_time)
    if end_time:
        payload["EndTime"] = format_time(end_time)

    r = requests.post(PULSEEM_URL, json=payload, headers=auth.headers(), timeout=timeout_seconds)
    r.raise_for_status()
    return r.json()

def get_last_sms_datetime(
    virtual_number: str,
    auth: PulseemAuth,
    lookback_seconds: int = 600,
) -> datetime:
    """Get the timestamp of the most recent SMS before we request OTP"""
    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    end = datetime.now()

    try:
        data = get_incoming_sms_report(auth, search_txt=vn, start_time=start, end_time=end)
    except Exception:
        print("[Pulseem] Warning: could not check last SMS")
        return datetime.min

    if str(data.get("status", "")).lower() != "success":
        print("[Pulseem] Warning: could not check last SMS")
        return datetime.min

    reports = data.get("IncomingSmsReports", []) or []
    if not reports:
        return datetime.min

    return max((parse_reply_date(str(m.get("ReplyDate", ""))) for m in reports), default=datetime.min)

def wait_for_otp_from_pulseem(
    virtual_number: str,
    auth: PulseemAuth,
    after_datetime: Optional[datetime] = None,
    lookback_seconds: int = 240,
    max_wait_seconds: int = 90,
    poll_every_seconds: float = 2.0,
) -> Tuple[str, Dict[str, Any]]:
    """Wait for a NEW OTP SMS (with ReplyDate > after_datetime). Treat 'NO DATA FOUND' as 'not yet'."""
    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    deadline = time.time() + max_wait_seconds

    last_seen_dt = after_datetime or datetime.min
    print(f"[OTP] Waiting for SMS newer than: {last_seen_dt.isoformat() if last_seen_dt != datetime.min else 'N/A'}")

    while time.time() < deadline:
        end = datetime.now()
        data = get_incoming_sms_report(auth, search_txt=vn, start_time=start, end_time=end)

        status = str(data.get("status", "")).lower()
        if status != "success":
            err = str(data.get("error") or "").lower()
            # Very common: Pulseem returns NO DATA FOUND when no messages match yet
            if "no data" in err:
                time.sleep(poll_every_seconds)
                continue
            raise RuntimeError(f"Pulseem error: {data.get('error')}")

        reports = data.get("IncomingSmsReports", []) or []
        if not reports:
            time.sleep(poll_every_seconds)
            continue

        # Filter for fresh messages only
        fresh = []
        newest_dt = last_seen_dt
        for msg in reports:
            dt_msg = parse_reply_date(str(msg.get("ReplyDate", "")))
            if dt_msg > newest_dt:
                newest_dt = dt_msg
            if dt_msg > last_seen_dt:
                fresh.append(msg)

        if newest_dt > last_seen_dt:
            last_seen_dt = newest_dt

        # Check fresh messages for OTP
        fresh_sorted = sorted(fresh, key=lambda x: parse_reply_date(str(x.get("ReplyDate", ""))), reverse=True)
        for msg in fresh_sorted:
            text = str(msg.get("ReplyText") or "")
            m = OTP_REGEX.search(text)
            if m:
                print(f"[OTP] Found code: {m.group(1)}")
                return m.group(1), msg

        time.sleep(poll_every_seconds)

    raise TimeoutError("No NEW OTP SMS arrived within the configured wait window.")

def build_pulseem_auth_from_env() -> PulseemAuth:
    """Build Pulseem auth object (only called when OTP is needed)"""
    api_key = os.getenv("PULSEEM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OTP required but missing PULSEEM_API_KEY in .env")
    return PulseemAuth(api_key=api_key, mode=PULSEEM_AUTH_MODE, header_name=PULSEEM_APIKEY_HEADER)

def _pulseem_status_is_success(data: Dict[str, Any]) -> bool:
    return str(data.get("status", "")).strip().lower() == "success"

def _pulseem_error_text(data: Dict[str, Any]) -> str:
    return str(data.get("error") or data.get("message") or "").strip()

def preflight_check_pulseem_or_die(virtual_number: str, lookback_seconds: int = 86400) -> None:
    """
    Validates Pulseem connectivity/auth BEFORE running the bot.
    Treats 'NO DATA FOUND' as OK (auth works, just no messages).
    Raises RuntimeError on auth/network/format issues.
    """
    print("[Preflight] Checking Pulseem API connectivity/auth...")

    api_key = os.getenv("PULSEEM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("[Preflight] Missing PULSEEM_API_KEY in .env")

    auth = build_pulseem_auth_from_env()

    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    end = datetime.now()

    try:
        data = get_incoming_sms_report(
            auth=auth,
            search_txt=vn,
            start_time=start,
            end_time=end,
            timeout_seconds=20,
        )
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"[Preflight] Pulseem HTTP error (check auth/header/key): {e}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"[Preflight] Pulseem network error: {e}") from e
    except Exception as e:
        raise RuntimeError(f"[Preflight] Pulseem unexpected error: {e}") from e

    if _pulseem_status_is_success(data):
        reports = data.get("IncomingSmsReports", []) or []
        print(f"[Preflight] Pulseem OK ✅ (success). Messages in lookback: {len(reports)}")
        return

    err = _pulseem_error_text(data)
    if "no data" in err.lower():
        print("[Preflight] Pulseem OK ✅ (NO DATA FOUND in lookback window)")
        return

    raise RuntimeError(f"[Preflight] Pulseem FAILED ❌ status={data.get('status')} error={err}")

# ----------------------------
# Playwright helpers
# ----------------------------
def safe_click(page, selector: str, timeout_ms: int = 15000) -> None:
    """Wait for element and click it"""
    page.wait_for_selector(selector, timeout=timeout_ms)
    page.click(selector)

def ensure_today_dir(base_dir: str) -> Path:
    """Create and return today's download directory"""
    today_dir = Path(base_dir) / datetime.now().date().isoformat()
    today_dir.mkdir(parents=True, exist_ok=True)
    return today_dir

def click_reconnect_link_if_present(page) -> None:
    """
    STEP 1: Check if the 'לחץ כאן' (Click here) reconnect link exists and click it.
    This handles the expired session screen.
    """
    print("[Step 1] Checking for 'לחץ כאן' reconnect link...")

    try:
        link = page.locator('a[href="/"]')
        if link.first.is_visible(timeout=3000):
            print("[Step 1] Found reconnect link, clicking...")
            with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
                link.first.click()
            print("[Step 1] Clicked reconnect link successfully")
        else:
            print("[Step 1] No reconnect link found, proceeding...")
    except PWTimeout:
        print("[Step 1] No reconnect link found (timeout), proceeding...")
    except Exception as e:
        print(f"[Step 1] Could not click reconnect link: {e}, proceeding...")

def fill_login_credentials(page, username: str, password: str) -> None:
    """
    STEP 2: Fill in username and password fields
    """
    print("[Step 2] Filling login credentials...")

    page.wait_for_selector("#input_1", timeout=20000)
    page.fill("#input_1", username)
    print(f"[Step 2] Filled username: {username[:3]}***")

    page.fill("#input_2", password)
    print("[Step 2] Filled password: ***")

def click_submit_button(page) -> None:
    """
    STEP 3: Click the submit button (אישור)
    """
    print("[Step 3] Clicking submit button...")

    submit_selector = 'input.credentials_input_submit[value="אישור"]'

    try:
        with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
            safe_click(page, submit_selector)
        print("[Step 3] Submit clicked with navigation")
    except PWTimeout:
        safe_click(page, submit_selector)
        print("[Step 3] Submit clicked (no navigation detected)")

def maybe_handle_otp(page, selectors: Dict[str, str]) -> None:
    """
    STEP 4: Handle OTP if screen appears.
    1) Try to fetch OTP from Pulseem.
    2) If Pulseem doesn't get an SMS in time, allow manual OTP entry via console.
    """

    otp_input = selectors.get("otp_input") or ""
    otp_submit = selectors.get("otp_submit") or ""

    if not otp_input or not otp_submit:
        print("[Step 4] OTP selectors not configured, skipping OTP handling")
        return

    print("[Step 4] Checking for OTP screen...")

    try:
        # Detect OTP input
        page.wait_for_selector(otp_input, timeout=7000)
        print("[Step 4] OTP screen detected!")

        # Give the site time to send the SMS
        print(f"[Step 4] Waiting {OTP_INITIAL_DELAY_SECONDS}s for SMS to be sent...")
        time.sleep(OTP_INITIAL_DELAY_SECONDS)

        # Optional: click "send code" button if exists
        send_btn = selectors.get("send_code_button") or ""
        if send_btn:
            try:
                print("[Step 4] Clicking 'send code' button...")
                safe_click(page, send_btn, timeout_ms=5000)
                print("[Step 4] Send code clicked")
                time.sleep(3)
            except PWTimeout:
                print("[Step 4] Send code button not found")

        otp: Optional[str] = None
        msg: Optional[Dict[str, Any]] = None

        # ---- Try Pulseem first ----
        try:
            auth = build_pulseem_auth_from_env()

            checkpoint_dt = get_last_sms_datetime(PULSEEM_VIRTUAL_NUMBER, auth, lookback_seconds=600)
            if checkpoint_dt != datetime.min:
                print(f"[Step 4] Checkpoint: last SMS at {checkpoint_dt.isoformat()}")
            else:
                print("[Step 4] Checkpoint: no previous SMS found")

            print("[Step 4] Waiting for OTP from Pulseem API...")
            otp, msg = wait_for_otp_from_pulseem(
                virtual_number=PULSEEM_VIRTUAL_NUMBER,
                auth=auth,
                after_datetime=checkpoint_dt,
                lookback_seconds=OTP_LOOKBACK_SECONDS,
                max_wait_seconds=OTP_MAX_WAIT_SECONDS,
                poll_every_seconds=OTP_POLL_SECONDS,
            )
            print(f"[Step 4] Received OTP from Pulseem: {otp}, ReplyDate: {msg.get('ReplyDate') if msg else None}")

        except Exception as e:
            print(f"[Step 4] Pulseem OTP failed / not received: {e}")

            # ---- Manual fallback ----
            if not MANUAL_OTP_FALLBACK:
                raise

            print("[Step 4] Manual OTP fallback ENABLED.")
            print(f"[Step 4] Please enter the OTP manually (you have up to {MANUAL_OTP_MAX_WAIT_SECONDS}s).")

            start = time.time()
            manual = ""
            while time.time() - start < MANUAL_OTP_MAX_WAIT_SECONDS:
                manual = input("Enter OTP code: ").strip()
                if manual:
                    break

            if not manual:
                raise TimeoutError("Manual OTP entry timed out / empty input.")

            otp = manual
            print("[Step 4] Manual OTP received.")

        # ---- Fill OTP ----
        if not otp:
            raise RuntimeError("OTP is empty - cannot continue.")

        page.wait_for_selector(otp_input, timeout=15000)
        page.fill(otp_input, "")
        page.type(otp_input, otp, delay=80)
        print("[Step 4] OTP entered")

        time.sleep(0.7)

        # ---- Submit OTP ----
        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=15000):
                safe_click(page, otp_submit, timeout_ms=15000)
            print("[Step 4] OTP submitted with navigation")
        except PWTimeout:
            safe_click(page, otp_submit, timeout_ms=15000)
            print("[Step 4] OTP submitted (AJAX/no navigation)")

        time.sleep(1.5)

    except PWTimeout:
        print("[Step 4] No OTP screen detected, continuing...")
    except Exception as e:
        print(f"[Step 4] OTP handling failed: {e}")
        raise


def download_one(page, click_selector: str, download_dir: Path) -> Path:
    """Download a file by clicking a selector"""
    with page.expect_download() as d:
        safe_click(page, click_selector, timeout_ms=20000)
    dl = d.value
    target = download_dir / dl.suggested_filename
    dl.save_as(str(target))
    return target

# ----------------------------
# Sites config (Harel)
# ----------------------------
SITES: List[Dict[str, Any]] = [
    {
        "name": "Harel Agents - My Policy",
        "login_url": "https://agents.harel-group.co.il/my.policy",
        "reports_url": None,
        "username": HAREL_USERNAME,
        "password": HAREL_PASSWORD,
        "selectors": {
            "user_input": "#input_1",
            "pass_input": "#input_2",
            "login_submit": 'input.credentials_input_submit[value="אישור"]',

            # OTP page selectors (based on your HTML)
            "otp_input": 'input[name="otpass"]#input_1',
            "otp_submit": 'input.credentials_input_submit[type="submit"][value="אישור"]',

            # optional if exists on OTP page:
            # "send_code_button": 'button:has-text("שלח")'
        },
        "downloads": [
            # Add download selectors once identified
        ],
    }
]

# ----------------------------
# Main
# ----------------------------
def main():
    download_dir = ensure_today_dir(BASE_DOWNLOAD_DIR)
    print("=" * 60)
    print("Harel Agents Automation Bot")
    print("=" * 60)
    print(f"Download folder: {download_dir}")
    print(f"Headless mode: {PLAYWRIGHT_HEADLESS}")
    print(f"Pulseem virtual number: {PULSEEM_VIRTUAL_NUMBER}")
    print("=" * 60)

    # ✅ Preflight: verify Pulseem connectivity/auth before running browser automation
    preflight_check_pulseem_or_die(PULSEEM_VIRTUAL_NUMBER)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        context = browser.new_context(accept_downloads=True)

        for site in SITES:
            print(f"\n{'=' * 60}")
            print(f"Processing: {site['name']}")
            print(f"{'=' * 60}")

            page = context.new_page()

            try:
                print(f"[Start] Navigating to {site['login_url']}")
                page.goto(site["login_url"], wait_until="domcontentloaded")

                # STEP 1
                click_reconnect_link_if_present(page)

                # STEP 2
                fill_login_credentials(page, site["username"], site["password"])

                # STEP 3
                click_submit_button(page)

                # STEP 4
                maybe_handle_otp(page, site["selectors"])

                # Navigate to reports page if specified
                if site.get("reports_url"):
                    print(f"[Navigation] Going to reports page: {site['reports_url']}")
                    page.goto(site["reports_url"], wait_until="domcontentloaded")

                # Execute any additional navigation steps
                for step in site["selectors"].get("nav_steps", []):
                    print(f"[Navigation] Clicking: {step}")
                    safe_click(page, step, timeout_ms=20000)

                # Download files if configured
                for item in site.get("downloads", []):
                    print(f"[Download] Downloading via: {item['click_selector']}")
                    saved = download_one(page, item["click_selector"], download_dir)
                    print(f"[Download] Saved to: {saved}")

                print(f"\n✓ {site['name']} completed successfully!")

            except Exception as e:
                print(f"\n✗ {site['name']} FAILED: {e}")
                try:
                    shot = download_dir / f"ERROR_{site['name'].replace(' ', '_')}.png"
                    page.screenshot(path=str(shot), full_page=True)
                    print(f"[Error] Screenshot saved to: {shot}")
                except Exception as screenshot_error:
                    print(f"[Error] Could not save screenshot: {screenshot_error}")

            finally:
                page.close()

        context.close()
        browser.close()

    print(f"\n{'=' * 60}")
    print("Bot execution completed")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
