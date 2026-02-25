"""
bot_all_in_one.py - Harel Agents Automation with Pulseem OTP (IMPROVED v5)

Fixes your error:
- STOP relying on dynamic JSS class: a.agNav-jss440  (it changes!)
- Click "ריכוז תשלומים" / "דוח ריכוז תשלומים" in Favorites by STABLE selectors:
  - href contains DocIdLookup.aspx?DocId=AGENTS-31-129
  - OR link has text "ריכוז תשלומים"
- Searches inside ALL FRAMES (main page + iframes) because the Favorites drawer often lives in an iframe.
- Wait/click strategy supports hidden tags: wait_for(state="attached") + visible-first click + fallback force click.
- Adds stronger portal validation after OTP: must load https://agents-int.harel-group.co.il/Pages/default.aspx
  and verify Favorites button exists.

Flow:
- Login
- OTP (Pulseem or manual)
- Go to MAIN_PORTAL_URL
- Open Favorites (מועדפים) and click "ריכוז תשלומים" (AGENTS-31-129)
- Continue existing report flow (company -> filter -> latest row -> popup -> Excel)
"""

import os
import re
import time
import sys
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


# ----------------------------
# Logging
# ----------------------------
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("harel_bot")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(handler)
    return logger


log = setup_logger()

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

# MAIN portal after OTP (your requirement)
MAIN_PORTAL_URL = os.getenv("MAIN_PORTAL_URL", "https://agents-int.harel-group.co.il/Pages/default.aspx").strip()

if not PULSEEM_VIRTUAL_NUMBER:
    raise RuntimeError("Missing PULSEEM_VIRTUAL_NUMBER in .env")

if not HAREL_USERNAME or not HAREL_PASSWORD:
    log.warning("Missing HAREL_USERNAME/HAREL_PASSWORD in .env")

# ----------------------------
# Click / wait tuning
# ----------------------------
CLICK_RETRIES = int(os.getenv("CLICK_RETRIES", "5"))
CLICK_RETRY_BASE_SLEEP = float(os.getenv("CLICK_RETRY_BASE_SLEEP", "0.8"))
POST_CLICK_SLEEP = float(os.getenv("POST_CLICK_SLEEP", "0.6"))
SETTLE_SLEEP = float(os.getenv("SETTLE_SLEEP", "0.35"))

# ----------------------------
# Pulseem settings
# ----------------------------
PULSEEM_URL = "https://api.pulseem.com/api/v1/SmsApi/GetIncomingSmsReport"
OTP_REGEX = re.compile(r"\b(\d{4,8})\b")


# ----------------------------
# Pulseem helpers
# ----------------------------
def normalize_il_number(num: str) -> str:
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
    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    end = datetime.now()

    try:
        data = get_incoming_sms_report(auth, search_txt=vn, start_time=start, end_time=end)
    except Exception:
        log.warning("[Pulseem] could not check last SMS")
        return datetime.min

    if str(data.get("status", "")).lower() != "success":
        log.warning("[Pulseem] could not check last SMS (status not success)")
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
    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    deadline = time.time() + max_wait_seconds

    last_seen_dt = after_datetime or datetime.min
    log.info(f"[OTP] Waiting for SMS newer than: {last_seen_dt.isoformat() if last_seen_dt != datetime.min else 'N/A'}")

    while time.time() < deadline:
        end = datetime.now()
        data = get_incoming_sms_report(auth, search_txt=vn, start_time=start, end_time=end)

        status = str(data.get("status", "")).lower()
        if status != "success":
            err = str(data.get("error") or "").lower()
            if "no data" in err:
                time.sleep(poll_every_seconds)
                continue
            raise RuntimeError(f"Pulseem error: {data.get('error')}")

        reports = data.get("IncomingSmsReports", []) or []
        if not reports:
            time.sleep(poll_every_seconds)
            continue

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

        fresh_sorted = sorted(fresh, key=lambda x: parse_reply_date(str(x.get("ReplyDate", ""))), reverse=True)
        for msg in fresh_sorted:
            text = str(msg.get("ReplyText") or "")
            m = OTP_REGEX.search(text)
            if m:
                log.info(f"[OTP] Found code: {m.group(1)}")
                return m.group(1), msg

        time.sleep(poll_every_seconds)

    raise TimeoutError("No NEW OTP SMS arrived within the configured wait window.")


def build_pulseem_auth_from_env() -> PulseemAuth:
    api_key = os.getenv("PULSEEM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OTP required but missing PULSEEM_API_KEY in .env")
    return PulseemAuth(api_key=api_key, mode=PULSEEM_AUTH_MODE, header_name=PULSEEM_APIKEY_HEADER)


def preflight_check_pulseem_or_die(virtual_number: str, lookback_seconds: int = 86400) -> None:
    log.info("[Preflight] Checking Pulseem API connectivity/auth...")

    api_key = os.getenv("PULSEEM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("[Preflight] Missing PULSEEM_API_KEY in .env")

    auth = build_pulseem_auth_from_env()
    vn = normalize_il_number(virtual_number)
    start = datetime.now() - timedelta(seconds=lookback_seconds)
    end = datetime.now()

    data = get_incoming_sms_report(
        auth=auth,
        search_txt=vn,
        start_time=start,
        end_time=end,
        timeout_seconds=20,
    )

    status = str(data.get("status", "")).strip().lower()
    err = str(data.get("error") or data.get("message") or "").strip()

    if status == "success":
        log.info(f"[Preflight] Pulseem OK ✅ (success). Messages in lookback: {len(data.get('IncomingSmsReports', []) or [])}")
        return
    if "no data" in err.lower():
        log.info("[Preflight] Pulseem OK ✅ (NO DATA FOUND in lookback window)")
        return

    raise RuntimeError(f"[Preflight] Pulseem FAILED ❌ status={data.get('status')} error={err}")


# ----------------------------
# Playwright helpers
# ----------------------------
def ensure_today_dir(base_dir: str) -> Path:
    today_dir = Path(base_dir) / datetime.now().date().isoformat()
    today_dir.mkdir(parents=True, exist_ok=True)
    return today_dir


def wait_page_ready(page, timeout_ms: int = 25000) -> None:
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_function(
            "document.readyState === 'complete' || document.readyState === 'interactive'",
            timeout=timeout_ms,
        )
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass
    time.sleep(SETTLE_SLEEP)


def dismiss_overlays(page) -> None:
    try:
        dialog = page.locator('div[role="dialog"]:visible, .modal:visible, .popup:visible').first
        if dialog.count() == 0:
            return
        close_btn = dialog.locator(
            'button[aria-label*="סגור"], button[aria-label*="Close"], '
            'button:has-text("סגור"), button:has-text("Close")'
        ).first
        if close_btn.is_visible(timeout=500):
            close_btn.click(timeout=1500, force=True)
            time.sleep(0.2)
    except Exception:
        pass


def robust_click(page, *, locator=None, selector: Optional[str] = None, description: str = "click", timeout_ms: int = 20000) -> None:
    """
    Supports hidden DOM:
    - wait_for(state="attached")
    - then click (normal -> force)
    """
    if locator is None and selector is None:
        raise ValueError("robust_click requires locator or selector")

    for attempt in range(1, CLICK_RETRIES + 1):
        try:
            wait_page_ready(page, timeout_ms=timeout_ms)
            dismiss_overlays(page)

            loc = locator if locator is not None else page.locator(selector).first
            loc.wait_for(state="attached", timeout=timeout_ms)

            try:
                loc.scroll_into_view_if_needed(timeout=timeout_ms)
            except Exception:
                pass

            try:
                loc.click(timeout=timeout_ms)
            except Exception:
                loc.click(timeout=timeout_ms, force=True)

            log.info(f"[Click] ✅ {description} (attempt {attempt})")
            wait_page_ready(page, timeout_ms=timeout_ms)
            time.sleep(POST_CLICK_SLEEP)
            return

        except Exception as e:
            log.warning(f"[Click] ❌ {description} failed (attempt {attempt}/{CLICK_RETRIES}): {e}")
            time.sleep(CLICK_RETRY_BASE_SLEEP * attempt)

    raise RuntimeError(f"Failed to click: {description} after {CLICK_RETRIES} attempts")


# ----------------------------
# Frame helpers (CRITICAL FIX)
# ----------------------------
def _all_frames(page):
    return page.frames  # includes main + iframes


def find_first_locator_in_any_frame(page, selector: str, timeout_ms_each: int = 1500):
    """
    Find selector across all frames; returns (frame, locator) if attached.
    """
    for fr in _all_frames(page):
        loc = fr.locator(selector).first
        try:
            if loc.count() > 0:
                loc.wait_for(state="attached", timeout=timeout_ms_each)
                return fr, loc
        except Exception:
            continue
    return None, None


def robust_click_in_frame(page, frame, locator, description: str, timeout_ms: int = 20000):
    """
    Click locator that belongs to a frame; supports hidden tags.
    """
    for attempt in range(1, CLICK_RETRIES + 1):
        try:
            wait_page_ready(page, timeout_ms=timeout_ms)
            dismiss_overlays(page)

            locator.wait_for(state="attached", timeout=timeout_ms)

            try:
                locator.scroll_into_view_if_needed(timeout=timeout_ms)
            except Exception:
                pass

            try:
                locator.click(timeout=timeout_ms)
            except Exception:
                locator.click(timeout=timeout_ms, force=True)

            log.info(f"[Click] ✅ {description} (attempt {attempt})")
            wait_page_ready(page, timeout_ms=timeout_ms)
            time.sleep(POST_CLICK_SLEEP)
            return
        except Exception as e:
            log.warning(f"[Click] ❌ {description} failed (attempt {attempt}/{CLICK_RETRIES}): {e}")
            time.sleep(CLICK_RETRY_BASE_SLEEP * attempt)

    raise RuntimeError(f"Failed to click: {description} after {CLICK_RETRIES} attempts")


# ----------------------------
# Navigation: go to portal after OTP
# ----------------------------
def goto_main_portal(page) -> None:
    log.info(f"[Nav] Going to main portal: {MAIN_PORTAL_URL}")
    page.goto(MAIN_PORTAL_URL, wait_until="domcontentloaded")
    wait_page_ready(page, timeout_ms=30000)

    # Validate Favorites button exists (stable)
    fav_btn = page.locator('button[data-hrl-bo="atm-drowerButton"][aria-label="מועדפים"]').first
    fav_btn.wait_for(state="attached", timeout=30000)
    log.info("[Nav] Main portal loaded (Favorites button present).")


# ----------------------------
# Favorites (FIXED)
# ----------------------------
def open_favorites(page) -> None:
    """
    Open Favorites drawer reliably.
    DO NOT use #navPanel (duplicated id).
    DO NOT wait for dynamic classes like agNav-jss440.
    """
    wait_page_ready(page, timeout_ms=30000)
    dismiss_overlays(page)

    fav_btn = page.locator('button[data-hrl-bo="atm-drowerButton"][aria-label="מועדפים"]').first
    fav_btn.wait_for(state="attached", timeout=30000)

    # If already expanded, skip click
    try:
        expanded = (fav_btn.get_attribute("aria-expanded") or "").strip().lower()
        if expanded == "true":
            log.info("[Fav] Favorites already expanded.")
            return
    except Exception:
        pass

    robust_click(page, locator=fav_btn, description="open Favorites (מועדפים)", timeout_ms=30000)


def click_favorite_report_link(page) -> None:
    """
    Click "ריכוז תשלומים" in Favorites.

    FIX: don't rely on a.agNav-jss440.
    Strategy:
    - open favorites
    - find by href (stable) across all frames
    - fallback: find by text "ריכוז תשלומים" across all frames
    """
    open_favorites(page)

    href_visible = 'a[href*="DocIdLookup.aspx?DocId=AGENTS-31-129"]:visible'
    href_any = 'a[href*="DocIdLookup.aspx?DocId=AGENTS-31-129"]'

    # text fallbacks (some sites show "דוח ריכוז תשלומים", you asked "ריכוז תשלומים")
    txt1_visible = 'a:has-text("ריכוז תשלומים"):visible'
    txt1_any = 'a:has-text("ריכוז תשלומים")'
    txt2_visible = 'a:has-text("דוח ריכוז תשלומים"):visible'
    txt2_any = 'a:has-text("דוח ריכוז תשלומים")'

    # 1) href visible
    fr, loc = find_first_locator_in_any_frame(page, href_visible)
    if loc:
        robust_click_in_frame(page, fr, loc, "click Favorites report (href visible)", timeout_ms=30000)
    else:
        # 2) href attached/hidden
        fr, loc = find_first_locator_in_any_frame(page, href_any)
        if loc:
            robust_click_in_frame(page, fr, loc, "click Favorites report (href attached/hidden)", timeout_ms=30000)
        else:
            # 3) text visible
            fr, loc = find_first_locator_in_any_frame(page, txt1_visible)
            if loc:
                robust_click_in_frame(page, fr, loc, 'click Favorites report (text "ריכוז תשלומים" visible)', timeout_ms=30000)
            else:
                fr, loc = find_first_locator_in_any_frame(page, txt2_visible)
                if loc:
                    robust_click_in_frame(page, fr, loc, 'click Favorites report (text "דוח ריכוז תשלומים" visible)', timeout_ms=30000)
                else:
                    # 4) text attached/hidden
                    fr, loc = find_first_locator_in_any_frame(page, txt1_any)
                    if loc:
                        robust_click_in_frame(page, fr, loc, 'click Favorites report (text "ריכוז תשלומים" attached/hidden)', timeout_ms=30000)
                    else:
                        fr, loc = find_first_locator_in_any_frame(page, txt2_any)
                        if loc:
                            robust_click_in_frame(page, fr, loc, 'click Favorites report (text "דוח ריכוז תשלומים" attached/hidden)', timeout_ms=30000)
                        else:
                            raise RuntimeError('Favorites report link not found (by href or text) in any frame.')

    # Confirm report loaded
    try:
        page.wait_for_url(re.compile(r".*DocIdLookup\.aspx.*DocId=AGENTS-31-129.*"), timeout=30000)
    except Exception:
        page.locator("div.ctrlbutton.cbo").first.wait_for(state="attached", timeout=30000)

    log.info("[Fav] Report opened successfully (AGENTS-31-129).")


# ----------------------------
# Paid popup handler + download
# ----------------------------
def handle_paid_window_and_download(paid_page, download_dir: Path) -> Path:
    log.info("[Paid] Handling paid.aspx window...")
    wait_page_ready(paid_page, timeout_ms=25000)

    robust_click(paid_page, selector="div.ctrlbutton.cbo", description="paid dropdown", timeout_ms=25000)

    sel_all = paid_page.locator('div.selectall[role="button"]').filter(has_text="בחר הכל").first
    robust_click(paid_page, locator=sel_all, description='paid "בחר הכל"', timeout_ms=25000)

    try:
        btn = paid_page.get_by_role("button", name=re.compile(r"^\s*סנן מידע\s*$"))
        robust_click(paid_page, locator=btn, description='paid "סנן מידע"', timeout_ms=25000)
    except Exception:
        robust_click(paid_page, selector="button.filter-apply", description='paid "סנן מידע" (fallback)', timeout_ms=25000)

    time.sleep(1.5)
    wait_page_ready(paid_page, timeout_ms=25000)

    excel_btn = paid_page.locator("button.bar-excel").first
    excel_btn.wait_for(state="attached", timeout=25000)

    with paid_page.expect_download(timeout=60000) as d:
        robust_click(paid_page, locator=excel_btn, description="paid Excel download", timeout_ms=25000)

    dl = d.value
    target = download_dir / dl.suggested_filename
    dl.save_as(str(target))
    log.info(f"[Paid] Excel saved to: {target}")
    return target


# ----------------------------
# Post-login / report flow
# ----------------------------
def _parse_il_date_ddmmyyyy(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    try:
        return datetime.strptime(s, "%d/%m/%Y")
    except Exception:
        return None


def run_payments_assembly_flow(page, download_dir: Path) -> None:
    log.info("[Post] Starting payments assembly flow...")

    # IMPORTANT: go to correct portal page first
    goto_main_portal(page)

    # Favorites -> report (FIXED)
    click_favorite_report_link(page)

    # Company dropdown
    robust_click(page, selector="div.ctrlbutton.cbo", description="company dropdown", timeout_ms=25000)

    company_regex = re.compile(r"^113005565\s+-\s+ידידים")
    company_btn = page.get_by_role("button", name=company_regex)
    robust_click(page, locator=company_btn, description="select company 113005565", timeout_ms=25000)

    # Filter
    filter_btn = page.get_by_role("button", name=re.compile(r"^\s*סנן מידע\s*$"))
    robust_click(page, locator=filter_btn, description='click "סנן מידע"', timeout_ms=25000)

    time.sleep(1.2)
    wait_page_ready(page, timeout_ms=25000)

    # Find latest date row via TR
    log.info('[Post] Finding latest "תאריך פעולה" row...')
    date_cells = page.locator('td[data_colid="Date_Hatama_Desc"]')
    date_cells.first.wait_for(state="attached", timeout=25000)

    best_cell = None
    best_dt = datetime.min
    for i in range(date_cells.count()):
        cell = date_cells.nth(i)
        try:
            txt = (cell.inner_text() or "").strip()
        except Exception:
            continue
        m = re.search(r"(\d{2}/\d{2}/\d{4})", txt)
        if not m:
            continue
        dt = _parse_il_date_ddmmyyyy(m.group(1))
        if dt and dt > best_dt:
            best_dt = dt
            best_cell = cell

    if best_cell is None:
        raise RuntimeError('Could not parse any dd/mm/yyyy from "תאריך פעולה" cells.')

    log.info(f"[Post] Latest date found: {best_dt.strftime('%d/%m/%Y')}")
    row = best_cell.locator("xpath=ancestor::tr[1]")

    # Click "נפרעים"
    nifraim = row.locator('td[data_colid="Schum_Nifraim"] span').first
    robust_click(page, locator=nifraim, description='click "נפרעים" (latest row)', timeout_ms=25000)

    # Validate agent number
    agent_cell = row.locator('td[data_colid="Sochen_ID"]').first
    agent_text = (agent_cell.inner_text() or "").strip()
    agent_digits = re.search(r"\d+", agent_text)
    agent_num = agent_digits.group(0) if agent_digits else ""
    if agent_num != "165":
        raise RuntimeError(f"[Post] Agent number mismatch: expected 165, got '{agent_text}'")
    log.info("[Post] Agent number OK (165).")

    # Click month amount -> popup
    month_span = row.locator('td[data_colid="_M2_Schum"] span').first
    month_span.wait_for(state="attached", timeout=25000)
    try:
        month_span.scroll_into_view_if_needed(timeout=25000)
    except Exception:
        pass

    ctx = page.context
    with ctx.expect_page(timeout=25000) as pop:
        robust_click(page, locator=month_span, description="open paid popup (month cell)", timeout_ms=25000)
    paid_page = pop.value

    saved_excel = handle_paid_window_and_download(paid_page, download_dir)
    log.info(f"[Post] Paid Excel downloaded: {saved_excel}")
    log.info("[Post] Payments assembly flow done ✅")


# ----------------------------
# Login helpers
# ----------------------------
def click_reconnect_link_if_present(page) -> None:
    log.info("[Step 1] Checking for reconnect link...")
    try:
        link = page.locator('a[href="/"]').first
        if link.count() > 0 and link.is_visible(timeout=2000):
            robust_click(page, locator=link, description="reconnect link", timeout_ms=12000)
    except Exception:
        pass


def fill_login_credentials(page, username: str, password: str) -> None:
    log.info("[Step 2] Filling login credentials...")
    page.wait_for_selector("#input_1", timeout=20000)
    page.fill("#input_1", username)
    page.fill("#input_2", password)
    log.info("[Step 2] Credentials filled")


def click_submit_button(page) -> None:
    log.info("[Step 3] Clicking submit button...")
    submit = page.locator('input.credentials_input_submit[value="אישור"]').first
    robust_click(page, locator=submit, description='submit login ("אישור")', timeout_ms=20000)


def maybe_handle_otp(page, selectors: Dict[str, str]) -> None:
    otp_input = selectors.get("otp_input") or ""
    otp_submit = selectors.get("otp_submit") or ""
    if not otp_input or not otp_submit:
        log.info("[Step 4] OTP selectors not configured, skipping OTP handling")
        return

    log.info("[Step 4] Checking for OTP screen...")
    try:
        page.wait_for_selector(otp_input, timeout=7000)
        log.info("[Step 4] OTP screen detected!")
    except PWTimeout:
        log.info("[Step 4] No OTP screen detected, continuing...")
        return

    log.info(f"[Step 4] Waiting {OTP_INITIAL_DELAY_SECONDS}s for SMS to be sent...")
    time.sleep(OTP_INITIAL_DELAY_SECONDS)

    otp: Optional[str] = None
    try:
        auth = build_pulseem_auth_from_env()
        checkpoint_dt = get_last_sms_datetime(PULSEEM_VIRTUAL_NUMBER, auth, lookback_seconds=600)
        log.info(f"[Step 4] Checkpoint last SMS: {checkpoint_dt.isoformat() if checkpoint_dt != datetime.min else 'None'}")

        otp, msg = wait_for_otp_from_pulseem(
            virtual_number=PULSEEM_VIRTUAL_NUMBER,
            auth=auth,
            after_datetime=checkpoint_dt,
            lookback_seconds=OTP_LOOKBACK_SECONDS,
            max_wait_seconds=OTP_MAX_WAIT_SECONDS,
            poll_every_seconds=OTP_POLL_SECONDS,
        )
        log.info(f"[Step 4] Received OTP from Pulseem (ReplyDate={msg.get('ReplyDate') if msg else None})")
    except Exception as e:
        log.warning(f"[Step 4] Pulseem OTP failed: {e}")
        if not MANUAL_OTP_FALLBACK:
            raise

        log.info("[Step 4] Manual OTP fallback ENABLED.")
        start = time.time()
        manual = ""
        while time.time() - start < MANUAL_OTP_MAX_WAIT_SECONDS:
            manual = input("Enter OTP code: ").strip()
            if manual:
                break
        if not manual:
            raise TimeoutError("Manual OTP entry timed out / empty input.")
        otp = manual

    if not otp:
        raise RuntimeError("OTP is empty - cannot continue.")

    page.fill(otp_input, "")
    page.fill(otp_input, otp)
    log.info("[Step 4] OTP entered")

    submit = page.locator(otp_submit).first
    robust_click(page, locator=submit, description='submit OTP ("אישור")', timeout_ms=20000)


# ----------------------------
# Sites config
# ----------------------------
SITES: List[Dict[str, Any]] = [
    {
        "name": "Harel Agents - My Policy",
        "login_url": "https://agents.harel-group.co.il/my.policy",
        "username": HAREL_USERNAME,
        "password": HAREL_PASSWORD,
        "selectors": {
            "otp_input": 'input[name="otpass"]#input_1',
            "otp_submit": 'input.credentials_input_submit[type="submit"][value="אישור"]',
        },
    }
]


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    download_dir = ensure_today_dir(BASE_DOWNLOAD_DIR)

    log.info("=" * 60)
    log.info("Harel Agents Automation Bot")
    log.info("=" * 60)
    log.info(f"Download folder: {download_dir}")
    log.info(f"Headless mode: {PLAYWRIGHT_HEADLESS}")
    log.info(f"Pulseem virtual number: {PULSEEM_VIRTUAL_NUMBER}")
    log.info(f"Main portal URL: {MAIN_PORTAL_URL}")
    log.info("=" * 60)

    preflight_check_pulseem_or_die(PULSEEM_VIRTUAL_NUMBER)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        context = browser.new_context(accept_downloads=True)

        for site in SITES:
            log.info("\n" + "=" * 60)
            log.info(f"Processing: {site['name']}")
            log.info("=" * 60)

            page = context.new_page()
            try:
                log.info(f"[Start] Navigating to {site['login_url']}")
                page.goto(site["login_url"], wait_until="domcontentloaded")
                wait_page_ready(page)

                click_reconnect_link_if_present(page)
                fill_login_credentials(page, site["username"], site["password"])
                click_submit_button(page)
                maybe_handle_otp(page, site["selectors"])

                run_payments_assembly_flow(page, download_dir)
                log.info(f"✓ {site['name']} completed successfully!")

            except Exception as e:
                log.exception(f"✗ {site['name']} FAILED: {e}")
                try:
                    shot = download_dir / f"ERROR_{site['name'].replace(' ', '_')}.png"
                    page.screenshot(path=str(shot), full_page=True)
                    log.info(f"[Error] Screenshot saved to: {shot}")
                except Exception as screenshot_error:
                    log.warning(f"[Error] Could not save screenshot: {screenshot_error}")

            finally:
                try:
                    page.close()
                except Exception:
                    pass

        context.close()
        browser.close()

    log.info("\n" + "=" * 60)
    log.info("Bot execution completed")
    log.info("=" * 60)


if __name__ == "__main__":
    main()