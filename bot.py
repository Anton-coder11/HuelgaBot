import os
import re
import time
import hashlib
import sqlite3
import threading
import logging
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
from html import escape
from typing import Optional, List, Dict, Any

import requests
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# =========================
# CONFIG
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

HOME_URL = "https://www.sindicatodeestudiantes.net/index.php"

ALLOWED_HOSTS = {
    "sindicatodeestudiantes.net",
    "www.sindicatodeestudiantes.net",
    "sindicatdestudiants.net",
    "www.sindicatdestudiants.net",
    "ikaslesindikatua.net",
    "www.ikaslesindikatua.net",
}

DB_PATH      = os.getenv("DB_PATH", "huelga_bot.db")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
SEND_DELAY   = float(os.getenv("SEND_DELAY_SECONDS", "0.20"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
USER_AGENT   = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; HuelgaTelegramBot/1.0)")

REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
}

HUELGA_RE = re.compile(r"\b(huelga|vaga)\s+estudiantil\b", re.IGNORECASE)
OFFICIAL_RE = re.compile(
    r"\bcomunicaci[oó]n oficial\b"
    r"|\bcomunicaci[oó] oficial\b"
    r"|\bcomunicat oficial\b",
    re.IGNORECASE,
)
EXCLUDE_PDF_RE = re.compile(
    r"\b(cartel|poster|afiche|flyer|pancarta|banner)\b",
    re.IGNORECASE,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("huelga-bot")

STOP_EVENT = threading.Event()

# =========================
# DATABASE
#
# Tables:
#   subscribers – chat_ids that opted in to automatic alerts
#   seen        – articles already processed by the monitor
#   meta        – key/value flags (e.g. "seeded")
#
# Migration: if the old "users" table exists it is renamed to
# "subscribers" so existing subscribers keep their opt-in.
# =========================

def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db_connect()
    cur  = conn.cursor()

    # ── migrate old table name ──────────────────────────────────────────
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if cur.fetchone():
        cur.execute("ALTER TABLE users RENAME TO subscribers")
        logger.info("Migrated 'users' table → 'subscribers'")

    # ── create tables if they don't exist ───────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id    INTEGER PRIMARY KEY,
            created_at TEXT    NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            uid        TEXT PRIMARY KEY,
            url        TEXT NOT NULL,
            title      TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

# ── meta helpers ────────────────────────────────────────────────────────

def set_meta(key: str, value: str):
    conn = db_connect()
    conn.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()

def get_meta(key: str, default: str = "") -> str:
    conn = db_connect()
    row  = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

# ── subscriber helpers ──────────────────────────────────────────────────

def subscribe(chat_id: int):
    conn = db_connect()
    conn.execute(
        "INSERT OR IGNORE INTO subscribers (chat_id, created_at) VALUES (?,?)",
        (chat_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

def unsubscribe(chat_id: int):
    conn = db_connect()
    conn.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def is_subscribed(chat_id: int) -> bool:
    conn = db_connect()
    row  = conn.execute("SELECT 1 FROM subscribers WHERE chat_id=?", (chat_id,)).fetchone()
    conn.close()
    return row is not None

def get_subscribers() -> List[int]:
    conn = db_connect()
    rows = conn.execute("SELECT chat_id FROM subscribers ORDER BY created_at ASC").fetchall()
    conn.close()
    return [r["chat_id"] for r in rows]

# ── seen helpers ────────────────────────────────────────────────────────

def is_seen(uid: str) -> bool:
    conn = db_connect()
    row  = conn.execute("SELECT 1 FROM seen WHERE uid=?", (uid,)).fetchone()
    conn.close()
    return row is not None

def mark_seen(uid: str, url: str, title: str):
    conn = db_connect()
    conn.execute(
        "INSERT OR IGNORE INTO seen (uid,url,title,created_at) VALUES (?,?,?,?)",
        (uid, url, title, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

# =========================
# MESSAGE TRACKING
#
# Keeps a list of PDF document message IDs per chat so we can delete
# them when the user navigates away or re-requests a PDF.
# =========================

def _track_msg(context: ContextTypes.DEFAULT_TYPE, msg_id: int):
    context.chat_data.setdefault("tracked_msgs", []).append(msg_id)

async def _delete_tracked_msgs(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    msg_ids: List[int] = context.chat_data.pop("tracked_msgs", [])
    for mid in msg_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            pass

# =========================
# TELEGRAM SENDERS  (sync – used by the monitor thread)
# =========================

def _tg_post(method: str, payload: dict) -> bool:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, headers=REQUEST_HEADERS, timeout=HTTP_TIMEOUT)
        if not resp.ok:
            logger.warning("%s failed: %s %s", method, resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        logger.warning("%s exception: %s", method, e)
        return False

def _notif_markup(article_url: str) -> dict:
    """Inline keyboard dict for automatic notification messages."""
    return {
        "inline_keyboard": [
            [{"text": "🔗 Read article", "url": article_url}],
            [{"text": "🔕 Unsubscribe",  "callback_data": "unsub"}],
        ]
    }

def telegram_notify(chat_id: int, text: str, article_url: str) -> bool:
    """Send a notification message with Read + Unsubscribe buttons."""
    return _tg_post("sendMessage", {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": False,
        "reply_markup":             _notif_markup(article_url),
    })

def telegram_send_pdf_sync(chat_id: int, pdf_url: str,
                           caption: str = "", referer: Optional[str] = None) -> bool:
    """Synchronous PDF sender – only used by the monitor thread."""
    send_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    pdf_url  = clean_pdf_url(pdf_url)
    try:
        headers = dict(REQUEST_HEADERS)
        if referer:
            headers["Referer"] = referer
        r = requests.get(pdf_url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        filename = pdf_url.split("/")[-1] or "comunicacion_oficial.pdf"
        if not filename.lower().endswith(".pdf"):
            filename = "comunicacion_oficial.pdf"
        resp = requests.post(
            send_url,
            data={"chat_id": chat_id, "caption": caption[:1024]},
            files={"document": (filename, r.content, "application/pdf")},
            timeout=HTTP_TIMEOUT,
        )
        if resp.ok:
            return True
        logger.warning("sendDocument upload failed %s: %s", chat_id, resp.status_code)
    except Exception as e:
        logger.warning("sendDocument upload exception %s: %s", chat_id, e)

    # Fallback: direct URL
    try:
        resp = requests.post(
            send_url,
            data={"chat_id": chat_id, "document": pdf_url, "caption": caption[:1024]},
            timeout=HTTP_TIMEOUT,
        )
        return resp.ok
    except Exception as e:
        logger.warning("sendDocument direct-url exception %s: %s", chat_id, e)
        return False

# =========================
# TELEGRAM SENDERS  (async – used by button callbacks)
# =========================

async def bot_send_pdf(context: ContextTypes.DEFAULT_TYPE, chat_id: int,
                       pdf_url: str, caption: str = "",
                       referer: Optional[str] = None):
    """
    Async PDF sender for callbacks.
    Returns the Message object so we can track its ID, or None on failure.
    """
    pdf_url = clean_pdf_url(pdf_url)
    headers = dict(REQUEST_HEADERS)
    if referer:
        headers["Referer"] = referer

    try:
        r = requests.get(pdf_url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        filename = pdf_url.split("/")[-1] or "comunicacion_oficial.pdf"
        if not filename.lower().endswith(".pdf"):
            filename = "comunicacion_oficial.pdf"
        buf      = BytesIO(r.content)
        buf.name = filename
        return await context.bot.send_document(
            chat_id=chat_id, document=buf, filename=filename, caption=caption[:1024]
        )
    except Exception as e:
        logger.warning("bot_send_pdf download failed: %s", e)

    try:
        return await context.bot.send_document(
            chat_id=chat_id, document=pdf_url, caption=caption[:1024]
        )
    except Exception as e:
        logger.warning("bot_send_pdf direct-url failed: %s", e)
        return None

# =========================
# SCRAPING HELPERS
# =========================

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def clean_pdf_url(url: str) -> str:
    return url.split("#", 1)[0].strip() if url else url

def normalize_url(url: str, base: str) -> str:
    return urljoin(base, url)

def strip_fq(url: str) -> str:
    parts    = list(urlsplit(url))
    parts[3] = parts[4] = ""
    return urlunsplit(parts)

def is_allowed_url(url: str) -> bool:
    return urlparse(url).netloc.lower() in ALLOWED_HOSTS

def make_uid(title: str, url: str) -> str:
    return hashlib.sha1(f"{title}\n{strip_fq(url)}".encode()).hexdigest()

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=REQUEST_HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.text

def fetch_page_soup(url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch_html(url), "html.parser")

def truncate(s: str, limit: int = 38) -> str:
    s = clean_text(s)
    return s if len(s) <= limit else s[: max(0, limit - 1)].rstrip() + "…"

def collect_pdf_candidates(soup: BeautifulSoup, base_url: str):
    candidates = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = strip_fq(normalize_url(href, base_url))
        if not is_allowed_url(full_url):
            continue
        href_low = full_url.lower()
        text_low = clean_text(a.get_text(" ", strip=True)).lower()

        if ".pdf" not in href_low:
            continue
        if EXCLUDE_PDF_RE.search(text_low) or EXCLUDE_PDF_RE.search(href_low):
            continue
        if "comunic" not in text_low and "comunic" not in href_low:
            continue

        score = 5
        if href_low.endswith(".pdf"): score += 1
        if "comunic" in text_low:     score += 6
        if "oficial" in text_low:     score += 2
        if "comunic" in href_low:     score += 3
        if "oficial" in href_low:     score += 1

        candidates.append((score, clean_pdf_url(full_url)))

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates

def extract_card_date(card: BeautifulSoup) -> str:
    node = card.select_one(".card-footer small.text-muted")
    return clean_text(node.get_text(" ", strip=True)) if node else ""

def _best_pdf(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    cands = collect_pdf_candidates(soup, base_url)
    return cands[0][1] if cands else None

def extract_pdf_from_html(html: str, base_url: str) -> Optional[str]:
    try:
        return _best_pdf(BeautifulSoup(html or "", "html.parser"), base_url)
    except Exception:
        return None

def extract_pdf_from_article(url: str) -> Optional[str]:
    try:
        return _best_pdf(fetch_page_soup(url), url)
    except Exception:
        return None

def resolve_pdf_for_item(item: Dict[str, Any]) -> Optional[str]:
    for candidate in [
        item.get("pdf_url"),
        extract_pdf_from_html(item.get("card_html", ""), item["url"]),
        extract_pdf_from_article(item["url"]),
    ]:
        if candidate:
            return clean_pdf_url(candidate)
    return None

# =========================
# DISCOVERY
# =========================

def get_latest_5_news() -> List[Dict[str, Any]]:
    soup       = fetch_page_soup(HOME_URL)
    cards      = soup.select("section.bottom-separation .card")
    results:   List[Dict[str, Any]] = []
    seen_urls: set = set()

    for card in cards:
        title_tag = card.select_one("h5.card-title a")
        if not title_tag:
            continue
        title = clean_text(title_tag.get_text(" ", strip=True))
        href  = (title_tag.get("href") or "").strip()
        if not href:
            continue
        url = strip_fq(normalize_url(href, HOME_URL))
        if not is_allowed_url(url) or url in seen_urls:
            continue

        card_html = str(card)
        results.append({
            "title":     title,
            "url":       url,
            "uid":       make_uid(title, url),
            "card_html": card_html,
            "pdf_url":   extract_pdf_from_html(card_html, url),
            "date":      extract_card_date(card),
        })
        seen_urls.add(url)
        if len(results) >= 5:
            break

    return results

def is_huelga(title: str, text: str) -> bool:
    return bool(HUELGA_RE.search(f"{title}\n{text}"))

def is_official_communication(item: Dict[str, Any]) -> bool:
    card_text = clean_text(
        BeautifulSoup(item.get("card_html", ""), "html.parser").get_text(" ", strip=True)
    )
    if OFFICIAL_RE.search(card_text):
        return True
    try:
        if OFFICIAL_RE.search(clean_text(fetch_page_soup(item["url"]).get_text(" ", strip=True))):
            return True
    except Exception:
        pass
    return False

# =========================
# UI HELPERS
# =========================

def build_main_menu(subscribed: bool = False) -> InlineKeyboardMarkup:
    sub_btn = (
        InlineKeyboardButton("🔕 Unsubscribe", callback_data="unsub")
        if subscribed else
        InlineKeyboardButton("🔔 Subscribe",   callback_data="sub")
    )
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏠 Menu",     callback_data="menu"),
            InlineKeyboardButton("📢 Latest 5", callback_data="latest"),
        ],
        [
            InlineKeyboardButton("📄 Find PDF", callback_data="findpdf"),
            sub_btn,
        ],
    ])

def build_latest_menu(items: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = []
    for idx, item in enumerate(items):
        prefix = "📄 " if item.get("pdf_url") else "📰 "
        rows.append([InlineKeyboardButton(
            f"{prefix}{idx + 1}. {truncate(item['title'], 34)}",
            callback_data=f"item:{idx}",
        )])
    rows.append([
        InlineKeyboardButton("🔄 Refresh", callback_data="latest"),
        InlineKeyboardButton("🏠 Menu",    callback_data="menu"),
    ])
    return InlineKeyboardMarkup(rows)

def build_item_menu(item: Dict[str, Any], idx: int,
                    pdf_url: Optional[str]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("🔗 Open article", url=item["url"])]]
    if pdf_url:
        rows.append([InlineKeyboardButton("📄 Send PDF", callback_data=f"pdf:{idx}")])
    rows.append([
        InlineKeyboardButton("⬅️ Back",  callback_data="latest"),
        InlineKeyboardButton("🏠 Menu", callback_data="menu"),
    ])
    return InlineKeyboardMarkup(rows)

def format_item_text(item: Dict[str, Any]) -> str:
    text = f"📌 <b>{escape(item['title'])}</b>"
    if item.get("date"):
        text += f"\n🗓 {escape(item['date'])}"
    return text + "\n\nChoose an action below."

MENU_TEXT = (
    "👋 <b>Huelga monitor</b>\n\n"
    "Browse the latest news or subscribe to get automatic alerts "
    "whenever a new article is published."
)

# ── cache helpers ────────────────────────────────────────────────────────

def cache_items(context: ContextTypes.DEFAULT_TYPE, items: List[Dict[str, Any]]):
    context.chat_data["latest_items"] = items

def get_cached_items(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    return context.chat_data.get("latest_items", [])

def refresh_items(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    items = get_latest_5_news()
    cache_items(context, items)
    return items

def get_item(context: ContextTypes.DEFAULT_TYPE, idx: int) -> Optional[Dict[str, Any]]:
    items = get_cached_items(context)
    if not items:
        try:
            items = refresh_items(context)
        except Exception:
            return None
    if not (0 <= idx < len(items)):
        try:
            items = refresh_items(context)
        except Exception:
            return None
        if not (0 <= idx < len(items)):
            return None
    return items[idx]

# =========================
# SHARED HELPERS
# =========================

async def _safe_edit(query, text: str, reply_markup=None):
    try:
        await query.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except Exception as e:
        if "not modified" not in str(e).lower():
            logger.warning("edit_text error: %s", e)

# =========================
# COMMANDS
# =========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        MENU_TEXT,
        parse_mode="HTML",
        reply_markup=build_main_menu(is_subscribed(chat_id)),
        disable_web_page_preview=True,
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        "Commands:\n"
        "/start – open menu\n"
        "/latest – latest 5 news\n"
        "/findpdf – send first comunicación oficial PDF\n"
        "/subscribe – subscribe to automatic alerts\n"
        "/unsubscribe – stop automatic alerts\n"
        "/help – this message",
        parse_mode="HTML",
        reply_markup=build_main_menu(is_subscribed(chat_id)),
    )

async def subscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    subscribe(chat_id)
    await update.message.reply_text(
        "🔔 <b>Subscribed!</b>\n\n"
        "You'll receive an alert here whenever a new article is published.",
        parse_mode="HTML",
        reply_markup=build_main_menu(subscribed=True),
    )

async def unsubscribe_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    unsubscribe(chat_id)
    await update.message.reply_text(
        "🔕 <b>Unsubscribed.</b>\n\nYou won't receive automatic alerts anymore.",
        parse_mode="HTML",
        reply_markup=build_main_menu(subscribed=False),
    )

async def latest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        items = refresh_items(context)
    except Exception as e:
        logger.warning("latest_cmd failed: %s", e)
        await update.message.reply_text(
            "Could not fetch the latest news.",
            reply_markup=build_main_menu(is_subscribed(update.effective_chat.id)),
        )
        return
    if not items:
        await update.message.reply_text("No news found.",
                                        reply_markup=build_main_menu(is_subscribed(update.effective_chat.id)))
        return
    await update.message.reply_text(
        "📢 <b>Latest 5 news</b>\n\nChoose one below.",
        parse_mode="HTML",
        reply_markup=build_latest_menu(items),
    )

async def findpdf_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        items = refresh_items(context)
    except Exception as e:
        logger.warning("findpdf_cmd failed: %s", e)
        await update.message.reply_text("Could not fetch the latest news.",
                                        reply_markup=build_main_menu(is_subscribed(chat_id)))
        return
    for item in items:
        pdf_url = resolve_pdf_for_item(item)
        if pdf_url:
            await update.message.reply_text(
                f"📄 Sending <b>comunicación oficial</b> PDF for:\n{escape(item['title'])}",
                parse_mode="HTML",
                reply_markup=build_main_menu(is_subscribed(chat_id)),
            )
            telegram_send_pdf_sync(chat_id, pdf_url, caption=item["title"], referer=item["url"])
            return
    await update.message.reply_text(
        "No comunicación oficial PDF found in the latest 5 news.",
        reply_markup=build_main_menu(is_subscribed(chat_id)),
    )

# =========================
# CALLBACKS
# =========================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = query.message.chat.id
    data    = query.data or ""

    # ── Menu ──────────────────────────────────────────────────────────────
    if data == "menu":
        await _delete_tracked_msgs(context, chat_id)
        await _safe_edit(query, MENU_TEXT,
                         reply_markup=build_main_menu(is_subscribed(chat_id)))
        return

    # ── Subscribe ─────────────────────────────────────────────────────────
    if data == "sub":
        subscribe(chat_id)
        await _safe_edit(
            query,
            "🔔 <b>Subscribed!</b>\n\n"
            "You'll receive an alert here whenever a new article is published.",
            reply_markup=build_main_menu(subscribed=True),
        )
        return

    # ── Unsubscribe ───────────────────────────────────────────────────────
    # Works both from the menu and from notification message buttons.
    if data == "unsub":
        unsubscribe(chat_id)
        await _safe_edit(
            query,
            "🔕 <b>Unsubscribed.</b>\n\nYou won't receive automatic alerts anymore.",
            reply_markup=build_main_menu(subscribed=False),
        )
        return

    # ── Latest 5 ─────────────────────────────────────────────────────────
    if data == "latest":
        await _delete_tracked_msgs(context, chat_id)
        try:
            items = refresh_items(context)
        except Exception as e:
            logger.warning("button latest failed: %s", e)
            await _safe_edit(query, "Could not fetch the latest news.",
                             reply_markup=build_main_menu(is_subscribed(chat_id)))
            return
        if not items:
            await _safe_edit(query, "No news found.",
                             reply_markup=build_main_menu(is_subscribed(chat_id)))
            return
        await _safe_edit(query, "📢 <b>Latest 5 news</b>\n\nChoose one below.",
                         reply_markup=build_latest_menu(items))
        return

    # ── Find PDF (menu shortcut) ──────────────────────────────────────────
    if data == "findpdf":
        await _delete_tracked_msgs(context, chat_id)
        try:
            items = refresh_items(context)
        except Exception as e:
            logger.warning("button findpdf failed: %s", e)
            await _safe_edit(query, "Could not fetch the latest news.",
                             reply_markup=build_main_menu(is_subscribed(chat_id)))
            return

        for item in items:
            pdf_url = resolve_pdf_for_item(item)
            if pdf_url:
                await _safe_edit(query,
                                 f"📤 Sending PDF…\n<b>{escape(item['title'])}</b>",
                                 reply_markup=None)
                msg = await bot_send_pdf(context, chat_id, pdf_url,
                                         caption=item["title"], referer=item["url"])
                if msg:
                    _track_msg(context, msg.message_id)
                await _safe_edit(query,
                                 f"✅ PDF sent for:\n<b>{escape(item['title'])}</b>",
                                 reply_markup=build_main_menu(is_subscribed(chat_id)))
                return

        await _safe_edit(query, "No comunicación oficial PDF found in the latest 5 news.",
                         reply_markup=build_main_menu(is_subscribed(chat_id)))
        return

    # ── Item detail ───────────────────────────────────────────────────────
    if data.startswith("item:"):
        try:
            idx = int(data.split(":", 1)[1])
        except Exception:
            return
        await _delete_tracked_msgs(context, chat_id)
        item = get_item(context, idx)
        if not item:
            await _safe_edit(query, "Item no longer available. Open Latest 5 again.",
                             reply_markup=build_main_menu(is_subscribed(chat_id)))
            return
        pdf_url = resolve_pdf_for_item(item)
        await _safe_edit(query, format_item_text(item),
                         reply_markup=build_item_menu(item, idx, pdf_url))
        return

    # ── Send PDF for item ─────────────────────────────────────────────────
    if data.startswith("pdf:"):
        try:
            idx = int(data.split(":", 1)[1])
        except Exception:
            return

        await _delete_tracked_msgs(context, chat_id)   # always wipe old PDF first

        item = get_item(context, idx)
        if not item:
            await _safe_edit(query, "Item no longer available. Open Latest 5 again.",
                             reply_markup=build_main_menu(is_subscribed(chat_id)))
            return

        pdf_url = resolve_pdf_for_item(item)
        if not pdf_url:
            await _safe_edit(
                query,
                f"⚠️ No comunicación oficial PDF found for:\n<b>{escape(item['title'])}</b>",
                reply_markup=build_item_menu(item, idx, None),
            )
            return

        await _safe_edit(query, f"📤 Sending PDF…\n<b>{escape(item['title'])}</b>",
                         reply_markup=None)
        msg = await bot_send_pdf(context, chat_id, pdf_url,
                                 caption=item["title"], referer=item["url"])
        if msg:
            _track_msg(context, msg.message_id)

        await _safe_edit(query, f"✅ PDF sent.\n\n{format_item_text(item)}",
                         reply_markup=build_item_menu(item, idx, pdf_url))
        return

# =========================
# MONITOR
#
# Polls the homepage every POLL_SECONDS.
# Every NEW article is sent to ALL subscribers.
# If the article is a huelga alert the header says 🚨 HUELGA ESTUDIANTIL.
# If a comunicación oficial PDF is found it is sent immediately after.
# =========================

def seed_existing_content():
    """Mark current articles as seen so the bot doesn't spam on first run."""
    if get_meta("seeded", "0") == "1":
        return
    try:
        for item in get_latest_5_news():
            mark_seen(item["uid"], item["url"], item["title"])
        set_meta("seeded", "1")
        logger.info("Seeded existing items – no alerts will fire for them.")
    except Exception as e:
        logger.warning("Seeding failed: %s", e)

def _build_notification_text(item: Dict[str, Any], huelga_flag: bool) -> str:
    if huelga_flag:
        header = "🚨 <b>HUELGA ESTUDIANTIL — nuevo artículo</b>"
    else:
        header = "🆕 <b>Nuevo artículo</b>"

    text = f"{header}\n\n📌 <b>{escape(item['title'])}</b>"
    if item.get("date"):
        text += f"\n🗓 {escape(item['date'])}"
    return text

def monitor_loop():
    while not STOP_EVENT.is_set():
        try:
            news        = get_latest_5_news()
            subscribers = get_subscribers()

            for item in news:
                if is_seen(item["uid"]):
                    continue

                try:
                    # Build full text for matching
                    card_text = clean_text(
                        BeautifulSoup(item["card_html"], "html.parser").get_text(" ", strip=True)
                    )
                    try:
                        article_text = clean_text(
                            fetch_page_soup(item["url"]).get_text(" ", strip=True)
                        )
                    except Exception:
                        article_text = ""

                    all_text    = f"{item['title']}\n{card_text}\n{article_text}"
                    huelga_flag = is_huelga(item["title"], all_text)
                    pdf_url     = resolve_pdf_for_item(item)

                    notif_text = _build_notification_text(item, huelga_flag)

                    # Send to every subscriber
                    for chat_id in subscribers:
                        telegram_notify(chat_id, notif_text, item["url"])
                        if pdf_url:
                            telegram_send_pdf_sync(
                                chat_id, pdf_url,
                                caption="Comunicación oficial",
                                referer=item["url"],
                            )
                        time.sleep(SEND_DELAY)

                    mark_seen(item["uid"], item["url"], item["title"])
                    logger.info("Notified %d subscribers: %s", len(subscribers), item["title"])

                except Exception as e:
                    logger.warning("Failed processing %s: %s", item["url"], e)

        except Exception as e:
            logger.warning("Monitor error: %s", e)

        STOP_EVENT.wait(POLL_SECONDS)

# =========================
# STARTUP
# =========================

async def post_init(app: Application):
    seed_existing_content()
    thread = threading.Thread(target=monitor_loop, daemon=True, name="monitor")
    thread.start()
    logger.info("Monitor thread started (poll every %ds)", POLL_SECONDS)

async def post_shutdown(app: Application):
    STOP_EVENT.set()

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is missing")

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start",       start))
    app.add_handler(CommandHandler("help",        help_cmd))
    app.add_handler(CommandHandler("subscribe",   subscribe_cmd))
    app.add_handler(CommandHandler("unsubscribe", unsubscribe_cmd))
    app.add_handler(CommandHandler("latest",      latest_cmd))
    app.add_handler(CommandHandler("findpdf",     findpdf_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_error_handler(
        lambda update, context: logger.exception("Unhandled error: %s", context.error)
    )

    print("Bot running…")
    app.run_polling()

if __name__ == "__main__":
    main()