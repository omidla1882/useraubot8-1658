import asyncio
from asyncio import Queue, Semaphore
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, User, UserStatusOnline, UserStatusRecently, UserStatusLastWeek, InputPeerEmpty, InputPeerUser, InputMessagesFilterEmpty
from telethon.errors import (
    FloodWaitError, ChannelPrivateError, ChannelsTooMuchError,
    UserPrivacyRestrictedError, UserNotMutualContactError, UserChannelsTooMuchError,
    ChatAdminRequiredError, ChatWriteForbiddenError, PeerFloodError, UserBannedInChannelError,
    UserIsBlockedError, UserIdInvalidError, InputUserDeactivatedError
)
from telethon.tl.functions.channels import JoinChannelRequest, InviteToChannelRequest, GetParticipantsRequest, LeaveChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import SearchGlobalRequest, CheckChatInviteRequest, ImportChatInviteRequest, DeleteChatUserRequest
from telethon.tl.types import ChannelParticipantsRecent, ChatInviteAlready, ChatInvite
from telethon.errors import InviteHashInvalidError, InviteHashExpiredError, UserAlreadyParticipantError
import random
from datetime import datetime, timedelta, date
import time
import re
import logging
import signal
import sys
import json
import gc  # 🧹 Garbage Collection برای Railway
import os  # 🖥️ برای متغیرهای محیطی
from pathlib import Path
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
import aiohttp
from aiohttp import web

# ═══════════════════════════════════════════════════════════
# 🚂 تنظیمات Railway - بهینه‌سازی منابع سرور
# ═══════════════════════════════════════════════════════════
RAILWAY_ENVIRONMENT = os.environ.get('RAILWAY_ENVIRONMENT', 'production')
IS_RAILWAY = 'RAILWAY' in os.environ or os.environ.get('RAILWAY_STATIC_URL', '') != ''

# 🧹 تنظیمات Garbage Collection برای Railway - تهاجمی‌تر
gc.enable()
gc.set_threshold(500, 8, 4)  # پاکسازی تهاجمی‌تر برای کاهش RAM

# 📊 محدودیت‌های منابع Railway - بهینه برای سرور
MAX_MEMORY_ITEMS = 500  # حداکثر آیتم در هر دیکشنری (کاهش از 1000)
MEMORY_WARNING_THRESHOLD = 300  # هشدار مصرف حافظه (کاهش از 500)
GC_INTERVAL = 180  # پاکسازی هر 3 دقیقه (کاهش از 5 دقیقه)

# ═══════════════════════════════════════════════════════════
# 🔇 نسخه بدون لاگ (Silent Mode) - بهینه برای سرور
# ═══════════════════════════════════════════════════════════
# تمام لاگ‌ها خاموش برای کاهش مصرف منابع سرور
# این نسخه برای استفاده در سرورهای production طراحی شده

# 🔕 حالت Silent - تمام لاگ‌ها خاموش
SILENT_MODE = True  # 🚀 True برای سرور Production | False برای دیباگ محلی

# تنظیم لاگینگ - خاموش کردن همه چیز
logging.basicConfig(
    level=logging.CRITICAL,
    format='',
    handlers=[logging.NullHandler()]
)

# غیرفعال کردن کامل تمام loggerها
logging.getLogger().disabled = True
logging.getLogger('telethon').disabled = True
logging.getLogger('asyncio').disabled = True

# تابع dummy برای جایگزینی logger
class DummyLogger:
    def info(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
    def debug(self, *args, **kwargs): pass
    def critical(self, *args, **kwargs): pass

logger = DummyLogger()

# 🔕 تابع لاگ خاموش - برای جایگزینی print
def slog(*args, **kwargs):
    """Silent log - فقط در حالت غیر Silent چاپ میکند"""
    if not SILENT_MODE:
        print(*args, **kwargs)

# تنظیمات API
api_id = 28652875
api_hash = '97469594916750008690bb4a21e2ebab'

session_name = 'my_session'

# ═══════════════════════════════════════════════════════════
# 🚀 تنظیمات بهینه‌شده عملکرد (RAILWAY OPTIMIZED v3.0)
# ═══════════════════════════════════════════════════════════

# 🎛️ حالت کاری Railway (انتخاب یکی)
# eco = حداقل مصرف منابع | normal = متعادل | performance = حداکثر کارایی
RAILWAY_MODE = 'normal'  # � حالت normal برای فعالیت بیشتر

# مدیریت صف (Queue Management)
MAX_QUEUE_SIZE = 200
MAX_CONCURRENT_TASKS = 3
MESSAGE_BATCH_SIZE = 2

# مدیریت تاخیر (Delay Management)
BASE_DELAY = (15, 35)
FLOOD_WAIT_MULTIPLIER = 1.5
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = (30, 90)

# مدیریت کانال (Channel Management)
MAX_CHANNELS_PER_SESSION = 150
CHANNEL_JOIN_DELAY = (20, 45)

# محدودیت‌های روزانه (Daily Limits)
DAILY_PM_LIMIT = 40
DAILY_JOIN_LIMIT = 50
DAILY_MESSAGE_LIMIT = 200
CHANNEL_CLEANUP_INTERVAL = 3600

# مدیریت حافظه (Memory Management)
MAX_HISTORY_SIZE = 500
MEMORY_CLEANUP_INTERVAL = 300
MAX_SCRAPED_USERS = 1000
MAX_GROUPS_IN_MEMORY = 200

# تنظیمات Member Adder ⚔️
# ═══════════════════════════════════════════════════════════
TARGET_GROUP = "@PharmaWebGp"  # گروه هدف برای اضافه کردن اعضا
GROUP_LINK = "https://t.me/PharmaWebGp"  # لینک دعوت گروه

# 🚀 تنظیمات Scraping
MEMBER_FETCH_LIMIT = 150
MEMBER_SCRAPE_INTERVAL = 180
SCRAPE_MULTIPLE_GROUPS = 3

# ⚔️ تنظیمات Direct Add
INVITE_DELAY_MIN = 30
INVITE_DELAY_MAX = 60
MAX_INVITES_PER_CYCLE = 5
INVITE_CYCLE_INTERVAL = 120
DAILY_INVITE_TARGET = 100

# 📨 تنظیمات PM
PM_DELAY_MIN = 60
PM_DELAY_MAX = 120
MAX_PM_PER_CYCLE = 3

# ═══════════════════════════════════════════════════════════
# 📢 تنظیمات Reliable Broadcast Controller (از examplebot ادغام شده - کلیدی برای تاخیرهای طولانی و ایمن)
# این مقادیر طولانی و تطبیقی هستند تا ریسک بن به شدت کاهش یابد.
# ═══════════════════════════════════════════════════════════
BROADCAST_MAX_PER_HOUR = 12
BROADCAST_MAX_PER_DAY = 80
BROADCAST_MIN_GLOBAL_INTERVAL = 180  # حداقل 3 دقیقه بین هر ارسال
BROADCAST_PER_GROUP_COOLDOWN_MIN = 300   # 5 دقیقه حداقل per group
BROADCAST_PER_GROUP_COOLDOWN_MAX = 600   # 10 دقیقه حداکثر per group
BROADCAST_POST_SEND_MIN = 60
BROADCAST_POST_SEND_MAX = 180   # 1-3 دقیقه post-send
BROADCAST_BATCH_SIZE = 3
BROADCAST_BATCH_REST_MIN = 300
BROADCAST_BATCH_REST_MAX = 600   # 5-10 دقیقه batch rest

# 🎯 فیلترینگ هوشمند
ACTIVE_DAYS_THRESHOLD = 28  # فقط اعضای فعال در 2 هفته اخیر
MIN_USER_QUALITY_SCORE = 0.1  # افزایش کیفیت
PRIORITIZE_PREMIUM_USERS = False  # 🔒 غیرفعال - همه کاربران یکسان

MEMBERS_DB_FILE = "members_database.json"  # فایل ذخیره حافظه

# تنظیمات بهینه‌سازی - متعادل‌تر
CLEANUP_INTERVAL = 1800  # هر 30 دقیقه پاکسازی (کاهش بار)
MESSAGE_RETENTION = 43200  # نگهداری پیام‌ها برای 12 ساعت (کاهش RAM)
INITIAL_RETRY_DELAY = 8  # تاخیر اولیه برای retry (ثانیه)

# 🎯 بهینه‌سازی‌های پیشرفته - Smart Activity
PEAK_HOURS = [0, 7, 9, 10 , 11, 12, 13, 15, 16,17, 18, 19, 20, 21, 22 , 23 , 1, 2, 3, 4, 5, 6, 7, 8, 14 ]  # ساعات پیک فعالیت
OFF_PEAK_HOURS = [ ]  # ساعات کم کار
PEAK_ACTIVITY_MULTIPLIER = 1.3  # 30% فعالیت بیشتر در ساعات پیک
OFF_PEAK_ACTIVITY_MULTIPLIER = 0.6  # 40% کاهش در ساعات کم کار
SLEEP_HOURS = [8 , 9, 10]  # ساعات استراحت کامل (رفتار انسانی)

# 🔄 چرخش هوشمند محتوا
MESSAGE_ROTATION_ENABLED = True  # فعال‌سازی چرخش پیام
MIN_MESSAGE_REUSE_DELAY = 7200  # حداقل 2 ساعت فاصله برای استفاده مجدد پیام
MESSAGE_VARIATION_ENABLED = True  # تنوع در پیام‌ها
MAX_SAME_MESSAGE_PER_DAY = 3  # حداکثر 3 بار یک پیام در روز

# 🛡️ Anti-Spam Detection پیشرفته
SPAM_DETECTION_ENABLED = True  # فعال‌سازی تشخیص اسپم
MAX_MESSAGES_PER_GROUP_HOURLY = 2  # حداکثر 2 پیام در ساعت به هر گروه
COOLDOWN_AFTER_SPAM_REPORT = 14400  # 4 ساعت استراحت بعد از report
GROUP_REPUTATION_TRACKING = True  # ردیابی reputation گروه‌ها

# 📊 Quality Score System
MIN_GROUP_QUALITY_SCORE = 3  # حداقل امتیاز کیفیت گروه (از 10)
QUALITY_FACTORS = {
    'member_count': 0.3,      # 30% وزن تعداد اعضا
    'activity_level': 0.4,    # 40% وزن سطح فعالیت
    'response_rate': 0.3      # 30% وزن نرخ پاسخ
}

# 🎯 Dynamic Rate Adjustment
DYNAMIC_RATE_ENABLED = True  # تنظیم پویای سرعت
SUCCESS_RATE_THRESHOLD = 0.7  # اگر موفقیت بالای 70%، سرعت افزایش
FAILURE_RATE_THRESHOLD = 0.3  # اگر شکست بالای 30%، سرعت کاهش
RATE_ADJUSTMENT_INTERVAL = 1800  # هر 30 دقیقه بررسی و تنظیم

# 📢 کنترل ارسال تبلیغات در گروه‌ها
# ═══════════════════════════════════════════════════════════
# برای غیرفعال کردن ارسال تبلیغات: False
# برای فعال کردن ارسال تبلیغات: True
# ═══════════════════════════════════════════════════════════
ENABLE_BROADCAST = True  # 🟢 ارسال پیام‌های تبلیغاتی فعال شد

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 PROFESSIONAL NATURAL AI + HUMAN SIM (ported + adapted from web3test best practices)
# These make the bot act like a real human: read first, type, natural delay + strict quality.
# ═══════════════════════════════════════════════════════════════════════════════

import re as _re  # local alias to avoid polluting top re if needed

# --- Human simulation (from web3test/core/telegram_userbot.py, adapted) ---
async def _simulate_human_delay(action: str = 'general', msg_len: int = 40):
    """More realistic variable human delay (length + time of day aware)."""
    if action == 'between_groups':
        delay = random.uniform(150, 380)
    elif action == 'typing':
        delay = random.uniform(3.2, 9.5)
        if msg_len > 70:
            delay += random.uniform(2.0, 4.5)
        if msg_len > 140:
            delay += random.uniform(1, 2)
    elif action == 'reading':
        delay = random.uniform(2.2, 8.5)
    elif action == 'pre_reply':
        delay = random.uniform(2.5, 7.5)
    else:
        delay = random.uniform(1.2, 4.0)

    hour = datetime.now().hour
    if hour in (1,2,3,4,5,23):
        delay *= random.uniform(1.1, 1.4)
    await asyncio.sleep(max(1.0, delay))

async def simulate_read_and_type(client, chat, msg_len: int = 40):
    """Simulate a human reading recent messages then typing before replying."""
    try:
        msgs = await client.get_messages(chat, limit=random.randint(4, 9))
        if msgs:
            await client.send_read_acknowledge(chat, msgs[-1])
        await _simulate_human_delay('reading', msg_len)
    except Exception:
        pass
    try:
        from telethon.tl.functions.messages import SetTypingRequest
        from telethon.tl.types import SendMessageTypingAction
        await client(SetTypingRequest(peer=chat, action=SendMessageTypingAction()))
        await _simulate_human_delay('typing', msg_len)
    except Exception:
        pass
    await _simulate_human_delay('pre_reply', msg_len)

# --- Context fetcher for natural replies ---
async def fetch_recent_group_context(client, chat_id: int, limit: int = 8) -> str:
    """Return recent chat lines as context string for LLM."""
    try:
        msgs = await client.get_messages(chat_id, limit=limit)
        lines = []
        for m in reversed(msgs or []):
            txt = (m.text or '').strip()
            if not txt:
                continue
            sender = 'کاربر'
            try:
                if m.sender and getattr(m.sender, 'first_name', None):
                    sender = m.sender.first_name[:12]
            except Exception:
                pass
            lines.append(f"{sender}: {txt[:220]}")
        return "\n".join(lines[-6:]) if lines else ""
    except Exception:
        return ""

# --- Strict quality + naturalness gate (inspired by web3test _validate_sentence + sanitize) ---
def _persian_normalize(t: str) -> str:
    if not t:
        return t
    t = t.translate(str.maketrans({'ي': 'ی', 'ك': 'ک', 'ة': 'ه'}))
    t = _re.sub(r'\bمی ([^\s‌])', r'می‌\1', t)
    t = _re.sub(r'\bنمی ([^\s‌])', r'نمی‌\1', t)
    t = _re.sub(r'  +', ' ', t)
    return t.strip()

def is_high_quality_natural(text: str) -> bool:
    if not text or len(text) < 22 or len(text) > 850:
        return False
    t = _persian_normalize(text)
    # Must contain Persian characters
    if not _re.search(r'[آ-ی]', t):
        return False
    # Stronger: require real verb forms + sentence terminators
    verb_mid = bool(_re.search(
        r'(می‌|میشه|میکنه|داره|هست|است|کرد|شد|گفت|دید|رفت|خواست|میره|میاد|'
        r'میگم|میدونم|میتونم|نمیدونم|بگید|بپرس|ببین|کنید|شده|داده|گفته|اومده|اومدم|'
        r'دارند|هستند|داریم|میخوام|میگه|میگن|باشه|باشد|هستی|میرسه|میکنم|میشم|گرفتم|تجربه|معمولا|'
        r'خوبی|چطوره|چطوری|میگذره)',
        t
    ))
    has_persian_content = len(_re.findall(r'[آ-ی]', t)) >= 8
    if not (verb_mid and has_persian_content):
        return False
    # Require at least one proper sentence end for "complete" feel
    ends = t.count('.') + t.count('؟') + t.count('!') + t.count('،')
    if ends < 1:
        return False
    # No prompt garbage leaking into output
    if any(bad in t[:70] for bad in ('قوانین:', 'نمونه خروجی', 'خروجی:', 'ساختار:', 'دستورالعمل:', 'قانون ۱', 'You are', 'Output only')):
        return False
    # Too structured / list spam (need 2+ markers or direct promo)
    spam_markers = ['۱)', '۲)', '۳)', '۴)', '📌', 'گام به گام']
    if sum(1 for m in spam_markers if m in t) >= 2:
        return False
    # Direct promo / site push (very common low-quality leak)
    promo = ['برای سفارش', 'به سایت مراجعه', 'با ادمین تماس', 'لینک زیر', 'سفارش بدید', 'خرید کنید از']
    if any(p in t for p in promo):
        return False
    # Garbled nonsense / training artifacts
    garbage = ['بازیکن', 'فولوور شما', 'AI assistant', 'User:', 'Assistant:', 'Human:']
    if any(g in t for g in garbage):
        return False
    # Reject English robotic preamble
    bad_starts = ('Sure!', 'Of course!', 'Certainly!', 'I am an AI', 'As an AI', 'Here is')
    if any(t.startswith(b) for b in bad_starts):
        return False
    # Reject self-identifying as AI or defensive "I'm not a bot" talk (very common small-model failure)
    ai_self = ['هوش مصنوعی هستم', 'ربات هستم', 'من یک ai', 'من رباتم', 'بات هستم', 'چرا فکر کردی رباتم', 'شبیه ربات', 'ربات نبودم', 'من ربات نیستم']
    if any(a in t for a in ai_self):
        return False
    if 'کجا شبیه ربات' in t or 'چرا فکر کردی' in t and 'ربات' in t:
        return False
    # Reject very repetitive single phrase or "آره خودم گرفتم" loops
    if len(set(t.split())) < 6 and len(t) > 30:
        return False
    if t.lower().count('خودم گرفتم') >= 2 or t.lower().count('آره خودم') >= 2:
        return False
    if any(x in t for x in ('تخمیر', 'هواپلتر', 'حماست', 'چیزی است که', 'به منظور انجام')):
        return False
    if t.startswith('فکر کنم') and any(x in t for x in ('چیزی است', 'به دلیل وجود', 'تو رباتی')):
        return False
    # Nonsense units (small-model hallucination: "۲ سانتیمتر طول میکشد")
    if _re.search(r'(سانتیمتر|سانتی‌متر|کیلومتر)\s*(طول|زمان)|طول میکشد\s*$', t):
        return False
    return True

# Formal → casual Persian (ported from web3test ai_service._clean_persian_text)
# Sorted longest-first to prevent shorter keys consuming longer matches
_FORMAL_TO_CASUAL = {
    'استفاده نمایید': 'استفاده کنید', 'مراجعه نمایید': 'مراجعه کنید',
    'توصیه می‌گردد': 'پیشنهاد میکنم', 'پیشنهاد می‌گردد': 'پیشنهاد میکنم',
    'لازم به ذکر است': 'باید بگم', 'شایان ذکر است': 'لازمه بدونید',
    'امکان‌پذیر است': 'میشه', 'امکان‌پذیر نیست': 'نمیشه',
    'قابل توجه است': 'مهمه که', 'مورد نیاز است': 'لازمه',
    'توجه فرمایید': 'دقت کنید', 'ملاحظه فرمایید': 'ببینید',
    'می‌توانید': 'میتونید', 'نمی‌توانید': 'نمیتونید',
    'می‌گویم': 'میگم', 'می‌دانم': 'میدونم', 'می‌دانید': 'میدونید',
    'نمی‌دانم': 'نمیدونم', 'می‌خواهم': 'میخوام',
    'می‌باشد': 'هست', 'نمی‌باشد': 'نیست',
    'می‌گردد': 'میشه', 'می‌شود': 'میشه', 'نمی‌شود': 'نمیشه',
    'می‌توان': 'میشه', 'نمی‌توان': 'نمیشه',
    'می‌بایست': 'باید', 'ضرورت دارد': 'لازمه', 'الزامی است': 'حتماً باید',
    'بنابراین': 'پس', 'لذا': 'پس', 'از این رو': 'به همین دلیل',
    'انجام میدهد': 'انجام میده', 'انجام می‌دهد': 'انجام میده',
    'درمان میکند': 'درمان میکنه', 'درمان می‌کند': 'درمان میکنه',
    'تأیید میشود': 'تأیید میشه', 'تأیید می‌شود': 'تأیید میشه',
    'به این موضوع اشاره می‌کنم': '', 'به این موضوع اشاره ميکنم': '',
}


# English → Persian common word map (ported from web3test _clean_persian_text)
_ENG_TO_FA = {
    'AI': 'هوش مصنوعی', 'assistant': 'دستیار', 'bot': 'ربات',
    'user': 'کاربر', 'system': '', 'prompt': '', 'instructions': '',
    'rules': '', 'developer': '', 'programmed': '', 'configured': '',
    'please': 'لطفاً', 'thanks': 'ممنون', 'sorry': 'متأسفم',
    'yes': 'بله', 'no': 'نه', 'ok': 'باشه', 'okay': 'باشه',
    'hello': 'سلام', 'hi': 'سلام', 'bye': 'خداحافظ',
    'however': '', 'although': '', 'note': '', 'important': '',
    'can': 'میشه', 'should': 'باید', 'must': 'باید', 'need': 'نیاز',
    'also': 'همچنین', 'but': 'ولی', 'because': 'چون', 'if': 'اگه',
    'help': 'کمک', 'question': 'سوال', 'answer': 'جواب',
    'medicine': 'دارو', 'drug': 'دارو', 'tablet': 'قرص',
    'capsule': 'کپسول', 'injection': 'آمپول', 'doctor': 'پزشک',
    'warning': '', 'caution': '', 'always': 'همیشه', 'never': 'هرگز',
}

def _clean_natural(text: str) -> str:
    if not text:
        return text
    # Strip Qwen3 thinking blocks FIRST
    text = _re.sub(r'<think>[\s\S]*?</think>', '', text)
    text = _re.sub(r'</?think[^>]*>', '', text)
    # Remove foreign languages (Chinese/Japanese/Korean, Cyrillic, Thai)
    text = _re.sub('[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]+', '', text)
    text = _re.sub('[\u0400-\u04ff]+', '', text)
    text = _re.sub('[\u0e00-\u0e7f]+', '', text)
    # Remove prompt-disclosure sentences
    text = _re.sub(r'[^.]*(?:دستورالعمل|قوانین من|برنامه‌ریزی شده|طراحی شده|دستور دارم)[^.]*\.?', '', text)
    # Remove over-cautious medical disclaimers
    text = _re.sub(r'[^.]*(?:با پزشک مشورت کنید|قبل از مصرف حتماً|خودسرانه مصرف نکنید)[^.]*\.?', '', text)
    # Strip markdown artifacts
    text = _re.sub(r'#{1,6}\s+', '', text)
    text = _re.sub(r'\*{2,}([^*]+)\*{2,}', r'\1', text)
    text = _re.sub(r'[-─═]{3,}', '', text)
    text = _re.sub(r'`[^`]*`', '', text)
    # English → Persian word substitution
    for eng, fa in _ENG_TO_FA.items():
        text = _re.sub(rf'\b{eng}\b', fa, text, flags=_re.IGNORECASE)
    # Fix brand name variations
    fixes = {
        'مدفارماوب': 'فارماوب', 'مد فارماوب': 'فارماوب',
        'Medpharmaweb': 'medpharmaweb.com', 'MedPharmaWeb': 'medpharmaweb.com',
        'iMed': 'فارماوب', 'آی‌مد': 'فارماوب',
    }
    for w, r in fixes.items():
        text = text.replace(w, r)
    # Formal → casual
    for formal, casual in _FORMAL_TO_CASUAL.items():
        text = text.replace(formal, casual)
    text = _re.sub(r'\n{3,}', '\n\n', text)
    lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
    if len(lines) > 8:
        lines = lines[:8]
    return _persian_normalize('\n'.join(lines))


def _repair_group_output(text: str) -> str:
    """Aggressive repair for small-model hallucinations and garbage (major anti-AI-tell hardening)."""
    if not text:
        return text
    # Fix brand hallucinations
    text = text.replace('مدفارماوب', 'فارماوب')
    text = _re.sub(r'medpharmaweb|imed|sara', 'فارماوب', text, flags=_re.I)
    # Fix "only tron" hallucination
    text = _re.sub(r'فقط\s*ترون[^.\n]*', '۸ ارز دیجیتال قبول می‌کنیم (از جمله USDT روی TRC20)', text, flags=_re.I)
    text = _re.sub(r'only\s*tron[^.\n]*', 'We accept 8 cryptos (incl. USDT on TRC20)', text, flags=_re.I)
    # Kill common garbage hallucinations seen in logs + defensive AI meta
    garbage_patterns = [
        r'بازیکن[^.،]*', r'شما\s*0\s*نفر[^.،]*', r'شیره\s*خرما[^.،]*', r'خمیر\s*خرما[^.،]*',
        r'معطوف[^.،]*', r'بستگه[^.،]*\s*بستگه', r'معمااً', r'معمولااً',
        r'چرا فکر کردی[^.،]*ربات[^.،]*', r'شبیه ربات[^.،]*', r'من ربات نیستم[^.،]*', r'آدم معمولی‌ام[^.،]*ربات'
    ]
    for pat in garbage_patterns:
        text = _re.sub(pat, '', text, flags=_re.I)
    # Remove repetitive lines
    lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
    seen = set()
    clean_lines = []
    for ln in lines:
        norm = ln[:60].lower()
        if norm not in seen:
            seen.add(norm)
            clean_lines.append(ln)
    text = '\n'.join(clean_lines)
    # Strip leading "آره خودم گرفتم" spam loops
    text = _re.sub(r'^(آره!?\s*خودم گرفتم[،.!\s]*){1,3}', '', text, flags=_re.I).strip()
    if len(text) > 620:
        text = text[:620].rsplit(' ', 1)[0] + '…'
    return text.strip()


# --- AI response logger (for verification) ---
import os as _os
_os.makedirs("remember/ai_logs", exist_ok=True)

def log_ai_response(summary: str, raw: str, final: str):
    try:
        from datetime import datetime as _dt
        ts = _dt.now().isoformat(timespec='seconds')
        entry = f"\n[{ts}] {summary}\nRAW: {raw[:450]!r}\nFINAL: {final[:450]!r}\n---\n"
        # main log
        with open("ai_responses.log", "a", encoding="utf-8") as f:
            f.write(entry)
        # also to dated log
        day = _dt.now().strftime("%Y-%m-%d")
        with open(f"remember/ai_logs/responses-{day}.log", "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception:
        pass

# --- Per-group exchange history for real multi-turn conversations ---
# Stores tuples of (role, text) where role is 'user' or 'bot'
group_exchange_history: Dict[int, deque] = defaultdict(lambda: deque(maxlen=10))

# Track recent bot outputs per group to avoid repetition
recent_bot_outputs: Dict[int, deque] = defaultdict(lambda: deque(maxlen=6))

# Phase 2: Simple persistent group notes (remembers group personality)
GROUP_NOTES_FILE = "remember/group_notes.json"
group_notes: Dict[int, list] = {}

def load_group_notes():
    global group_notes
    try:
        if os.path.exists(GROUP_NOTES_FILE):
            with open(GROUP_NOTES_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                group_notes = {int(k): v for k, v in raw.items()}
    except Exception:
        pass

def save_group_notes():
    try:
        os.makedirs(os.path.dirname(GROUP_NOTES_FILE), exist_ok=True)
        with open(GROUP_NOTES_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in group_notes.items()}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def add_group_note(chat_id: int, note: str):
    if chat_id not in group_notes:
        group_notes[chat_id] = []
    group_notes[chat_id].append(note[:180])
    if len(group_notes[chat_id]) > 6:
        group_notes[chat_id] = group_notes[chat_id][-6:]
    save_group_notes()

def get_group_notes(chat_id: int) -> str:
    notes = group_notes.get(chat_id, [])
    return "\n".join(notes[-3:]) if notes else ""

load_group_notes()

# === More Professional: Lightweight persistent user + group memory ===
USER_MEMORY_FILE = "remember/user_memory.json"
user_memory: Dict[str, dict] = {}  # key = f"{group_id}:{user_id}"

def load_user_memory():
    global user_memory
    try:
        if os.path.exists(USER_MEMORY_FILE):
            with open(USER_MEMORY_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                user_memory = raw
    except Exception:
        pass

def save_user_memory():
    try:
        os.makedirs(os.path.dirname(USER_MEMORY_FILE), exist_ok=True)
        with open(USER_MEMORY_FILE, "w", encoding="utf-8") as f:
            json.dump(user_memory, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def update_user_memory(group_id: int, user_id: int, topic: str, sentiment: str = "neutral"):
    key = f"{group_id}:{user_id}"
    if key not in user_memory:
        user_memory[key] = {"topics": [], "last_ts": 0, "engagement": 0}
    mem = user_memory[key]
    if topic and topic not in mem["topics"]:
        mem["topics"].append(topic[:60])
        if len(mem["topics"]) > 5:
            mem["topics"] = mem["topics"][-5:]
    mem["last_ts"] = time.time()
    mem["engagement"] = mem.get("engagement", 0) + 1
    save_user_memory()

def get_user_context(group_id: int, user_id: int) -> str:
    key = f"{group_id}:{user_id}"
    mem = user_memory.get(key, {})
    topics = ", ".join(mem.get("topics", [])[-3:])
    return f"کاربر قبلاً در مورد {topics} حرف زده." if topics else ""

load_user_memory()

def _hash_text(t: str) -> str:
    return str(hash(t.lower().strip()[:120]))[:12]

def _is_repetitive(chat_id: int, text: str) -> bool:
    h = _hash_text(text)
    if h in recent_bot_outputs[chat_id]:
        return True
    # Extra fuzzy similarity to last 3 (prevent near-duplicates even with small edits)
    norm = text.lower().strip()[:100]
    for prev_h in list(recent_bot_outputs[chat_id])[-3:]:
        # We store hashes, so also keep a small text cache for similarity
        pass
    return False

# Keep a small text cache per group for fuzzy similarity (in addition to hashes)
_recent_bot_texts: Dict[int, deque] = defaultdict(lambda: deque(maxlen=5))

def _is_repetitive_or_similar(chat_id: int, text: str) -> bool:
    if _is_repetitive(chat_id, text):
        return True
    norm = _normalize_for_rep(text)[:140]
    for prev in list(_recent_bot_texts[chat_id])[-4:]:
        if not prev:
            continue
        p = prev[:140]
        if norm == p or norm in p or p in norm:
            return True
        aw = set(norm.split())
        bw = set(p.split())
        if aw and bw and len(aw & bw) / max(len(aw), 1) >= 0.72:
            return True
        # also check start similarity (common in repetitive answers)
        if norm[:60] == p[:60] and len(norm) > 30:
            return True
    return False

def _record_bot_output(chat_id: int, text: str):
    h = _hash_text(text)
    recent_bot_outputs[chat_id].append(h)
    _recent_bot_texts[chat_id].append(_normalize_for_rep(text))

def _sanitize_group_output(text: str) -> str:
    """Ultimate runtime sanitizer: remove any defensive or AI-meta language. Called before any group send."""
    if not text:
        return text
    bad = ['چرا فکر کردی رباتم', 'شبیه ربات', 'آدم معمولی‌ام', 'ربات کجا شبیه من', 'من ربات نیستم', 'هوش مصنوعی هستم', 'رباتم']
    for b in bad:
        if b in text:
            # Replace with safe neutral continuation
            text = text.replace(b, 'من یکی از اعضا هستم')
    return text.strip()

# --- Reply style variety for more human feel ---
def choose_reply_style() -> str:
    styles = [
        "informative",      # straight useful info
        "curious",          # ask a gentle follow-up
        "agree_add",        # agree and add a small detail
        "practical_tip",    # share a small practical note
    ]
    return random.choice(styles)

async def generate_natural_valuable_post(topic_hint: str = "") -> str:
    """AI-first valuable non-spam post (to be used when broadcast enabled)."""
    probe = topic_hint or "یه سوال یا نظر کوتاه طبیعی برای گپ گروهی، نه تبلیغ"
    ctx = []
    resp = await call_qwen3_natural(ctx, probe)
    if resp and is_high_quality_natural(resp):
        return resp
    return "راستی این روزا اینترنت خیلی بی‌ثبات شده. شما هم مشکل دارین یا فقط منه؟"

# ═══════════════════════════════════════════════════════════════════════════════
# 🤖 تنظیمات هوش مصنوعی گروه (GROUP AI - Qwen3)
# ═══════════════════════════════════════════════════════════════════════════════
ENABLE_GROUP_AI = True  # 🟢 فعال | 🔴 غیرفعال

# URL سرویس Qwen3 — داخل Railway از railway.internal و برای تست محلی localhost
QWEN3_BASE_URL = os.environ.get('QWEN3_BASE_URL', 'http://qwen3.railway.internal:11434')
QWEN3_MODEL = os.environ.get('QWEN3_MODEL', 'qwen3:1.7b')

# محدودیت: حداکثر 1 پاسخ AI در هر گروه در این بازه زمانی (ثانیه) — STRONG anti-spam
GROUP_AI_COOLDOWN_SECONDS = 70    # حضور طبیعی‌تر در گروه‌های خود کاربر
GROUP_AI_TIMEOUT_SECONDS = 110    # هم‌تراز با QWEN_INFERENCE_TIMEOUT روی سرور
QWEN3_MAX_RETRIES = 1             # یک تلاش اضافه کافی است؛ بیشتر مدل CPU را قفل می‌کند
_last_global_qwen = 0.0
MIN_GLOBAL_QWEN_INTERVAL = 10     # حداقل فاصله بین دو فراخوانی مدل (کل گروه‌ها)

# ═══════════════════════════════════════════════════════════
# 🛡️ SMART ANTI-SPAM / ANTI-DUPLICATE GUARD
# ═══════════════════════════════════════════════════════════
MIN_GROUP_BOT_INTERVAL = 70   # حداقل فاصله بین هر پیام ربات در یک گروه
last_group_bot_send: Dict[int, float] = {}

# Premium custom emoji (Telegram Premium) — filled after client.start
_ACCOUNT_PREMIUM = False
_CUSTOM_EMOJI_IDS: Dict[str, int] = {}


def _utf16_len(s: str) -> int:
    return len((s or "").encode("utf-16-le")) // 2


async def init_human_style():
    """Detect Premium and cache custom-emoji document ids when available."""
    global _ACCOUNT_PREMIUM, _CUSTOM_EMOJI_IDS
    try:
        me = await client.get_me()
        _ACCOUNT_PREMIUM = bool(getattr(me, "premium", False))
        slog(f"human_style premium={_ACCOUNT_PREMIUM}")
        if not _ACCOUNT_PREMIUM:
            return
        from telethon.tl.functions.messages import SearchCustomEmojiRequest
        for emo in ["😂", "😅", "🔥", "👍", "❤️", "🤔", "✌️", "🙂", "💪", "🎬"]:
            try:
                res = await client(SearchCustomEmojiRequest(emoticon=emo, hash=0))
                ids = list(getattr(res, "document_id", []) or [])
                if ids:
                    _CUSTOM_EMOJI_IDS[emo] = int(ids[0])
            except Exception:
                continue
        slog(f"custom_emoji cached={len(_CUSTOM_EMOJI_IDS)}")
    except Exception as e:
        slog(f"human_style init skip: {e}")


async def send_group_human(chat, text: str, reply_to=None):
    """Send a group message with at most one natural emoji (premium custom if cached)."""
    try:
        from ai.human_style import decorate_human_text
        text, emo = decorate_human_text(text or "")
    except Exception:
        emo = ""
    kwargs = {}
    if reply_to:
        kwargs["reply_to"] = reply_to
    if emo and emo in _CUSTOM_EMOJI_IDS:
        try:
            from telethon.tl.types import MessageEntityCustomEmoji
            offset = _utf16_len(text) - _utf16_len(emo)
            if offset < 0:
                offset = 0
            kwargs["formatting_entities"] = [
                MessageEntityCustomEmoji(
                    offset=offset,
                    length=_utf16_len(emo),
                    document_id=_CUSTOM_EMOJI_IDS[emo],
                )
            ]
        except Exception:
            pass
    return await client.send_message(chat, text, **kwargs)

# PM anti-duplicate: per-user cooldown (نه فقط یک‌بار اولیه)
_pm_last_reply: Dict[int, float] = {}   # user_id -> timestamp آخرین پاسخ
PM_REPLY_COOLDOWN = 60  # حداقل 60 ثانیه بین دو پاسخ به یک کاربر در PM
_pm_processing: set = set()  # قفل در حین پردازش — جلوگیری از race condition

def can_send_to_group_safely(gid: int) -> bool:
    """Central guard: returns False if we sent to this group too recently.
    Also consults SmartTimeManager hourly limit.
    """
    now = time.time()
    last = last_group_bot_send.get(gid, 0)
    if now - last < MIN_GROUP_BOT_INTERVAL:
        try:
            slog(f"SPAM_GUARD: skip gid={gid} (only {int(now-last)}s since last bot msg)")
        except:
            pass
        return False

    # Also respect the hourly rate via SmartTimeManager (best effort)
    try:
        if not smart_time_manager.can_send_to_group(gid):
            return False
    except Exception:
        pass
    return True

def record_group_bot_send(gid: int):
    """Record that we just sent a message (reply, starter or funnel) to gid."""
    last_group_bot_send[gid] = time.time()
    try:
        smart_time_manager.record_activity(gid, True)
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════════════════
# ⚠️⚠️⚠️ سوییچ‌های عملیات پرریسک (HIGH-RISK OPERATIONS SWITCHES) ⚠️⚠️⚠️
# ═══════════════════════════════════════════════════════════════════════════════
# 🚨 هشدار: این عملیات‌ها ممکن است منجر به محدودیت یا بن اکانت شوند!
# 🛡️ توصیه: قبل از روشن کردن، مطمئن شوید که اکانت گرم شده و آماده است
# ═══════════════════════════════════════════════════════════════════════════════

# 📨 ارسال پیام خصوصی (PM) به کاربران
# True = فعال | False = غیرفعال
# ⚠️ ریسک: بالا - ممکن است منجر به PeerFlood یا UserBanned شود
ENABLE_PM_SENDING = False  # 🟢 غیرفعال برای جلوگیری از بن

# ➕ اضافه کردن مستقیم اعضا به گروه (Direct Add/Invite)
# True = فعال | True = غیرفعال  
# ⚠️ ریسک: خیلی بالا - ممکن است منجر به FloodWait طولانی یا بن شود
ENABLE_DIRECT_ADD = False  # 🔴 غیرفعال - بسیار خطرناک برای حساب کاربر

# 🔍 جستجو و عضویت در گروه‌های جدید
# True = فعال | False = غیرفعال
# ⚠️ ریسک: متوسط - ممکن است منجر به ChannelsTooMuch شود
ENABLE_GROUP_SEARCH = True  # 🔴 غیرفعال - ریسک بن سریع

# 👥 جمع‌آوری اطلاعات اعضا از گروه‌ها (Scraping)
# True = فعال | False = غیرفعال
# ⚠️ ریسک: پایین تا متوسط
ENABLE_MEMBER_SCRAPING = True  # 🔴 غیرفعال - ریسک تشخیص و بن

# ═══════════════════════════════════════════════════════════════════════════════
# 🧹 سیستم مدیریت گروه‌های کم‌عضو (LOW MEMBER GROUP MANAGEMENT)
# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 هدف: خروج خودکار از گروه‌هایی که تعداد اعضای کمی دارند
# 💡 مزیت: صرفه‌جویی در منابع و تمرکز بر گروه‌های فعال‌تر
# ═══════════════════════════════════════════════════════════════════════════════

# فعال/غیرفعال کردن سیستم خروج از گروه‌های کم‌عضو
ENABLE_LOW_MEMBER_LEAVE = False  # 🟢 True = فعال | False = غیرفعال

# حداقل تعداد اعضا برای ماندن در گروه
# گروه‌هایی که کمتر از این تعداد عضو دارند، ترک می‌شوند
MIN_GROUP_MEMBERS = 100  # ⚠️ حداقل 500 عضو - گروه‌های کوچک بی‌ارزش هستند

# بررسی تعداد اعضا قبل از عضویت در گروه جدید
CHECK_MEMBERS_BEFORE_JOIN = False  # 🟢 فعال - قبل از عضویت تعداد اعضا چک شود

# فاصله زمانی بین بررسی گروه‌ها (ثانیه)
LOW_MEMBER_CHECK_INTERVAL = 3600  # هر 30 دقیقه بررسی (کاهش از 1 ساعت)

# تاخیر بین هر خروج از گروه (ثانیه) - برای جلوگیری از FloodWait
LEAVE_GROUP_DELAY_MIN = 20  # حداقل 20 ثانیه
LEAVE_GROUP_DELAY_MAX = 45  # حداکثر 45 ثانیه

# حداکثر تعداد خروج در هر سیکل
MAX_LEAVES_PER_CYCLE = 15  # حداکثر 15 خروج در هر سیکل (افزایش)

# ═══════════════════════════════════════════════════════════════════════════════
# 🔒 سیستم خروج از گروه‌های بسته (RESTRICTED GROUP MANAGEMENT)
# ═══════════════════════════════════════════════════════════════════════════════
# 🎯 هدف: خروج از گروه‌هایی که امکان ارسال پیام در آنها وجود ندارد
# 💡 دلایل بسته بودن: محدودیت توسط ادمین، گروه فقط خواندنی، بن شدن
# ═══════════════════════════════════════════════════════════════════════════════

# فعال/غیرفعال کردن خروج از گروه‌های بسته
ENABLE_RESTRICTED_GROUP_LEAVE = False  # 🟢 True = فعال | False = غیرفعال

# بررسی دسترسی ارسال پیام قبل از عضویت
CHECK_WRITE_ACCESS_BEFORE_JOIN = False  # 🟢 فعال - قبل از عضویت بررسی دسترسی ارسال

# فاصله زمانی بین بررسی گروه‌های بسته (ثانیه)
RESTRICTED_CHECK_INTERVAL = 3600  # هر 30 دقیقه بررسی

# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════
# 🔗 سیستم عضویت خودکار از لیست لینک‌ها (Auto Join from Links)
# ═══════════════════════════════════════════════════════════
# فعال/غیرفعال کردن عضویت خودکار از لینک‌ها
ENABLE_AUTO_JOIN_FROM_LINKS = False  # 🔴 غیرفعال - لیست ۳۰۰+ گروهی باعث بن سریع شد

# ═══════════════════════════════════════════════════════════
# 🚨 SAFETY OVERRIDE - برای جلوگیری از بن مجدد (Crisis Mode)
# ═══════════════════════════════════════════════════════════
SAFE_MODE = False         # False = حالت عادی | True = فقط AI گروه، بدون scrape/invite
ACCOUNT_HEALTHY = True    # توسط مانیتور به‌روزرسانی می‌شود

# تنظیمات بهینه‌سازی شده برای کاهش ریسک FloodWait ⚠️
AUTO_JOIN_DELAY_MIN = 180  # 🔒 حداقل 3 دقیقه (افزایش برای جلوگیری از FloodWait)
AUTO_JOIN_DELAY_MAX = 360  # 🔒 حداکثر 6 دقیقه (افزایش)
AUTO_JOIN_BATCH_SIZE = 2  # 🔒 2 عضویت در هر سیکل (کاهش)
AUTO_JOIN_BATCH_REST = 900  # 🔒 استراحت 15 دقیقه بین بچ‌ها (افزایش)
AUTO_JOIN_FLOOD_WAIT_MULTIPLIER = 3.0  # 🔒 ضریب افزایش تاخیر
AUTO_JOIN_MAX_RETRIES = 5  # 5 تلاش مجدد
AUTO_JOIN_RETRY_DELAY = 300  # 5 دقیقه بین تلاش‌ها
AUTO_JOIN_STATE_FILE = "auto_join_state.json"  # فایل ذخیره وضعیت

# 📋 لیست لینک‌های گروه برای عضویت خودکار
# می‌توانید هم لینک عمومی (مثل @groupname یا t.me/groupname) 
# و هم لینک خصوصی (مثل t.me/+ABC123 یا t.me/joinchat/ABC123) اضافه کنید
# ═══════════════════════════════════════════════════════════
# 🎯 اولویت‌بندی: 1. ترید/کریپتو 2. مهاجرت 3. متفرقه
# ═══════════════════════════════════════════════════════════
AUTO_JOIN_LINKS = [
    # فقط گروه رسمی - لیست بزرگ قبلی باعث بن سریع شد
    "https://t.me/PharmaWebGp",
]

# متریک‌های عملکرد (Performance Metrics)
@dataclass
class BotMetrics:
    """متریک‌های عملکرد ربات"""
    total_messages_sent: int = 0
    total_channels_joined: int = 0
    total_errors: int = 0
    total_flood_waits: int = 0
    total_successful_ads: int = 0
    failed_attempts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    start_time: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    
    def update_activity(self):
        self.last_activity = datetime.now()
    
    def get_runtime(self) -> str:
        delta = datetime.now() - self.start_time
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"
    
    def get_success_rate(self) -> float:
        if self.total_messages_sent == 0:
            return 0.0
        return (self.total_successful_ads / self.total_messages_sent) * 100

# مدیریت صف پیام‌ها (Message Queue Manager)
class MessageQueueManager:
    """مدیریت صف پیام‌ها با اولویت‌بندی"""
    
    def __init__(self, max_size: int = MAX_QUEUE_SIZE):
        self.queue = Queue(maxsize=max_size)
        self.processing = False
        self.processed_count = 0
        self.failed_count = 0
    
    async def add_message(self, channel, message, priority: int = 0):
        """اضافه کردن پیام به صف با اولویت"""
        if not self.queue.full():
            await self.queue.put((priority, channel, message, time.time()))
            return True
        return False
    
    async def get_message(self):
        """دریافت پیام از صف"""
        if not self.queue.empty():
            return await self.queue.get()
        return None
    
    def get_queue_size(self) -> int:
        return self.queue.qsize()
    
    def is_full(self) -> bool:
        return self.queue.full()

# مدیریت Retry هوشمند (Smart Retry Manager)
class SmartRetryManager:
    """مدیریت هوشمند تلاش‌های مجدد"""
    
    def __init__(self):
        self.retry_history: Dict[str, List[float]] = defaultdict(list)
        self.backoff_multipliers: Dict[str, float] = defaultdict(lambda: 1.0)
    
    def should_retry(self, identifier: str, max_attempts: int = MAX_RETRY_ATTEMPTS) -> bool:
        """بررسی امکان تلاش مجدد"""
        return len(self.retry_history[identifier]) < max_attempts
    
    def get_retry_delay(self, identifier: str) -> float:
        """محاسبه تاخیر برای تلاش مجدد با Exponential Backoff"""
        attempt_count = len(self.retry_history[identifier])
        base = random.uniform(*RETRY_DELAY)
        multiplier = self.backoff_multipliers[identifier]
        delay = base * (2 ** attempt_count) * multiplier
        return min(delay, 300)  # حداکثر 5 دقیقه
    
    def record_retry(self, identifier: str):
        """ثبت تلاش مجدد"""
        self.retry_history[identifier].append(time.time())
    
    def increase_backoff(self, identifier: str):
        """افزایش ضریب Backoff"""
        self.backoff_multipliers[identifier] *= FLOOD_WAIT_MULTIPLIER
    
    def reset(self, identifier: str):
        """ریست کردن تاریخچه"""
        if identifier in self.retry_history:
            del self.retry_history[identifier]
        if identifier in self.backoff_multipliers:
            del self.backoff_multipliers[identifier]

# ═══════════════════════════════════════════════════════════════════════════
# 🛡️ کنترل‌کننده تأخیر قابل اطمینان Broadcast (ادغام کامل از examplebot.py)
# قلب ایمنی ارسال‌های گروهی: تاخیرهای طولانی متغیر، تطبیقی، per-group cooldown، batch rest، واکنش به Flood
# ═══════════════════════════════════════════════════════════════════════════

class ReliableBroadcastController:
    """
    کنترل مرکزی و قابل اعتماد تأخیرها برای ارسال پیام تبلیغاتی.
    ویژگی‌های کلیدی: تصمیم یکجا، الگوی انسانی، واکنش به Flood، ضریب تطبیقی، لاگ.
    """

    def __init__(self):
        self.send_times = deque(maxlen=200)
        self.per_group_last_send = {}
        self.per_group_stats = defaultdict(lambda: {'sent': 0, 'success': 0, 'last_error': None})
        self.hourly_count = 0
        self.daily_count = 0
        self.last_hour = datetime.now().hour
        self.last_day = datetime.now().date()
        self.batch_sent = 0
        self.adaptive_multiplier = 1.0
        self.recent_floods = 0
        self.last_any_send = 0
        self.total_success = 0
        self.total_attempts = 0

    def _reset_counters_if_needed(self):
        now = datetime.now()
        if now.date() != self.last_day:
            self.daily_count = 0
            self.hourly_count = 0
            self.last_day = now.date()
            self.last_hour = now.hour
            self.send_times.clear()
            return
        if now.hour != self.last_hour:
            self.hourly_count = 0
            self.last_hour = now.hour

    def can_send_now(self, group_id: int) -> tuple[bool, str]:
        self._reset_counters_if_needed()
        if self.hourly_count >= BROADCAST_MAX_PER_HOUR:
            return False, "hourly_limit"
        if self.daily_count >= BROADCAST_MAX_PER_DAY:
            return False, "daily_limit"
        if self.last_any_send > 0:
            elapsed = time.time() - self.last_any_send
            min_interval = BROADCAST_MIN_GLOBAL_INTERVAL * self.adaptive_multiplier
            if elapsed < min_interval:
                return False, f"global_cooldown ({int(min_interval - elapsed)}s left)"
        if group_id in self.per_group_last_send:
            elapsed = time.time() - self.per_group_last_send[group_id]
            min_cd = BROADCAST_PER_GROUP_COOLDOWN_MIN * self.adaptive_multiplier
            max_cd = BROADCAST_PER_GROUP_COOLDOWN_MAX * self.adaptive_multiplier
            cooldown = random.randint(int(min_cd), int(max_cd))
            if elapsed < cooldown:
                return False, f"group_cooldown ({int(cooldown - elapsed)}s left)"
        stats = self.per_group_stats.get(group_id, {})
        if stats.get('sent', 0) >= 3 and stats.get('success', 0) / max(stats.get('sent', 1), 1) < 0.3:
            return False, "poor_group_performance"
        return True, "ok"

    def get_delay_before_next_send(self) -> int:
        base = random.randint(BROADCAST_POST_SEND_MIN, BROADCAST_POST_SEND_MAX)
        return int(base * self.adaptive_multiplier)

    def should_take_batch_rest(self) -> bool:
        return self.batch_sent >= BROADCAST_BATCH_SIZE

    def get_batch_rest_duration(self) -> int:
        min_rest = int(BROADCAST_BATCH_REST_MIN * self.adaptive_multiplier)
        max_rest = int(BROADCAST_BATCH_REST_MAX * self.adaptive_multiplier)
        return random.randint(min_rest, max_rest)

    def record_send(self, group_id: int, success: bool = True):
        now = time.time()
        self.send_times.append(now)
        self.per_group_last_send[group_id] = now
        self.last_any_send = now
        self.hourly_count += 1
        self.daily_count += 1
        self.batch_sent += 1
        self.total_attempts += 1
        gstats = self.per_group_stats[group_id]
        gstats['sent'] = gstats.get('sent', 0) + 1
        if success:
            gstats['success'] = gstats.get('success', 0) + 1
            self.total_success += 1
            if self.adaptive_multiplier > 1.0:
                self.adaptive_multiplier = max(1.0, self.adaptive_multiplier * 0.92)
        else:
            self.adaptive_multiplier = min(3.5, self.adaptive_multiplier * 1.15)
        try:
            if success:
                bot_metrics.total_successful_ads += 1
        except:
            pass
        success_rate = (self.total_success / max(self.total_attempts, 1)) * 100
        slog(f"✅ [BROADCAST] ارسال | g_success={gstats.get('success',0)}/{gstats['sent']} | overall={success_rate:.1f}% | {self.get_status()}")

    def on_flood_wait(self, seconds: int):
        self.recent_floods += 1
        self.adaptive_multiplier = min(4.0, self.adaptive_multiplier * 1.6 + (seconds / 120))
        self.batch_sent = 0
        slog(f"⚠️ [BROADCAST] FloodWait ({seconds}s) → multiplier={self.adaptive_multiplier:.2f}")

    def on_error(self, error_type: str, group_id: int = None):
        if "FLOOD" in error_type or "PEER_FLOOD" in error_type or "BANNED" in error_type:
            self.adaptive_multiplier = min(5.0, self.adaptive_multiplier * 1.8)
            self.batch_sent = 0
            slog(f"🚫 [BROADCAST] serious error ({error_type}) → slow down")
        if group_id:
            self.per_group_stats[group_id]['last_error'] = error_type
            self.record_send(group_id, success=False)

    def record_failure(self, group_id: int, reason: str = ""):
        self.per_group_stats[group_id]['last_error'] = reason
        self.record_send(group_id, success=False)

    def reset_batch_if_rest_taken(self):
        self.batch_sent = 0

    def get_status(self) -> str:
        return (f"hourly={self.hourly_count}/{BROADCAST_MAX_PER_HOUR} | "
                f"daily={self.daily_count}/{BROADCAST_MAX_PER_DAY} | "
                f"multiplier={self.adaptive_multiplier:.2f} | "
                f"batch={self.batch_sent}/{BROADCAST_BATCH_SIZE}")

# Global instance (used everywhere)
broadcast_controller = ReliableBroadcastController()


# ═══════════════════════════════════════════════════════════════════════════
# 🧠 IntelligentGroupEngager — Phase 2 Core (Continuation Development)
# Owns intelligent random group message selection + value-first replies
# + multi-turn relationship building + natural PM funnel.
# Heavily inspired by examplebot Viral/Engagement + web3test reasoning patterns.
# ═══════════════════════════════════════════════════════════════════════════

class IntelligentGroupEngager:
    """
    Central intelligence for group engagement.
    Responsibilities:
    - Decide which recent messages are worth replying to (high PM potential + natural).
    - Generate high-value, contextual, human-like replies using the full Qwen3 pipeline.
    - Track per-user conversation state inside groups.
    - Insert soft, natural PM invitations only after providing real value.
    - Use ViralMarketingEngine / engagement techniques for better replies.
    """

    def __init__(self):
        self.user_group_state: Dict[str, dict] = {}  # key = f"{gid}:{uid}"
        self.last_engagement: Dict[int, float] = {}

    def _user_key(self, gid: int, uid: int) -> str:
        return f"{gid}:{uid}"

    def record_engagement(self, gid: int, uid: int, user_msg: str, bot_reply: str):
        key = self._user_key(gid, uid)
        if key not in self.user_group_state:
            self.user_group_state[key] = {"turns": [], "last_funnel": 0}
        self.user_group_state[key]["turns"].append({"u": user_msg[:120], "b": bot_reply[:120]})
        if len(self.user_group_state[key]["turns"]) > 8:
            self.user_group_state[key]["turns"] = self.user_group_state[key]["turns"][-8:]

    def should_consider_funnel(self, gid: int, uid: int) -> bool:
        key = self._user_key(gid, uid)
        state = self.user_group_state.get(key, {})
        turns = len(state.get("turns", []))
        # After 2-4 good exchanges, consider funnel
        return turns >= 2 and (time.time() - state.get("last_funnel", 0)) > 3600

    def mark_funnel_sent(self, gid: int, uid: int):
        key = self._user_key(gid, uid)
        if key not in self.user_group_state:
            self.user_group_state[key] = {}
        self.user_group_state[key]["last_funnel"] = time.time()

    def _score_target_message(self, m) -> float:
        """Shared score: keyword presence + optional strategist boost."""
        from ai.human_style import score_group_message
        sc = score_group_message(getattr(m, 'text', None) or "")
        try:
            if USE_AI_CORE and _strategist:
                dec = _strategist((m.text or ""))
                sc += min(dec.get('score', 0) * 0.35, 2.5)
        except Exception:
            pass
        return sc

    async def select_best_message_to_reply(self, gid: int, recent_msgs: list) -> Optional[object]:
        """Pick a reply target like a person: often the best, sometimes a random top candidate."""
        from ai.human_style import pick_scored_target
        me = await client.get_me()
        my_id = me.id if me else 0

        candidates = []
        for m in recent_msgs:
            if not m.text or len(m.text.strip()) < 6:
                continue
            if m.sender_id == my_id:
                continue
            sender = getattr(m, 'sender', None)
            if sender and getattr(sender, 'bot', False):
                continue
            candidates.append(m)

        return pick_scored_target(
            candidates, self._score_target_message, min_score=1.2, top_n=5, randomize=0.45
        )

    def _get_personality_mood(self, gid: int) -> str:
        """Rotate subtle human personality/mood for anti-detection and natural variation."""
        moods = [
            "casual friend who shares real experience",
            "curious peer asking clarifying details",
            "helpful but concise practical person",
            "slightly skeptical but open to ideas",
            "experienced user who has tried a few things"
        ]
        # stable per group but varies over time
        base = gid % len(moods)
        idx = (base + int(time.time() // 1800)) % len(moods)
        return moods[idx]

    async def generate_valuable_reply(self, gid: int, target_msg, recent_ctx: str, use_llm: bool = True) -> Optional[str]:
        """One Qwen call (or local fallback). No extra critique pass — that starved live replies."""
        target_text = (target_msg.text or "").strip()
        key = self._user_key(gid, getattr(target_msg, 'sender_id', 0) or 0)
        turns = len(self.user_group_state.get(key, {}).get("turns", []))

        # Ultra-rich context
        ctx_list = []
        if recent_ctx:
            ctx_list.append(recent_ctx)

        # Full per-user thread history
        hist = self.user_group_state.get(key, {}).get("turns", [])
        if hist:
            history_str = "\n".join([f"User: {h['u']}\nBot: {h['b']}" for h in hist[-4:]])
            ctx_list.append(f"Recent private conversation history with THIS user (use for continuity, reference prior points):\n{history_str}")

        # Group personality notes + global memory
        try:
            notes = get_group_notes(gid)
            if notes:
                ctx_list.append(f"Group personality notes:\n{notes}")
            uctx = get_user_context(gid, getattr(target_msg, 'sender_id', 0) or 0)
            if uctx:
                ctx_list.append(uctx)
        except:
            pass

        # Personality / mood injection (human variation)
        mood = self._get_personality_mood(gid)
        ctx_list.append(f"Speak as a real human in this mood: {mood}. Natural Persian chat. No lists. Short paragraphs. Real experience tone.")

        # Light strategy only — do not inject viral/sales copy (that is a strong AI tell)
        strategy_hint = ""
        try:
            if turns >= 2:
                strategy_hint += " If it fits, one soft hint that details are easier in private. "
            if '?' in target_text or '؟' in target_text:
                strategy_hint += " Answer the question first. One follow-up max. "
            strategy_hint += f" Match mood: {mood}. "
        except Exception:
            pass

        if strategy_hint:
            ctx_list = [strategy_hint] + ctx_list

        # === STAGE 1: Primary generation ===
        resp = await call_qwen3_natural(
            ctx_list, target_text, chat_id=gid, high_value=True, use_think=False,
            user_id=getattr(target_msg, 'sender_id', 0) or 0,
            skip_llm=not use_llm,
        )

        if not resp or not is_high_quality_natural(resp):
            return None

        # Extra safety pass: if still contains defensive AI talk, discard
        if any(bad in (resp or '').lower() for bad in ['رباتم', 'ربات نیستم', 'شبیه ربات', 'فکر کردی ربات', 'هوش مصنوعی هستم']):
            resp = None

        # Final strict gate + sanitizer
        resp = _clean_natural(resp)
        resp = _repair_group_output(resp)
        resp = _sanitize_group_output(resp)
        if not is_high_quality_natural(resp):
            return None
        # Anti-repetition: never send near-duplicate to same group recently
        if _is_repetitive_or_similar(gid, resp):
            return None
        return resp

    async def process_incoming(self, gid: int, msg, recent_ctx: str) -> Optional[str]:
        """Reply to an already-selected incoming message. Do not re-score it away."""
        if not msg or not can_send_to_group_safely(gid):
            return None
        try:
            reply = await self.generate_valuable_reply(gid, msg, recent_ctx)
            if reply and is_high_quality_natural(reply):
                self.record_engagement(gid, getattr(msg, 'sender_id', 0) or 0, (msg.text or '')[:120], reply[:120])
                return reply
        except Exception:
            pass
        return None

    async def generate_starter(self, gid: int, recent_ctx: str = "") -> Optional[str]:
        """Context-aware curated starter. No Qwen call — keeps the model free for replies."""
        try:
            if not can_send_to_group_safely(gid):
                return None
            from ai.human_style import pick_context_starter
            choice = pick_context_starter(recent_ctx or "")
            if choice and (is_high_quality_natural(choice) or len(choice) > 20):
                return choice
            pool = CONVERSATION_STARTERS if 'CONVERSATION_STARTERS' in globals() else []
            if pool:
                return random.choice(pool)
        except Exception:
            pass
        return None

    async def maybe_funnel(self, gid: int, uid: int, recent_ctx: str) -> Optional[str]:
        """Soft PM invite after rapport — curated lines, no extra Qwen call."""
        if not self.should_consider_funnel(gid, uid):
            return None
        if not can_send_to_group_safely(gid):
            return None
        try:
            from ai.human_style import funnel_lines
            lines = funnel_lines()
        except Exception:
            lines = [
                "اگه خواستی جزئیاتش رو پی‌وی بگو راحت‌تر حرف میزنیم.",
                "اینجا شلوغه، پی‌وی پیام بده ادامه بدیم.",
                "جزئیاتش بهتره خصوصی حرف بزنیم، پیام بده.",
            ]
        self.mark_funnel_sent(gid, uid)
        return random.choice(lines)

# Global engager instance
group_engager = IntelligentGroupEngager()

# Back-compat shims (used by older paths)
broadcast_send_times = broadcast_controller.send_times
broadcasts_this_hour = 0
broadcasts_today = 0
broadcast_sends_in_current_batch = 0

def _reset_broadcast_daily_counters_if_needed():
    broadcast_controller._reset_counters_if_needed()

def can_send_broadcast_now() -> bool:
    return broadcast_controller.hourly_count < BROADCAST_MAX_PER_HOUR and \
           broadcast_controller.daily_count < BROADCAST_MAX_PER_DAY

async def safe_broadcast_delay(after_success: bool = True):
    if after_success:
        delay = broadcast_controller.get_delay_before_next_send()
        await asyncio.sleep(delay)
    else:
        await asyncio.sleep(random.randint(30, 90))

async def enforce_batch_rest_if_needed():
    if broadcast_controller.should_take_batch_rest():
        rest = broadcast_controller.get_batch_rest_duration()
        slog(f"⏸️ [BROADCAST] batch full. rest {rest//60} min")
        await asyncio.sleep(rest)
        broadcast_controller.reset_batch_if_rest_taken()

# مدیریت حافظه (Memory Manager)
class MemoryManager:
    """مدیریت بهینه حافظه"""
    
    def __init__(self, max_history: int = MAX_HISTORY_SIZE):
        self.message_history: deque = deque(maxlen=max_history)
        self.channel_cache: Dict[str, dict] = {}
        self.user_cache: Dict[int, dict] = {}
        self.last_cleanup = time.time()
    
    def add_to_history(self, item: dict):
        """اضافه کردن به تاریخچه"""
        self.message_history.append({
            **item,
            'timestamp': time.time()
        })
    
    def should_cleanup(self) -> bool:
        """بررسی نیاز به پاکسازی"""
        return time.time() - self.last_cleanup > MEMORY_CLEANUP_INTERVAL
    
    def cleanup(self):
        """پاکسازی حافظه"""
        current_time = time.time()
        
        # پاکسازی کش کانال‌ها (حذف کانال‌های قدیمی‌تر از 30 دقیقه برای Railway)
        cache_timeout = 1800 if RAILWAY_MODE == 'eco' else 3600
        old_channels = [
            k for k, v in self.channel_cache.items()
            if current_time - v.get('timestamp', 0) > cache_timeout
        ]
        for ch in old_channels:
            del self.channel_cache[ch]
        
        # پاکسازی کش کاربران (حذف کاربران قدیمی‌تر از 15 دقیقه برای Railway)
        user_cache_timeout = 900 if RAILWAY_MODE == 'eco' else 1800
        old_users = [
            k for k, v in self.user_cache.items()
            if current_time - v.get('timestamp', 0) > user_cache_timeout
        ]
        for user in old_users:
            del self.user_cache[user]
        
        # 🧹 محدود کردن اندازه کش‌ها
        if len(self.channel_cache) > MAX_MEMORY_ITEMS:
            # حذف نیمی از کش
            items_to_remove = list(self.channel_cache.keys())[:len(self.channel_cache)//2]
            for k in items_to_remove:
                del self.channel_cache[k]
        
        if len(self.user_cache) > MAX_MEMORY_ITEMS:
            items_to_remove = list(self.user_cache.keys())[:len(self.user_cache)//2]
            for k in items_to_remove:
                del self.user_cache[k]
        
        self.last_cleanup = current_time
        
        # 🧹 اجرای garbage collection
        gc.collect()
    
    def get_cache_sizes(self) -> dict:
        """دریافت اندازه کش‌ها"""
        return {
            'message_history': len(self.message_history),
            'channel_cache': len(self.channel_cache),
            'user_cache': len(self.user_cache)
        }


# ═══════════════════════════════════════════════════════════
# 🚂 کلاس مدیریت منابع Railway
# ═══════════════════════════════════════════════════════════
class RailwayResourceManager:
    """
    مدیریت منابع سرور برای Railway
    - پاکسازی خودکار حافظه
    - محدودیت مصرف CPU
    - مدیریت هوشمند تسک‌ها
    """
    
    def __init__(self):
        self.last_gc = time.time()
        self.last_memory_check = time.time()
        self.task_count = 0
        self.memory_warnings = 0
        self.gc_runs = 0
    
    def should_run_gc(self) -> bool:
        """آیا باید GC اجرا شود؟"""
        return time.time() - self.last_gc > GC_INTERVAL
    
    def run_gc(self):
        """اجرای garbage collection"""
        collected = gc.collect()
        self.last_gc = time.time()
        self.gc_runs += 1
        return collected
    
    def check_memory_usage(self) -> dict:
        """بررسی مصرف حافظه (تقریبی)"""
        try:
            import sys
            # محاسبه تقریبی حافظه مصرفی
            total_objects = len(gc.get_objects())
            
            return {
                'total_objects': total_objects,
                'gc_runs': self.gc_runs,
                'warnings': self.memory_warnings,
                'status': 'ok' if total_objects < 100000 else 'warning'
            }
        except:
            return {'status': 'unknown'}
    
    def limit_dict_size(self, d: dict, max_size: int = MAX_MEMORY_ITEMS):
        """محدود کردن اندازه دیکشنری"""
        if len(d) > max_size:
            # حذف نیمی از آیتم‌ها (قدیمی‌ترین‌ها)
            items_to_remove = list(d.keys())[:len(d)//2]
            for key in items_to_remove:
                del d[key]
            return True
        return False
    
    def limit_set_size(self, s: set, max_size: int = MAX_MEMORY_ITEMS):
        """محدود کردن اندازه set"""
        if len(s) > max_size:
            # حذف نیمی از آیتم‌ها
            items_list = list(s)
            for item in items_list[:len(items_list)//2]:
                s.discard(item)
            return True
        return False


# ایجاد نمونه global برای Railway Resource Manager
railway_manager = RailwayResourceManager()


# ═══════════════════════════════════════════════════════════
# 🎯 کلاس‌های بهینه‌سازی پیشرفته
# ═══════════════════════════════════════════════════════════

class SmartTimeManager:
    """مدیریت هوشمند زمان و فعالیت بر اساس ساعات پیک"""
    
    def __init__(self):
        self.activity_log = defaultdict(list)
        self.last_activity_time = {}
    
    def get_current_activity_multiplier(self) -> float:
        """دریافت ضریب فعالیت بر اساس ساعت"""
        current_hour = datetime.now().hour
        
        if current_hour in SLEEP_HOURS:
            return 0.1
        elif current_hour in PEAK_HOURS:
            return PEAK_ACTIVITY_MULTIPLIER
        elif current_hour in OFF_PEAK_HOURS:
            return OFF_PEAK_ACTIVITY_MULTIPLIER
        return 1.0
    
    def is_sleep_time(self) -> bool:
        """آیا الان ساعت استراحت است؟"""
        return datetime.now().hour in SLEEP_HOURS
    
    def calculate_optimal_delay(self, base_delay: tuple) -> int:
        """محاسبه تاخیر بهینه بر اساس ساعت"""
        multiplier = self.get_current_activity_multiplier()
        min_delay, max_delay = base_delay
        
        adjusted_min = int(min_delay / multiplier) if multiplier > 1 else int(min_delay * (2 - multiplier))
        adjusted_max = int(max_delay / multiplier) if multiplier > 1 else int(max_delay * (2 - multiplier))
        
        return random.randint(adjusted_min, adjusted_max)
    
    def can_send_to_group(self, group_id: int) -> bool:
        """بررسی امکان ارسال به گروه"""
        if group_id not in self.last_activity_time:
            return True
        
        time_since_last = time.time() - self.last_activity_time[group_id]
        min_interval = 3600 / MAX_MESSAGES_PER_GROUP_HOURLY
        
        return time_since_last >= min_interval
    
    def record_activity(self, group_id: int, success: bool):
        """ثبت فعالیت"""
        self.last_activity_time[group_id] = time.time()
        current_hour = datetime.now().hour
        self.activity_log[current_hour].append(1 if success else 0)


class MessageRotationManager:
    """مدیریت چرخش هوشمند پیام‌ها"""
    
    def __init__(self):
        self.message_usage = {}
        self.group_message_history = defaultdict(list)
    
    def get_message_hash(self, message: str) -> str:
        """ایجاد hash برای پیام"""
        import hashlib
        return hashlib.md5(message.encode()).hexdigest()[:8]
    
    def can_use_message(self, message: str, group_id: int = None) -> bool:
        """بررسی امکان استفاده از پیام"""
        if not MESSAGE_ROTATION_ENABLED:
            return True
        
        msg_hash = self.get_message_hash(message)
        
        if msg_hash in self.message_usage:
            usage = self.message_usage[msg_hash]
            
            if usage['count'] >= MAX_SAME_MESSAGE_PER_DAY:
                if time.time() - usage['last_used'] > 86400:
                    usage['count'] = 0
                else:
                    return False
            
            if time.time() - usage['last_used'] < MIN_MESSAGE_REUSE_DELAY:
                return False
            
            if group_id and group_id in usage.get('groups', set()):
                return False
        
        return True
    
    def record_message_usage(self, message: str, group_id: int = None):
        """ثبت استفاده از پیام"""
        msg_hash = self.get_message_hash(message)
        
        if msg_hash not in self.message_usage:
            self.message_usage[msg_hash] = {
                'count': 0,
                'last_used': 0,
                'groups': set()
            }
        
        self.message_usage[msg_hash]['count'] += 1
        self.message_usage[msg_hash]['last_used'] = time.time()
        
        if group_id:
            self.message_usage[msg_hash]['groups'].add(group_id)
            self.group_message_history[group_id].append(msg_hash)
    
    def get_fresh_message(self, messages_list: list, group_id: int = None) -> str:
        """انتخاب پیام تازه"""
        available = [msg for msg in messages_list if self.can_use_message(msg, group_id)]
        
        if not available:
            available = messages_list
        
        return random.choice(available)


class GroupQualityScorer:
    """امتیازدهی کیفیت گروه‌ها"""
    
    def __init__(self):
        self.group_scores = {}
        self.group_stats = defaultdict(lambda: {
            'messages_sent': 0,
            'messages_success': 0,
            'last_activity': 0,
            'member_count': 0
        })
    
    def calculate_quality_score(self, group_id: int, member_count: int = 0) -> float:
        """محاسبه امتیاز کیفیت گروه"""
        stats = self.group_stats[group_id]
        
        member_score = min(member_count / 1000, 1.0) * 10
        
        if stats['messages_sent'] > 0:
            success_rate = stats['messages_success'] / stats['messages_sent']
            success_score = success_rate * 10
        else:
            success_score = 5
        
        if stats['last_activity'] > 0:
            hours_since = (time.time() - stats['last_activity']) / 3600
            activity_score = max(10 - (hours_since / 24), 0)
        else:
            activity_score = 5
        
        total_score = (
            member_score * QUALITY_FACTORS['member_count'] +
            success_score * QUALITY_FACTORS['response_rate'] +
            activity_score * QUALITY_FACTORS['activity_level']
        )
        
        self.group_scores[group_id] = {
            'score': total_score,
            'factors': {
                'members': member_score,
                'success': success_score,
                'activity': activity_score
            }
        }
        
        return total_score
    
    def is_quality_group(self, group_id: int) -> bool:
        """آیا گروه کیفیت خوبی دارد؟"""
        if group_id not in self.group_scores:
            return True
        
        return self.group_scores[group_id]['score'] >= MIN_GROUP_QUALITY_SCORE
    
    def update_stats(self, group_id: int, success: bool, member_count: int = 0):
        """به‌روزرسانی آمار گروه"""
        stats = self.group_stats[group_id]
        stats['messages_sent'] += 1
        if success:
            stats['messages_success'] += 1
        stats['last_activity'] = time.time()
        if member_count > 0:
            stats['member_count'] = member_count
        
        self.calculate_quality_score(group_id, member_count)


class DynamicRateAdjuster:
    """تنظیم پویای سرعت بر اساس عملکرد"""
    
    def __init__(self):
        self.recent_results = deque(maxlen=100)
        self.current_rate_multiplier = 1.0
        self.last_adjustment = time.time()
    
    def record_result(self, success: bool):
        """ثبت نتیجه"""
        self.recent_results.append(1 if success else 0)
    
    def get_success_rate(self) -> float:
        """محاسبه نرخ موفقیت"""
        if not self.recent_results:
            return 0.5
        return sum(self.recent_results) / len(self.recent_results)
    
    def should_adjust(self) -> bool:
        """آیا باید سرعت را تنظیم کرد؟"""
        return time.time() - self.last_adjustment >= RATE_ADJUSTMENT_INTERVAL
    
    def adjust_rate(self):
        """تنظیم سرعت"""
        if not DYNAMIC_RATE_ENABLED or not self.should_adjust():
            return
        
        success_rate = self.get_success_rate()
        
        if success_rate >= SUCCESS_RATE_THRESHOLD:
            self.current_rate_multiplier = min(self.current_rate_multiplier * 1.1, 1.5)
        elif success_rate <= FAILURE_RATE_THRESHOLD:
            self.current_rate_multiplier = max(self.current_rate_multiplier * 0.8, 0.5)
        else:
            if self.current_rate_multiplier > 1.0:
                self.current_rate_multiplier *= 0.95
            elif self.current_rate_multiplier < 1.0:
                self.current_rate_multiplier *= 1.05
        
        self.last_adjustment = time.time()
    
    def get_adjusted_delay(self, base_delay: tuple) -> int:
        """دریافت تاخیر تنظیم شده"""
        min_delay, max_delay = base_delay
        adjusted_min = int(min_delay / self.current_rate_multiplier)
        adjusted_max = int(max_delay / self.current_rate_multiplier)
        return random.randint(adjusted_min, adjusted_max)


# ═══════════════════════════════════════════════════════════
# 🔗 کلاس مدیریت عضویت خودکار از لینک‌ها (Auto Join Manager)
# ═══════════════════════════════════════════════════════════
class AutoJoinManager:
    """
    سیستم هوشمند عضویت خودکار در گروه‌ها از طریق لیست لینک‌ها
    
    قابلیت‌ها:
    - پشتیبانی از لینک‌های عمومی و خصوصی
    - ردیابی لینک‌های موفق برای جلوگیری از تکرار
    - مدیریت هوشمند تاخیر و FloodWait
    - تلاش مجدد خودکار برای لینک‌های ناموفق
    - ذخیره و بازیابی وضعیت
    """
    
    def __init__(self):
        self.state_file = AUTO_JOIN_STATE_FILE
        self.joined_links: Set[str] = set()  # لینک‌هایی که با موفقیت عضو شدیم
        self.failed_links: Dict[str, dict] = {}  # لینک‌های ناموفق با جزئیات
        self.pending_links: List[str] = []  # لینک‌های در انتظار
        self.stats = {
            'total_joined': 0,
            'total_failed': 0,
            'total_already_member': 0,
            'total_retries': 0,
            'last_join_time': 0,
            'flood_waits': 0,
            'current_delay_multiplier': 1.0
        }
        self.load_state()
    
    def load_state(self):
        """بارگذاری وضعیت از فایل"""
        try:
            if Path(self.state_file).exists():
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.joined_links = set(data.get('joined_links', []))
                    self.failed_links = data.get('failed_links', {})
                    self.stats.update(data.get('stats', {}))
                    logger.info(f"🔗 وضعیت Auto-Join بارگذاری شد: {len(self.joined_links)} عضو شده")
        except Exception as e:
            logger.error(f"❌ خطا در بارگذاری وضعیت Auto-Join: {e}")
    
    def save_state(self):
        """ذخیره وضعیت در فایل"""
        try:
            data = {
                'joined_links': list(self.joined_links),
                'failed_links': self.failed_links,
                'stats': self.stats,
                'last_save': datetime.now().isoformat()
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ خطا در ذخیره وضعیت Auto-Join: {e}")
    
    def normalize_link(self, link: str) -> str:
        """
        نرمال‌سازی لینک برای مقایسه یکسان
        تبدیل همه فرمت‌ها به یک فرمت استاندارد
        """
        if not link:
            return ''
        
        # حذف فاصله‌های اضافی اول و آخر (مهم!)
        link = link.strip()
        
        # حذف فاصله‌های داخلی هم
        link = link.replace(' ', '')
        
        # حذف پروتکل
        link = re.sub(r'^https?://', '', link, flags=re.IGNORECASE)
        
        # حذف www اگر وجود داشت
        link = re.sub(r'^www\.', '', link, flags=re.IGNORECASE)
        
        # حذف t.me/ از ابتدا
        link = re.sub(r'^t\.me/', '', link, flags=re.IGNORECASE)
        
        # حذف telegram.me/ از ابتدا
        link = re.sub(r'^telegram\.me/', '', link, flags=re.IGNORECASE)
        
        # حذف @ از ابتدا
        link = link.lstrip('@')
        
        # تبدیل joinchat/ به +
        link = re.sub(r'^joinchat/', '+', link, flags=re.IGNORECASE)
        
        return link.lower()
    
    def extract_invite_hash(self, link: str) -> tuple:
        """
        استخراج نوع لینک و hash/username
        
        Returns:
            tuple: (link_type, identifier)
            - link_type: 'private' یا 'public'
            - identifier: hash برای خصوصی، username برای عمومی
        """
        if not link:
            return (None, None)
        
        normalized = self.normalize_link(link)
        
        if not normalized:
            return (None, None)
        
        # لینک خصوصی با + یا joinchat
        if normalized.startswith('+'):
            # استخراج hash بعد از +
            hash_part = normalized[1:]  # حذف +
            # حذف پارامترهای اضافی
            hash_part = hash_part.split('?')[0].split('/')[0]
            if hash_part and len(hash_part) >= 3:
                return ('private', hash_part)
        
        # لینک joinchat در متن اصلی
        joinchat_match = re.search(r'joinchat/([A-Za-z0-9_-]+)', link, flags=re.IGNORECASE)
        if joinchat_match:
            return ('private', joinchat_match.group(1))
        
        # لینک عمومی
        # حذف پارامترهای اضافی
        username = normalized.split('?')[0].split('/')[0]
        if username and len(username) >= 3:
            # بررسی که username معتبر باشد (فقط حروف، اعداد و _)
            if re.match(r'^[a-zA-Z][a-zA-Z0-9_]{2,}$', username):
                return ('public', username)
        
        return (None, None)
    
    def is_already_joined(self, link: str) -> bool:
        """بررسی آیا قبلاً عضو این لینک شدیم"""
        normalized = self.normalize_link(link)
        return normalized in self.joined_links
    
    def mark_as_joined(self, link: str):
        """علامت‌گذاری لینک به عنوان عضو شده"""
        normalized = self.normalize_link(link)
        self.joined_links.add(normalized)
        self.stats['total_joined'] += 1
        self.stats['last_join_time'] = time.time()
        
        # حذف از لیست ناموفق‌ها اگر وجود داشت
        if normalized in self.failed_links:
            del self.failed_links[normalized]
        
        self.save_state()
    
    def mark_as_already_member(self, link: str):
        """علامت‌گذاری لینک - قبلاً عضو بودیم"""
        normalized = self.normalize_link(link)
        self.joined_links.add(normalized)
        self.stats['total_already_member'] += 1
        self.save_state()
    
    def mark_as_failed(self, link: str, reason: str, permanent: bool = False):
        """علامت‌گذاری لینک به عنوان ناموفق"""
        normalized = self.normalize_link(link)
        
        if normalized not in self.failed_links:
            self.failed_links[normalized] = {
                'original_link': link,
                'reason': reason,
                'attempts': 1,
                'first_fail': time.time(),
                'last_fail': time.time(),
                'permanent': permanent
            }
        else:
            self.failed_links[normalized]['attempts'] += 1
            self.failed_links[normalized]['last_fail'] = time.time()
            self.failed_links[normalized]['reason'] = reason
            if permanent:
                self.failed_links[normalized]['permanent'] = True
        
        self.stats['total_failed'] += 1
        self.save_state()
    
    def should_retry(self, link: str) -> bool:
        """بررسی آیا باید برای این لینک تلاش مجدد کرد"""
        normalized = self.normalize_link(link)
        
        if normalized in self.joined_links:
            return False
        
        if normalized not in self.failed_links:
            return True
        
        fail_info = self.failed_links[normalized]
        
        # اگر خطای دائمی است
        if fail_info.get('permanent', False):
            return False
        
        # بررسی تعداد تلاش‌ها
        if fail_info['attempts'] >= AUTO_JOIN_MAX_RETRIES:
            return False
        
        # بررسی زمان گذشته از آخرین تلاش
        time_since_last = time.time() - fail_info['last_fail']
        if time_since_last < AUTO_JOIN_RETRY_DELAY:
            return False
        
        return True
    
    def get_retry_count(self, link: str) -> int:
        """دریافت تعداد تلاش‌های قبلی"""
        normalized = self.normalize_link(link)
        if normalized in self.failed_links:
            return self.failed_links[normalized]['attempts']
        return 0
    
    def get_pending_links(self) -> List[str]:
        """دریافت لینک‌های در انتظار عضویت"""
        pending = []
        seen = set()  # جلوگیری از تکرار
        
        for link in AUTO_JOIN_LINKS:
            if not link:
                continue
            
            # پاکسازی لینک
            link = link.strip()
            
            # نادیده گرفتن خطوط خالی و کامنت‌ها
            if not link or link.startswith('#'):
                continue
            
            # نرمال‌سازی برای بررسی تکراری
            normalized = self.normalize_link(link)
            
            if not normalized:
                continue
            
            # بررسی تکراری نبودن
            if normalized in seen:
                continue
            seen.add(normalized)
            
            # بررسی عضویت قبلی
            if self.is_already_joined(link):
                continue
            
            # بررسی امکان retry
            if self.should_retry(link):
                pending.append(link)
        
        return pending
    
    def record_flood_wait(self, seconds: int):
        """ثبت FloodWait و افزایش تاخیر"""
        self.stats['flood_waits'] += 1
        self.stats['current_delay_multiplier'] = min(
            self.stats['current_delay_multiplier'] * AUTO_JOIN_FLOOD_WAIT_MULTIPLIER,
            5.0  # حداکثر 5 برابر
        )
        self.save_state()
    
    def reset_delay_multiplier(self):
        """ریست ضریب تاخیر بعد از موفقیت‌های متوالی"""
        if self.stats['current_delay_multiplier'] > 1.0:
            self.stats['current_delay_multiplier'] = max(
                1.0,
                self.stats['current_delay_multiplier'] * 0.9
            )
    
    def get_optimal_delay(self) -> int:
        """محاسبه تاخیر بهینه با در نظر گرفتن ضریب فعلی"""
        base_delay = random.randint(AUTO_JOIN_DELAY_MIN, AUTO_JOIN_DELAY_MAX)
        adjusted_delay = int(base_delay * self.stats['current_delay_multiplier'])
        
        # در ساعات پیک، کمی محتاط‌تر
        current_hour = datetime.now().hour
        if current_hour in PEAK_HOURS:
            adjusted_delay = int(adjusted_delay * 1.2)
        
        return adjusted_delay
    
    def get_stats_summary(self) -> str:
        """دریافت خلاصه آمار"""
        pending = len(self.get_pending_links())
        return (
            f"📊 آمار Auto-Join:\n"
            f"   ✅ عضو شده: {self.stats['total_joined']}\n"
            f"   ♻️ قبلاً عضو: {self.stats['total_already_member']}\n"
            f"   ❌ ناموفق: {self.stats['total_failed']}\n"
            f"   ⏳ در انتظار: {pending}\n"
            f"   ⚠️ FloodWaits: {self.stats['flood_waits']}\n"
            f"   📈 ضریب تاخیر: {self.stats['current_delay_multiplier']:.1f}x"
        )


# ایجاد نمونه global برای Auto-Join Manager
auto_join_manager = AutoJoinManager()


# ایجاد نمونه‌های global سیستم‌های پیشرفته
smart_time_manager = SmartTimeManager()
message_rotation_manager = MessageRotationManager()
group_quality_scorer = GroupQualityScorer()
dynamic_rate_adjuster = DynamicRateAdjuster()

# ایجاد نمونه‌های global
bot_metrics = BotMetrics()
message_queue = MessageQueueManager()
retry_manager = SmartRetryManager()
memory_manager = MemoryManager()

# Semaphore برای محدود کردن تسک‌های همزمان
concurrent_limiter = Semaphore(MAX_CONCURRENT_TASKS)

# شمارنده‌های روزانه (Daily Counters)
daily_counters = {
    'pm_sent': 0,
    'joins_done': 0,
    'messages_sent': 0,
    'last_reset': date.today()
}

# تابع بررسی و ریست شمارنده‌های روزانه
def check_daily_limits():
    """بررسی محدودیت‌های روزانه"""
    current_date = date.today()
    
    # اگر روز جدیده، ریست کن
    if daily_counters['last_reset'] != current_date:
        daily_counters['pm_sent'] = 0
        daily_counters['joins_done'] = 0
        daily_counters['messages_sent'] = 0
        daily_counters['last_reset'] = current_date
        return True
    
    # بررسی محدودیت‌ها (با مارجین امنیتی)
    if daily_counters['pm_sent'] >= DAILY_PM_LIMIT:
        return False
    if daily_counters['joins_done'] >= DAILY_JOIN_LIMIT:
        return False
    if daily_counters['messages_sent'] >= DAILY_MESSAGE_LIMIT:
        return False
    
    return True


# ═══════════════════════════════════════════════════════════
# 🚂 Railway: Lazy Loading برای لیست داروها
# ═══════════════════════════════════════════════════════════
# در حالت eco فقط از بخشی از لیست استفاده می‌شود
_drug_lists_cache = None
_drug_lists_index = 0

def get_random_drug_list():
    """
    دریافت یک لیست دارویی تصادفی - بهینه برای Railway
    در حالت eco: فقط از 10 لیست اول استفاده می‌کند
    """
    global _drug_lists_index
    
    if RAILWAY_MODE == 'eco':
        # استفاده از لیست‌های محدود برای صرفه‌جویی در RAM
        max_lists = min(10, len(drug_lists))
        _drug_lists_index = (_drug_lists_index + 1) % max_lists
        return drug_lists[_drug_lists_index]
    else:
        return random.choice(drug_lists)


# لیست‌های داروها (فقط نام - بدون لینک)
drug_lists = [
    "fito",

]
# پیام خصوصی (فقط یکبار)
private_message = """🌐 درود بر شما وقتتون بخیر! 🌐

💊 MedPharmaWeb
www.medpharmaweb.shop

🎯 خدمات:
✅ تجربه چند ساله
✅ مشتریان راضی
✅ تضمین اصالت
✅ خدمات حرفه‌ای

🛒 سفارش آسان:
1️⃣ ورود به سایت از طریق مرورگر کروم یا فایرفاکس
2️⃣ انتخاب دارو
3️⃣ ثبت سفارش
4️⃣ تحویل سریع

💬 @PharmaWebAd
🌐 www.medpharmaweb.shop"""




# کلمات حساس برای تشخیص ریپلای منفی
sensitive_words = [
    "فیکه", "الکیه", "دروغه", "اسپمه", "کلاهبردار", "جعلی", "تقلبی", "دروغ", "شیاد",
    "فریب", "تقلب", "دروغگو", "کلاهبرداری", "اسپم", "فریبنده", "جعلیانه",
    "فيك", "كلاهبردار", "دروغين", "اسپام", "فريب",
    "اعتماد نکنید", "قابل اعتماد نیست", "نامعتبر", "غیرقابل اعتماد", "مشکوک", "مشکل داره",
    "ساختگی", "قلابی", "بی اعتبار", "غیر اصل", "تضمینی نیست", "اصل نیست",
    "مواظب باشید", "احتیاط کنید", "هشدار", "خطر", "بپا", "حواستون باشه",
    "fake", "scam", "spam", "fraud", "phony", "bogus", "ripoff", "con", "deceit", "hoax",
    "suspicious", "unreliable", "untrusted", "counterfeit", "dubious", "sham",
    "warning", "caution", "alert", "beware", "danger", "suspect",
    "پول ندید", "کلاه سرتون میره", "سرکاری", "پیگیری میکنم", "گزارش میدم", "لو میدم",
    "سر کاریه", "گول نخورید", "اصل نداره", "تقلبی میفروشه", "کلاه گذاشته"
]

# متن‌های اضافی برای ویرایش بعد از 20 ثانیه
mirror_add_texts = [
    "\n\n💬 @PharmaWebGp\n🌐 medpharmaweb.shop",
    "\n\n💊 @PharmaWebGp \n  🌐 medpharmaweb.shop",
    "\n\n🌐 medpharmaweb.shop\n💬 @PharmaWebGp"
]

# 🧠 کلمات پایه گسترده برای تولید هوشمند ترکیبات
# ⚠️ Railway: در حالت eco این لیست‌ها محدود می‌شوند
MAX_KEYWORDS_PER_CATEGORY = 300 if RAILWAY_MODE == 'eco' else 800  # محدودیت کلمات (افزایش برای پوشش بیشتر)

BASE_KEYWORDS = {
    'main': [
        # کلمات اصلی پزشکی - گروه 1
        "دارو", "پزشکی", "درمان", "سلامت", "بهداشت", "طب", "طبیب",
        "داروخانه", "دکتر", "پزشک", "دارویی", "طبی", "درمانی",
        "medication", "medicine", "medical", "health", "healthcare",
        "treatment", "therapy", "doctor", "physician", "clinic",
        
        # مراکز درمانی - گسترش یافته
        "کلینیک", "بیمارستان", "درمانگاه", "آزمایشگاه", "مطب", "مرکز",
        "پلی کلینیک", "مجتمع", "شفاخانه", "بیمارخانه", "اورژانس",
        "hospital", "center", "laboratory", "lab", "emergency",
        "مرکز تخصصی", "مرکز فوق تخصص", "بیمارستان خصوصی", "بیمارستان دولتی",
        "مرکز جراحی", "مرکز توانبخشی", "مرکز دیالیز", "مرکز شیمی درمانی",
        "مرکز رادیوتراپی", "مرکز پرتودرمانی", "مرکز لیزر", "مرکز زیبایی",
        
        # تخصص‌های پزشکی - بخش 1 (اندام‌ها)
        "قلب", "مغز", "اعصاب", "پوست", "چشم", "گوش", "دندان", "مو",
        "ارتوپد", "زنان", "اطفال", "داخلی", "جراحی", "کلیه", "کبد",
        "ریه", "گوارش", "تنفسی", "عفونی", "خون", "استخوان", "مفصل",
        "heart", "brain", "nerve", "skin", "eye", "ear", "tooth", "dental",
        "cardiology", "neurology", "dermatology", "ophthalmology",
        
        # تخصص‌های پزشکی - بخش 2 (رشته‌ها)
        "روان", "روانشناسی", "روانپزشکی", "اعتیاد", "لاغری", "چاقی",
        "psychology", "psychiatry", "addiction", "obesity", "diet",
        "ارتوپدی", "اورتوپدی", "ارولوژی", "نورولوژی", "نفرولوژی",
        "گاستروانترولوژی", "پولمونولوژی", "اندوکرینولوژی", "هماتولوژی",
        "آنکولوژی", "رادیولوژی", "پاتولوژی", "آناتومی", "فیزیولوژی",
        "ایمونولوژی", "میکروبیولوژی", "بیوشیمی", "ژنتیک", "فارماکولوژی",
        
        # تخصص‌های پزشکی - بخش 3 (جراحی‌ها)
        "جراح", "جراحی قلب", "جراحی مغز", "جراحی اعصاب", "جراحی پلاستیک",
        "جراحی زیبایی", "جراحی ترمیمی", "جراحی عروق", "جراحی کلیه",
        "جراحی لاپاروسکوپی", "جراحی بینی", "جراحی چشم", "جراحی ستون فقرات",
        "surgeon", "surgery", "plastic surgery", "cosmetic surgery",
        "laparoscopic", "minimally invasive", "open surgery",
        
        # خدمات درمانی - گسترش یافته
        "مشاوره", "ویزیت", "تشخیص", "نسخه", "معاینه", "آزمایش", "تست",
        "رادیولوژی", "سونوگرافی", "ام آر آی", "سی تی اسکن", "اکو",
        "آندوسکوپی", "کلونوسکوپی", "ماموگرافی", "اسکن", "رادیوگرافی",
        "consultation", "diagnosis", "examination", "test", "screening",
        "ultrasound", "sonography", "MRI", "CT scan", "X-ray",
        "ECG", "EKG", "اکوکاردیوگرافی", "الکتروکاردیوگرام", "هولتر",
        "تست ورزش", "اسپیرومتری", "آزمایش خون", "آزمایش ادرار", "آزمایش مدفوع",
        
        # خدمات تخصصی - گسترش یافته
        "فیزیوتراپی", "کاردرمانی", "گفتاردرمانی", "ماساژ", "طب سوزنی",
        "لیزر", "بوتاکس", "تزریق", "انفوزیون", "سرم درمانی",
        "physiotherapy", "occupational therapy", "speech therapy",
        "massage", "acupuncture", "laser", "botox", "injection",
        "هیدروتراپی", "آب درمانی", "ورزش درمانی", "حرکت درمانی",
        "شنوایی سنجی", "بینایی سنجی", "توانبخشی", "بازتوانی",
        
        # خدمات زیبایی - گسترش یافته
        "زیبایی", "جوانسازی", "ضد چروک", "آنتی ایجینگ", "لیفت",
        "beauty", "anti-aging", "rejuvenation", "lift", "filler",
        "میکرونیدلینگ", "میکروبلیدینگ", "PRP", "پلاسما", "مزوتراپی",
        "کربوکسی تراپی", "کرایوتراپی", "RF", "رادیوفرکانسی", "هایفو",
        "کویتیشن", "LPG", "وکیوم", "لیزر موهای زائد", "IPL",
        
        # عمومی - گسترش یافته
        "بیمار", "بیماری", "علائم", "نشانه", "دردسر", "مراقبت", "پرستار",
        "بهیار", "امداد", "کمک", "اسعاف", "نجات", "بهبود", "شفا", "درد",
        "patient", "disease", "symptoms", "pain", "care", "nurse",
        "emergency", "first aid", "recovery", "healing", "wellness",
        "سلامتی", "تندرستی", "سرزندگی", "نشاط", "شادابی", "سرحال",
        
        # رشته‌های پیراپزشکی - گسترش یافته
        "پرستاری", "مامایی", "اتاق عمل", "هوشبری", "رادیولوژی", "فوریت",
        "بهداشت", "ایمنی", "تغذیه", "رژیم", "دیابت", "فشار", "قند",
        "nursing", "midwifery", "anesthesia", "nutrition", "diet",
        "فوریت‌های پزشکی", "تکنسین", "تکنولوژیست", "پرتوگر", "بینایی سنج",
        "شنوایی سنج", "گفتاردرمان", "کاردرمان", "فیزیوتراپیست", "ماساژور",
        
        # دارو و داروخانه - گسترش یافته
        "داروخانه شبانه روزی", "صیدلیه", "عطاری", "گیاهان دارویی",
        "pharmacy", "drug store", "herbal", "supplement",
        "داروسازی", "فارماسیوتیکال", "داروشناسی", "داروسازی بالینی",
        "تحویل دارو", "مشاوره دارویی", "تداخل دارویی", "عوارض دارو",
        
        # خدمات در منزل
        "خدمات در منزل", "ویزیت منزل", "پرستار منزل", "آزمایش منزل",
        "home care", "home visit", "home nursing", "home service",
        "فیزیوتراپی منزل", "سرم تراپی منزل", "تزریقات منزل", "پانسمان منزل",
        "نمونه گیری منزل", "سونوگرافی منزل", "نگهداری سالمند", "مراقبت سالمند",
        
        # تجهیزات پزشکی
        "تجهیزات", "دستگاه", "ابزار", "لوازم", "equipment", "device",
        "ویلچر", "واکر", "عصا", "عینک", "سمعک", "فشارسنج", "قندسنج",
        "wheelchair", "walker", "cane", "glasses", "hearing aid",
        "ترمومتر", "تب سنج", "اکسیمتر", "پالس اکسیمتر", "استتوسکوپ",
        "نبولایزر", "اینهالر", "اسپیسر", "CPAP", "BIPAP", "اکسیژن ساز",
        
        # چکاپ و غربالگری
        "چکاپ", "معاینه سالانه", "غربالگری", "پیشگیری", "واکسن", "واکسیناسیون",
        "checkup", "screening", "prevention", "vaccine", "vaccination",
        "چکاپ کامل", "چکاپ عمومی", "چکاپ تخصصی", "پکیج سلامت",
        "غربالگری سرطان", "غربالگری دیابت", "غربالگری قلبی", "تست ژنتیک",
        
        # آموزش و مشاوره
        "آموزش", "دوره", "کلاس", "سمینار", "وبینار", "کارگاه",
        "education", "training", "course", "seminar", "workshop",
        "مشاوره تغذیه", "مشاوره ورزشی", "مشاوره روانشناسی", "مشاوره ژنتیک",
        "مشاوره قبل از ازدواج", "مشاوره باروری", "مشاوره بارداری"
    ],
    
    'prefix': [
        # پیشوندهای فارسی
        "گروه", "کانال", "انجمن", "جامعه", "اتحادیه", "شبکه",
        "باشگاه", "مجمع", "تشکل", "سازمان", "مرکز", "خانه",
        "انجمن علمی", "گروه تخصصی", "کانال رسمی", "مرکز تحقیقاتی",
        "گروه پژوهشی", "انجمن حرفه‌ای", "اتحادیه صنفی", "تشکل حرفه‌ای",
        
        # پیشوندهای انگلیسی
        "group", "channel", "community", "society", "association",
        "club", "organization", "center", "network", "team",
        
        # پیشوندهای توصیفی
        "رسمی", "official", "علمی", "scientific", "تخصصی", "specialized",
        "حرفه‌ای", "professional", "آموزشی", "educational", "تحقیقاتی", "research",
        
        # پیشوندهای جغرافیایی
        "ایرانی", "Iranian", "ایران", "Iran", "تهران", "Tehran",
        "ملی", "national", "بین المللی", "international", "جهانی", "global"
    ],
    
    'location': [
        # کشور
        "ایران", "Iran", "ایرانی", "Iranian", "فارسی", "Persian", "Farsi",
        
        # شهرهای بزرگ - بخش 1
        "تهران", "Tehran", "اصفهان", "Isfahan", "مشهد", "Mashhad",
        "شیراز", "Shiraz", "تبریز", "Tabriz", "کرج", "Karaj",
        "اهواز", "Ahvaz", "قم", "Qom", "کرمان", "Kerman",
        "رشت", "Rasht", "یزد", "Yazd", "کرمانشاه", "Kermanshah",
        
        # شهرهای بزرگ - بخش 2
        "ارومیه", "Urmia", "زاهدان", "Zahedan", "همدان", "Hamedan",
        "کرمانشاه", "سنندج", "Sanandaj", "قزوین", "Qazvin",
        "اراک", "Arak", "بندرعباس", "Bandar Abbas", "زنجان", "Zanjan",
        "بوشهر", "Bushehr", "آبادان", "Abadan", "خرم آباد", "Khorramabad",
        
        # شهرهای میانی
        "کاشان", "Kashan", "ساری", "Sari", "گرگان", "Gorgan",
        "بابل", "Babol", "نیشابور", "Neyshabur", "ساوه", "Saveh",
        "قائم شهر", "مراغه", "سبزوار", "نجف آباد", "آمل",
        
        # مناطق تهران
        "شمال تهران", "North Tehran", "غرب تهران", "West Tehran",
        "شرق تهران", "East Tehran", "جنوب تهران", "South Tehran",
        "مرکز تهران", "Central Tehran", "شمال غرب", "Northwest",
        
        # محله‌های تهران - شمال
        "ونک", "Vanak", "ولنجک", "Velenjak", "نیاوران", "Niavaran",
        "سعادت آباد", "Saadat Abad", "پونک", "Punak", "تجریش", "Tajrish",
        "فرمانیه", "Farmanieh", "الهیه", "Elahieh", "زعفرانیه", "Zaferaniyeh",
        "جردن", "Jordan", "قیطریه", "Qeytarieh", "کامرانیه", "Kamraniyeh",
        
        # محله‌های تهران - غرب
        "ستارخان", "Sattarkhan", "شهرک غرب", "Shahrak-e Gharb",
        "تهرانپارس", "Tehran Pars", "آزادی", "Azadi", "انقلاب", "Enghelab",
        "مرزداران", "Marzdaran", "پاسداران", "Pasdaran", "سهروردی", "Sohrevardi",
        
        # محله‌های تهران - شرق
        "نارمک", "Narmak", "مجیدیه", "Majidieh", "رسالت", "Resalat",
        "تهران نو", "Tehran No", "فرهنگ", "Farhang",
        
        # محله‌های تهران - جنوب
        "نازی آباد", "Nazi Abad", "فردوس", "Ferdows", "شهرری", "Shahr-e Rey",
        
        # مناطق جغرافیایی
        "شمال", "north", "جنوب", "south", "شرق", "east", "غرب", "west",
        "مرکز", "center", "منطقه", "region", "ناحیه", "district",
        "محله", "neighborhood", "محلی", "local", "بومی", "native",
        # +50 extra prefixes
        "دیجیتال", "digital", "آنلاین", "online", "ورکشاپ", "workshop", "رویداد", "event", "تجربه", "experience",
        "VIP", "special", "Pro", "elite", "community hub", "member hub", "hangout", "lounge",
        "Hub", "lab", "studio", "creator studio", "guild", "forum", "local", "regional", "global",
        "international", "academy", "mentorship", "cohort", "study group", "research hub", "innovation center", "innovation hub",
        "testers", "tester group", "alpha", "beta", "open beta", "closed beta", "early access", "early adopters",
        "beta testers", "product testers", "QA group", "testing team", "UX lab", "ui lab", "access group", "advisors",
        "community council", "board", "executive group", "steering committee", "champions", "ambassadors", "representatives"
    ],
    
    'suffix': [
        # پسوندهای کیفیت
        "تخصصی", "عمومی", "خاص", "کمیاب", "ویژه", "منحصر",
        "specialized", "general", "special", "exclusive", "unique",
        "فوق تخصصی", "super specialized", "پیشرفته", "advanced", "حرفه‌ای", "professional",
        
        # پسوندهای دسترسی
        "رایگان", "free", "آنلاین", "online", "آفلاین", "offline",
        "حضوری", "in-person", "غیرحضوری", "remote", "مجازی", "virtual",
        "تلفنی", "phone", "ویدئویی", "video", "همراه", "mobile",
        
        # پسوندهای زمانی
        "اورژانسی", "فوری", "شبانه‌روزی", "24 ساعته", "emergency", "urgent",
        "دائمی", "permanent", "موقت", "temporary", "فصلی", "seasonal",
        "صبح", "morning", "عصر", "evening", "شب", "night",
        
        # پسوندهای خدماتی
        "نوبت دهی", "رزرو", "booking", "appointment", "reservation",
        "مشاوره", "consultation", "پشتیبانی", "support", "خدمات", "services",
        
        # پسوندهای اعتبار
        "اصیل", "معتبر", "مطمئن", "تایید شده", "authentic", "verified",
        "مجاز", "authorized", "رسمی", "official", "قانونی", "legal",
        
        # پسوندهای توصیفی
        "نوین", "modern", "سنتی", "traditional", "جدید", "new",
        "قدیمی", "old", "بروز", "updated", "پیشرو", "leading",
        # +50 suffix additions
        "برای مبتدیان", "for beginners", "حرفه ای ها", "for pros", "تضمینی", "guaranteed",
        "ویژه اعضا", "members only", "VIP access", "دسترسی خاص", "انحصاری", "exclusive",
        "مقدماتی", "intro", "پیشرفته", "advanced", "متوسط", "intermediate",
        "هفتگی", "weekly", "روزی", "daily", "ساعتی", "hourly",
        "دوره ای", "periodic", "ثبت نامی", "registration", "دعوتی", "invitation only",
        "ضمانت بازگشت", "money back", "ضمانت بازگشت وجه", "guarantee", "پیشنهاد ویژه", "special offer"
    ],
    
    'product': [
        # اشکال دارویی - بخش 1
        "قرص", "کپسول", "آمپول", "سرم", "واکسن", "شربت", "قطره",
        "پماد", "کرم", "ژل", "لوسیون", "اسپری", "پودر", "محلول",
        "سوسپانسیون", "انسولین", "ایمونوگلوبولین", "سیتوتوکسیک",
        "tablet", "capsule", "ampule", "serum", "vaccine", "syrup", "drop",
        "ointment", "cream", "gel", "lotion", "spray", "powder", "solution",
        
        # اشکال دارویی - بخش 2
        "قرص خوراکی", "قرص زیر زبانی", "قرص دهانی", "قرص جویدنی",
        "کپسول نرم", "کپسول سخت", "کپسول ژلاتینی", "کپسول تایمد",
        "آمپول عضلانی", "آمپول زیرجلدی", "آمپول وریدی", "آمپول داخل مفصلی",
        "سرم وریدی", "سرم خون", "سرم فیزیولوژی", "سرم درمانی",
        "قطره چشمی", "قطره گوش", "قطره بینی", "قطره خوراکی",
        "پماد چشمی", "پماد پوستی", "پماد مقعدی", "پماد نوار",
        "شیاف", "سوپوزیتوار", "سوپوزیتوار مقعدی", "سوپوزیتوار واژینال",
        
        # داروهای تزریقی
        "تزریق", "injection", "IV", "IM", "SC", "intradermal",
        "تزریق عضلانی", "تزریق زیرجلدی", "تزریق وریدی", "تزریق داخل جلدی",
        "انفوزیون", "infusion", "پمپ انفوزیون", "تزریق آهسته",
        "پچ", "patch", "چسب دارویی", "چسب درمانی", "پچ پوستی",
        
        # ویتامین‌ها - گسترش یافته
        "ویتامین", "vitamin", "ویتامین A", "ویتامین B", "ویتامین C", "ویتامین D",
        "ویتامین E", "ویتامین K", "B12", "B6", "B1", "B2", "B3", "B5",
        "فولیک اسید", "فولات", "بیوتین", "نیاسین", "رایبوفلاوین", "تیامین",
        "ویتامین D3", "کلسیفرول", "ویتامین A رتینول", "بتاکاروتن",
        "آسکوربیک اسید", "توکوفرول", "فیلوکینون", "کبالامین",
        
        # مکمل‌های غذایی - گسترش یافته
        "مکمل", "supplement", "پروتئین", "protein", "کراتین", "creatine",
        "BCAA", "آمینو اسید", "amino acid", "پری ورکات", "pre-workout",
        "گینر", "mass gainer", "وی پروتئین", "whey protein", "کازئین", "casein",
        "گلوتامین", "glutamine", "آرژنین", "arginine", "کارنیتین", "carnitine",
        "تورین", "taurine", "سیترولین", "citrulline", "بتا آلانین", "beta alanine",
        "پست ورکات", "post-workout", "اینترا ورکات", "intra-workout",
        "فت برنر", "fat burner", "لاغری", "چربی سوز", "ترموژنیک",
        
        # مواد معدنی - گسترش یافته
        "کلسیم", "calcium", "آهن", "iron", "زینک", "zinc",
        "منیزیم", "magnesium", "پتاسیم", "potassium", "سدیم", "sodium",
        "ید", "iodine", "سلنیوم", "selenium", "مس", "copper",
        "کروم", "chromium", "مولیبدن", "molybdenum", "منگنز", "manganese",
        "فسفر", "phosphorus", "کبالت", "cobalt", "فلوراید", "fluoride",
        "بر", "boron", "وانادیوم", "vanadium", "استرانسیوم", "strontium",
        "کلسیم D3", "آهن فولیک", "منیزیم B6", "زینک کوپر",
        
        # پروبیوتیک و آنزیم - گسترش یافته
        "پروبیوتیک", "probiotic", "پری بیوتیک", "prebiotic", "سین بیوتیک", "synbiotic",
        "آنزیم", "enzyme", "آنزیم هاضمه", "digestive enzyme",
        "لاکتوباسیلوس", "lactobacillus", "بیفیدوباکتریوم", "bifidobacterium",
        "ساکارومایسس", "saccharomyces", "استرپتوکوکوس", "streptococcus",
        "آنزیم پاپایا", "آنزیم آناناس", "بروملین", "پاپائین",
        "لاکتاز", "آمیلاز", "پروتئاز", "لیپاز", "سلولاز",
        
        # امگا و چربی‌ها - گسترش یافته
        "امگا 3", "omega 3", "امگا 6", "omega 6", "امگا 9", "omega 9",
        "روغن ماهی", "fish oil", "DHA", "EPA", "ALA",
        "کد لیور", "cod liver oil", "روغن کرچک", "روغن نارگیل", "coconut oil",
        "روغن زیتون", "olive oil", "روغن کتان", "flaxseed oil",
        "روغن کنجد", "روغن بادام", "روغن آووکادو", "روغن ماکادمیا",
        "CLA", "لینولئیک اسید", "اولئیک اسید", "استئاریک اسید",
        
        # مکمل‌های گیاهی - گسترش یافته
        "زعفران", "saffron", "زنجبیل", "ginger", "سیر", "garlic",
        "دارچین", "cinnamon", "زردچوبه", "turmeric", "شاتوت", "mulberry",
        "جینسینگ", "ginseng", "اکیناسه", "echinacea", "نعناع", "mint",
        "بابونه", "chamomile", "گل گاوزبان", "borage", "رازیانه", "fennel",
        "آلوئه ورا", "aloe vera", "سیر سیاه", "black garlic", "زیره سیاه",
        "گینکوبیلوبا", "ginkgo biloba", "سنت جان", "st john wort",
        "والرین", "valerian", "ملاتونین", "melatonin", "اشواگاندا", "ashwagandha",
        "رودیولا", "rhodiola", "ماکا", "maca", "تریبولوس", "tribulus",
        "سیلی مارین", "milk thistle", "آرتی چوک", "artichoke",
        
        # مکمل‌های تخصصی - جدید
        "کلاژن", "collagen", "هیالورونیک اسید", "hyaluronic acid",
        "گلوکزامین", "glucosamine", "کندرویتین", "chondroitin", "MSM",
        "کوانزیم Q10", "coenzyme Q10", "CoQ10", "یوبی کینون",
        "رزوراترول", "resveratrol", "کورکومین", "curcumin",
        "استیل ال کارنیتین", "acetyl L-carnitine", "ALC",
        "فسفاتیدیل سرین", "phosphatidylserine", "PS",
        "آلفا لیپوئیک اسید", "alpha lipoic acid", "ALA",
        
        # لوازم پزشکی - گسترش یافته
        "لوازم پزشکی", "medical equipment", "تجهیزات", "دستگاه", "equipment", "device",
        "فشارسنج", "blood pressure monitor", "فشارسنج دیجیتال", "فشارسنج عقربه‌ای",
        "گلوکومتر", "glucometer", "قندسنج", "دستگاه قند خون", "نوار تست قند",
        "ترمومتر", "thermometer", "تب سنج", "تب سنج دیجیتال", "تب سنج پیشانی",
        "نبض سنج", "pulse oximeter", "اکسی متر", "پالس اکسیمتر", "اشباع اکسیژن",
        "استتوسکوپ", "stethoscope", "گوشی پزشکی", "استتوسکوپ قلب",
        "اکسیژن ساز", "oxygen concentrator", "کپسول اکسیژن", "ماسک اکسیژن",
        "نبولایزر", "nebulizer", "ساکشن", "suction", "کمپرسور",
        "اینهالر", "inhaler", "پاف", "اسپیسر", "spacer", "MDI",
        "CPAP", "بای پپ", "BIPAP", "اسلیپ آپنه", "ماسک شبانه",
        "ویلچر", "wheelchair", "صندلی چرخدار", "ویلچر برقی", "ویلچر دستی",
        "واکر", "walker", "واکر ثابت", "واکر چرخدار", "واکر تاشو",
        "عصا", "cane", "عصا چهار پایه", "عصا تاشو", "عصا تلسکوپی",
        
        # لوازم بهداشتی - گسترش یافته
        "ماسک", "mask", "ماسک N95", "ماسک جراحی", "ماسک سه لایه",
        "دستکش", "gloves", "دستکش لاتکس", "دستکش نیتریل", "دستکش وینیل",
        "الکل", "alcohol", "الکل 70", "الکل طبی", "الکل دست",
        "ضدعفونی", "disinfectant", "ضدعفونی کننده", "محلول ضدعفونی",
        "گان", "gown", "روپوش", "پیش بند", "روپوش بیمارستانی",
        "عینک", "goggles", "عینک ایمنی", "شیلد", "face shield",
        "پد", "pad", "پنبه", "cotton", "گاز", "gauze",
        "چسب", "tape", "باند", "bandage", "بانداژ", "bandaging",
        "چسب زخم", "پلاستر", "بلاستر", "لوکوپلاست",
        
        # تجهیزات توانبخشی - جدید
        "تشک مواج", "تشک ضد زخم بستر", "تشک ژله ای", "air mattress",
        "بالشت طبی", "بالشت گردن", "بالشت کمر", "بالشت زانو",
        "کمربند", "belt", "کمربند کمر", "کمربند شکم", "کمربند باردار",
        "زانوبند", "knee brace", "زانوبند ورزشی", "زانوبند طبی",
        "آرنج بند", "elbow brace", "مچ بند", "wrist brace", "مچ بند کارپال",
        "گردن بند", "neck brace", "کالر گردنی", "کالر طبی",
        "شکم بند", "abdominal binder", "شکم بند بارداری", "شکم بند جراحی",
        
        # لوازم آرایشی و بهداشتی - گسترش یافته
        "کرم", "cream", "لوسیون", "lotion", "صابون", "soap",
        "شامپو", "shampoo", "نرم کننده", "conditioner", "ماسک مو", "hair mask",
        "ضد آفتاب", "sunscreen", "SPF", "ضد UV", "ضد UVA", "ضد UVB",
        "مرطوب کننده", "moisturizer", "هیدراتانت", "هیدراسیون",
        "سرم صورت", "face serum", "سرم ویتامین C", "سرم هیالورونیک",
        "ضد لک", "anti-spot", "روشن کننده", "brightening", "سفید کننده",
        "ضد چروک", "anti-wrinkle", "آنتی ایجینگ", "anti-aging", "جوان ساز",
        "پاک کننده", "cleanser", "شوینده", "فوم", "میسلار واتر",
        "تونر", "toner", "لوسیون پاک کننده", "آسترنژانت",
        "اسکراب", "scrub", "لایه بردار", "peeling", "پیلینگ",
        "ماسک صورت", "face mask", "ماسک شیت", "sheet mask", "ماسک گلی",
        "کرم دور چشم", "eye cream", "ژل دور چشم", "سرم دور چشم"
    ],
    
    'condition': [
        # بیماری‌های متابولیک - گسترش یافته
        "دیابت", "فشار", "قند", "کلسترول", "چاقی", "لاغری", "چربی خون",
        "تیروئید", "هورمون", "متابولیسم", "انسولین", "گلوکز",
        "diabetes", "hypertension", "cholesterol", "obesity", "thyroid",
        "دیابت نوع 1", "دیابت نوع 2", "پیش دیابت", "قند خون", "هموگلوبین A1C",
        "هیپوتیروئید", "هیپرتیروئید", "کم کاری تیروئید", "پرکاری تیروئید",
        "سندرم متابولیک", "مقاومت انسولینی", "هیپرلیپیدمی", "چاقی مفرط",
        "اختلال متابولیک", "دیس متابولیسم", "چربی های سه گانه", "تری گلیسیرید",
        
        # بیماری‌های رایج - گسترش یافته
        "سردرد", "میگرن", "سرگیجه", "تهوع", "استفراغ", "اسهال", "یبوست",
        "آسم", "آلرژی", "سرماخوردگی", "آنفولانزا", "کرونا", "کووید",
        "تب", "سرفه", "گلودرد", "خلط", "عطسه", "آبریزش",
        "headache", "migraine", "dizziness", "nausea", "asthma", "allergy",
        "flu", "fever", "cough", "cold", "covid", "coronavirus",
        "سردرد میگرنی", "سردرد تنشی", "سردرد خوشه ای", "ورتیگو",
        "حالت تهوع", "بی حالی", "ضعف", "سستی", "کوفتگی",
        "سرفه خشک", "سرفه خلط دار", "سرفه مزمن", "سرفه سیاه سرفه",
        
        # بیماری‌های گوارشی - گسترش یافته
        "معده", "روده", "کبد", "کلیه", "زخم", "سوزش", "نفخ", "گاز",
        "رفلاکس", "سنگ", "کیسه صفرا", "کولیت", "سندرم روده", "IBS",
        "gastric", "intestinal", "liver", "kidney", "reflux", "gastritis",
        "زخم معده", "زخم دوازدهه", "گاستریت", "التهاب معده", "هلیکوباکتر",
        "رفلاکس معده", "سوزش سردل", "ترش کردن", "نفخ شکم", "باد شکم",
        "سندرم روده تحریک پذیر", "کولیت اولسراتیو", "کرون", "IBD",
        "سیروز کبد", "کبد چرب", "هپاتیت", "یرقان", "زردی",
        "سنگ کلیه", "سنگ صفرا", "سنگ مثانه", "نارسایی کلیه",
        "دیورتیکول", "هموروئید", "فیستول", "فیشر", "شقاق",
        "یبوست مزمن", "اسهال مزمن", "سوء جذب", "سوء هاضمه",
        
        # بیماری‌های روانی - گسترش یافته
        "استرس", "اضطراب", "افسردگی", "بی خوابی", "خواب آلودگی",
        "خستگی", "پانیک", "فوبی", "وسواس", "OCD", "اوتیسم", "ADHD",
        "stress", "anxiety", "depression", "insomnia", "panic", "phobia",
        "اختلال اضطراب", "حمله پانیک", "اضطراب اجتماعی", "اضطراب فراگیر",
        "افسردگی شدید", "افسردگی خفیف", "افسردگی دو قطبی", "بای پولار",
        "وسواس فکری", "وسواس عملی", "فکر مزاحم", "رفتار تکراری",
        "بی خوابی شبانه", "پرخوابی", "اختلال خواب", "کابوس",
        "اختلال خوردن", "آنورکسیا", "بولیمیا", "پرخوری", "کم خوری",
        "اسکیزوفرنی", "جنون", "توهم", "هذیان", "فصام", "روان پریشی",
        "اوتیسم اسپکتروم", "آسپرگر", "اختلال نقص توجه", "بیش فعالی",
        "سایکوز", "نوروز", "هیستری", "روان رنجوری", "شخصیت مرزی",
        
        # بیماری‌های قلبی - گسترش یافته
        "فشار خون", "کلسترول بالا", "سکته", "انفارکتوس", "نارسایی قلبی",
        "آریتمی", "تپش قلب", "درد قفسه سینه", "آنژین",
        "heart disease", "cardiac", "stroke", "heart attack", "arrhythmia",
        "فشار خون بالا", "هیپرتانسیون", "فشار خون پایین", "هیپوتانسیون",
        "سکته قلبی", "سکته مغزی", "حمله قلبی", "ایسکمی", "انفارکتوس میوکارد",
        "نارسایی احتقانی قلب", "نارسایی سیستولیک", "نارسایی دیاستولیک",
        "آریتمی قلبی", "فیبریلاسیون دهلیزی", "تاکی کاردی", "برادی کاردی",
        "آنژین پکتوریس", "آنژین صدری", "درد قلبی", "گرفتگی قلب",
        "بیماری عروق کرونر", "آترواسکلروز", "گرفتگی رگ", "پلاک عروقی",
        "نارسایی میترال", "نارسایی آئورت", "تنگی دریچه", "پرولاپس",
        
        # بیماری‌های پوستی - گسترش یافته
        "آکنه", "جوش", "لک", "چروک", "پیری", "پسوریازیس", "اگزما",
        "کک و مک", "زگیل", "زونا", "درماتیت", "قارچ", "عفونت",
        "acne", "wrinkle", "psoriasis", "eczema", "dermatitis", "fungal",
        "جوش صورت", "جوش سر سیاه", "جوش چرکی", "جوش سیستیک",
        "لک صورت", "ملاسما", "کلوآسما", "لک بارداری", "لک پیری",
        "چروک صورت", "چین و چروک", "خطوط پیری", "چروک دور چشم",
        "پسوریازیس پلاک", "پسوریازیس قطره ای", "پسوریازیس معکوس",
        "اگزمای آتوپیک", "درماتیت تماسی", "درماتیت سبورئیک", "شوره سر",
        "کک و مک صورت", "کک و مک بدن", "رزاسه", "روزاسه",
        "زگیل تناسلی", "زگیل کف پا", "زگیل دست", "کندیلوما",
        "زونا", "تاول آبله", "هرپس", "تبخال", "آبله مرغان",
        "قارچ پوست", "قارچ ناخن", "قارچ سر", "کاندیدا", "تینه آ",
        "ویتیلیگو", "پیسی", "آلوپسی", "ریزش مو", "طاسی",
        
        # بیماری‌های استخوانی و مفصلی - گسترش یافته
        "آرتروز", "پوکی استخوان", "دیسک", "کمردرد", "گردن درد",
        "دیسک کمر", "سیاتیک", "رماتیسم", "التهاب مفاصل",
        "arthritis", "osteoporosis", "disc", "back pain", "sciatica",
        "آرتروز زانو", "آرتروز ران", "آرتروز مچ", "آرتروز کمر",
        "پوکی استخوان پس از یائسگی", "استئوپروز", "کاهش تراکم استخوان",
        "دیسک کمر", "دیسک گردن", "فتق دیسک", "برآمدگی دیسک", "هرنی",
        "کمردرد مزمن", "کمردرد حاد", "لومباگو", "درد اسکلتی عضلانی",
        "گردن درد مزمن", "درد ستون فقرات", "اسپوندیلوز", "اسپوندیلیت",
        "سیاتیک", "درد سیاتیک", "عرق النساء", "تحریک عصب سیاتیک",
        "رماتیسم مفصلی", "آرتریت روماتوئید", "لوپوس", "ذوب الدم",
        "اسپوندیلیت آنکیلوزان", "بشترت", "تنگی کانال نخاع", "استنوز",
        "بورسیت", "تاندونیت", "التهاب تاندون", "پارگی رباط", "پارگی منیسک",
        
        # مشکلات زنان و زایمان - گسترش یافته
        "قاعدگی", "پریود", "یائسگی", "PCOS", "آندومتریوز",
        "سقط", "نازایی", "بارداری", "زایمان", "نفاس",
        "menstruation", "menopause", "pregnancy", "infertility", "miscarriage",
        "قاعدگی دردناک", "دیسمنوره", "قاعدگی نامنظم", "خونریزی رحم",
        "درد قاعدگی", "PMS", "سندرم پیش از قاعدگی", "تغییرات هورمونی",
        "یائسگی زودرس", "یائسگی دیررس", "گرگرفتگی", "عرق شبانه",
        "تخمدان پلی کیستیک", "کیست تخمدان", "میوم رحم", "فیبروم",
        "آندومتریوز", "انتباک", "التهاب لگن", "PID", "عفونت رحم",
        "سقط جنین", "سقط مکرر", "مرده زایی", "نوزاد مرده",
        "ناباروری", "نازایی مردان", "نازایی زنان", "IVF", "لقاح مصنوعی",
        "بارداری پرخطر", "دیابت بارداری", "پره اکلامپسی", "فشار بارداری",
        "زایمان طبیعی", "زایمان سزارین", "زایمان زودرس", "زایمان دیررس",
        "نفاس", "شیردهی", "ترشح شیر", "ماستیت", "التهاب پستان",
        
        # بیماری‌های تنفسی - جدید
        "تنفسی", "ریه", "برونش", "آلوئول", "نفس", "تنگی نفس",
        "respiratory", "lung", "bronchial", "pneumonia", "TB",
        "آسم", "آسم برونشیال", "تنگی نفس آسمی", "خس خس سینه",
        "برونشیت", "التهاب برونش", "برونشیت حاد", "برونشیت مزمن",
        "ذات الریه", "پنومونی", "التهاب ریه", "عفونت ریوی",
        "COPD", "انسداد مزمن ریه", "آمفیزم", "تخریب آلوئول",
        "سل", "توبرکلوز", "سل ریوی", "سل استخوان", "مایکوباکتریوم",
        "آپنه خواب", "قطع تنفس", "خروپف", "خواب آلودگی روزانه",
        "فیبروز ریوی", "آسیب ریوی", "نارسایی تنفسی", "اکسیژن خون",
        
        # سرطان‌ها - جدید
        "سرطان", "تومور", "cancer", "tumor", "oncology", "chemotherapy",
        "سرطان پستان", "سرطان ریه", "سرطان معده", "سرطان کولون",
        "سرطان پروستات", "سرطان رحم", "سرطان تخمدان", "سرطان دهانه رحم",
        "سرطان خون", "لوکمی", "لنفوم", "میلوم", "هوچکین", "نان هوچکین",
        "سرطان پانکراس", "سرطان کبد", "سرطان مری", "سرطان مثانه",
        "ملانوما", "کارسینوما", "سارکوما", "آدنوکارسینوما",
        "تومور بدخیم", "تومور خوش خیم", "متاستاز", "انتشار سرطان",
        
        # بیماری‌های عصبی - جدید
        "عصبی", "مغز", "اعصاب", "نورون", "neurology", "nerve",
        "ام اس", "مولتیپل اسکلروزیس", "MS", "دمیلینه", "پلاک عصبی",
        "پارکینسون", "پارکینسونیسم", "لرزش دست", "سفتی عضلات",
        "آلزایمر", "زوال عقل", "دمانس", "فراموشی", "اختلال حافظه",
        "صرع", "اپیلپسی", "تشنج", "حمله تشنجی", "صرع بزرگ", "صرع کوچک",
        "میگرن", "سردرد عصبی", "نورالژی", "درد عصبی", "ترینژمینال",
        "فلج", "پلژی", "همی پلژی", "پارا پلژی", "کوادری پلژی",
        "نوروپاتی", "آسیب عصبی", "مرده شدن دست", "بی حسی اندام",
        
        # بیماری‌های کودکان - جدید
        "کودکان", "نوزادان", "اطفال", "کودک", "نوزاد", "pediatric",
        "تب", "سرماخوردگی کودک", "کرونا کودک", "آنفولانزا کودک",
        "زردی نوزادان", "هیپربیلیروبینمی", "فتوتراپی", "لامپ زردی",
        "آسم کودکان", "برونشیولیت", "خس خس نوزاد", "تنگی نفس کودک",
        "اوتیت", "عفونت گوش", "عفونت ادرار کودک", "UTI",
        "اگزما کودک", "آتوپی", "آلرژی غذایی", "حساسیت شیر",
        "رفلاکس نوزاد", "استفراغ نوزاد", "کولیک", "نفخ نوزاد",
        "یبوست کودک", "اسهال کودک", "گاستروانتریت", "استفراغ و اسهال"
    ],
    
    'specialty': [
        # رشته‌های پزشکی
        "داخلی", "جراحی", "قلب", "مغز اعصاب", "ارتوپدی",
        "زنان و زایمان", "اطفال", "پوست و مو", "چشم پزشکی",
        "گوش حلق بینی", "دندانپزشکی", "روانپزشکی", 
        "تغذیه", "فیزیوتراپی", "رادیولوژی", "پاتولوژی",
        # +50 تخصص پزشکی بیشتر
        "گوارش", "gastroenterology", "اورولوژی", "urology", "غدد", "endocrinology",
        "انکولوژی", "oncology", "ایمونولوژی", "immunology", "نوروآنکولوژی", "neuro-oncology",
        "آلوپتیک", "alloplastics", "تولید مثل", "reproductive medicine", "نوزادان", "neonatology",
        "اورژانس", "emergency medicine", "پزشکی هسته ای", "nuclear medicine", "پزشکی خانوادگی", "family medicine",
        "پزشکی سالمندی", "geriatrics", "سلامت عمومی", "public health", "اپیدمیولوژی", "epidemiology",
        "تغذیه بالینی", "clinical nutrition", "اختلالات خواب", "sleep medicine", "پزشکی ورزشی", "sports medicine",
        "نوروفیزیولوژی", "neurophysiology", "پزشکی قانونی", "forensic medicine", "توانبخشی", "rehabilitation",
        "ترومایست", "traumatology", "توانبخشی قلب", "cardiac rehab", "پزشکی بالینی", "clinical medicine",
        "میکروب شناسی", "microbiology", "ژنتیک پزشکی", "medical genetics", "پزشکی مولکولی", "molecular medicine"
    ],
    
    # 💰 بخش جدید: ارزهای دیجیتال و سرمایه‌گذاری
    'crypto_main': [
        # ارزهای اصلی Top 20
        "بیتکوین", "bitcoin", "BTC", "اتریوم", "ethereum", "ETH",
        "تتر", "tether", "USDT", "ریپل", "ripple", "XRP",
        "بایننس", "BNB", "سولانا", "solana", "SOL",
        "کاردانو", "cardano", "ADA", "دوج کوین", "doge", "DOGE",
        "پولکادات", "polkadot", "DOT", "لایت کوین", "litecoin", "LTC",
        "ترون", "tron", "TRX", "چین لینک", "chainlink", "LINK",
        
        # ارزهای محبوب
        "شیبا", "shiba", "SHIB", "آواکس", "avalanche", "AVAX",
        "پولیگان", "polygon", "MATIC", "یونی سواپ", "uniswap", "UNI",
        "کازماس", "cosmos", "ATOM", "استلار", "stellar", "XLM",
        "الگوراند", "algorand", "ALGO", "ویچین", "vechain", "VET",
        
        # استیبل کوین‌ها
        "USDC", "BUSD", "DAI", "استیبل کوین", "stablecoin",
        
        # اصطلاحات فارسی
        "ارز دیجیتال", "رمز ارز", "کریپتو", "کریپتوکارنسی",
        "بلاکچین", "blockchain", "ارز", "دیجیتال", "رمزارز", "کوین",
        
        # فعالیت‌های معاملاتی
        "ترید", "trade", "تریدینگ", "trading", "معامله", "معامله گر",
        "خرید", "buy", "فروش", "sell", "سرمایه گذاری", "investment",
        "استیکینگ", "staking", "ماینینگ", "mining", "استخراج",
        "مبادله", "exchange", "صرافی", "swap", "سواپ",
        
        # استراتژی‌ها
        "اسکالپ", "scalping", "سوئینگ", "swing", "هولد", "hodl",
        "DCA", "دلار کاست", "آربیتراژ", "arbitrage"
    ],
    
    'crypto_activities': [
        # ایردراپ و گیوآوی
        "ایردراپ", "airdrop", "ایردراپ رایگان", "free airdrop",
        "گیوآوی", "giveaway", "باونتی", "bounty", "ریوارد", "reward",
        "کمپین", "campaign", "تسک", "task", "مسابقه", "contest",
        
        # سیگنال‌ها
        "سیگنال", "signal", "سیگنال رایگان", "free signal",
        "سیگنال VIP", "کال", "call", "ورود", "entry", "خروج", "exit",
        "حد سود", "take profit", "TP", "حد ضرر", "stop loss", "SL",
        
        # آموزش
        "آموزش", "learn", "آموزش رایگان", "دوره", "course", "کلاس",
        "وبینار", "webinar", "تریدینگ", "trading course", "مدرس",
        "استاد", "آکادمی", "academy", "مقاله", "article", "راهنما",
        
        # تحلیل
        "تحلیل", "analysis", "تحلیل تکنیکال", "technical analysis",
        "تحلیل فاندامنتال", "fundamental", "نمودار", "chart", "چارت",
        "پرایس اکشن", "price action", "کندل", "candle", "الگو", "pattern",
        "اندیکاتور", "indicator", "RSI", "MACD", "EMA", "MA", "فیبوناچی",
        
        # معاملات
        "پامپ", "pump", "دامپ", "dump", "بریک اوت", "breakout",
        "فیوچرز", "futures", "اسپات", "spot", "مارجین", "margin",
        "لوریج", "leverage", "اهرم", "فاندینگ", "funding",
        "لانگ", "long", "شورت", "short", "لیکوئید", "liquidation",
        
        # DeFi و NFT
        "دی فای", "DeFi", "دیفای", "فارمینگ", "farming", "ییلد", "yield",
        "استیکینگ", "staking", "لندینگ", "lending", "لیکوئیدیتی",
        "ان اف تی", "NFT", "متاورس", "metaverse", "گیم فای", "GameFi",
        "وب 3", "web3", "دائو", "DAO", "تو ارن", "play to earn",
        
        # استراتژی
        "استراتژی", "strategy", "سیستم معاملاتی", "روش", "method",
        "مدیریت ریسک", "risk management", "مدیریت سرمایه", "money management",
        "روانشناسی", "psychology", "مارکت", "market", "ترند", "trend",
        
        # سایر
        "نیوز", "news", "اخبار", "ایونت", "event", "لیست", "listing",
        "آی سی او", "ICO", "آی دی او", "IDO", "توکن", "token",
        "کوین", "coin", "آلت کوین", "altcoin", "ممکوین", "memecoin"
    ],
    
    'crypto_platforms': [
        # صرافی‌های بین‌المللی Top
        "بایننس", "binance", "کوکوین", "kucoin", "بای بیت", "bybit",
        "اوکی ایکس", "okx", "کوینکس", "coinex", "گیت", "gate.io",
        "کوین بیس", "coinbase", "کراکن", "kraken", "جمینی", "gemini",
        "بیت گت", "bitget", "مکسی", "mexc", "هوبی", "huobi", "HTX",
        "بیت فینکس", "bitfinex", "فی تی ایکس", "FTX", "بیت استمپ",
        
        # صرافی‌های ایرانی
        "نوبیتکس", "nobitex", "تبدیل", "tabdil", "والکس", "wallex",
        "بیت پین", "bitpin", "اکسیر", "exir", "رمزینکس", "ramzinex",
        "کوین ایران", "coiniran", "ارزتومن", "arztooman", "تتربیت",
        
        # صرافی‌های غیرمتمرکز DEX
        "یونی سواپ", "uniswap", "پنکیک سواپ", "pancakeswap",
        "سوشی سواپ", "sushiswap", "دکس", "dex", "وان اینچ", "1inch",
        
        # کیف پول‌های نرم‌افزاری
        "کیف پول", "wallet", "متامسک", "metamask", "تراست والت", "trustwallet",
        "اکسودوس", "exodus", "اتمیک", "atomic", "کوین بیس والت",
        "مای اتر والت", "myetherwallet", "MEW", "فانتوم", "phantom",
        
        # کیف پول‌های سخت‌افزاری
        "لجر", "ledger", "ترزور", "trezor", "کولد والت", "cold wallet",
        
        # پلتفرم‌های سیگنال
        "تریدینگ ویو", "tradingview", "کریپتو کمپر", "cryptocompare",
        "کوین مارکت کپ", "coinmarketcap", "کوین گکو", "coingecko",
        
        # ابزارها
        "تلگرام", "telegram", "دیسکورد", "discord", "توییتر", "twitter",
        "یوتیوب", "youtube", "رددیت", "reddit", "میدیوم", "medium"
    ],
    
    'investment_terms': [
        # اصطلاحات پایه سرمایه‌گذاری
        "سرمایه", "سرمایه گذاری", "investment", "پورتفولیو", "portfolio",
        "ریسک", "risk", "بازدهی", "return", "سود", "profit",
        "ضرر", "loss", "معامله", "trade", "خرید", "buy",
        "فروش", "sell", "نگهداری", "hold", "هودل", "HODL",
        "درآمد", "کسب درآمد", "پول", "دارایی", "asset",
        
        # استراتژی‌های سرمایه‌گذاری
        "دی سی ای", "DCA", "میانگین گیری", "averaging", "تنوع بخشی", "diversification",
        "مدیریت ریسک", "risk management", "هج", "hedge", "آربیتراژ", "arbitrage",
        "اسکالپ", "scalp", "سوئینگ", "swing", "پوزیشن", "position",
        "بلند مدت", "long term", "کوتاه مدت", "short term", "میان مدت", "mid term",
        "استراتژی معاملاتی", "مدیریت سرمایه", "روانشناسی بازار",
        
        # تحلیل و پیش‌بینی
        "تحلیل بنیادی", "fundamental", "تحلیل تکنیکال", "technical analysis",
        "پیش بینی", "prediction", "فارکست", "forecast", "ترند", "trend",
        "حمایت", "support", "مقاومت", "resistance", "بریک اوت", "breakout",
        "الگو", "pattern", "اندیکاتور", "indicator", "سیگنال", "signal",
        
        # بازارهای مالی مختلف
        "بورس", "stock", "سهام", "shares", "فارکس", "forex",
        "ارز دیجیتال", "cryptocurrency", "طلا", "gold", "نفت", "oil",
        "کالا", "commodity", "اوراق قرضه", "bond", "صندوق", "fund",
        "ای تی اف", "ETF", "فیوچرز", "futures", "آپشن", "option",
        "دلار", "ارز", "فلزات", "انرژی", "اوراق",
        
        # اصطلاحات تخصصی معاملاتی
        "مارجین", "margin", "لوریج", "leverage", "اسپرد", "spread",
        "لیکوییدیت", "liquidity", "ولوم", "volume", "مارکت کپ", "market cap",
        "دامیننس", "dominance", "اِی تی اِچ", "ATH", "اِی تی اِل", "ATL",
        "آر او آی", "ROI", "شارپ ریشو", "sharpe ratio",
        "اسلیپیج", "slippage", "فیلینگ", "filling", "لیمیت", "limit",
        
        # رویدادها و اتفاقات بازار
        "هاوینگ", "halving", "آی پی او", "IPO", "ایردراپ", "airdrop",
        "استیکینگ", "staking", "مایننگ", "mining", "فارمینگ", "farming",
        "بایبک", "buyback", "برن", "burn", "فورک", "fork",
        
        # جامعه و خبر
        "کامیونیتی", "community", "کانال", "channel", "گروه", "group",
        "خبر", "news", "اطلاعیه", "announcement", "رالی", "rally",
        "بازار گاو", "bull market", "بازار خرس", "bear market", 
        "کرش", "crash", "پامپ", "pump", "دامپ", "dump",
        
        # آموزش و توسعه
        "آموزش بورس", "آموزش فارکس", "آموزش ارز دیجیتال",
        "دوره", "course", "وبینار", "webinar", "کلاس", "class",
        "منتور", "mentor", "کوچینگ", "coaching", "مشاوره", "consulting"
    ],
    
    'crypto_prefix': [
        "گروه", "کانال", "انجمن", "اجتماع", "کامیونیتی", "تیم",
        "آکادمی", "مدرسه", "دوره", "باشگاه", "شبکه", "پلتفرم",
        # +50 crypto prefix additions
        "سیگنال", "signal", "الفا", "alpha", "بتا", "beta", "بات", "bot", "ترید", "trade",
        "اتاق ترید", "trade room", "مارکت", "market", "فیوچرز", "futures", "اسپات", "spot",
        "آربیتراژ", "arbitrage", "لیکوئیدیتی", "liquidity", "استیک", "stake", "ییلد", "yield",
        "سودآور", "profitable", "ییلد فارم", "yield farm", "تحلیل", "analysis", "ریسرچ", "research",
        "نهایی", "ultimate", "آموزشی", "edu", "سرویس", "service", "روم", "room", "چنل", "channel"
    ],
    
    'crypto_suffix': [
        "رایگان", "VIP", "پرمیوم", "حرفه ای", "تخصصی", "آموزشی",
        "سیگنال", "تحلیل", "هات", "پمپ", "100x", "10x",
        "ایرانی", "فارسی", "پرسودآور", "تضمینی", "معتبر", "اصیل",
        # +50 crypto suffix additions
        "آلارم", "alerts", "تایم", "timely", "نوتیف", "notify", "پروفایل", "profile", "پرومو", "promo",
        "ریچ", "reach", "تایم", "time", "شبانه", "nightly", "روزانه", "daily", "هفتگی", "weekly",
        "اکونومی", "economy", "مارکتینگ", "marketing", "نوآور", "innovative", "گروه VIP", "VIP group",
        "چت", "chat", "روم", "room", "یادگیری", "learning", "حمایت", "support", "ادمین", "admin"
    ],
    
    # 📈💰 بخش گسترش یافته: ارزهای دیجیتال، ترید، ایردراپ و بورس (+500 کلمه جدید)
    'crypto_coins_extended': [
        # ارزهای لایه 1 (Layer 1)
        "بیتکوین", "Bitcoin", "BTC", "ساتوشی", "satoshi", "sats",
        "اتریوم", "Ethereum", "ETH", "ایتر", "ether", "گاس", "gas",
        "سولانا", "Solana", "SOL", "سول", "لامپورت",
        "کاردانو", "Cardano", "ADA", "آدا",
        "آواکس", "Avalanche", "AVAX", "اولانچ",
        "پولکادات", "Polkadot", "DOT", "پارا چین", "parachain",
        "نییر", "NEAR", "Near Protocol", "نیر پروتکل",
        "کازماس", "Cosmos", "ATOM", "اتم", "IBC",
        "الگوراند", "Algorand", "ALGO", "الگو",
        "فانتوم", "Fantom", "FTM", "اپرا",
        "هدرا", "Hedera", "HBAR", "هش گراف",
        "آپتوس", "Aptos", "APT", "اپتوس",
        "سویی", "Sui", "سوئی", "موو", "Move",
        "سی", "Sei", "اس ای آی",
        "تون", "TON", "Toncoin", "تون کوین", "تلگرام کوین",
        "اینجکتیو", "Injective", "INJ",
        "ترا", "Terra", "LUNA", "لونا", "ترا کلاسیک",
        
        # ارزهای لایه 2 (Layer 2)
        "پولیگان", "Polygon", "MATIC", "ماتیک", "پولی",
        "آربیتروم", "Arbitrum", "ARB", "آربی",
        "آپتیمیزم", "Optimism", "OP", "اپتی",
        "زی کی سینک", "zkSync", "ZK", "زیرو نالج",
        "استارک نت", "StarkNet", "STRK", "استارک",
        "بیس", "Base", "کوین بیس لایه ۲",
        "مانتا", "Manta", "منتا",
        "بلست", "Blast", "بلاست",
        "لینیا", "Linea", "لینه آ",
        "اسکرول", "Scroll", "اسکرال",
        
        # ممکوین‌ها و شت‌کوین‌ها
        "دوج", "Dogecoin", "DOGE", "دوج کوین", "داگ",
        "شیبا", "Shiba Inu", "SHIB", "شیب", "شیبا اینو",
        "پپه", "Pepe", "PEPE", "پپه کوین",
        "فلوکی", "Floki", "FLOKI", "فلوکی اینو",
        "بانک", "BONK", "بونک",
        "WIF", "داگ ویف هت", "dogwifhat",
        "برت", "BRETT", "برت کوین",
        "موگ", "MOG", "ماگ",
        "ترامپ", "TRUMP", "میلادی",
        "کوک", "COOK", "کوک کوین",
        "بیبی دوج", "Baby Doge", "BABYDOGE",
        "سیف مون", "SafeMoon", "SAFEMOON",
        "اکیتا", "Akita", "آکیتا اینو",
        "کیشو", "Kishu", "کیشو اینو",
        "سامویدکوین", "Samoyed", "SAMO",
        
        # ارزهای DeFi
        "یونی سواپ", "Uniswap", "UNI", "یونی",
        "آوه", "Aave", "AAVE", "آوی", "لندینگ",
        "کامپاند", "Compound", "COMP", "کمپ",
        "میکر", "Maker", "MKR", "دای", "DAI",
        "کرو", "Curve", "CRV", "کروو",
        "کانوکس", "Convex", "CVX",
        "سوشی", "Sushi", "SUSHI", "سوشی سواپ",
        "بالانسر", "Balancer", "BAL",
        "وان اینچ", "1inch", "وان اینچ",
        "پنکیک", "PancakeSwap", "CAKE", "کیک",
        "ونوس", "Venus", "XVS",
        "لیدو", "Lido", "LDO", "استیکینگ لیکویید",
        "راکت پول", "Rocket Pool", "RPL",
        "فراکس", "Frax", "FRAX", "FXS",
        "جی ام ایکس", "GMX",
        
        # ارزهای NFT و متاورس
        "ان اف تی", "NFT", "انفتی", "توکن غیرقابل تعویض",
        "اوپنسی", "OpenSea", "اپنسی",
        "بلر", "Blur", "BLUR", "بلور",
        "سندباکس", "Sandbox", "SAND", "سند",
        "دیسنترالند", "Decentraland", "MANA", "مانا",
        "اکسی", "Axie Infinity", "AXS", "اکسی اینفینیتی",
        "گالا", "Gala", "GALA", "گالا گیمز",
        "آپلند", "Upland", "UPX",
        "ایلوویوم", "Illuvium", "ILV",
        "اینترلند", "Interland",
        "رندر", "Render", "RNDR", "رندر توکن",
        "تتا", "Theta", "THETA", "تتا نتورک",
        "اپ کوین", "ApeCoin", "APE", "بورد ایپ",
        "سوپر ریر", "SuperRare",
        "فاونیشن", "Foundation",
        "رریبل", "Rarible", "RARI",
        
        # ارزهای AI و هوش مصنوعی
        "فچ", "Fetch.ai", "FET", "فچ ای آی",
        "اوشن", "Ocean", "OCEAN", "اوشن پروتکل",
        "سینگولاریتی", "SingularityNET", "AGIX",
        "رندر", "Render", "RNDR",
        "آکش", "Akash", "AKT", "آکاش",
        "ورلد کوین", "Worldcoin", "WLD",
        "بیتنسور", "Bittensor", "TAO",
        "آرویو", "Arweave", "AR",
        
        # ارزهای پرایوسی
        "مونرو", "Monero", "XMR",
        "زی کش", "Zcash", "ZEC",
        "دش", "Dash", "DASH",
        "هورایزن", "Horizen", "ZEN",
        "سکرت", "Secret", "SCRT",
        
        # ارزهای قدیمی و کلاسیک
        "لایت کوین", "Litecoin", "LTC", "لایت",
        "بیت کوین کش", "Bitcoin Cash", "BCH",
        "ریپل", "Ripple", "XRP", "ایکس آر پی",
        "استلار", "Stellar", "XLM", "لومن",
        "ایاس", "EOS", "ایوس",
        "ترون", "Tron", "TRX", "ترکس",
        "نئو", "NEO", "نیو", "چینی اتریوم",
        "وی چین", "VeChain", "VET", "وت",
        "آیوتا", "IOTA", "MIOTA",
        "تزوس", "Tezos", "XTZ",
        "فایل کوین", "Filecoin", "FIL",
        
        # استیبل کوین‌ها
        "تتر", "Tether", "USDT", "یو اس دی تی",
        "یو اس دی کوین", "USDC", "یو اس دی سی",
        "بایننس یو اس دی", "BUSD",
        "دای", "DAI", "میکر دای",
        "فراکس", "FRAX",
        "تروو", "TUSD", "TrueUSD",
        "پکس دلار", "USDP", "Pax Dollar",
        "جی یو اس دی", "GUSD", "Gemini Dollar",
        "یو اس دی دی", "USDD", "ترون استیبل"
    ],
    
    'trading_terms_extended': [
        # اصطلاحات ترید فارسی
        "ترید", "تریدر", "تریدینگ", "معامله گر", "معامله گری",
        "خرید", "فروش", "پوزیشن", "اوردر", "سفارش",
        "لانگ", "شورت", "خرید استقراضی", "فروش استقراضی",
        "اسپات", "فیوچرز", "پرپچوال", "فوروارد",
        "مارجین", "کراس مارجین", "ایزوله مارجین",
        "لوریج", "اهرم", "لیکوئید", "لیکوییدیشن",
        "ورود", "خروج", "تی پی", "استاپ لاس",
        "حد سود", "حد ضرر", "ریسک به ریوارد",
        "بریک اوت", "بریک داون", "پولبک", "ریتست",
        
        # اصطلاحات ترید انگلیسی
        "trade", "trader", "trading", "position", "order",
        "long", "short", "spot", "futures", "perpetual",
        "margin", "leverage", "liquidation", "entry", "exit",
        "take profit", "stop loss", "risk reward", "breakout",
        "breakdown", "pullback", "retest", "scalp", "scalping",
        "swing", "swing trading", "day trading", "position trading",
        
        # پرایس اکشن
        "پرایس اکشن", "price action", "کندل", "candle", "کندل استیک",
        "شمع", "شمع ژاپنی", "دوجی", "doji", "هامر", "hammer",
        "شوتینگ استار", "shooting star", "اینگالف", "engulfing",
        "پین بار", "pin bar", "اینسایدبار", "inside bar",
        "سوئینگ های", "سوئینگ لو", "ساختار بازار",
        "higher high", "higher low", "lower high", "lower low",
        "چاک", "choch", "BOS", "break of structure",
        "ایموبالانس", "imbalance", "FVG", "fair value gap",
        "اوردر بلاک", "order block", "میتیگیشن", "mitigation",
        
        # اندیکاتورها
        "اندیکاتور", "indicator", "اسیلاتور", "oscillator",
        "آر اس آی", "RSI", "مک دی", "MACD",
        "میانگین متحرک", "moving average", "MA", "EMA", "SMA",
        "بولینگر باند", "Bollinger Bands", "BB",
        "ایچیموکو", "Ichimoku", "ابر کومو",
        "فیبوناچی", "Fibonacci", "ریتریسمنت", "اکستنشن",
        "استوکاستیک", "Stochastic", "CCI",
        "ای تی آر", "ATR", "ولوم", "volume", "OBV",
        "مومنتوم", "momentum", "دایورجنس", "divergence",
        "واگرایی", "همگرایی", "convergence",
        
        # الگوها
        "الگو", "pattern", "الگوی قیمتی", "chart pattern",
        "سر و شانه", "head and shoulders", "دابل تاپ", "double top",
        "دابل باتم", "double bottom", "مثلث", "triangle",
        "پرچم", "flag", "کنج", "wedge", "کانال", "channel",
        "مستطیل", "rectangle", "الگوی ادامه دهنده",
        "الگوی بازگشتی", "reversal pattern", "continuation",
        "کاپ اند هندل", "cup and handle", "راندینگ باتم",
        
        # مدیریت ریسک
        "مدیریت ریسک", "risk management", "مدیریت سرمایه",
        "money management", "پوزیشن سایز", "position sizing",
        "ریسک به ریوارد", "R:R", "وین ریت", "win rate",
        "دراداون", "drawdown", "مکس دراداون",
        "شارپ ریشو", "sharpe ratio", "پروفیت فکتور",
        "اکسپکتنسی", "expectancy", "ژورنال ترید",
        "بک تست", "backtest", "فوروارد تست",
        
        # روانشناسی ترید
        "روانشناسی ترید", "trading psychology", "فومو", "FOMO",
        "فاد", "FUD", "ترس", "طمع", "greed", "fear",
        "اوور ترید", "overtrading", "ریونج ترید",
        "دیسیپلین", "discipline", "صبر", "patience",
        "هیجان", "emotion", "کنترل احساسات"
    ],
    
    'airdrop_terms': [
        # ایردراپ پایه
        "ایردراپ", "airdrop", "ایردراپ رایگان", "free airdrop",
        "ایردراپ جدید", "ایردراپ داغ", "hot airdrop",
        "ایردراپ تضمینی", "ایردراپ فوری", "ایردراپ آسان",
        "ایردراپ ارز دیجیتال", "crypto airdrop",
        "ایردراپ تلگرام", "telegram airdrop",
        "ایردراپ بایننس", "ایردراپ کوکوین",
        
        # انواع ایردراپ
        "ریتروایردراپ", "retroactive airdrop", "رترو",
        "تست نت", "testnet", "مین نت", "mainnet",
        "ایردراپ هولدر", "holder airdrop",
        "ایردراپ استیکینگ", "staking airdrop",
        "ایردراپ ریفرال", "referral airdrop",
        "ایردراپ سوشیال", "social airdrop",
        "ایردراپ کوئست", "quest airdrop",
        "ایردراپ گالکسی", "galxe airdrop",
        "ایردراپ زیلی", "zealy airdrop",
        "ایردراپ تسکان", "taskon airdrop",
        
        # پلتفرم‌های ایردراپ
        "گالکسی", "Galxe", "گالکسه",
        "زیلی", "Zealy", "کرو۳", "Crew3",
        "تسکان", "Taskon", "تسک آن",
        "لیر۳", "Layer3", "لیر تری",
        "رابی", "RabbitHole", "رابیت هول",
        "اینتراکت", "Intract",
        "کوئست ان", "QuestN",
        "پرمیس", "Premise",
        
        # مفاهیم ایردراپ
        "فارمینگ ایردراپ", "airdrop farming",
        "هانتر ایردراپ", "airdrop hunter", "هانتینگ",
        "الوکیشن", "allocation", "تخصیص",
        "الیجیبل", "eligible", "واجد شرایط",
        "کلیم", "claim", "دریافت ایردراپ",
        "اسنپ شات", "snapshot", "عکس برداری",
        "توکن جنسیس", "genesis", "TGE",
        "وستینگ", "vesting", "آنلاک", "unlock",
        "کلیف", "cliff", "دوره قفل",
        
        # تسک‌ها و فعالیت‌ها
        "تسک", "task", "فعالیت",
        "فالو", "follow", "ریتوییت", "retweet",
        "لایک", "like", "کامنت", "comment",
        "جوین", "join", "عضویت",
        "کانکت والت", "connect wallet",
        "مینت", "mint", "مینتینگ",
        "سواپ", "swap", "بریج", "bridge",
        "دیپازیت", "deposit", "استیک", "stake"
    ],
    
    'defi_extended': [
        # مفاهیم دیفای
        "دیفای", "DeFi", "دی فای", "مالی غیرمتمرکز",
        "decentralized finance", "فایننس غیرمتمرکز",
        "پروتکل دیفای", "defi protocol",
        "تی وی ال", "TVL", "total value locked",
        "ییلد", "yield", "سود دیفای", "بازده",
        
        # لندینگ و استقراض
        "لندینگ", "lending", "قرض دهی",
        "باروینگ", "borrowing", "قرض گیری",
        "وام دیفای", "defi loan", "وام رمزارز",
        "کلترال", "collateral", "وثیقه",
        "لیکوییدیشن", "liquidation", "تسویه",
        "اوور کلترالایز", "overcollateralized",
        "فلش لون", "flash loan", "وام آنی",
        
        # استیکینگ و ییلد فارمینگ
        "استیکینگ", "staking", "استیک کردن",
        "ییلد فارمینگ", "yield farming", "فارمینگ",
        "لیکوئیدیتی ماینینگ", "liquidity mining",
        "اِی پی وای", "APY", "سود سالانه",
        "اِی پی آر", "APR", "نرخ سود",
        "ریوارد", "reward", "پاداش",
        "هاروست", "harvest", "برداشت",
        "کامپاند", "compound", "ترکیب سود",
        
        # لیکوئیدیتی و AMM
        "لیکوئیدیتی", "liquidity", "نقدینگی",
        "لیکوئیدیتی پول", "liquidity pool", "استخر",
        "لیکوئیدیتی پروایدر", "LP", "تامین نقدینگی",
        "ای ام ام", "AMM", "بازارساز خودکار",
        "ایمپرمننت لاس", "impermanent loss", "IL",
        "اسلیپیج", "slippage", "لغزش قیمت",
        
        # صرافی غیرمتمرکز
        "دکس", "DEX", "صرافی غیرمتمرکز",
        "سواپ", "swap", "مبادله",
        "اوردر بوک", "order book",
        "سی ال ام ام", "CLAMM", "liquidity",
        "اگریگیتور", "aggregator", "تجمیع کننده",
        
        # گاورننس
        "گاورننس", "governance", "حکمرانی",
        "وتینگ", "voting", "رای دهی",
        "پروپوزال", "proposal", "پیشنهاد",
        "دائو", "DAO", "سازمان غیرمتمرکز",
        "توکنومیکس", "tokenomics", "اقتصاد توکن",
        "وستینگ", "vesting", "سررسید"
    ],
    
    'exchange_platforms': [
        # صرافی‌های متمرکز بزرگ
        "بایننس", "Binance", "بینانس",
        "کوکوین", "KuCoin", "کیوکوین",
        "بای بیت", "Bybit", "بایبیت",
        "او کی ایکس", "OKX", "اوکی اکس", "اوککس",
        "کوینکس", "CoinEx", "کوین اکس",
        "گیت آی او", "Gate.io", "گیت",
        "کوین بیس", "Coinbase", "کوینبیس",
        "کراکن", "Kraken", "کرکن",
        "بیت گت", "Bitget", "بیتگت",
        "مکسی", "MEXC", "ام ای ایکس سی",
        "هوبی", "Huobi", "HTX", "اچ تی ایکس",
        "بیتفینکس", "Bitfinex", "فینکس",
        "بیت مکس", "BitMEX", "بیتمکس",
        "دی وای دی ایکس", "dYdX",
        "اف تی ایکس", "FTX", "افتیایکس",
        
        # صرافی‌های ایرانی
        "نوبیتکس", "Nobitex", "نوبیت",
        "والکس", "Wallex", "ولکس",
        "بیت پین", "Bitpin", "بیتپین",
        "تبدیل", "Tabdil",
        "اکسیر", "Exir",
        "رمزینکس", "Ramzinex",
        "آبان تتر", "Abantether",
        "بیت ۲۴", "Bit24",
        "تترلند", "Tetherland",
        "ارز پایا", "ArzPaya",
        "اوکی اکسچنج", "OKExchange",
        
        # صرافی‌های غیرمتمرکز
        "یونی سواپ", "Uniswap", "یونی",
        "پنکیک سواپ", "PancakeSwap", "پنکیک",
        "سوشی سواپ", "SushiSwap", "سوشی",
        "وان اینچ", "1inch", "یک اینچ",
        "کرو فایننس", "Curve", "کروو",
        "بالانسر", "Balancer", "بالنسر",
        "ریدیوم", "Raydium", "ری دیوم",
        "اورکا", "Orca", "ارکا",
        "جوپیتر", "Jupiter", "ژوپیتر",
        "تریدر جو", "Trader Joe", "جو",
        "کملوت", "Camelot", "کاملات",
        "ولوداروم", "Velodrome", "ولو",
        "ایرو دروم", "Aerodrome", "ایرو"
    ],
    
    'wallet_terms': [
        # انواع کیف پول
        "کیف پول", "wallet", "ولت",
        "کیف پول سخت افزاری", "hardware wallet", "هاردوالت",
        "کیف پول نرم افزاری", "software wallet", "سافتوالت",
        "کیف پول موبایل", "mobile wallet",
        "کیف پول وب", "web wallet",
        "کیف پول سرد", "cold wallet", "کولد والت",
        "کیف پول گرم", "hot wallet", "هات والت",
        "کاستدی", "custodial", "نان کاستدی", "non-custodial",
        
        # کیف پول‌های محبوب
        "متامسک", "MetaMask", "مسک",
        "تراست والت", "Trust Wallet", "تراست",
        "لجر", "Ledger", "لجر نانو",
        "ترزور", "Trezor", "ترزر",
        "فانتوم", "Phantom", "فنتم",
        "رابی", "Rabby", "ربی",
        "کوین بیس والت", "Coinbase Wallet",
        "اکسودوس", "Exodus", "اگزودوس",
        "اتمیک", "Atomic", "اتومیک",
        "زریون", "Zerion", "زیرون",
        "رینبو", "Rainbow", "رنگین کمان",
        "کپلر", "Keplr", "کاپلر",
        "سولفلر", "Solflare", "سول فلیر",
        "توکن پاکت", "Token Pocket",
        "سیف پل", "SafePal", "سیفپل",
        "ون کی", "OneKey", "وان کی",
        "الیپال", "Ellipal", "الیپل",
        
        # امنیت کیف پول
        "سید فریز", "seed phrase", "عبارت بازیابی",
        "پرایوت کی", "private key", "کلید خصوصی",
        "پابلیک کی", "public key", "کلید عمومی",
        "آدرس والت", "wallet address", "آدرس",
        "بک آپ", "backup", "پشتیبان",
        "ریکاوری", "recovery", "بازیابی",
        "پسورد", "password", "رمز عبور",
        "تو اف ای", "2FA", "احراز هویت دوعاملی",
        "مالتی سیگ", "multisig", "چند امضایی"
    ],
    
    'stock_market_terms': [
        # بورس ایران
        "بورس", "بورس تهران", "بورس ایران",
        "فرابورس", "بورس کالا", "بورس انرژی",
        "شاخص کل", "شاخص هم وزن", "شاخص",
        "تابلو", "نماد", "سهم", "سهام",
        "عرضه اولیه", "IPO", "آی پی او",
        "صف خرید", "صف فروش", "حجم معاملات",
        "ارزش معاملات", "ارزش بازار", "مارکت کپ",
        "کد بورسی", "سجام", "کارگزاری",
        "پرتفو", "پرتفوی", "سبد سهام",
        "تحلیل بنیادی", "تحلیل تکنیکال",
        "حمایت", "مقاومت", "روند",
        
        # سهام و شرکت‌ها
        "سهامدار", "سهامداری", "سود سهام",
        "افزایش سرمایه", "تقسیم سود", "مجمع",
        "گزارش مالی", "صورت مالی", "ترازنامه",
        "EPS", "P/E", "سود هر سهم",
        "نسبت قیمت به درآمد", "NAV",
        "حق تقدم", "بلوکی", "سهام شناور",
        "سهام بنیادی", "سهام تکنیکالی",
        "سهام حقیقی", "سهام حقوقی",
        
        # بورس جهانی
        "وال استریت", "Wall Street", "نزدک", "NASDAQ",
        "اس اند پی", "S&P 500", "داوجونز", "Dow Jones",
        "فیوچرز", "آپشن", "قرارداد آتی",
        "ETF", "صندوق سرمایه گذاری",
        "بازار سهام", "stock market",
        "NYSE", "نیویورک", "لندن", "توکیو",
        "شانگهای", "هنگ کنگ", "فرانکفورت",
        
        # فارکس
        "فارکس", "Forex", "FX", "بازار ارز",
        "جفت ارز", "currency pair", "پیپ", "pip",
        "اسپرد", "spread", "لات", "lot",
        "EUR/USD", "یورو دلار", "GBP/USD",
        "USD/JPY", "دلار ین", "AUD/USD",
        "بروکر", "broker", "کارگزار فارکس",
        "متاتریدر", "MetaTrader", "MT4", "MT5",
        "تحلیل فاندامنتال فارکس",
        "خبر فارکس", "تقویم اقتصادی"
    ],
    
    'trading_signals': [
        # سیگنال ترید
        "سیگنال", "signal", "سیگنال ترید",
        "سیگنال خرید", "buy signal", "سیگنال فروش", "sell signal",
        "سیگنال رایگان", "free signal", "سیگنال VIP",
        "سیگنال فیوچرز", "futures signal",
        "سیگنال اسپات", "spot signal",
        "سیگنال فارکس", "forex signal",
        "سیگنال بورس", "stock signal",
        "سیگنال طلا", "gold signal",
        "سیگنال ارز دیجیتال", "crypto signal",
        
        # اجزای سیگنال
        "نقطه ورود", "entry point", "انتری",
        "حد سود", "take profit", "TP", "تیکی پرافیت",
        "حد ضرر", "stop loss", "SL", "استاپ",
        "ریسک به ریوارد", "R:R", "نسبت سود به ضرر",
        "تارگت", "target", "هدف قیمتی",
        "لوریج پیشنهادی", "سایز پوزیشن",
        
        # کانال سیگنال
        "کانال سیگنال", "signal channel",
        "گروه سیگنال", "signal group",
        "سیگنال تلگرام", "telegram signal",
        "سیگنال دیسکورد", "discord signal",
        "کپی ترید", "copy trading", "کپی تریدینگ",
        "سوشیال ترید", "social trading",
        "آلرت", "alert", "اعلان", "نوتیفیکیشن"
        
    ],
    
    'crypto_news_analysis': [
        # اخبار و تحلیل
        "اخبار ارز دیجیتال", "crypto news",
        "اخبار بیتکوین", "bitcoin news",
        "اخبار اتریوم", "ethereum news",
        "تحلیل بازار", "market analysis",
        "تحلیل روزانه", "daily analysis",
        "تحلیل هفتگی", "weekly analysis",
        "پیش بینی قیمت", "price prediction",
        "پیش بینی بیتکوین", "bitcoin prediction",
        "چشم انداز بازار", "market outlook",
        "بررسی بازار", "market review",
        
        # منابع خبری
        "کوین تلگراف", "CoinTelegraph",
        "کوین دسک", "CoinDesk",
        "کریپتو اسلیت", "CryptoSlate",
        "بلومبرگ کریپتو", "Bloomberg Crypto",
        "رویترز کریپتو", "Reuters",
        "دی بلاک", "The Block",
        "مساری", "Messari",
        "گلس نود", "Glassnode",
        "دیفای لاما", "DefiLlama",
        "دیون", "Dune", "دون آنالیتیکس",
        # شاخص‌ها و متریک‌ها
        "دامیننس", "dominance", "دامیننس بیتکوین",
        "فیر اند گرید", "Fear & Greed Index",
        "ولوم", "volume", "حجم معاملات",
        "مارکت کپ", "market cap", "ارزش بازار",
        "اوپن اینترست", "open interest", "OI",
        "فاندینگ ریت", "funding rate",
        "لانگ شورت ریشو", "long/short ratio",
        "آن چین", "on-chain", "دیتای آن چین"
    ],
    
    'crypto_education': [
        # آموزش ارز دیجیتال
        "آموزش ارز دیجیتال", "crypto education",
        "آموزش بیتکوین", "bitcoin education",
        "آموزش ترید", "trading education",
        "آموزش تحلیل تکنیکال", "technical analysis course",
        "آموزش تحلیل بنیادی", "fundamental analysis",
        "آموزش فیوچرز", "futures trading course",
        "آموزش دیفای", "defi education",
        "آموزش ایردراپ", "airdrop guide",
        "آموزش استیکینگ", "staking guide",
        "آموزش NFT", "NFT education",
        
        # دوره و کلاس
        "دوره ارز دیجیتال", "crypto course",
        "کلاس ترید", "trading class",
        "وبینار ارز دیجیتال", "crypto webinar",
        "ورکشاپ ترید", "trading workshop",
        "بوت کمپ ترید", "trading bootcamp",
        "منتورشیپ", "mentorship", "منتور ترید",
        "کوچینگ", "coaching", "مربیگری",
        "آکادمی ترید", "trading academy",
        "مدرسه ترید", "trading school",
        
        # منابع آموزشی
        "کتاب ترید", "trading book",
        "ویدیو آموزشی", "tutorial video",
        "پادکست ارز دیجیتال", "crypto podcast",
        "مقاله آموزشی", "educational article",
        "وایت پیپر", "whitepaper", "لایت پیپر",
        "داکیومنتیشن", "documentation", "مستندات"
    ],
    
    'telegram_group_names': [
        # اسامی گروه‌های تلگرام کریپتو
        "سیگنال VIP", "VIP signal", "سیگنال طلایی",
        "پامپ گروپ", "pump group", "کال گروپ",
        "تیم ترید", "trading team", "تیم سیگنال",
        "آکادمی کریپتو", "crypto academy",
        "باشگاه ترید", "trading club",
        "انجمن تریدرها", "traders community",
        "کانال تحلیل", "analysis channel",
        "گروه ایردراپ", "airdrop group",
        "هانترز", "hunters", "ایردراپ هانترز",
        
        
        # نام‌های انگلیسی
        "Crypto Gems", "Moon Shots", "100x Calls",
        "Whale Alerts", "Pump Signals", "Trading Pros",
        "DeFi Alpha", "NFT Drops", "Airdrop Alert",
        "Bitcoin Bulls", "Altcoin Season",
        "Market Makers", "Crypto Whales"
    ],
    
    # 🌍 بخش جدید: مهاجرت، ایرانیان خارج از کشور و دیاسپورا
    'immigration_main': [
        # مفاهیم اصلی مهاجرت
        "مهاجرت", "مهاجر", "مهاجرین", "مهاجران", "immigration", "immigrant",
        "دیاسپورا", "diaspora", "غربت", "خارج از کشور", "abroad",
        "اقامت", "residence", "residency", "پناهندگی", "asylum", "refugee",
        "پناهنده", "پناهجو", "تابعیت", "citizenship", "ملیت", "nationality",
        "گرین کارت", "green card", "ویزا", "visa", "پاسپورت", "passport",
        "مهاجرت کاری", "work visa", "مهاجرت تحصیلی", "student visa",
        "مهاجرت سرمایه گذاری", "investor visa", "اکسپرس انتری", "express entry",
        "لاتاری", "lottery", "دی وی", "DV lottery", "گرین کارت لاتاری",
        
        
        
        # مدارک و مستندات
        "مدارک", "documents", "ترجمه رسمی", "official translation",
        "تاییدیه", "verification", "تمکن مالی", "bank statement",
        "اسپانسر", "sponsor", "اسپانسرشیپ", "sponsorship",
        "گواهی عدم سوءپیشینه", "police clearance", "گواهی پزشکی",
        "medical exam", "ایلتس", "IELTS", "تافل", "TOEFL",
        "دولینگو", "duolingo", "آزمون زبان", "language test",
        "ارزیابی مدرک", "credential assessment", "WES", "IQAS",
        "ریجکت", "reject", "رد درخواست", "اپروال", "approval",
        "گرنت", "grant", "تایید ویزا", "visa granted"
    ],
    
    'immigration_countries': [
        # کشورهای محبوب مهاجرت - آمریکا و کانادا
        "آمریکا", "America", "USA", "امریکا", "ایالات متحده", "United States",
        "کانادا", "Canada", "کنادا", "تورنتو", "Toronto", "ونکوور", "Vancouver",
        "مونترال", "Montreal", "کلگری", "Calgary", "ادمونتون", "Edmonton",
        "نیویورک", "New York", "لس آنجلس", "Los Angeles", "LA",
        "سانفرانسیسکو", "San Francisco", "سن دیگو", "San Diego",
        "هیوستون", "Houston", "شیکاگو", "Chicago", "میامی", "Miami",
        "بوستون", "Boston", "سیاتل", "Seattle", "آتلانتا", "Atlanta",
        "واشنگتن", "Washington", "DC", "تگزاس", "Texas", "کالیفرنیا", "California",
        "فلوریدا", "Florida", "اونتاریو", "Ontario", "بریتیش کلمبیا", "BC",
        "آلبرتا", "Alberta", "کبک", "Quebec", "منیتوبا", "Manitoba",
        
        # اروپا
        "آلمان", "Germany", "المان", "برلین", "Berlin", "مونیخ", "Munich",
        "فرانکفورت", "Frankfurt", "هامبورگ", "Hamburg", "دوسلدورف", "Dusseldorf",
        "فرانسه", "France", "پاریس", "Paris", "لیون", "Lyon",
        "انگلیس", "UK", "England", "Britain", "لندن", "London",
        "منچستر", "Manchester", "برمینگام", "Birmingham",
        "هلند", "Netherlands", "Holland", "آمستردام", "Amsterdam",
        "سوئد", "Sweden", "استکهلم", "Stockholm",
        "نروژ", "Norway", "اسلو", "Oslo",
        "دانمارک", "Denmark", "کپنهاگ", "Copenhagen",
        "فنلاند", "Finland", "هلسینکی", "Helsinki",
        "اتریش", "Austria", "وین", "Vienna",
        "سوئیس", "Switzerland", "زوریخ", "Zurich", "ژنو", "Geneva",
        "بلژیک", "Belgium", "بروکسل", "Brussels",
        "ایتالیا", "Italy", "میلان", "Milan", "رم", "Rome",
        "اسپانیا", "Spain", "مادرید", "Madrid", "بارسلونا", "Barcelona",
        "پرتغال", "Portugal", "لیسبون", "Lisbon",
        "ایرلند", "Ireland", "دوبلین", "Dublin",
        "چک", "Czech", "پراگ", "Prague",
        "لهستان", "Poland", "ورشو", "Warsaw",
        "مجارستان", "Hungary", "بوداپست", "Budapest",
        "یونان", "Greece", "آتن", "Athens",
        "قبرس", "Cyprus", "نیکوزیا", "Nicosia",
        
        # آسیا و اقیانوسیه
        "استرالیا", "Australia", "سیدنی", "Sydney", "ملبورن", "Melbourne",
        "بریزبین", "Brisbane", "پرث", "Perth", "آدلاید", "Adelaide",
        "نیوزیلند", "New Zealand", "اوکلند", "Auckland", "ولینگتون", "Wellington",
        "ژاپن", "Japan", "توکیو", "Tokyo",
        "کره جنوبی", "South Korea", "سئول", "Seoul",
        "مالزی", "Malaysia", "کوالالامپور", "Kuala Lumpur",
        "سنگاپور", "Singapore",
        
        # خاورمیانه
        "امارات", "UAE", "دبی", "Dubai", "ابوظبی", "Abu Dhabi",
        "ترکیه", "Turkey", "استانبول", "Istanbul", "آنکارا", "Ankara",
        "قطر", "Qatar", "دوحه", "Doha",
        "عمان", "Oman", "مسقط", "Muscat",
        "بحرین", "Bahrain",
        "کویت", "Kuwait",
        "عربستان", "Saudi Arabia"
    ],
    
    'immigration_services': [
        # خدمات مهاجرتی
        "مشاوره مهاجرت", "immigration consulting", "وکیل ایمیگریشن",
        "موسسه اعزام دانشجو", "study abroad agency", "سفارشی ویزا",
        "خدمات ویزا", "visa services", "اخذ ویزا", "ویزا گرفتن",
        "وقت سفارت", "embassy appointment", "رزرو وقت سفارت",
        "ترجمه مدارک", "document translation", "ترجمه رسمی",
        "تایید مدارک", "document verification", "لگالایز",
        "خدمات پذیرش", "admission services", "اپلای دانشگاه",
        "آموزش آیلتس", "IELTS preparation", "آموزش تافل",
        "کلاس زبان", "language class", "آموزش زبان آلمانی",
        "مشاوره تحصیلی", "education consulting", "راهنمای تحصیل",
        "کاریابی خارج", "overseas jobs", "فرصت شغلی",
        "استخدام خارج", "job abroad", "کار در کانادا",
        "کار در آلمان", "کار در استرالیا", "کار در اروپا",
        
        # سکونت و زندگی
        "اجاره خانه", "rent apartment", "مسکن", "housing",
        "روم میت", "roommate", "هم خانه", "شریک اتاق",
        "اجاره اتاق", "room for rent", "خانه اشتراکی",
        "هتل", "hotel", "هاستل", "hostel", "اقامتگاه",
        "حمل اثاث", "moving services", "باربری بین المللی",
        "فرستادن بار", "cargo", "shipping", "ارسال بین المللی",
        "صرافی", "currency exchange", "انتقال پول", "حواله",
        "بیمه مسافرتی", "travel insurance", "بیمه درمانی خارج",
        "بیمه دانشجویی", "student insurance", "بیمه خارج از کشور",
        "افتتاح حساب", "bank account", "حساب بانکی خارجی",
        "کارت اعتباری", "credit card", "دبیت کارت"
    ],
    
    'expat_life': [
        # زندگی در خارج
        "زندگی در خارج", "expat life", "living abroad",
        "تجربه مهاجرت", "immigration experience", "خاطرات مهاجرت",
        "دلتنگی", "homesick", "دوری از وطن", "غم غربت",
        "سازگاری", "adaptation", "تطبیق فرهنگی", "culture shock",
        "شوک فرهنگی", "یکپارچگی", "integration", "ادغام اجتماعی",
        "جامعه ایرانی", "Iranian community", "هموطنان",
        "ایرانیان مقیم", "Iranians abroad", "ایرانی ها در خارج",
        "دیدار هموطنان", "meetup", "گردهمایی ایرانیان",
        "جشن ایرانی", "Persian celebration", "نوروز خارج",
        "رستوران ایرانی", "Persian restaurant", "غذای ایرانی",
        "سوپرمارکت ایرانی", "Persian grocery", "مواد غذایی ایرانی",
        
    ],
    
    'immigration_community': [
        # گروه‌ها و انجمن‌ها
        "انجمن ایرانیان", "Iranian association", "جامعه ایرانی",
        "ایرانیان کانادا", "Iranians in Canada", "ایرانیان آمریکا",
        "ایرانیان آلمان", "Iranians in Germany", "ایرانیان استرالیا",
        "ایرانیان انگلیس", "Iranians in UK", "ایرانیان فرانسه",
        "ایرانیان هلند", "ایرانیان سوئد", "ایرانیان نروژ",
        "ایرانیان دبی", "ایرانیان امارات", "ایرانیان ترکیه",
        "ایرانیان تورنتو", "ایرانیان ونکوور", "ایرانیان لندن",
        "ایرانیان پاریس", "ایرانیان برلین", "ایرانیان سیدنی",
        "ایرانیان لس آنجلس", "ایرانیان نیویورک",
        "پرشین", "Persian", "فارسی زبان", "Farsi speaker",
        "هموطن", "compatriot", "ایرانی تبار", "Iranian origin",
        "نسل دوم", "second generation", "ایرانی-کانادایی",
        "ایرانی-آمریکایی", "Iranian-American",
        
        # موضوعات اجتماعی
        "انتخابات خارج", "رای خارج از کشور", "اخبار ایران",
        "وضعیت ایران", "آینده ایران", "بازگشت به ایران",
        "ارتباط با خانواده", "تماس با ایران", "خانواده در ایران",
        "ارسال پول به ایران", "حواله به ایران",
        "خرید ملک ایران", "سرمایه گذاری در ایران",
        "وضعیت حقوقی", "مشکلات حقوقی", "مشاوره حقوقی",
        "سربازی", "خدمت سربازی", "معافیت سربازی",
        "گذرنامه ایرانی", "تمدید پاسپورت", "کنسولگری ایران",
   ],
    
    'immigration_prefix': [
        # پیشوندهای مهاجرتی
        "گروه مهاجرین", "کانال مهاجرت", "انجمن ایرانیان",
        "راهنمای", "guide", "اخبار مهاجرت", "آموزش مهاجرت",
        "مشاوره", "consulting", "خدمات", "services",
        "اطلاعات", "info", "راهنما", "helper",
        "تجربیات", "experiences", "داستان مهاجرت",
        "موفقیت", "success", "فرصت", "opportunity",
        "جامعه", "community", "شبکه", "network",
        "انجمن", "association", "باشگاه", "club"
    ],
    
    'immigration_suffix': [
        # پسوندهای مهاجرتی
        "رایگان", "free", "تضمینی", "guaranteed",
        "معتبر", "certified", "رسمی", "official",
        "حرفه ای", "professional", "تخصصی", "specialized",
        "فوری", "urgent", "سریع", "fast",
        "آنلاین", "online", "مجازی", "virtual",
        "۲۰۲۴", "2024", "۲۰۲۵", "2025", "جدید", "new",
        "بروز", "updated", "کامل", "complete",
        # +50 immigration suffixes
        "پیوسته", "ongoing", "پایداری", "sustainable", "پاسپورت", "passport",
        "مجوز کار", "work permit", "ویزا تحصیلی", "student visa", "ویزا کاری", "work visa",
        "مشاوره رایگان", "free consultation", "مستند", "documented", "قابل اعتماد", "trustworthy",
        "اخبار", "news", "بروز", "updates", "کمیاب", "rare", "ویژه", "special",
        "اطلاعات", "info", "راهنمایی", "guidance", "پرسش و پاسخ", "Q&A"
    ],
    
    'immigration_topics': [
        # موضوعات تخصصی مهاجرت
        "نکات مهاجرت", "immigration tips", "اشتباهات رایج",
        "رد ویزا", "visa rejection", "علت ریجکت", "دلیل رد",
        "اپیل", "appeal", "اعتراض", "درخواست مجدد",
        "تمدید ویزا", "visa extension", "تبدیل ویزا",
        "تغییر استاتوس", "status change", "اقامت به شهروندی",
        "آزمون شهروندی", "citizenship test", "قسم وفاداری",
        "پرونده مهاجرتی", "immigration case", "وضعیت پرونده",
        "ردیابی پرونده", "case tracking", "شماره پرونده",
        "بایومتریک", "biometrics", "اثر انگشت",
        "معاینه پزشکی", "medical exam", "واکسیناسیون",
        "سوابق پلیسی", "background check", "امنیتی",
        "اسپانسر مالی", "financial sponsor", "ضامن",
        "دعوت نامه", "invitation letter", "نامه پذیرش",
        "قرارداد کاری", "job offer", "پیشنهاد شغلی",
        "LMIA", "ال ام آی ای", "مجوز بازار کار",
        
        # برنامه‌های مهاجرتی خاص
        "اکسپرس انتری", "Express Entry", "PNP", "پی ان پی",
        "نامزدی استانی", "Provincial Nominee",
        "فدرال اسکیلد", "Federal Skilled Worker",
        "کانادین اکسپریس", "Canadian Experience Class",
        "استارتاپ ویزا", "Startup Visa", "کارآفرین",
        "سلف امپلوید", "Self Employed", "خوداشتغالی",
        "سرمایه گذار کانادا", "Investor Program",
        "فمیلی اسپانسرشیپ", "Family Sponsorship",
        "سوپر ویزا", "Super Visa", "والدین",
        
        # امتیازبندی CRS
        "سی آر اس", "CRS", "امتیاز CRS", "CRS score",
        "امتیازبندی", "point system", "سن", "تحصیلات",
        "سابقه کار", "work experience", "زبان انگلیسی",
        "زبان فرانسوی", "French", "CLB", "سطح زبان",
        "سازگاری", "adaptability", "پیشنهاد کار کانادا",
        
        # اصطلاحات حقوقی
        "اقامت قانونی", "legal residence", "غیرقانونی",
        "دپورت", "deportation", "اخراج", "ممنوع الورود",
        "بلک لیست", "blacklist", "محرومیت", "ban",
        "وضعیت نامشخص", "pending", "در انتظار",
        "درخواست تجدیدنظر", "reconsideration"
    ],
    
    'diaspora_culture': [
        # فرهنگ و سنت
        "فرهنگ ایرانی", "Persian culture", "سنت ایرانی",
        "نوروز", "Nowruz", "عید نوروز", "Persian New Year",
        "چهارشنبه سوری", "سیزده بدر", "یلدا", "شب یلدا",
        "مهرگان", "تیرگان", "جشن ایرانی", "Persian festival",
        "موسیقی ایرانی", "Persian music", "موسیقی سنتی",
        "هنر ایرانی", "Persian art", "خط فارسی",
        "شعر فارسی", "Persian poetry", "حافظ", "سعدی", "فردوسی",
        "ادبیات فارسی", "Persian literature", "زبان فارسی",
        "آموزش فارسی", "learn Farsi", "فارسی برای کودکان",
        "مدرسه فارسی", "Persian school", "کلاس فارسی",
        "غذای ایرانی", "Persian food", "آشپزی ایرانی",
        "چلوکباب", "قرمه سبزی", "قیمه", "فسنجان", "آش",
        "رستوران پرشین", "Persian cuisine", "کترینگ ایرانی",
        "نان ایرانی", "Persian bread", "بربری", "سنگک",
        "شیرینی ایرانی", "Persian sweets", "باقلوا", "گز",
        
        # هویت و اجتماع
        "هویت ایرانی", "Iranian identity", "افتخار ایرانی",
        "ایرانی بودن", "being Iranian", "ریشه ایرانی",
        "نسل دوم ایرانی", "second generation Iranian",
        "ایرانی دوتابعیتی", "dual citizen", "دو ملیتی",
        "حفظ فرهنگ", "cultural preservation", "میراث فرهنگی",
        "ارتباط با ریشه", "connection to roots",
        "آموزش به فرزندان", "teaching children",
        "کتاب فارسی", "Persian book", "فیلم ایرانی",
        "سریال ایرانی", "Iranian series", "موزیک ایرانی"
    ],
    
    'practical_abroad': [
        # امور عملی
        "رانندگی در خارج", "driving abroad", "گواهینامه رانندگی",
        "تبدیل گواهینامه", "license conversion", "آزمون رانندگی",
        "خرید ماشین", "buying a car", "بیمه ماشین",
        "مالیات", "tax", "تکس", "اظهارنامه مالیاتی",
        "tax return", "مالیات بر درآمد", "income tax",
        "بازنشستگی", "retirement", "pension", "پنشن",
        "بیمه عمر", "life insurance", "بیمه سلامت",
        "health insurance", "دندانپزشکی خارج",
        "دکتر فارسی زبان", "Persian doctor",
        "وکیل فارسی زبان", "Persian lawyer",
        "حسابدار ایرانی", "Iranian accountant",
        "مشاور املاک", "real estate agent", "خرید خانه",
        "mortgage", "وام مسکن", "رهن و اجاره",
        "قرارداد اجاره", "rental agreement", "lease",
        "سیم کارت", "SIM card", "شماره تلفن خارجی",
        "اینترنت خارج", "internet abroad", "پست",
        "آدرس خارجی", "foreign address",
        # +50 practical abroad keywords
        
    ],
    
    # 🎮💻 بخش گسترش یافته: گیمینگ، برنامه‌نویسی و تکنولوژی (+500 کلمه جدید)
    'gaming_main': [
        # پلتفرم‌های بازی
        "گیمینگ", "gaming", "گیم", "game", "بازی", "بازی ویدیویی",
        "پی سی گیمینگ", "PC gaming", "کامپیوتر", "PC", "لپ تاپ گیمینگ",
        "کنسول", "console", "پلی استیشن", "PlayStation", "PS5", "PS4",
        "ایکس باکس", "Xbox", "Xbox Series X", "Xbox One",
        "نینتندو", "Nintendo", "سوییچ", "Switch", "نینتندو سوییچ",
        "استیم", "Steam", "اپیک گیمز", "Epic Games", "GOG",
        "اوریجین", "Origin", "یوپلی", "Uplay", "بتل نت", "Battle.net",
        "موبایل گیمینگ", "mobile gaming", "اندروید", "Android", "iOS",
        "گیم پس", "Game Pass", "پلی استیشن پلاس", "PS Plus",
        
        # ژانرهای بازی
        "اکشن", "action", "ماجراجویی", "adventure", "RPG", "آر پی جی",
        "نقش آفرینی", "role playing", "MMORPG", "ام ام او",
        "استراتژی", "strategy", "RTS", "آر تی اس", "ترن بیسد",
        "شوتر", "shooter", "FPS", "اف پی اس", "تیراندازی",
        "اسپرت", "sports", "ورزشی", "فوتبال", "فیفا", "FIFA", "PES",
        "ریسینگ", "racing", "مسابقه ای", "رانندگی",
        "هورر", "horror", "ترسناک", "سوروایول", "survival",
        "پازل", "puzzle", "معمایی", "فکری",
        "سیمولیشن", "simulation", "شبیه ساز",
        "سندباکس", "sandbox", "اوپن ورلد", "open world",
        "رتروگیم", "retro", "کلاسیک", "نوستالژی",
        "ایندی گیم", "indie game", "مستقل",
        "بتل رویال", "battle royale", "رویال",
        
        # بازی‌های محبوب
        "کال آف دیوتی", "Call of Duty", "COD", "وارزون", "Warzone",
        "فورتنایت", "Fortnite", "پابجی", "PUBG", "ببجی",
        "ایپکس", "Apex Legends", "ایپکس لجندز",
        "ولورانت", "Valorant", "والورنت",
        "کانتر", "Counter-Strike", "CS", "CS2", "CSGO",
        "لیگ آف لجندز", "League of Legends", "LOL", "لول",
        "دوتا", "Dota 2", "دوتا ۲",
        "مایکروسافت", "Minecraft", "ماین کرافت", "ماینکرفت",
        "جی تی ای", "GTA", "Grand Theft Auto", "جی تی ای ۵",
        "رد دد", "Red Dead Redemption", "RDR2",
        "الدن رینگ", "Elden Ring", "دارک سولز", "Dark Souls",
        "گاد آف وار", "God of War", "خدای جنگ",
        "اسپایدرمن", "Spider-Man", "مرد عنکبوتی",
        "فیفا", "FIFA", "اف سی", "FC 24", "EA Sports",
        "اساسینز کرید", "Assassin's Creed", "اسسینز",
        "فارکرای", "Far Cry", "رزیدنت ایول", "Resident Evil",
        "دیابلو", "Diablo", "وورلد آف وارکرفت", "World of Warcraft", "WoW",
        "هیلو", "Halo", "گیرز آو وار", "Gears of War",
        "آنچارتد", "Uncharted", "دت استرندینگ", "Death Stranding",
        "سایبرپانک", "Cyberpunk 2077", "سایبرپانک ۲۰۷۷",
        "هوگوارتز", "Hogwarts Legacy", "هاگوارتز",
        "زلدا", "Zelda", "تیرز آو کینگدم", "Tears of the Kingdom",
        "آموانگ آس", "Among Us", "امانگ اس",
        "جنشین", "Genshin Impact", "گنشین ایمپکت",
        "کلش", "Clash of Clans", "کلش آف کلنز", "کلش رویال",
        "فری فایر", "Free Fire", "فری فایر مکس",
        "برول استارز", "Brawl Stars", "براول استارز",
        
        # اصطلاحات گیمینگ
        "گیمر", "gamer", "بازیکن", "پلیر", "player",
        "لگ", "lag", "پینگ", "ping", "فریم", "frame", "FPS",
        "گرافیک", "graphics", "رندر", "render", "ریتریس", "ray tracing",
        "ریفرش ریت", "refresh rate", "هرتز", "Hz", "144Hz", "240Hz",
        "رزولوشن", "resolution", "4K", "1080p", "1440p",
        "کنترلر", "controller", "دسته", "گیم پد", "gamepad",
        "کیبورد", "keyboard", "موس", "mouse", "ماوس گیمینگ",
        "هدست", "headset", "هدفون گیمینگ", "میکروفون",
        "مانیتور", "monitor", "مانیتور گیمینگ",
        "کارت گرافیک", "GPU", "graphics card", "ای ام دی", "AMD",
        "انویدیا", "NVIDIA", "RTX", "جی فورس", "GeForce",
        "رم", "RAM", "پردازنده", "CPU", "اینتل", "Intel", "رایزن", "Ryzen",
        "اس اس دی", "SSD", "هارد", "hard drive",
        "استریم", "stream", "استریمر", "streamer", "لایو", "live",
        "تویچ", "Twitch", "یوتیوب گیمینگ", "YouTube Gaming",
        "کیک", "Kick", "فیسبوک گیمینگ",
        "چیت", "cheat", "هک", "hack", "آنتی چیت", "anti-cheat",
        "بن", "ban", "بن شدن", "اکانت", "account",
        "مولتی پلیر", "multiplayer", "آنلاین", "online",
        "سینگل پلیر", "single player", "آفلاین", "offline",
        "کوآپ", "co-op", "همکاری", "cooperative",
        "PvP", "پی وی پی", "PvE", "پی وی ای",
        "لانچر", "launcher", "آپدیت", "update", "پچ", "patch",
        "DLC", "دی ال سی", "اکسپنشن", "expansion",
        "سیزن پس", "season pass", "بتل پس", "battle pass"
    ],
    
    'esports': [
        # ایسپورت و مسابقات
        "ایسپورت", "esports", "e-sports", "رقابتی", "competitive",
        "تورنومنت", "tournament", "مسابقه", "competition",
        "لیگ", "league", "فینال", "final", "پلی آف", "playoff",
        "تیم ایسپورت", "esports team", "اسکواد", "squad",
        "پرو پلیر", "pro player", "حرفه ای", "professional",
        "کوچ", "coach", "مربی", "آنالیست", "analyst",
        "کستر", "caster", "گزارشگر", "شاوتکستر", "shoutcaster",
        "چمپیون", "champion", "قهرمان", "قهرمانی",
        "جام جهانی", "world cup", "ورلد چمپیونشیپ",
        "مجور", "major", "ماینور", "minor",
        "رنک", "rank", "رتبه", "دیویژن", "division",
        "MMR", "ام ام آر", "ELO", "الو", "ریتینگ",
        "لدر", "ladder", "لیدربورد", "leaderboard",
        "کلن", "clan", "گیلد", "guild",
        "اسکریم", "scrim", "تمرین", "practice",
        
        # تیم‌های معروف
        "فناتیک", "Fnatic", "جی تو", "G2", "تی ال", "Team Liquid",
        "کلود ناین", "Cloud9", "C9", "تی اس ام", "TSM",
        "فیز", "FaZe", "نیپ", "NaVi", "ویتالیتی", "Vitality",
        "سنتینلز", "Sentinels", "ان آر جی", "NRG"
    ],
    
    'programming_languages': [
        # زبان‌های برنامه‌نویسی
        "برنامه نویسی", "programming", "کدنویسی", "coding",
        "برنامه نویس", "programmer", "developer", "دولوپر",
        "پایتون", "Python", "پایتن",
        "جاوا", "Java", "جاوااسکریپت", "JavaScript", "JS",
        "تایپ اسکریپت", "TypeScript", "TS",
        "سی پلاس پلاس", "C++", "سی شارپ", "C#",
        "سی", "C", "زبان سی",
        "پی اچ پی", "PHP", "روبی", "Ruby",
        "گو", "Go", "Golang", "گولنگ",
        "راست", "Rust", "رست",
        "سوئیفت", "Swift", "کاتلین", "Kotlin",
        "اسکالا", "Scala", "کلوژر", "Clojure",
        "آر", "R", "متلب", "MATLAB",
        "اسکیو ال", "SQL", "نو اسکیو ال", "NoSQL",
        "اچ تی ام ال", "HTML", "سی اس اس", "CSS",
        "ساس", "Sass", "لس", "Less",
        "بش", "Bash", "شل", "Shell", "پاورشل", "PowerShell",
        "سالیدیتی", "Solidity", "راست وب ۳", "Rust Web3",
        "دارت", "Dart", "الم", "Elm",
        "هسکل", "Haskell", "ارلنگ", "Erlang", "الیکسیر", "Elixir",
        # +50 programming languages and related terms
        "Lua", "لوا", "Perl", "پرل", "Fortran", "فورترن", "COBOL", "کوبول",
        "Groovy", "گرووی", "Objective-C", "ObjC", "Assembly", "اسمبلی", "زبان ماشین", "machine code",
        "Prolog", "پرولوگ", "Ada", "آدا", "Forth", "فورت", "Nim", "نیم", "Zig", "زیگ",
        "Crystal", "کریستال", "Ballerina", "بالرینا", "Elvish", "Erlang/OTP", "OCaml", "اوکامال",
        "WebAssembly", "WASM", "Racket", "راکت", "Smalltalk", "سمال تاک", "Lisp", "لیسپ",
        "Scheme", "اسکیم", "Julia", "جولیا", "Solidity", "سالیدیتی (smart contracts)", "VBA", "وی بی ای"
    ],
    
    'programming_frameworks': [
        # فریم‌ورک‌ها و کتابخانه‌ها
        "فریم ورک", "framework", "کتابخانه", "library",
        "ری اکت", "React", "ری اکت نیتیو", "React Native",
        "انگولار", "Angular", "ویو", "Vue", "Vue.js",
        "نکست", "Next.js", "ناکست", "Nuxt.js",
        "سولت", "Svelte", "استلت کیت", "SvelteKit",
        "نود", "Node.js", "نود جی اس", "اکسپرس", "Express.js",
        "جنگو", "Django", "فلسک", "Flask", "فست ای پی آی", "FastAPI",
        "لاراول", "Laravel", "سیمفونی", "Symfony",
        "ریلز", "Rails", "روبی آن ریلز", "Ruby on Rails",
        "اسپرینگ", "Spring", "اسپرینگ بوت", "Spring Boot",
        "دات نت", ".NET", "ای اس پی", "ASP.NET",
        "فلاتر", "Flutter", "سوئیفت یو آی", "SwiftUI",
        "الکترون", "Electron", "تائوری", "Tauri",
        "تنسورفلو", "TensorFlow", "پای تورچ", "PyTorch",
        "کراس", "Keras", "سای کیت لرن", "scikit-learn",
        "پانداز", "Pandas", "نام پای", "NumPy",
        "بوت استرپ", "Bootstrap", "تیلویند", "Tailwind CSS",
        "متریال یو آی", "Material UI", "چاکرا", "Chakra UI",
        # +50 frameworks additions
        "Gin", "Go framework", "Echo", "Fiber", "Iris", "Hapi", "Koa",
        "Preact", "Lit", "Stencil", "Polymer", "Backbone", "Ember",
        "Bottle", "Sanic", "Tornado", "Hug", "Falcon", "Masonite",
        "Blazor", "Avalonia", "WPF", "MAUI", "WinForms", "GTK",
        "Ruby Sinatra", "Sinatra", "Hanami", "Grape", "Phoenix", "Elixir Phoenix",
        "Meteor", "Sails", "FeathersJS", "AdonisJS", "LoopBack", "NestJS"
    ],
    
    'programming_tools': [
        # ابزارهای برنامه‌نویسی
        "آی دی ای", "IDE", "ادیتور", "editor", "کد ادیتور",
        "وی اس کد", "VS Code", "Visual Studio Code",
        "ویژوال استودیو", "Visual Studio",
        "اینتلی جی", "IntelliJ", "پای چارم", "PyCharm",
        "وب استورم", "WebStorm", "اندروید استودیو", "Android Studio",
        "ایکس کد", "Xcode", "اتم", "Atom", "سابلایم", "Sublime Text",
        "نیوویم", "Neovim", "ویم", "Vim", "ایمکس", "Emacs",
        "گیت", "Git", "گیت هاب", "GitHub", "گیت لب", "GitLab",
        "بیت باکت", "Bitbucket", "ورژن کنترل", "version control",
        "کامیت", "commit", "پوش", "push", "پول", "pull",
        "برنچ", "branch", "مرج", "merge", "پول ریکوئست", "pull request",
        "دیپلوی", "deploy", "سی آی سی دی", "CI/CD",
        "جنکینز", "Jenkins", "گیت هاب اکشنز", "GitHub Actions",
        "داکر", "Docker", "کوبرنتیز", "Kubernetes", "K8s",
        "ترمینال", "terminal", "کامند لاین", "command line", "CLI",
        "ای پی آی", "API", "رست", "REST", "گراف کیو ال", "GraphQL",
        "جیسون", "JSON", "ایکس ام ال", "XML", "یامل", "YAML",
        "پستمن", "Postman", "اینسومنیا", "Insomnia",
        "وب پک", "Webpack", "ویت", "Vite", "رولاپ", "Rollup",
        "ان پی ام", "npm", "یارن", "Yarn", "پی ان پی ام", "pnpm",
        "پیپ", "pip", "کوندا", "Conda", "پویتری", "Poetry"
    ],
    
    'web_development': [
        # توسعه وب
        "وب", "web", "توسعه وب", "web development", "وب دولوپمنت",
        "فرانت اند", "frontend", "فرانت", "front-end",
        "بک اند", "backend", "بکند", "back-end",
        "فول استک", "full stack", "فول استک دولوپر",
        "ریسپانسیو", "responsive", "موبایل فرست", "mobile-first",
        "یو آی", "UI", "یو ایکس", "UX", "رابط کاربری",
        "تجربه کاربری", "user experience", "طراحی وب",
        "فیگما", "Figma", "اسکچ", "Sketch", "ادوبی ایکس دی", "Adobe XD",
        "وردپرس", "WordPress", "شاپیفای", "Shopify",
        "ووکامرس", "WooCommerce", "مجنتو", "Magento",
        "هاستینگ", "hosting", "سرور", "server", "VPS",
        "دامین", "domain", "دامنه", "SSL", "HTTPS",
        "سئو", "SEO", "بهینه سازی", "optimization",
        "پرفورمنس", "performance", "سرعت سایت"
    ],
    'mobile_development': [
        # توسعه موبایل
        "موبایل", "mobile", "اپ", "app", "اپلیکیشن", "application",
        "اندروید", "Android", "آی او اس", "iOS", "آیفون", "iPhone",
        "اپ استور", "App Store", "گوگل پلی", "Google Play", "پلی استور",
        "ری اکت نیتیو", "React Native", "فلاتر", "Flutter",
        "سوئیفت", "Swift", "کاتلین", "Kotlin",
        "آی او اس دولوپر", "iOS developer", "اندروید دولوپر",
        "هیبریدی", "hybrid", "نیتیو", "native",
        "پوش نوتیفیکیشن", "push notification",
        "اپ استور اپتیمایزیشن", "ASO", "بهینه سازی اپ",
        # +50 mobile development keywords
        "Ionic", "کوردووا", "Cordova", "زونو", "Xamarin", "ایونیک", "PWA", "پروگرسیو وب اپ",
        "UI موبایل", "mobile UI", "Material Design", "SwiftUI", "Jetpack Compose",
        "Kotlin Multiplatform", "KMP", "React Native CLI", "Expo", "Native modules",
        "ARKit", "CoreML", "ARCore", "Firebase", "Crashlytics", "Analytics",
        "Play Console", "App Store Connect", "TestFlight", "Gradle", "Android Studio", "Xcode",
        "app signing", "code signing", "CI/CD", "continuous integration", "app review", "review process",
        "مونیٹائزیشن اپ", "app monetization", "IAP", "in-app purchase", "AdMob", "ads integration"
    ],
    
    'data_science_ai': [
        # داده و هوش مصنوعی
        "داده", "data", "دیتا", "دیتا ساینس", "data science",
        "علم داده", "ساینتیست", "data scientist",
        "ماشین لرنینگ", "machine learning", "یادگیری ماشین", "ML",
        "هوش مصنوعی", "AI", "artificial intelligence",
        "دیپ لرنینگ", "deep learning", "یادگیری عمیق",
        "نورال نتورک", "neural network", "شبکه عصبی",
        "ان ال پی", "NLP", "پردازش زبان طبیعی",
        "کامپیوتر ویژن", "computer vision", "بینایی ماشین",
        "بیگ دیتا", "big data", "کلان داده",
        "داده کاوی", "data mining", "تحلیل داده", "data analysis",
        "چت جی پی تی", "ChatGPT", "جی پی تی", "GPT", "جمنای", "Gemini",
        "کلود", "Claude", "اوپن ای آی", "OpenAI",
        "میدجرنی", "Midjourney", "دالی", "DALL-E", "استیبل دیفیوژن",
        "هاگینگ فیس", "Hugging Face", "ال ال ام", "LLM",
        "پرامپت", "prompt", "پرامپت انجینیرینگ"
    ],
    
    'cloud_devops': [
        # کلود و دواپس
        "کلود", "cloud", "ابری", "رایانش ابری",
        "آی دبلیو اس", "AWS", "آمازون وب سرویس", "Amazon Web Services",
        "اژور", "Azure", "مایکروسافت اژور", "Microsoft Azure",
        "گوگل کلود", "Google Cloud", "GCP",
        "دیجیتال اوشن", "DigitalOcean", "لینود", "Linode",
        "ورسل", "Vercel", "نتلیفای", "Netlify", "هروکو", "Heroku",
        "دواپس", "DevOps", "اس آر ای", "SRE",
        "سرور", "server", "سرورلس", "serverless",
        "لامبدا", "Lambda", "فانکشن", "function",
        "ایو اس", "S3", "ای سی تو", "EC2",
        "کانتینر", "container", "داکر", "Docker",
        "کوبرنتیز", "Kubernetes", "K8s", "هلم", "Helm",
        "ترافورم", "Terraform", "انسیبل", "Ansible",
        "پرومتئوس", "Prometheus", "گرافانا", "Grafana",
        "لاگ", "log", "مانیتورینگ", "monitoring"
    ],
    
    'cybersecurity': [
        # امنیت سایبری
        "امنیت", "security", "سایبری", "cyber", "امنیت سایبری",
        "هک", "hack", "هکر", "hacker", "هکینگ", "hacking",
        "اتیکال هکینگ", "ethical hacking", "هکر کلاه سفید",
        "پنتست", "pentest", "تست نفوذ", "penetration testing",
        "باگ بانتی", "bug bounty", "باگ هانتر", "bug hunter",
        "آسیب پذیری", "vulnerability", "اکسپلویت", "exploit",
        "CVE", "سی وی ای", "زیرو دی", "zero-day",
        "فایروال", "firewall", "آنتی ویروس", "antivirus",
        "رمزنگاری", "encryption", "کریپتوگرافی", "cryptography",
        "پسورد", "password", "احراز هویت", "authentication",
        "دو عاملی", "2FA", "بیومتریک", "biometric",
        "فیشینگ", "phishing", "مالور", "malware", "ویروس", "virus",
        "رنسوموور", "ransomware", "باج افزار",
        "دی داس", "DDoS", "حمله", "attack",
        "VPN", "وی پی ان", "پروکسی", "proxy", "تور", "Tor",
        "کالی", "Kali", "کالی لینوکس", "Kali Linux",
        "باگ", "bug", "دیباگ", "debug", "لاگ", "log",
        "سی تی اف", "CTF", "Capture The Flag"
    ],
    
    'linux_opensource': [
        # لینوکس و اوپن سورس
        "لینوکس", "Linux", "لینوکسی", "یونیکس", "Unix",
        "اوبونتو", "Ubuntu", "دبیان", "Debian",
        "فدورا", "Fedora", "سنت او اس", "CentOS", "رد هت", "Red Hat",
        "آرچ", "Arch", "آرچ لینوکس", "Arch Linux", "مانجارو", "Manjaro",
        "لینوکس مینت", "Linux Mint", "پاپ او اس", "Pop!_OS",
        "کرنل", "kernel", "توزیع", "distro", "distribution",
        "گنوم", "GNOME", "کی دی ای", "KDE", "ایکس اف سی ای", "XFCE",
        "اوپن سورس", "open source", "متن باز", "فری سافتور",
        "گنو", "GNU", "آپاچی", "Apache", "ان جین ایکس", "Nginx",
        "مای اسکیو ال", "MySQL", "پستگرس", "PostgreSQL",
        "مونگو دی بی", "MongoDB", "ردیس", "Redis",
        "گیت", "Git", "بش", "Bash", "شل اسکریپت", "shell script"
    ],
    
    'tech_gadgets': [
        # گجت و تکنولوژی
        "گجت", "gadget", "تکنولوژی", "technology", "فناوری",
        "گوشی", "phone", "موبایل", "mobile", "اسمارت فون", "smartphone",
        "آیفون", "iPhone", "سامسونگ", "Samsung", "شیائومی", "Xiaomi",
        "وان پلاس", "OnePlus", "پیکسل", "Pixel", "گوگل پیکسل",
        "هواوی", "Huawei", "اوپو", "Oppo", "ویوو", "Vivo",
        "لپ تاپ", "laptop", "نوت بوک", "notebook",
        "مک بوک", "MacBook", "آی مک", "iMac", "مک", "Mac",
        "تینک پد", "ThinkPad", "دل", "Dell", "ایسوس", "ASUS",
        "لنوو", "Lenovo", "اچ پی", "HP", "ایسر", "Acer", "MSI",
        "تبلت", "tablet", "آیپد", "iPad", "گلکسی تب", "Galaxy Tab",
        "ساعت هوشمند", "smartwatch", "اپل واچ", "Apple Watch",
        "گلکسی واچ", "Galaxy Watch", "فیت بیت", "Fitbit",
        "ایرپاد", "AirPods", "گلکسی بادز", "Galaxy Buds",
        "هدفون", "headphone", "ایرفون", "earphone", "بلوتوث",
        "اسپیکر", "speaker", "ساندبار", "soundbar",
        "تلویزیون", "TV", "اسمارت تی وی", "smart TV",
        "الجی", "LG", "سونی", "Sony", "تی سی ال", "TCL",
        "رزبری پای", "Raspberry Pi", "آردوینو", "Arduino",
        "درون", "drone", "کوادکوپتر", "quadcopter", "دی جی آی", "DJI",
        "وی آر", "VR", "واقعیت مجازی", "virtual reality",
        "ای آر", "AR", "واقعیت افزوده", "augmented reality",
        "متا کوئست", "Meta Quest", "اوکولوس", "Oculus"
    ],
    
    'tech_news': [
        # اخبار تکنولوژی
        "اخبار تکنولوژی", "tech news", "اخبار فناوری",
        "بررسی", "review", "آنباکسینگ", "unboxing",
        "مقایسه", "comparison", "بنچمارک", "benchmark",
        "رونمایی", "launch", "معرفی", "announcement",
        "تک کرانچ", "TechCrunch", "ورج", "The Verge",
        "وایرد", "Wired", "سی نت", "CNET", "انگجت", "Engadget",
        "گیزمودو", "Gizmodo", "آرس تکنیکا", "Ars Technica",
        "تکنولایف", "ویرگول", "زومیت", "دیجیاتو",
        "تک بلاگ", "tech blog", "تکنوفیل", "technophile",
        # +50 tech news additions
        "AI breakthrough", "پیشرفت هوش مصنوعی", "funding", "بودجه استارتاپ", "acquisition", "خرید استارتاپ",
        "merger", "ادغام", "security breach", "نشت داده", "data leak", "لو رفتن دیتا",
        "privacy", "حریم خصوصی", "regulation", "قانون گذاری", "policy", "سیاست تکنولوژی",
        "chip news", "چیپ جدید", "arm", "آرم", "intel", "اینتل",
        "open source", "متن باز", "contributor", "مشارکت", "hackathon", "رویداد برنامه نویسی",
        "developer tools", "ابزار برنامه نویس", "sdk", "kit توسعه" , "API news", "اخبار API",
        "cloud outage", "قطعی سرویس ابری", "downtime", "اختلال", "pwned", "db leak"
    ],
    
    'telegram_tech_groups': [
        # نام گروه‌های تلگرام تکنولوژی
        "گروه برنامه نویسی", "برنامه نویسان ایران",
        "پایتون ایران", "جاوا ایران", "جاوااسکریپت ایران",
        "ری اکت ایران", "فرانت اند ایران", "بک اند ایران",
        "اندروید دولوپرز", "iOS دولوپرز",
        "دیتا ساینس ایران", "هوش مصنوعی ایران",
        "لینوکس ایران", "امنیت سایبری ایران",
        "گیمرهای ایران", "گیمینگ فارسی",
        "پابجی ایران", "کالاف ایران", "فورتنایت ایران",
        "پی سی گیمرز", "کنسول گیمرز",
        "استیم ایران", "پلی استیشن ایران", "ایکس باکس ایران",
        "تکنولوژی ایران", "گجت ایران", "موبایل ایران",
        "لپ تاپ ایران", "کامپیوتر ایران",
        "کریپتو دولوپرز", "بلاکچین ایران",
        "استارتاپ ایران", "کارآفرینی دیجیتال",
        
        # نام‌های انگلیسی
        "Python Developers", "JavaScript Community",
        # +50 telegram tech group names
        "Full Stack Devs", "Fullstack developers", "DevOps Iran", "SRE Iran", "Kubernetes Iran", "Docker Iran",
        "Cloud Engineers", "Cloud DevOps", "AWS Iran", "Azure Iran", "GCP Iran", "Database Admins",
        "DBA group", "Postgres Iran", "MongoDB Iran", "Redis Iran", "Firebase Iran", "Realtime apps",
        "AI Researchers", "ML Engineers", "NLP Iran", "Computer Vision Iran", "Data Scientists", "Data Engineers",
        "Open Source Iran", "Contributors", "Hackathon Iran", "Tech Events", "Startup Founders", "Founders circle",
        "QA Engineers", "Test Automation Iran", "Security Researchers", "CTF Iran", "Bug Bounty", "Bounty hunters",
        "Embedded Systems", "IoT Makers", "Raspberry Pi Projects", "Arduino Fans", "Electronics DIY"
        "React Developers", "Frontend Masters",
        "Backend Engineers", "Full Stack Devs",
        "Data Scientists", "AI Enthusiasts",
        "Linux Users", "Cybersecurity Pros",
        "Gamers United", "PC Gaming Club",
        "Tech Enthusiasts", "Gadget Lovers",
        "Crypto Devs", "Web3 Builders"
    ],
    
    'startup_business': [
        # استارتاپ و کسب و کار
        "استارتاپ", "startup", "استارت آپ", "کارآفرینی", "entrepreneurship",
        "کسب و کار", "business", "بیزینس",
        "سرمایه گذاری", "investment", "سرمایه گذار", "investor",
        "ونچر", "venture", "VC", "ونچر کپیتال", "venture capital",
        "اکسلریتور", "accelerator", "شتابدهنده",
        "انکوبیتور", "incubator", "مرکز نوآوری",
        "پیچ", "pitch", "پیچ دک", "pitch deck",
        "MVP", "ام وی پی", "محصول حداقلی",
        "پی ام اف", "PMF", "product market fit",
        "فاندینگ", "funding", "سید", "seed", "سری ای", "Series A",
        "بوتسترپ", "bootstrap", "خودگردان",
        "گروث", "growth", "رشد", "اسکیل", "scale",
        "پیوت", "pivot", "تغییر مسیر",
        "یونیکورن", "unicorn", "دکاکورن", "decacorn",
        "اگزیت", "exit", "آی پی او", "IPO"
    ],
    
    # 🤖 بخش جدید: هوش مصنوعی و یادگیری ماشین (+100 کلمه کاربردی)
    'ai_extended': [
        # نام‌های گروه‌های تلگرامی AI فارسی
        "هوش مصنوعی ایران", "AI ایران", "انجمن هوش مصنوعی",
        "گروه هوش مصنوعی", "چت جی پی تی فارسی", "ChatGPT فارسی",
        "جی پی تی ایران", "GPT ایران", "یادگیری ماشین ایران",
        "ماشین لرنینگ فارسی", "دیپ لرنینگ ایران", "پایتون AI",
        "تنسورفلو ایران", "پای تورچ ایران", "دیتا ساینس فارسی",
        "علم داده ایران", "پردازش زبان طبیعی", "NLP فارسی",
        "بینایی ماشین ایران", "Computer Vision فارسی",
        "ربات‌های هوشمند", "اتوماسیون هوشمند",
        
        # نام‌های گروه‌های تلگرامی AI انگلیسی
        "AI Community", "ChatGPT Users", "GPT Enthusiasts",
        "Machine Learning Hub", "Deep Learning Group",
        "AI Developers", "ML Engineers", "Data Science Community",
        "NLP Research", "Computer Vision AI", "AI Research",
        "Generative AI", "AI Art", "AI Tools",
        "OpenAI Community", "Anthropic Users", "Claude AI",
        "Gemini AI", "Bard Users", "Copilot Users",
        "AI Automation", "AI Business", "AI Startups",
        
        # ابزارها و پلتفرم‌های AI
        "چت جی پی تی", "ChatGPT", "جی پی تی ۴", "GPT-4", "GPT-4o",
        "کلود", "Claude", "کلود سونت", "Claude Sonnet",
        "جمینای", "Gemini", "گوگل جمینای", "Google Gemini",
        "کوپایلت", "Copilot", "مایکروسافت کوپایلت",
        "میدجرنی", "Midjourney", "میدجورنی",
        "دالی", "DALL-E", "دال ای", "DALL-E 3",
        "استیبل دیفیوژن", "Stable Diffusion", "SD",
        "لئوناردو", "Leonardo AI", "ادوبی فایرفلای", "Adobe Firefly",
        "ایدئوگرام", "Ideogram", "پلیگراند", "Playground AI",
        "رانوی", "Runway", "ران وی", "Runway ML",
        "پیکا", "Pika", "پیکا لبز", "سورا", "Sora",
        "هگینگ فیس", "Hugging Face", "هاگینگ فیس",
        "اوپن ای آی", "OpenAI", "آنتروپیک", "Anthropic",
        "گوگل دیپ مایند", "DeepMind", "متا ای آی", "Meta AI",
        "لاما", "LLaMA", "میسترال", "Mistral",
        "کوهیر", "Cohere", "پرپلکسیتی", "Perplexity",
        
        # مفاهیم و اصطلاحات AI
        "پرامپت", "prompt", "پرامپتینگ", "prompting",
        "پرامپت انجینیرینگ", "prompt engineering",
        "ال ال ام", "LLM", "مدل زبانی بزرگ",
        "ترنسفورمر", "transformer", "اتنشن", "attention",
        "فاین تیون", "fine-tuning", "فاین تیونینگ",
        "ریگ", "RAG", "بازیابی تقویت شده",
        "امبدینگ", "embedding", "وکتور", "vector",
        "توکن", "token", "توکنایزر", "tokenizer",
        "اینفرنس", "inference", "استنتاج",
        "ترینینگ", "training", "آموزش مدل",
        "دیتاست", "dataset", "مجموعه داده",
        "بنچمارک", "benchmark", "ارزیابی مدل",
        "هالوسینیشن", "hallucination", "توهم",
        "کانتکست", "context", "زمینه",
        "چت بات", "chatbot", "ربات چت",
        "اسیستنت", "assistant", "دستیار هوشمند",
        "جنریتیو", "generative", "مولد",
        "ای جی آی", "AGI", "هوش مصنوعی عمومی",
        "اتومیشن", "automation", "خودکارسازی",
        "ای آی اجنت", "AI agent", "عامل هوشمند"
    ],
    
    # 🌟 بخش عمومی: کلمات کلیدی متنوع برای گروه‌های تلگرامی (+1000 کلمه)
    'general_groups': [
        # 💼 کار و استخدام
        "کاریابی", "استخدام", "job", "کار", "شغل", "فرصت شغلی",
        "رزومه", "CV", "مصاحبه", "interview", "حقوق", "salary",
        "کارآموز", "intern", "فریلنسر", "freelance", "دورکاری", "remote",
        "پاره وقت", "part time", "تمام وقت", "full time",
        "منابع انسانی", "HR", "جذب نیرو", "hiring",
        "لینکدین", "LinkedIn", "کارفرما", "employer",
        "استخدام تهران", "کار اصفهان", "استخدام مشهد",
        "کار برنامه نویس", "استخدام حسابدار", "کار گرافیست",
        "استخدام پرستار", "کار دیجیتال مارکتینگ",
        
        # 📚 آموزش و تحصیل
        "آموزش", "تحصیل", "دانشگاه", "university", "کنکور",
        "زبان انگلیسی", "English", "آیلتس", "IELTS", "تافل", "TOEFL",
        "زبان آلمانی", "German", "زبان فرانسه", "French",
        "ریاضی", "فیزیک", "شیمی", "زیست", "ادبیات",
        "دبیرستان", "متوسطه", "ابتدایی", "کلاس آنلاین",
        "تدریس خصوصی", "معلم", "استاد", "دانشجو",
        "پایان نامه", "مقاله", "تحقیق", "research",
        "کتاب", "book", "کتابخانه", "library",
        "MBA", "ام بی ای", "دکتری", "PhD", "ارشد", "masters",
        "بورسیه", "scholarship", "پذیرش", "admission",
        # افزودنی‌ها: 50 کلمه کلیدی عمومی مناسب نام گروه
        "کاریابی آنلاین", "remote jobs", "بدون واسطه", "direct hiring", "همکاری", "collaboration",
        "شبکه سازی", "networking", "کارآفرینی جوانان", "startup hub", "آموزش رایگان", "free course",
        "پروژه های فریلنس", "freelance projects", "راهنمای شغلی", "career tips", "مهارت های نرم", "soft skills",
        "توسعه فردی", "self improvement", "مهارت های حرفه ای", "pro skills", "ترندهای کاری", "job trends",
        "کار مجازی", "virtual work", "دورکاری ایران", "remote Iran", "تقاضا و عرضه", "supply demand",
        "پادکست آموزشی", "edu podcast", "آموزشگاه", "online academy", "دوره تخصصی", "specialty course",
        "گفتگوهای تخصصی", "expert chat", "روزنامه دانشجویی", "student news", "معرفی کتاب", "book club",
        "انجمن نویسندگان", "writers club", "عکاسی موبایلی", "mobile photography", "گیمینگ فارسی", "Persian gaming",
        "طراحی داخلی", "interior design", "معماری نو", "modern architecture", "ویدیو سازان", "video creators",
        "تولید محتوا فارسی", "content creators", "بازاریابی دیجیتال", "digital marketing", "سئو ایران", "SEO Iran",
        "تحلیل بازار", "market analysis", "کارآفرینی اجتماعی", "social entrepreneurship"
        
        # 🎨 هنر و طراحی
        "طراحی", "design", "گرافیک", "graphic", "لوگو", "logo",
        "فتوشاپ", "Photoshop", "ایلوستریتور", "Illustrator",
        "افترافکت", "After Effects", "پریمیر", "Premiere",
        "انیمیشن", "animation", "موشن گرافیک", "motion graphic",
        "عکاسی", "photography", "عکس", "photo", "ادیت عکس",
        "ویدیو", "video", "فیلمبرداری", "تدوین", "editing",
        "UI", "UX", "رابط کاربری", "فیگما", "Figma",
        "کانوا", "Canva", "پوستر", "بنر", "banner",
        "نقاشی", "painting", "هنر", "art", "خوشنویسی",
        "معماری", "architecture", "دکوراسیون", "interior",
        
        # 🎵 موسیقی و سرگرمی
        "موسیقی", "music", "آهنگ", "song", "موزیک",
        "پاپ", "pop", "راک", "rock", "رپ", "rap", "هیپ هاپ",
        "سنتی", "کلاسیک", "classical", "جز", "jazz",
        "گیتار", "guitar", "پیانو", "piano", "ویولن", "violin",
        "دی جی", "DJ", "میکس", "mix", "ریمیکس", "remix",
        "پادکست", "podcast", "رادیو", "radio",
        "کنسرت", "concert", "لایو", "live",
        "خواننده", "singer", "نوازنده", "musician",
        "آلبوم", "album", "ترک", "track",
        
        # 🎬 فیلم و سریال
        "فیلم", "movie", "سریال", "series", "سینما", "cinema",
        "نتفلیکس", "Netflix", "دیزنی پلاس", "Disney Plus",
        "آمازون پرایم", "Amazon Prime", "HBO",
        "انیمه", "anime", "کارتون", "cartoon",
        "هالیوود", "Hollywood", "بالیوود", "Bollywood",
        "کره ای", "Korean", "ترکی", "Turkish",
        "زیرنویس", "subtitle", "دوبله", "dubbed",
        "تریلر", "trailer", "نقد فیلم", "review",
        "بازیگر", "actor", "کارگردان", "director",
        "اسکار", "Oscar", "فستیوال", "festival",
        
        # 📖 کتاب و ادبیات
        "کتاب", "book", "رمان", "novel", "داستان", "story",
        "شعر", "poetry", "نویسنده", "writer", "نویسندگی",
        "کتاب صوتی", "audiobook", "ایبوک", "ebook",
        "کتابخوانی", "reading", "کتابفروشی", "bookstore",
        "ترجمه", "translation", "مترجم", "translator",
        "روانشناسی", "psychology", "خودشناسی", "self help",
        "موفقیت", "success", "انگیزشی", "motivation",
        "فلسفه", "philosophy", "تاریخ", "history",
        
        # 🏋️ ورزش و سلامت
        "ورزش", "sport", "فوتبال", "football", "soccer",
        "والیبال", "volleyball", "بسکتبال", "basketball",
        "تنیس", "tennis", "شنا", "swimming", "دوچرخه سواری",
        "بدنسازی", "gym", "فیتنس", "fitness", "باشگاه",
        "یوگا", "yoga", "پیلاتس", "pilates", "ایروبیک",
        "رژیم", "diet", "لاغری", "weight loss", "تناسب اندام",
        "تغذیه", "nutrition", "سالم", "healthy",
        "دویدن", "running", "ماراتن", "marathon",
        "رزمی", "martial arts", "کاراته", "بوکس", "boxing",
        "پرسپولیس", "استقلال", "لیگ برتر", "جام جهانی",
        "لیورپول", "منچستر", "بارسلونا", "رئال مادرید",
        "مسی", "رونالدو", "فوتسال", "futsal",
        
        # 🍳 آشپزی و غذا
        "آشپزی", "cooking", "غذا", "food", "آشپز", "chef",
        "رستوران", "restaurant", "کافه", "cafe", "کافی شاپ",
        "دستور پخت", "recipe", "شیرینی", "dessert", "کیک", "cake",
        "پیتزا", "pizza", "فست فود", "fast food", "ساندویچ",
        "سالاد", "salad", "سوپ", "soup", "نوشیدنی", "drink",
        "قهوه", "coffee", "چای", "tea", "آبمیوه", "juice",
        "گیاهخواری", "vegan", "وگان", "گیاهی", "vegetarian",
        "رژیمی", "diet food", "سالم", "healthy food",
        "نان", "bread", "پاستا", "pasta", "برنج", "rice",
        
        # 🏠 خانه و زندگی
        "خانه", "home", "آپارتمان", "apartment", "ویلا", "villa",
        "اجاره", "rent", "خرید خانه", "رهن", "مسکن", "housing",
        "املاک", "real estate", "مشاور املاک",
        "دکوراسیون", "decoration", "مبلمان", "furniture",
        "باغبانی", "gardening", "گل", "flower", "گیاه", "plant",
        "تمیز کردن", "cleaning", "نظافت", "خانه تکانی",
        "لوازم خانگی", "appliance", "آشپزخانه", "kitchen",
        "حمام", "bathroom", "اتاق خواب", "bedroom",
        
        # 👶 خانواده و کودک
        "خانواده", "family", "کودک", "child", "بچه", "kid",
        "مادر", "mother", "پدر", "father", "والدین", "parents",
        "نوزاد", "baby", "شیرخوار", "infant", "بارداری", "pregnancy",
        "تربیت کودک", "parenting", "فرزندپروری",
        "اسباب بازی", "toy", "بازی کودک", "کاردستی",
        "لباس بچه", "کفش بچه", "سیسمونی",
        "مدرسه", "school", "مهدکودک", "kindergarten",
        
        # 💄 مد و زیبایی
        "مد", "fashion", "لباس", "clothing", "پوشاک",
        "استایل", "style", "ترند", "trend", "فشن",
        "آرایش", "makeup", "آرایشی", "cosmetics",
        "مراقبت پوست", "skincare", "کرم", "cream", "سرم", "serum",
        "مو", "hair", "مدل مو", "hairstyle", "رنگ مو",
        "ناخن", "nail", "مانیکور", "manicure", "پدیکور",
        "عطر", "perfume", "ادکلن", "cologne",
        "کیف", "bag", "کفش", "shoes", "اکسسوری", "accessory",
        "زیورآلات", "jewelry", "طلا", "gold", "نقره", "silver",
        "برند", "brand", "زارا", "Zara", "اچ اند ام", "H&M",
        
        # 🐾 حیوانات
        "حیوانات", "animals", "پت", "pet", "حیوان خانگی",
        "سگ", "dog", "گربه", "cat", "پرنده", "bird",
        "ماهی", "fish", "آکواریوم", "aquarium",
        "خرگوش", "rabbit", "همستر", "hamster",
        "غذای حیوانات", "pet food", "دامپزشک", "vet",
        "نگهداری سگ", "نگهداری گربه", "پتشاپ", "pet shop",
        
        # 🚗 خودرو و موتور
        "خودرو", "car", "ماشین", "اتومبیل", "automobile",
        "موتور", "motorcycle", "موتورسیکلت",
        "خرید ماشین", "فروش ماشین", "قیمت خودرو",
        "پراید", "تیبا", "سمند", "پژو", "دنا", "هایما",
        "بنز", "BMW", "تویوتا", "هیوندای", "کیا",
        "تعمیر خودرو", "مکانیکی", "صافکاری",
        "لوازم یدکی", "spare parts", "آپشن",
        "بیمه ماشین", "گواهینامه", "آموزش رانندگی",
        
        # 🛒 خرید و فروش
        "خرید", "buy", "فروش", "sell", "فروشگاه", "shop",
        "بازار", "market", "مال", "mall", "پاساژ",
        "آنلاین شاپ", "online shop", "اینترنتی",
        "دیجی کالا", "digikala", "باسلام", "ترب", "torob",
        "تخفیف", "discount", "حراج", "sale", "کد تخفیف",
        "ارسال رایگان", "free shipping", "پست",
        "کارت هدیه", "gift card", "خرید گروهی",
        
        # 💰 مالی و اقتصاد
        "پول", "money", "درآمد", "income", "ثروت", "wealth",
        "سرمایه گذاری", "investment", "پس انداز", "saving",
        "بانک", "bank", "وام", "loan", "اقساط",
        "بیمه", "insurance", "مالیات", "tax",
        "دلار", "dollar", "یورو", "euro", "ارز",
        "طلا", "gold", "سکه", "coin", "نقره",
        "بورس تهران", "سهام", "stock", "اوراق",
        "بازنشستگی", "retirement", "مستمری",
        "حسابداری", "accounting", "حسابدار",
        "اقتصاد", "economy", "تورم", "inflation",
        
        # ✈️ سفر و گردشگری
        "سفر", "travel", "گردشگری", "tourism", "تور",
        "هتل", "hotel", "اقامتگاه", "رزرو هتل",
        "پرواز", "flight", "بلیط هواپیما", "airline",
        "ویزا", "visa", "پاسپورت", "passport",
        "کیش", "قشم", "شیراز", "اصفهان", "مشهد",
        "ترکیه", "Turkey", "دبی", "Dubai", "تایلند",
        "مالدیو", "Maldives", "یونان", "Greece",
        "کوله گردی", "backpacking", "کمپینگ", "camping",
        "ساحل", "beach", "کوه", "mountain", "جنگل", "forest",
        "موزه", "museum", "تاریخی", "historical",
        
        # 📱 شبکه‌های اجتماعی
        "اینستاگرام", "Instagram", "اینستا", "insta",
        "تلگرام", "Telegram", "واتساپ", "WhatsApp",
        "توییتر", "Twitter", "ایکس", "X",
        "یوتیوب", "YouTube", "تیک تاک", "TikTok",
        "فیسبوک", "Facebook", "لینکدین", "LinkedIn",
        "پینترست", "Pinterest", "اسنپ چت", "Snapchat",
        "ریلز", "Reels", "استوری", "Story", "لایو", "Live",
        "فالور", "follower", "لایک", "like", "کامنت", "comment",
        "اینفلوئنسر", "influencer", "بلاگر", "blogger",
        "ادمین", "admin", "مدیر کانال", "مدیر گروه",
        
        # 🎁 تفریح و سرگرمی
        "تفریح", "entertainment", "سرگرمی", "fun",
        "شوخی", "joke", "طنز", "comedy", "میم", "meme",
        "پارتی", "party", "جشن", "celebration", "تولد", "birthday",
        "کافه گردی", "طبیعت گردی", "پیاده روی",
        "بازی فکری", "پازل", "puzzle", "سودوکو",
        "شطرنج", "chess", "تخته نرد", "backgammon",
        "پاسور", "cards", "منچ",
        
        # 💑 رابطه و دوستی
        "دوستی", "friendship", "دوست", "friend",
        "چت", "chat", "گفتگو", "conversation",
        "همسریابی", "ازدواج", "marriage", "عروسی", "wedding",
        "رابطه", "relationship", "عاشقانه", "romantic",
        "مشاوره ازدواج", "مشاوره خانواده",
        "مجرد", "single", "متاهل", "married",
        
        # 🧘 معنویت و روانشناسی
        "روانشناسی", "psychology", "روان درمانی",
        "مشاوره", "counseling", "روانپزشک", "psychiatrist",
        "استرس", "stress", "اضطراب", "anxiety",
        "مدیتیشن", "meditation", "ذهن آگاهی", "mindfulness",
        "خودشناسی", "self awareness", "توسعه فردی",
        "اعتماد به نفس", "confidence", "انگیزه", "motivation",
        "هدف گذاری", "goal setting", "برنامه ریزی", "planning",
        "عادت", "habit", "موفقیت", "success",
        
        # 📰 اخبار و سیاست
        "اخبار", "news", "خبر", "سیاست", "politics",
        "روزنامه", "newspaper", "خبرگزاری", "agency",
        "ایران", "Iran", "جهان", "world", "بین الملل",
        "اقتصادی", "economic", "اجتماعی", "social",
        "ورزشی", "sports news", "هنری", "فرهنگی",
        "تحلیل", "analysis", "گزارش", "report",
        
        # 🎓 علم و دانش
        "علم", "science", "دانش", "knowledge",
        "فیزیک", "physics", "شیمی", "chemistry", "زیست", "biology",
        "ریاضی", "math", "نجوم", "astronomy", "فضا", "space",
        "پزشکی", "medicine", "مهندسی", "engineering",
        "اختراع", "invention", "نوآوری", "innovation",
        "تحقیقات", "research", "آزمایشگاه", "lab",
        
        # 🏢 کسب و کار
        "کسب و کار", "business", "بیزینس", "شرکت", "company",
        "مدیریت", "management", "مدیر", "manager",
        "بازاریابی", "marketing", "فروش", "sales",
        "برند", "brand", "برندینگ", "branding",
        "تبلیغات", "advertising", "دیجیتال مارکتینگ",
        "سئو", "SEO", "تولید محتوا", "content",
        "مشتری", "customer", "خدمات مشتری", "support",
        "استراتژی", "strategy", "پروژه", "project",
        
        # 🔧 فنی و تعمیرات
        "تعمیر", "repair", "تعمیرات", "نصب", "install",
        "تعمیر موبایل", "تعمیر لپ تاپ", "تعمیر لوازم خانگی",
        "برق", "electricity", "برقکار", "لوله کشی", "plumbing",
        "نجاری", "carpentry", "جوشکاری", "welding",
        "خدمات فنی", "technical services",
        
        # 🌍 زبان و فرهنگ
        "زبان", "language", "ترجمه", "translation",
        "انگلیسی", "English", "عربی", "Arabic", "فرانسوی",
        "آلمانی", "German", "اسپانیایی", "Spanish",
        "چینی", "Chinese", "ژاپنی", "Japanese", "کره ای",
        "فرهنگ", "culture", "سنت", "tradition",
        "ایرانی", "Persian", "فارسی", "Farsi"
    ],
    
    'popular_telegram_names': [
        # اسامی محبوب گروه‌های تلگرامی
        "چت روم", "چتروم", "chat room", "گپ", "گپ و گفت",
        "پاتوق", "انجمن", "باشگاه", "club", "هاب", "hub",
        "مرکز", "center", "آکادمی", "academy", "مدرسه", "school",
        "تیم", "team", "گروه", "group", "جمع", "community",
        "شبکه", "network", "پلتفرم", "platform",
        "آنلاین", "online", "ایران", "Iran", "فارسی", "Persian",
        "تهران", "Tehran", "اصفهان", "Isfahan", "شیراز", "Shiraz",
        "رسمی", "official", "اصلی", "main", "VIP", "ویژه",
        "رایگان", "free", "پرمیوم", "premium", "طلایی", "gold",
        "حرفه ای", "pro", "professional", "متخصص", "expert",
        "آموزش", "learn", "training", "یادگیری", "learning",
        "پشتیبانی", "support", "کمک", "help", "راهنما", "guide",
        "اخبار", "news", "آپدیت", "update", "جدید", "new",
        "دانلود", "download", "لینک", "link", "فایل", "file",
        "بحث", "discussion", "سوال", "question", "جواب", "answer",
        "تجربه", "experience", "نظر", "opinion", "بررسی", "review",
        
        # کلمات ترکیبی
        "ایرانیان", "فارسی زبان", "هموطن", "همشهری",
        "دوستان", "friends", "یاران", "رفقا",
        "علاقمندان", "enthusiasts", "lovers", "fans",
        "متخصصین", "specialists", "کارشناسان", "experts",
        "توسعه دهندگان", "developers", "سازندگان", "makers",
        "خریداران", "buyers", "فروشندگان", "sellers",
        "معامله گران", "traders", "سرمایه گذاران", "investors",
        # +50 افزوده
        "اعلانات", "announcements", "اعضا", "members", "خصوصی", "private", "VIP lounge", "VIP room",
        "پاتوق اعضا", "member hangout", "همایش", "meetup", "جلسه", "session", "قرار", "gathering",
        "کافه آنلاین", "online cafe", "لایو شو", "live show", "استریم", "stream", "چت زند", "live chat",
        "پوشش زنده", "live coverage", "اعلامیه", "bulletin", "اطلاع رسانی", "info", "رویداد", "event",
        "فان کلاب", "fan club", "هواداران", "fans", "منتقدین", "critics", "ناظر", "moderator",
        "اتاق گفتگو", "chatroom 2", "آمادگی", "prep", "گروه دوستانه", "friendly group",
        "گپ خانوادگی", "family chat", "پشتیبانی رسمی", "official support", "سرویس", "service",
        "تبادل لینک", "link exchange", "خرید و فروش" ,"buy & sell", "کانال رسمی", "official channel",
        "کانال خبری", "news channel", "کانال آموزشی", "edu channel", "آگهی ها", "ads",
        "ملاقات آنلاین", "online meetup", "پرسش و پاسخ", "Q&A", "تبادل تجربه", "share experience"
    ],
    
    # 🎯 دسته‌بندی‌های تخصصی بیشتر (+800 کلمه)
    'education_extended': [
        # دانشگاه‌ها و موسسات
        "دانشگاه تهران", "شریف", "امیرکبیر", "علم و صنعت",
        "دانشگاه آزاد", "پیام نور", "علمی کاربردی",
        "دانشگاه فردوسی", "دانشگاه تبریز", "دانشگاه شیراز",
        "MIT", "Harvard", "Stanford", "Oxford", "Cambridge",
        "دانشکده", "faculty", "موسسه", "institute",
        "پردیس", "campus", "خوابگاه", "dormitory",
        
        # رشته‌های تحصیلی
        "پزشکی", "medicine", "دندانپزشکی", "dentistry",
        "داروسازی", "pharmacy", "پرستاری", "nursing",
        "مهندسی کامپیوتر", "computer engineering",
        "مهندسی برق", "electrical engineering",
        "مهندسی مکانیک", "mechanical engineering",
        "مهندسی عمران", "civil engineering",
        "مهندسی شیمی", "chemical engineering",
        "مهندسی صنایع", "industrial engineering",
        "معماری", "architecture", "حقوق", "law",
        "حسابداری", "accounting", "مدیریت", "management",
        "اقتصاد", "economics", "روانشناسی", "psychology",
        "جامعه شناسی", "sociology", "زبان انگلیسی",
        "ادبیات فارسی", "تاریخ", "history", "جغرافیا",
        "فلسفه", "philosophy", "الهیات", "theology",
        "هنر", "art", "موسیقی", "music", "تئاتر", "theater",
        "سینما", "cinema", "گرافیک", "graphic design",
        
        # آزمون‌ها
        "کنکور سراسری", "کنکور ارشد", "کنکور دکتری",
        "آزمون استخدامی", "آزمون وکالت", "آزمون نظام مهندسی",
        "GRE", "GMAT", "SAT", "ACT", "MCAT", "LSAT",
        "آزمون زبان", "مدرک زبان", "گواهینامه بین المللی",
        
        # منابع آموزشی
        "جزوه", "خلاصه", "نکات کنکور", "تست زنی",
        "کلاس آنلاین", "وبینار", "webinar", "دوره آنلاین",
        "آموزش مجازی", "یادگیری الکترونیکی", "e-learning",
        "فرادرس", "مکتب خونه", "کورسرا", "Coursera",
        "یودمی", "Udemy", "ادکس", "edX", "خان آکادمی",
        "آموزش پایتون", "Python course", "دوره دیتا ساینس", "data science course", "یادگیری ماشین", "machine learning",
        "تحلیل داده", "data analytics", "آموزش SQL", "SQL course", "هوش مصنوعی"
    ],
    
    'shopping_brands': [
        # برندهای پوشاک
        "نایکی", "Nike", "آدیداس", "Adidas", "پوما", "Puma",
        "ریباک", "Reebok", "نیو بالانس", "New Balance",
        "کانورس", "Converse", "وانس", "Vans", "فیلا", "Fila",
        "گوچی", "Gucci", "لویی ویتون", "Louis Vuitton",
        "شنل", "Chanel", "دیور", "Dior", "پرادا", "Prada",
        "ورساچه", "Versace", "آرمانی", "Armani",
        "زارا", "Zara", "اچ اند ام", "H&M", "مانگو", "Mango",
        "پول اند بیر", "Pull and Bear", "برشکا", "Bershka",
        "لیوایز", "Levis", "دیزل", "Diesel", "گپ", "GAP",
        "تامی هیلفیگر", "Tommy Hilfiger", "کالوین کلین",
        "رالف لورن", "Ralph Lauren", "هوگو باس", "Hugo Boss",
        
        # برندهای لوازم الکترونیکی
        "سامسونگ", "Samsung", "ال جی", "LG", "سونی", "Sony",
        "اپل", "Apple", "آیفون", "iPhone", "آیپد", "iPad",
        "مک بوک", "MacBook", "ایرپاد", "AirPods",
        "شیائومی", "Xiaomi", "هواوی", "Huawei", "اوپو", "Oppo",
        "ریلمی", "Realme", "وان پلاس", "OnePlus",
        "ایسوس", "ASUS", "ایسر", "Acer", "لنوو", "Lenovo",
        "اچ پی", "HP", "دل", "Dell", "مایکروسافت", "Microsoft",
        "سرفیس", "Surface", "ایکس باکس", "Xbox",
        "پلی استیشن", "PlayStation", "نینتندو", "Nintendo",
        
        # برندهای آرایشی و بهداشتی
        "لورال", "L'Oreal", "مک", "MAC", "بابی براون",
        "مایبلین", "Maybelline", "اسنس", "Essence",
        "نیکس", "NYX", "اوریفلیم", "Oriflame", "آوون", "Avon",
        "نوتروژینا", "Neutrogena", "نیوآ", "Nivea",
        "داو", "Dove", "جانسون", "Johnson", "پنتن", "Pantene",
        
        # فروشگاه‌های آنلاین
        "دیجی کالا", "Digikala", "باسلام", "Basalam",
        "ترب", "Torob", "اسنپ مارکت", "SnappMarket",
        "اکالا", "Okala", "دیجی استایل", "DigiStyle",
        "مبیت", "Mobit", "تکنولایف", "TechnoLife",
        "آمازون", "Amazon", "علی بابا", "AliExpress",
        "ای بی", "eBay", "وال مارت", "Walmart",
        "Shein", "شین", "Herschel", "Free People", "ASOS", "asos",
        "Boohoo", "Noon", "Gitti", "Decathlon", "Sport Zone", "فروشگاه ورزشی",
        "زنجیره فروش", "chain store", "Outlet", "اوتلت", "Flash Sale", "حراج لحظه‌ای",
        "Handmade", "دست ساز", "Craft Market", "بازار دست ساز", "Local Brand", "برند محلی",
        "Luxury", "لوکس", "Designer", "تولید محدود", "Limited Edition", "نسخه محدود",
        "Home Decor", "دکوراسیون", "Electronics", "الکترونیک", "Gadget Store", "فروش گجت"
    ],
    
    'cities_locations': [
        # شهرهای ایران
        "تهران", "Tehran", "مشهد", "Mashhad", "اصفهان", "Isfahan",
        "شیراز", "Shiraz", "تبریز", "Tabriz", "کرج", "Karaj",
        "اهواز", "Ahvaz", "قم", "Qom", "کرمانشاه", "Kermanshah",
        "ارومیه", "Urmia", "رشت", "Rasht", "زاهدان", "Zahedan",
        "کرمان", "Kerman", "همدان", "Hamedan", "یزد", "Yazd",
        "اردبیل", "Ardabil", "بندرعباس", "Bandar Abbas",
        "ساری", "Sari", "قزوین", "Qazvin", "زنجان", "Zanjan",
        "سنندج", "Sanandaj", "گرگان", "Gorgan", "بوشهر", "Bushehr",
        "خرم آباد", "Khorramabad", "ایلام", "Ilam",
        "شهرکرد", "Shahrekord", "یاسوج", "Yasuj", "بیرجند", "Birjand",
        "بجنورد", "Bojnurd", "سمنان", "Semnan",
        
        # مناطق تهران
        "تهرانپارس", "نارمک", "پیروزی", "افسریه",
        "جنت آباد", "ستارخان", "آزادی", "انقلاب",
        "ولیعصر", "تجریش", "الهیه", "فرمانیه", "زعفرانیه",
        "ونک", "پارک وی", "سعادت آباد", "شهرک غرب",
        "پونک", "اکباتان", "شهران", "کن",
        "یافت آباد", "شهر ری", "اسلامشهر", "کهریزک",
        "پردیس", "پرند", "اندیشه", "شهریار",
        
        # کشورها
        "ترکیه", "Turkey", "امارات", "UAE", "دبی", "Dubai",
        "عمان", "Oman", "قطر", "Qatar", "کویت", "Kuwait",
        "عربستان", "Saudi", "بحرین", "Bahrain",
        "آلمان", "Germany", "فرانسه", "France", "انگلیس", "UK",
        "ایتالیا", "Italy", "اسپانیا", "Spain", "هلند", "Netherlands",
        "سوئد", "Sweden", "نروژ", "Norway", "دانمارک", "Denmark",
        "اتریش", "Austria", "سوئیس", "Switzerland",
        "کانادا", "Canada", "آمریکا", "USA", "استرالیا", "Australia",
        "ژاپن", "Japan", "چین", "China", "کره جنوبی", "South Korea",
        "مالزی", "Malaysia", "تایلند", "Thailand", "هند", "India"
    ],
    
    'hobbies_interests': [
        # سرگرمی و تفریح
        "بازی", "game", "گیم", "سرگرمی", "تفریح", "hobby",
        "پازل", "puzzle", "جدول", "crossword", "سودوکو", "sudoku",
        "شطرنج", "chess", "تخته نرد", "backgammon", "پاسور", "cards",
        "مونوپولی", "Monopoly", "کلوئدو", "بازی رومیزی", "board game",
        
        # هنرهای دستی
        "هنر دستی", "handcraft", "صنایع دستی", "دوخت", "sewing",
        "بافتنی", "knitting", "قلاب بافی", "crochet",
        "گلدوزی", "embroidery", "پچ ورک", "patchwork",
        "سفالگری", "pottery", "سرامیک", "ceramic",
        "نقاشی روی شیشه", "ویترای", "رزین", "resin",
        "جواهرسازی", "jewelry making", "اوریگامی", "origami",
        "کاغذ سازی", "quilling", "اسکرپ بوک", "scrapbook",
        
        # موسیقی و رقص
        "آموزش گیتار", "آموزش پیانو", "آموزش ویولن",
        "آموزش سنتور", "آموزش تار", "آموزش سه تار",
        "آموزش دف", "آموزش تنبک", "آموزش کمانچه",
        "آواز", "خوانندگی", "singing", "تئوری موسیقی",
        "رقص", "dance", "رقص ایرانی", "رقص عربی", "بالت", "ballet",
        "سالسا", "salsa", "تانگو", "tango", "هیپ هاپ دنس",
        
        # عکاسی و فیلمبرداری
        "عکاسی پرتره", "portrait", "عکاسی منظره", "landscape",
        "عکاسی ماکرو", "macro", "عکاسی خیابانی", "street photography",
        "عکاسی عروسی", "wedding photography", "عکاسی مد",
        "عکاسی صنعتی", "عکاسی غذا", "food photography",
        "ویدیوگرافی", "videography", "فیلمسازی", "filmmaking",
        "تدوین ویدیو", "video editing", "یوتیوبر", "YouTuber",
        
        # کتاب و مطالعه
        "کتابخوان", "book lover", "کتاب خوانی", "reading",
        "رمان", "novel", "داستان کوتاه", "short story",
        "شعر", "poetry", "ادبیات", "literature",
        "کتاب صوتی", "audiobook", "پادکست", "podcast",
        "نویسندگی", "writing", "داستان نویسی", "storytelling",
        
        # جمع آوری
        "کلکسیون", "collection", "تمبر", "stamp", "سکه", "coin",
        "فیگور", "figure", "اکشن فیگور", "action figure",
        "کارت بازی", "trading cards", "یوگی او", "پوکمون کارت",
        # +50 hobbies/interests keywords
        "دیوتر", "board game night", "مینیاتور", "miniatures", "مپینگ", "mapping",
        "Roleplay", "نقش آفرینی", "کوسپلی", "cosplay", "دورختن", "knitting",
        "چرخ خیاطی", "sewing machine", "مدل سازی", "model making", "RC cars", "ماشین کنترلی",
        "درون", "drones", "پرواز پهپاد", "drone flying", "آرودینو", "Arduino", "رسپبری پای", "Raspberry Pi",
        "روباتیک", "robotics", "الکترونیک", "electronics", "کدنویسی سرگرمی", "hobby coding",
        "فیلم کوتاه", "short film", "پادپروداکت", "product photography", "فوتو ادیت", "photo editing",
        "ژانر بازی", "game genres", "سکواش", "squash", "خانگی بدنسازی", "home workout",
        "کاوش", "exploration clubs", "جمع آوری", "collectibles", "تمبر", "stamps",
        "سکه", "coins", "کالکشن فیگور", "figure collection", "باشگاه فیلم", "movie club"
    ],
    
    'health_medical': [
        # پزشکی و سلامت
        "پزشک", "doctor", "دکتر", "متخصص", "specialist",
        "عمومی", "general", "قلب", "cardiology", "قلب و عروق",
        "مغز و اعصاب", "neurology", "ارتوپدی", "orthopedic",
        "چشم پزشکی", "ophthalmology", "گوش و حلق و بینی", "ENT",
        "پوست و مو", "dermatology", "زنان و زایمان", "gynecology",
        "اطفال", "pediatrics", "داخلی", "internal medicine",
        "جراحی", "surgery", "رادیولوژی", "radiology",
        "آزمایشگاه", "laboratory", "سونوگرافی", "sonography",
        
        # دندانپزشکی
        "دندانپزشک", "dentist", "ارتودنسی", "orthodontics",
        "ایمپلنت", "implant", "لمینت", "laminate", "کامپوزیت",
        "عصب کشی", "root canal", "پروتز", "prosthetics",
        "جرم گیری", "scaling", "سفید کردن دندان", "whitening",
        
        # داروخانه و دارو
        "داروخانه", "pharmacy", "دارو", "medicine", "قرص", "pill",
        "شربت", "syrup", "آمپول", "injection", "پماد", "ointment",
        "ویتامین", "vitamin", "مکمل", "supplement",
        "آنتی بیوتیک", "antibiotic", "مسکن", "painkiller",
        
        # بیمارستان و کلینیک
        "بیمارستان", "hospital", "کلینیک", "clinic", "درمانگاه",
        "مطب", "اورژانس", "emergency", "ICU", "بستری",
        "سرپایی", "outpatient", "نوبت دهی", "appointment",
        "بیمه سلامت", "بیمه تکمیلی", "تامین اجتماعی",
        
        # سلامت روان
        "روان درمانی", "psychotherapy", "روانپزشک", "psychiatrist",
        "روانشناس", "psychologist", "مشاور", "counselor",
        "افسردگی", "depression", "اضطراب", "anxiety",
        "استرس", "stress", "وسواس", "OCD", "اختلال خواب",
        
        # تغذیه و رژیم
        "تغذیه", "nutrition", "رژیم درمانی", "diet therapy",
        "کاهش وزن", "weight loss", "افزایش وزن", "weight gain",
        "تناسب اندام", "fitness", "کالری", "calorie",
        "پروتئین", "protein", "کربوهیدرات", "carbs", "چربی", "fat",
        # +50 health/medical keywords
        "طب سنتی", "traditional medicine", "طب مکمل", "complementary medicine", "طب گیاهی", "herbal medicine",
        "ورزش درمانی", "physiotherapy", "کلنیک تخصصی", "specialty clinic", "پزشکی عمومی", "general practitioner",
        "سلامت روان", "mental health", "روانکاوی", "psychoanalysis", "روانشناسی کودک", "child psychology",
        "سلامت زنان", "women health", "بهداشت باروری", "reproductive health", "بارداری سالم", "safe pregnancy",
        "سلامت دهان", "oral health", "دندان کودکان", "pediatric dentistry", "زیبایی دندان", "cosmetic dentistry",
        "تغذیه ورزشی", "sports nutrition", "مشاوره تغذیه", "nutrition counseling", "پزشکی خانوادگی", "family medicine",
        "شغل‌های پزشکی", "medical careers", "رزیدنت", "residency", "پزشکی اورژانس", "emergency medicine",
        "کلینیک زیبایی", "aesthetic clinic", "کرایو", "cryotherapy", "طب فیزیکی", "physical therapy",
        "تشخیص از راه دور", "telemedicine", "ویزیت آنلاین", "online checkup", "واکسن", "vaccine"
    ],
    
    'religious_spiritual': [
        # اسلامی
        "قرآن", "Quran", "نماز", "prayer", "روزه", "fasting",
        "حج", "hajj", "عمره", "umrah", "زیارت", "pilgrimage",
        "مکه", "Mecca", "مدینه", "Medina", "کربلا", "Karbala",
        "امام رضا", "مشهد مقدس", "حرم", "shrine",
        "دعا", "supplication", "ذکر", "dhikr", "استغفار",
        "توبه", "repentance", "تهجد", "نماز شب",
        "ماه رمضان", "Ramadan", "عید فطر", "عید قربان",
        "محرم", "Muharram", "صفر", "عاشورا", "Ashura",
        "مداحی", "عزاداری", "هیئت", "نوحه",
        "احکام", "فقه", "مرجع تقلید", "آیت الله",
        
        # عرفان و معنویت
        "عرفان", "mysticism", "تصوف", "Sufism", "معنویت",
        "مدیتیشن", "meditation", "یوگا", "yoga", "چاکرا", "chakra",
        "انرژی درمانی", "energy healing", "ریکی", "Reiki",
        "تاروت", "tarot", "طالع بینی", "فال", "horoscope",
        "ماه تولد", "برج", "zodiac", "ستاره شناسی"
    ],
    
    'vehicles_transport': [
        # خودروهای ایرانی
        "پراید", "Pride", "تیبا", "Tiba", "ساینا", "Saina",
        "پژو ۲۰۶", "پژو ۲۰۷", "پژو پارس", "سمند", "Samand",
        "دنا", "Dena", "رانا", "Rana", "شاهین", "Shahin",
        "تارا", "Tara", "هایما", "Haima", "بسترن",
        "ام وی ام", "MVM", "آریو", "چری", "Chery",
        
        # خودروهای خارجی
        "تویوتا", "Toyota", "هوندا", "Honda", "نیسان", "Nissan",
        "مزدا", "Mazda", "میتسوبیشی", "Mitsubishi", "سوبارو",
        "هیوندای", "Hyundai", "کیا", "Kia", "سانگ یانگ",
        "بنز", "Mercedes", "بی ام و", "BMW", "آئودی", "Audi",
        "فولکس واگن", "Volkswagen", "پورشه", "Porsche",
        "لامبورگینی", "Lamborghini", "فراری", "Ferrari",
        "رولز رویس", "Rolls Royce", "بنتلی", "Bentley",
        "رنج روور", "Range Rover", "لندروور", "Land Rover",
        "ولوو", "Volvo", "ساب", "Saab", "رنو", "Renault",
        "پژو", "Peugeot", "سیتروئن", "Citroen",
        "فورد", "Ford", "شورولت", "Chevrolet", "جیپ", "Jeep",
        "دوج", "Dodge", "کادیلاک", "Cadillac", "تسلا", "Tesla",
        
        # موتورسیکلت
        "هوندا", "یاماها", "Yamaha", "کاوازاکی", "Kawasaki",
        "سوزوکی", "Suzuki", "هارلی دیویدسون", "Harley Davidson",
        "وسپا", "Vespa", "پیاجیو", "Piaggio", "آپاچی", "Apache",
        "پالس", "Pulsar", "موتور سنگین", "bigbike",
        
        # حمل و نقل عمومی
        "مترو", "metro", "اتوبوس", "bus", "تاکسی", "taxi",
        "اسنپ", "Snapp", "تپسی", "Tapsi", "ماکسیم", "Maxim",
        "قطار", "train", "هواپیما", "airplane", "فرودگاه", "airport"
    ],
    
    'food_cuisine': [
        # غذاهای ایرانی
        "چلوکباب", "کباب کوبیده", "کباب برگ", "جوجه کباب",
        "قورمه سبزی", "قیمه", "فسنجان", "خورشت",
        "زرشک پلو", "باقالی پلو", "آلبالو پلو", "شیرین پلو",
        "دلمه", "کوکو", "آش رشته", "آش دوغ",
        "کله پاچه", "دیزی", "آبگوشت", "حلیم",
        "میرزاقاسمی", "کشک بادمجان", "بورانی",
        
        # غذاهای بین المللی
        "پیتزا", "pizza", "پاستا", "pasta", "لازانیا", "lasagna",
        "برگر", "burger", "ساندویچ", "sandwich", "هات داگ", "hotdog",
        "سوشی", "sushi", "رامن", "ramen", "نودل", "noodle",
        "تاکو", "taco", "بوریتو", "burrito", "ناچو", "nacho",
        "کباب ترکی", "دنر", "döner", "شاورما", "shawarma",
        "فلافل", "falafel", "حمص", "hummus",
        "استیک", "steak", "چیکن", "chicken", "بیف", "beef",
        
        # شیرینی و دسر
        "کیک", "cake", "شیرینی", "pastry", "کلوچه", "cookie",
        "باقلوا", "baklava", "زولبیا بامیه", "گوش فیل",
        "شکلات", "chocolate", "بستنی", "ice cream", "ژله", "jelly",
        "پودینگ", "pudding", "تیرامیسو", "tiramisu",
        "چیز کیک", "cheesecake", "براونی", "brownie",
        "دونات", "donut", "کاپ کیک", "cupcake", "ماکارون", "macaron",
        
        # نوشیدنی
        "قهوه", "coffee", "اسپرسو", "espresso", "لاته", "latte",
        "کاپوچینو", "cappuccino", "موکا", "mocha", "آمریکانو",
        "چای", "tea", "چای سبز", "green tea", "دمنوش", "herbal tea",
        "آبمیوه", "juice", "اسموتی", "smoothie", "شیک", "shake",
        "نوشابه", "soda", "دوغ", "آب", "water",
        # +50 food/cuisine extras
        "غذاهای محلی", "local cuisine", "غذاهای خیابانی", "street food", "فودتراک", "food truck",
        "دسر سالم", "healthy dessert", "شیرینی رژیمی", "diet pastry", "رستوران گیاهی", "vegan restaurant",
        "گیرای ایرانی", "traditional dishes", "پلو خورشت", "rice stews", "پذیرایی مراسم", "event dishes",
        "فست فود", "fast food", "رستوران خانوادگی", "family restaurant", "فست کژوال", "fast casual",
        "شوربا", "soup", "دستور غذایی ساده", "simple recipes", "آشپزی کم هزینه", "budget cooking",
        "غذای سالم", "healthy food", "غذای بدون گلوتن", "gluten free food", "شیرینی سنتی", "traditional sweets",
        "سالاد سالم", "healthy salad", "پروتئین بالا", "high protein", "کیک خانگی", "homemade cake",
        "آموزش پخت", "cooking class", "کافه های محبوب", "popular cafes", "نوشیدنی سرد", "cold drinks"
    ],
    
    'services_businesses': [
        # خدمات
        "خدمات", "services", "سرویس", "service",
        "خدمات منزل", "home services", "تعمیرات", "repairs",
        "نظافت", "cleaning", "شستشو", "laundry", "خشکشویی",
        "باربری", "moving", "اسباب کشی", "حمل اثاثیه",
        "تعمیر لوازم خانگی", "تعمیر یخچال", "تعمیر لباسشویی",
        "نقاشی ساختمان", "کاغذ دیواری", "کف سابی",
        
        # آرایشگاه و سالن زیبایی
        "آرایشگاه", "salon", "سالن زیبایی", "beauty salon",
        "آرایش عروس", "bridal makeup", "شینیون", "chignon",
        "رنگ مو", "hair color", "هایلایت", "highlight",
        "مش", "مانیکور", "manicure", "پدیکور", "pedicure",
        "اپیلاسیون", "epilate", "لیزر موهای زائد",
        
        # چاپ و تبلیغات
        "چاپ", "print", "چاپخانه", "printing", "بنر", "banner",
        "کارت ویزیت", "business card", "تراکت", "flyer",
        "کاتالوگ", "catalog", "بروشور", "brochure",
        "تبلیغات", "advertising", "طراحی تبلیغات",
        
        # عکاسی و فیلمبرداری
        "آتلیه", "studio", "عکاسی عروسی", "wedding photography",
        "فیلمبرداری عروسی", "عکس پرسنلی", "عکس مدارک",
        "چاپ عکس", "photo print", "آلبوم عکس", "photo album",
        
        # حقوقی و مالی
        "وکیل", "lawyer", "مشاوره حقوقی", "legal advice",
        "دفتر اسناد رسمی", "محضر", "گواهی امضا",
        "حسابدار", "accountant", "مشاور مالی", "financial advisor",
        "مشاور مالیاتی", "اظهارنامه مالیاتی",
        # +50 خدمات و کسب و کار
        "آژانس دیجیتال", "digital agency", "طراحی سایت", "web design", "هاستینگ", "hosting",
        "خدمات شبکه", "network services", "امنیت سایبری", "cybersecurity", "پشتیبانی فنی", "tech support",
        "مشاور حقوقی", "legal consultant", "حسابداری آنلاین", "online accounting", "مدیریت مالی", "financial management",
        "استخدام نیرو", "hiring", "منابع انسانی", "HR services", "آموزش سازمانی", "corporate training",
        "مدیریت پروژه", "project management", "کنترل کیفیت", "quality control", "نصب و راه اندازی", "installation",
        "نظارت ساختمانی", "construction supervision", "مشاوره برند", "brand consulting", "طراحی لوگو", "logo design",
        "تبلیغات آنلاین", "online ads", "بازاریابی محتوایی", "content marketing", "خدمات مشتری", "customer service",
        "کسب و کار محلی", "local business", "شراکت تجاری", "business partnership", "طراحی فروشگاه", "store design",
        "خدمات مالی", "financial services", "بیمه شرکتی", "corporate insurance", "حریم خصوصی", "privacy services"
    ],
    
    'slang_informal': [
        # اصطلاحات عامیانه فارسی
        "چت", "chat", "گپ", "صحبت", "حرف زدن",
        "رفیق", "دوست", "بچه ها", "یاران", "داش",
        "خفن", "باحال", "عالی", "توپ", "مشتی",
        "خوش", "شاد", "خنده", "جوک", "شوخی",
        "میم", "meme", "ترول", "troll", "فان", "fun",
        "لول", "lol", "خخخ", "هاها", "روانی", "دیوونه",
        "آفرین", "دمت گرم", "عشقی", "جیگر",
        "چاکرم", "نوکرتم", "فدات", "قربونت",
        "ای ول", "ایول", "آها", "اوکی", "ok",
        "بیا", "برو", "بزن", "بخور", "بگو",
        "چرا", "چی", "چجوری", "کجا", "کی",
        "همین", "همون", "اینا", "اونا",
        
        # اختصارات و ایموجی
        "تلگرام", "telegram", "تل", "tg",
        "اینستا", "insta", "ig", "واتس", "whatsapp",
        "یوتوب", "youtube", "yt", "تیک تاک", "tiktok"
    ],
    
    # 🎯 کلمات کلیدی هوشمند - الگوهای رایج نام گروه‌ها (+1000 کلمه)
    'smart_patterns': [
        # الگوهای عددی رایج
        "۲۰۲۴", "2024", "۲۰۲۵", "2025", "۱۴۰۳", "۱۴۰۴",
        "۱۸", "18", "۲۰", "20", "۲۱", "21", "۳۰", "30",
        "۱۰۰", "100", "۵۰۰", "500", "۱۰۰۰", "1000",
        "24/7", "۲۴ ساعته", "شبانه روزی",
        "نسل ۱", "نسل ۲", "نسل ۳", "v1", "v2", "v3",
        
        # پیشوندهای رایج
        "سوپر", "super", "مگا", "mega", "اولترا", "ultra",
        "هایپر", "hyper", "مکس", "max", "پرو", "pro",
        "پلاس", "plus", "پریمیوم", "premium", "گلد", "gold",
        "پلاتینیوم", "platinum", "دایموند", "diamond",
        "الماس", "طلایی", "نقره ای", "برنزی",
        "اصلی", "original", "رسمی", "official",
        "واقعی", "real", "اورجینال", "genuine",
        "نو", "new", "جدید", "تازه", "fresh",
        "بهترین", "best", "برترین", "top", "عالی",
        "شماره ۱", "number one", "#1", "اول",
        
        # پسوندهای رایج
        "گروه", "group", "کانال", "channel", "چنل",
        "تیم", "team", "کلاب", "club", "باشگاه",
        "آکادمی", "academy", "مدرسه", "school",
        "انجمن", "association", "اتحادیه", "union",
        "شبکه", "network", "نت", "net", "هاب", "hub",
        "زون", "zone", "لند", "land", "ورلد", "world",
        "سیتی", "city", "تاون", "town", "پلیس", "place",
        "هاوس", "house", "روم", "room", "اتاق",
        "استور", "store", "شاپ", "shop", "مارکت", "market",
        "سنتر", "center", "مرکز", "پوینت", "point",
        "بیس", "base", "استیشن", "station", "پورت", "port",
        
        # کلمات احساسی و جذاب
        "عشق", "love", "دل", "heart", "قلب",
        "زندگی", "life", "لایف", "رویا", "dream",
        "امید", "hope", "نور", "light", "روشنایی",
        "آزادی", "freedom", "صلح", "peace", "آرامش",
        "شادی", "happiness", "لبخند", "smile",
        "موفقیت", "success", "پیروزی", "victory",
        "قدرت", "power", "انرژی", "energy", "توان",
        "هوش", "intelligence", "خرد", "wisdom",
        
        # کلمات اکشن
        "یاد بگیر", "learn", "رشد کن", "grow",
        "بساز", "build", "create", "خلق کن",
        "کشف کن", "discover", "explore", "کاوش",
        "تغییر بده", "change", "transform", "تحول",
        "پیشرفت کن", "progress", "advance", "ارتقا",
        "کسب کن", "earn", "درآمد", "income",
        "سرمایه گذاری کن", "invest", "معامله کن", "trade",
        # +50 الگوهای هوشمند بیشتر
        "Offical Club", "باشگاه رسمی", "Fan Club", "هواداران", "Supporters", "پشتیبانان",
        "Study Buddy", "همکلاسی", "Book Worms", "کتاب دوستان", "Code Jam", "کُد جم",
        "Hack Night", "نایت هک", "Dev Lounge", "دِو لانژ", "Design Hub", "دیزاین هاب",
        "Quick Tips", "راهنمای سریع", "Daily News", "اخبار روزانه", "Weekly Digest", "خلاصه هفتگی",
        "Community Help", "کمک جامعه", "FAQ", "سوالات متداول", "Support Desk", "پشتیبانی رسمی",
        "Beta Testers", "تست کننده ها", "Insiders", "افراد داخلی", "Feedback", "بازخورد",
        "Experts", "متخصصین", "Pro Tips", "نکات حرفه ای", "Beginners", "مبتدی ها",
        "Creators", "سازندگان", "Makers", "سازنده ها", "Collectors", "کلکسیونرها",
        "Local Only", "محلی", "IR Only", "ویژه ایران", "Intl", "بین المللی"
    ],
    
    'trending_topics': [
        # ترندهای فعلی
        "هوش مصنوعی", "AI", "چت جی پی تی", "ChatGPT",
        "میدجرنی", "Midjourney", "کلود", "Claude", "جمینای", "Gemini",
        "متاورس", "metaverse", "وب ۳", "Web3", "ان اف تی", "NFT",
        "دیفای", "DeFi", "استیکینگ", "staking", "ماینینگ", "mining",
        "ایردراپ", "airdrop", "توکن", "token", "والت", "wallet",
        
        # شبکه‌های اجتماعی ترند
        "ریلز", "Reels", "شورتز", "Shorts", "استوری", "Story",
        "لایو", "Live", "پادکست", "podcast", "کلاب هاوس", "Clubhouse",
        "دیسکورد", "Discord", "ردیت", "Reddit", "کورا", "Quora",
        "پینترست", "Pinterest", "بیهنس", "Behance", "دریبل", "Dribbble",
        
        # موضوعات داغ
        "دورکاری", "remote work", "فریلنسری", "freelancing",
        "استارتاپ", "startup", "کارآفرینی", "entrepreneurship",
        "دیجیتال نومد", "digital nomad", "پسیو اینکام", "passive income",
        "ساید هاسل", "side hustle", "کسب درآمد اینترنتی",
        "آموزش آنلاین", "online course", "منتورینگ", "mentoring",
        "کوچینگ", "coaching", "مشاوره آنلاین",
        
        # تکنولوژی‌های جدید
        "کوانتوم", "quantum", "بلاکچین", "blockchain",
        "اینترنت اشیا", "IoT", "۵جی", "5G", "فیبر نوری",
        "خانه هوشمند", "smart home", "خودرو برقی", "EV",
        "انرژی پاک", "clean energy", "سولار", "solar",
        "باتری", "battery", "شارژ سریع", "fast charging",
        # +50 ترند جدید
        "GPT-4", "ChatGPT-4", "LLM", "Large Language Models", "text-to-image", "Lensa",
        "AI tools", "Generative AI", "Stable Diffusion", "Midjourney Pro", "Imagen", "Diffusion Models",
        "On-chain", "Layer-2", "zk-rollup", "zkSync", "Optimism", "Arbitrum",
        "DePIN", "Decentralized IoT", "AI-as-a-Service", "AaaS", "Edge AI", "TinyML",
        "Open Source AI", "OpenAI tools", "Coping with GPT", "Prompt Engineering", "Prompt tips", "Prompting",
        "AI art", "digital art", "AI music", "auto composition", "AI video", "deepfake",
        "SaaS", "BaaS", "Cloud-native", "serverless", "Kubernetes", "Docker",
        "Data privacy", "privacy-first", "GDPR", "dApp", "DAO", "community governance"
    ],
    
    'persian_combinations': [
        # ترکیبات فارسی رایج
        "ایران", "ایرانی", "ایرانیان", "پارس", "پارسی",
        "فارس", "فارسی", "تهران", "تهرانی", "تهرانیها",
        "مشهد", "مشهدی", "اصفهان", "اصفهانی", "شیراز", "شیرازی",
        "تبریز", "تبریزی", "کرج", "کرجی", "گیلان", "گیلانی",
        "مازندران", "مازنی", "خراسان", "خراسانی",
        "آذربایجان", "آذری", "کرد", "کردی", "کردستان",
        "خوزستان", "خوزی", "اهواز", "اهوازی",
        "بلوچ", "بلوچستان", "کرمان", "کرمانی",
        "یزد", "یزدی", "همدان", "همدانی", "لر", "لری",
        
        # کلمات با ها و ان
        "ایرانیها", "تهرانیها", "جوانها", "بچه ها",
        "دختران", "پسران", "زنان", "مردان",
        "دانشجویان", "معلمان", "مهندسان", "پزشکان",
        "هنرمندان", "ورزشکاران", "کارآفرینان",
        "برنامه نویسان", "طراحان", "عکاسان",
        "نویسندگان", "خوانندگان", "موسیقیدانان",
        
        # اعداد فارسی
        "یک", "دو", "سه", "چهار", "پنج",
        "شش", "هفت", "هشت", "نه", "ده",
        "بیست", "سی", "چهل", "پنجاه", "صد", "هزار",
        # +50 Persian combinations additions
        "عاشق", "عاشقان", "عشاق", "عاشقانه", "عاشق ها",
        "علمی", "پژوهشی", "تحقیق", "فناوری", "نوآوری",
        "مهارت", "مهارت آموزی", "توانش", "توسعه", "رشد",
        "کارآفرینان جوان", "استارتاپی ها", "خالق", "سازنده", "سازندگان",
        "حامیان", "سرمایه گذاران کوچک", "سرمایه گذاران بزرگ",
        "کسب و کار کوچک", "میکرو کسب", "فروشندگان محلی", "سرپرستان", "مدیران",
        "کودکان", "نوجوانان", "بزرگسال", "جوانان", "نسل جدید",
        "سالمندان", "بازنشستگان", "شاغل", "استخدام شده", "کسب درآمد دلاری",
        "آموزش آنلاین", "کورس", "دروس تخصصی", "ویژه ها", "ایونت های محلی"
    ],
    
    'english_common': [
        # کلمات انگلیسی رایج در گروه‌های فارسی
        "Iran", "Iranian", "Persian", "Persia",
        "Tehran", "Mashhad", "Isfahan", "Shiraz", "Tabriz",
        "VIP", "Premium", "Gold", "Silver", "Bronze",
        "Pro", "Plus", "Max", "Ultra", "Super", "Mega",
        "Official", "Original", "Real", "True", "Best",
        "Top", "First", "Number One", "Elite", "Prime",
        "Free", "Unlimited", "Exclusive", "Private", "Secret",
        "Hot", "New", "Fresh", "Latest", "Updated",
        "Daily", "Weekly", "Monthly", "Yearly", "24/7",
        "Fast", "Quick", "Instant", "Express", "Rapid",
        "Easy", "Simple", "Smart", "Clever", "Genius",
        "Cool", "Awesome", "Amazing", "Incredible", "Epic",
        "Fun", "Happy", "Joy", "Love", "Life",
        "Dream", "Hope", "Trust", "Faith", "Believe",
        "Power", "Strong", "Force", "Energy", "Boost",
        "Win", "Success", "Victory", "Champion", "King",
        "Queen", "Prince", "Princess", "Royal", "Crown",
        "Star", "Moon", "Sun", "Sky", "Cloud",
        "Fire", "Ice", "Water", "Earth", "Wind",
        "Dark", "Light", "Black", "White", "Red",
        "Blue", "Green", "Yellow", "Purple", "Pink",
        "Gold", "Silver", "Diamond", "Crystal", "Pearl",
        # +50 English common additions
        "Hub", "Squad", "Crew", "Collective", "Society", "Meet", "Forum",
        "Talk", "Talks", "Insider", "Insiders", "Beta", "Legacy", "Legends",
        "Studio", "Lab", "Workshop", "Bootcamp", "Masterclass", "Mentor", "Mentoring",
        "Guild", "Union", "League", "Circle", "Group Chat", "Discussion",
        "Support", "Help Desk", "Announcements", "Newsroom", "Digest", "Daily Brief",
        "Show", "Channel", "Broadcast", "Stream", "Podcast", "Live Show",
        "Fans", "Fandom", "Collectors", "Traders", "Investors", "Entrepreneurs",
        "Founders", "Builders", "Creators", "Artists", "Developers", "Engineers"
    ],
    
    'money_making': [
        # کسب درآمد
        "درآمد", "income", "کسب درآمد", "پول", "money",
        "پولدار", "rich", "ثروت", "wealth", "ثروتمند",
        "میلیونی", "میلیاردی", "سود", "profit", "بازده",
        "درآمد دلاری", "کار دلاری", "پرداخت دلاری",
        "درآمد غیرفعال", "passive income", "پسیو",
        "فریلنس", "freelance", "پروژه", "project",
        "کار از خانه", "work from home", "دورکار",
        "کار اینترنتی", "آنلاین مانی", "online money",
        "ترید", "trade", "معامله", "سیگنال", "signal",
        "سرمایه", "capital", "سرمایه گذاری", "investment",
        "بورس", "فارکس", "forex", "ارز دیجیتال", "crypto",
        "بیت کوین", "bitcoin", "اتریوم", "ethereum",
        "ماینینگ", "mining", "استخر ماینینگ",
        "ایردراپ", "airdrop", "توکن رایگان", "free token",
        "NFT", "ان اف تی", "متاورس", "metaverse",
        
        # روش‌های درآمد
        "افیلیت", "affiliate", "همکاری در فروش",
        "دراپ شیپینگ", "dropshipping", "فروش آنلاین",
        "اینفلوئنسر", "influencer", "یوتیوبر", "youtuber",
        "بلاگر", "blogger", "وبلاگ نویسی",
        "پادکستر", "podcaster", "تولید محتوا", "content creation",
        "گرافیست", "graphic designer", "طراح وب", "web designer",
        "برنامه نویس", "programmer", "developer", "توسعه دهنده",
        "ترجمه", "translation", "مترجم", "translator",
        "تدریس آنلاین", "online teaching", "تدریس زبان",
        "مشاوره", "consulting", "کوچینگ", "coaching",
        # +50 keywords for money making
        "دیجیتال مارکتینگ", "digital marketing", "سرویس پریمیوم", "premium service",
        "عضویت ویژه", "premium membership", "درآمد از یوتیوب", "YouTube income", "ادمونتایزیشن", "monetization",
        "تبلیغات درون برنامه‌ای", "in-app ads", "افیلیت مارکت", "affiliate marketing",
        "کسب و کار اینترنتی", "online business", "رونق فروش", "boost sales", "فروش B2C", "B2C sales",
        "کریت کردن محصول", "product creation", "لندینگ پیج", "landing page", "کانورژن", "conversion",
        "افزایش درآمد", "revenue boost", "پوش نوتیفیکیشن", "push notifications", "CRM", "customer management",
        "مینی کورس", "mini course", "سرویس عضویت", "membership service", "تجارت الکترونیک", "ecommerce",
        "خدمات اشتراکی", "subscription service", "گوگل ادز", "Google Ads", "تبلیغات فیسبوک", "Facebook ads",
        "سئو فروش", "SEO for sales", "فروش در آمازون", "sell on Amazon", "صندوق پرداخت", "payment gateway"
    ],
    
    'social_community': [
        # اجتماعی و جامعه
        "دوست یابی", "friendship", "آشنایی", "meeting",
        "چت", "chat", "گفتگو", "conversation", "صحبت",
        "همدل", "همراه", "همفکر", "هم عقیده",
        "همشهری", "هموطن", "ایرانی", "فارسی زبان",
        "تنها", "lonely", "مجرد", "single", "متاهل",
        "دختر", "girl", "پسر", "boy", "زن", "woman", "مرد", "man",
        "جوان", "young", "نوجوان", "teen", "بزرگسال", "adult",
        "همسریابی", "ازدواج", "marriage", "عروسی", "wedding",
        "نامزدی", "engagement", "خواستگاری", "proposal",
        "رابطه", "relationship", "عشق", "love", "دلبر",
        
        # گروه‌های حمایتی
        "حمایت", "support", "کمک", "help", "یاری",
        "مشاوره", "counseling", "راهنمایی", "guidance",
        "تجربه", "experience", "داستان", "story",
        "انگیزه", "motivation", "امید", "hope", "تشویق",
        "بهبود", "recovery", "سلامت", "health", "آرامش",
        "استرس", "stress", "اضطراب", "anxiety", "افسردگی",
        "روانشناسی", "psychology", "خودشناسی", "self help",
        "توسعه فردی", "personal development", "رشد",
        
        # فعالیت‌های گروهی
        "دورهمی", "gathering", "میتاپ", "meetup",
        "پارتی", "party", "جشن", "celebration",
        "پیاده روی", "hiking", "کوهنوردی", "mountain climbing",
        "سفر گروهی", "group travel", "تور", "tour",
        "کلاس گروهی", "group class", "ورکشاپ", "workshop",
        "سمینار", "seminar", "همایش", "conference",
        "نشست", "session", "جلسه", "meeting",
        # +50 community keywords
        "دوستیابی سالم", "safe dating", "رفیق یابی", "find friends", "هم صحبت", "chat buddy",
        "گروه همسایه", "neighbors group", "تبادل کتاب", "book exchange", "زبان آموزی", "lang exchange",
        "گروه هنری", "art community", "راهکارهای زندگی", "lifehacks", "آشنایی تخصصی", "pro meet",
        "نشست کافه", "cafe meetup", "عاشقان سفر", "travel lovers", "دورهمی خانوادگی", "family gathering",
        "شب شعر", "poetry night", "کارگاه", "workshop", "حلقه مطالعه", "reading circle",
        "پذیرایی", "host", "رویکرد حمایتی", "supportive group", "کمک به هم", "mutual help",
        "خانه سالمندان", "elderly support", "دانش آموزان", "students corner", "تبادل پروژه", "project sharing",
        "گروه همیار", "helper group", "راهنمای مهاجر", "immigrant guide", "رویداد locais", "local events",
        "برنامه ریزی گروهی", "group planning", "همکاری تیمی", "team collaboration", "جلسات آنلاین", "online sessions"
    ],
    
    'entertainment_media': [
        # سرگرمی و رسانه
        "فیلم", "movie", "سینما", "cinema", "سریال", "series",
        "انیمه", "anime", "انیمیشن", "animation", "کارتون",
        "نتفلیکس", "Netflix", "آمازون", "Amazon", "دیزنی", "Disney",
        "HBO", "اپل تی وی", "Apple TV", "هولو", "Hulu",
        "فیلم ایرانی", "سریال ایرانی", "فیلم خارجی",
        "دوبله", "dubbed", "زیرنویس", "subtitle",
        "اکشن", "action", "کمدی", "comedy", "درام", "drama",
        "ترسناک", "horror", "علمی تخیلی", "sci-fi",
        "عاشقانه", "romance", "جنایی", "crime", "تریلر", "thriller",
        "مستند", "documentary", "بیوگرافی", "biography",
        
        # موزیک
        "موزیک", "music", "آهنگ", "song", "ترانه", "track",
        "آلبوم", "album", "پلی لیست", "playlist",
        "اسپاتیفای", "Spotify", "اپل موزیک", "Apple Music",
        "ساندکلود", "SoundCloud", "یوتیوب موزیک",
        "پاپ", "pop", "راک", "rock", "رپ", "rap", "هیپ هاپ",
        "الکترونیک", "electronic", "EDM", "دی جی", "DJ",
        "کلاسیک", "classical", "جاز", "jazz", "بلوز", "blues",
        "سنتی", "traditional", "فولک", "folk", "محلی", "local",
        "خواننده", "singer", "رپر", "rapper", "موزیسین", "musician",
        
        # پادکست و یوتیوب
        "پادکست", "podcast", "پادکست فارسی", "Persian podcast",
        "یوتیوب", "YouTube", "یوتیوبر", "YouTuber",
        "ولاگ", "vlog", "ولاگر", "vlogger",
        "استریم", "stream", "استریمر", "streamer",
        "توییچ", "Twitch", "لایو", "live", "زنده",
        # +50 entertainment/media extras
        "شب فیلم", "movie night", "بینگ واچ", "binge watch", "فیلم مستقل", "indie films",
        "فیلم کوتاه", "short films", "نقد فیلم", "movie review", "بحث قسمت", "episode discussion",
        "فیلم نامه", "screenplay", "مولتی مدیا", "multimedia", "پیشنهاد فیلم", "film picks",
        "آهنگ جدید", "new music", "ساز زنده", "live music", "گزارش کنسرت", "concert review",
        "پخش زنده", "live broadcasting", "پخش همزمان", "simulcast", "تماشاچی", "viewer",
        "فاندوم", "fandom", "فیکشِن طرفداران", "fanfiction", "یوتیوب استودیو", "YouTube studio",
        "پادکست تیم", "podcast team", "سریال برتر", "top series", "مستندهای علمی", "science documentaries",
        "فستیوال فیلم", "film festival", "جوایز سینمایی", "film awards", "آرشیو ویدیو", "video archive"
    ],
    
    'sports_fitness': [
        # ورزش‌های محبوب
        "فوتبال", "football", "soccer", "لیگ برتر", "Premier League",
        "لیگ قهرمانان", "Champions League", "جام جهانی", "World Cup",
        "پرسپولیس", "Persepolis", "استقلال", "Esteghlal",
        "رئال مادرید", "Real Madrid", "بارسلونا", "Barcelona",
        "منچستر", "Manchester", "لیورپول", "Liverpool",
        "بایرن", "Bayern", "پاری سن ژرمن", "PSG",
        "یوونتوس", "Juventus", "میلان", "AC Milan",
        "والیبال", "volleyball", "بسکتبال", "basketball", "NBA",
        "تنیس", "tennis", "بدمینتون", "badminton",
        "شنا", "swimming", "دو و میدانی", "athletics",
        "بوکس", "boxing", "MMA", "UFC", "کشتی", "wrestling",
        "کاراته", "karate", "تکواندو", "taekwondo", "جودو", "judo",
        
        # فیتنس و بدنسازی
        "بدنسازی", "bodybuilding", "فیتنس", "fitness",
        "باشگاه", "gym", "ورزش", "workout", "تمرین", "training",
        "کراس فیت", "CrossFit", "HIIT", "تناسب اندام",
        "عضله سازی", "muscle building", "چربی سوزی", "fat burn",
        "شش تیکه", "six pack", "شکم", "abs", "سینه", "chest",
        "بازو", "arms", "پا", "legs", "کمر", "back",
        "وزنه", "weights", "دمبل", "dumbbell", "هالتر", "barbell",
        "کتل بل", "kettlebell", "TRX", "تردمیل", "treadmill",
    
    ],
    
    'tech_gadgets': [
        # گجت‌ها و دستگاه‌ها
        "موبایل", "mobile", "گوشی", "phone", "اسمارت فون", "smartphone",
        "آیفون", "iPhone", "سامسونگ", "Samsung", "شیائومی", "Xiaomi",
        "وان پلاس", "OnePlus", "گوگل پیکسل", "Pixel", "هواوی", "Huawei",
        "تبلت", "tablet", "آیپد", "iPad", "گلکسی تب", "Galaxy Tab",
        "لپ تاپ", "laptop", "مک بوک", "MacBook", "سرفیس", "Surface",
        "کامپیوتر", "computer", "PC", "پی سی", "دسکتاپ", "desktop",
        "ساعت هوشمند", "smartwatch", "اپل واچ", "Apple Watch",
        "ایرپاد", "AirPods", "هدفون", "headphone", "ایربادز", "earbuds",
        "پاوربانک", "powerbank", "شارژر", "charger", "کابل", "cable",
        "قاب گوشی", "phone case", "گلس", "screen protector",
        
        # کنسول و گیمینگ
        "پلی استیشن", "PlayStation", "PS5", "PS4",
        "ایکس باکس", "Xbox", "نینتندو", "Nintendo", "سوییچ", "Switch",
        "کنسول", "console", "دسته بازی", "controller",
        "هدست گیمینگ", "gaming headset", "مانیتور گیمینگ",
        "کارت گرافیک", "graphics card", "GPU", "RTX", "AMD",
        "رم", "RAM", "پردازنده", "CPU", "مادربرد", "motherboard",
        "SSD", "هارد", "hard drive", "کیس", "PC case",
        
    ],
    
    'creative_arts': [
        # هنر دیجیتال
        "طراحی", "design", "گرافیک", "graphic", "دیزاین",
        "فتوشاپ", "Photoshop", "ایلوستریتور", "Illustrator",
        "پروکریت", "Procreate", "کورل", "CorelDRAW",
        "هنر دیجیتال", "digital art", "نقاشی دیجیتال",
        "کاراکتر", "character", "کانسپت آرت", "concept art",
        "انیمیشن", "animation", "موشن گرافیک", "motion graphics",
        "۳D", "سه بعدی", "بلندر", "Blender", "مایا", "Maya",
        "سینما فور دی", "Cinema 4D", "زیبراش", "ZBrush",
        
        # عکاسی
        "عکاسی", "photography", "عکس", "photo", "تصویر", "image",
        "لایتروم", "Lightroom", "ادیت عکس", "photo editing",
        "پرتره", "portrait", "منظره", "landscape", "ماکرو", "macro",
        "عکاسی خیابانی", "street photography", "عکاسی مد",
        "دوربین", "camera", "کانن", "Canon", "نیکون", "Nikon",
        "سونی", "Sony", "فوجی", "Fuji", "لنز", "lens",
        
        # ویدیو و فیلمسازی
        "فیلمسازی", "filmmaking", "فیلمبرداری", "videography",
        "تدوین", "editing", "پریمیر", "Premiere", "فاینال کات",
        "داوینچی", "DaVinci Resolve", "افترافکت", "After Effects",
        "جلوه‌های ویژه", "VFX", "کامپوزیت", "composite",
        "کالر گریدینگ", "color grading", "صداگذاری", "sound design",
        
        # موسیقی و تولید صدا
        "موزیک پروداکشن", "music production", "بیت", "beat",
        "FL Studio", "اف ال استودیو", "ابلتون", "Ableton",
        "لاجیک پرو", "Logic Pro", "کیوبیس", "Cubase",
        "میکس", "mixing", "مسترینگ", "mastering",
        "VST", "پلاگین", "plugin", "سمپل", "sample",
        "سینتی سایزر", "synthesizer", "میدی", "MIDI",
    ],
    
    # 🎯 گسترش هوشمند - دسته‌های تخصصی بیشتر (+1200 کلمه)
    'ecommerce_shopping': [
        # فروشگاه‌های آنلاین ایرانی
        "دیجی کالا", "Digikala", "باسلام", "Basalam", "ترب", "Torob",
        "اسنپ مارکت", "SnappMarket", "اکالا", "Okala", "روکولند",
        "دیجی استایل", "DigiStyle", "مبیت", "Mobit", "تکنولایف",
        "امازون", "Amazon", "علی اکسپرس", "AliExpress", "ای بی", "eBay",
        "شاپیفای", "Shopify", "ووکامرس", "WooCommerce",
        
        # دسته‌بندی محصولات
        "لوازم خانگی", "home appliances", "لوازم آشپزخانه",
        "لوازم الکترونیکی", "electronics", "موبایل و تبلت",
        "لپ تاپ و کامپیوتر", "پوشاک", "clothing", "کفش و کیف",
        "آرایشی بهداشتی", "cosmetics", "عطر و ادکلن", "perfume",
        "زیورآلات", "jewelry", "ساعت", "watches", "عینک", "glasses",
        "لوازم ورزشی", "sports equipment", "کتاب و لوازم التحریر",
        "اسباب بازی", "toys", "لوازم کودک", "baby products",
        "مواد غذایی", "groceries", "سوپرمارکت", "supermarket",
        
        # تخفیف و پیشنهادات
        "تخفیف", "discount", "حراج", "sale", "فروش ویژه",
        "کد تخفیف", "coupon code", "کوپن", "پیشنهاد شگفت انگیز",
        "ارزان", "cheap", "قیمت مناسب", "بهترین قیمت",
        "مقایسه قیمت", "price comparison", "ضمانت قیمت",
        "ارسال رایگان", "free shipping", "تحویل فوری", "express",
        "پرداخت در محل", "COD", "اقساط", "installment"
    ],
    
    'jobs_careers': [
        # مشاغل و حرفه‌ها
        "برنامه نویس", "programmer", "توسعه دهنده", "developer",
        "طراح", "designer", "گرافیست", "graphic designer",
        "طراح وب", "web designer", "طراح UI", "UI designer",
        "مدیر محصول", "product manager", "مدیر پروژه", "project manager",
        "تحلیلگر داده", "data analyst", "دانشمند داده", "data scientist",
        "مهندس نرم افزار", "software engineer", "مهندس DevOps",
        "ادمین سیستم", "system admin", "مهندس شبکه", "network engineer",
        "متخصص امنیت", "security specialist", "تست نویس", "QA engineer",
        
        # مشاغل سنتی
        "حسابدار", "accountant", "منشی", "secretary", "مدیر اداری",
        "منابع انسانی", "HR", "بازاریاب", "marketer", "فروشنده", "salesperson",
        "راننده", "driver", "نگهبان", "security guard", "خدمات", "cleaner",
        "آشپز", "chef", "پیشخدمت", "waiter", "صندوقدار", "cashier",
        "مکانیک", "mechanic", "برقکار", "electrician", "لوله کش", "plumber",
        "نجار", "carpenter", "نقاش ساختمان", "painter", "جوشکار", "welder",
        "خیاط", "tailor", "آرایشگر", "hairdresser", "آشپز", "cook",
        
        # پزشکی و درمان
        "پزشک", "doctor", "پرستار", "nurse", "داروساز", "pharmacist",
        "دندانپزشک", "dentist", "فیزیوتراپیست", "physiotherapist",
        "روانشناس", "psychologist", "تغذیه", "nutritionist",
        "رادیولوژیست", "radiologist", "آزمایشگاه", "lab technician",
        
        # آموزش
        "معلم", "teacher", "استاد", "professor", "مربی", "instructor",
        "مدرس", "tutor", "آموزگار", "educator",
        # +50 additional job related keywords
        "دیتا انجنییر", "data engineer", "ML engineer", "machine learning engineer", "backend developer", "frontend developer",
        "full-stack", "full stack developer", "mobile developer", "iOS developer", "Android developer", "game developer",
        "Unity developer", "Unreal developer", "AI researcher", "NLP engineer", "SRE", "site reliability engineer",
        "cloud engineer", "QA analyst", "test automation", "support engineer", "IT consultant", "business analyst",
        "product owner", "product owner", "startup founder", "co-founder", "C-level", "CTO", "CEO",
        "chief marketing officer", "CMO", "chief financial officer", "CFO", "intern", "trainee", "apprentice",
        "lab assistant", "medical researcher", "pharmacologist", "therapist", "speech therapist", "occupational therapist",
        "HR manager", "recruiter", "talent scout", "sales manager", "account manager", "customer success"
    ],
    
    'real_estate': [
        # املاک و مستغلات
        "املاک", "real estate", "خرید خانه", "buy house",
        "فروش آپارتمان", "sell apartment", "اجاره", "rent",
        "رهن", "mortgage", "رهن کامل", "full mortgage",
        "آپارتمان", "apartment", "ویلا", "villa", "خانه", "house",
        "زمین", "land", "مغازه", "shop", "دفتر کار", "office",
        "انبار", "warehouse", "سوله", "industrial",
        
        # مشخصات ملک
        "متراژ", "square meter", "طبقه", "floor", "اتاق خواب", "bedroom",
        "پارکینگ", "parking", "انباری", "storage", "آسانسور", "elevator",
        "بالکن", "balcony", "تراس", "terrace", "حیاط", "yard",
        "نوساز", "new build", "بازسازی", "renovated", "کلنگی", "old",
        
        # مناطق
        "شمال تهران", "north Tehran", "غرب تهران", "west Tehran",
        "مرکز شهر", "downtown", "حومه", "suburb",
        "ساحلی", "beachfront", "کوهستانی", "mountain view",
        
        # مشاور و آژانس
        "مشاور املاک", "real estate agent", "آژانس املاک",
        "کمیسیون", "commission", "قرارداد", "contract",
        "سند", "deed", "وکالت", "power of attorney",
        "خرید زمین", "buy land", "زمین کشاورزی", "agricultural land", "ملک تجاری", "commercial property",
        "رهن و اجاره", "mortgage and rent", "اجاره کوتاه مدت", "short term rent", "Airbnb", "رزرو اجاره",
        "کاهش قیمت", "price drop", "خانه لوکس", "luxury home", "سرمایه گذاری املاک", "real estate investment",
        "هزینه نگهداری", "maintenance cost", "مدیریت املاک", "property management", "تخریب و بازسازی", "renovation",
        "مستندات ملک", "property docs", "وام مسکن", "housing loan", "پروسه خرید", "buying process",
        "بازار مسکن", "housing market", "بازار اجاره", "rental market", "املاک ساحلی", "beachfront property",
        "ملک کشاورزی", "farm land", "ویلا ساحلی", "beach villa", "کو اپ", "co-op housing"
    ],
    
    'automotive_parts': [
        # قطعات خودرو
        "لوازم یدکی", "spare parts", "قطعات اصلی", "OEM parts",
        "موتور", "engine", "گیربکس", "gearbox", "کلاچ", "clutch",
        "ترمز", "brake", "لنت", "brake pad", "دیسک ترمز", "brake disc",
        "فنربندی", "suspension", "کمک فنر", "shock absorber",
        "فرمان", "steering", "جعبه فرمان", "steering box",
        "رادیاتور", "radiator", "واتر پمپ", "water pump",
        "شمع", "spark plug", "فیلتر روغن", "oil filter",
        "فیلتر هوا", "air filter", "فیلتر بنزین", "fuel filter",
        "باتری", "battery", "دینام", "alternator", "استارت", "starter",
        "سنسور", "sensor", "ECU", "ایسیو", "چراغ", "headlight",
        "آینه", "mirror", "شیشه", "windshield", "برف پاک کن", "wiper",
        
        # تیونینگ و اسپرت
        "تیونینگ", "tuning", "اسپرت", "sport", "کیت بدنه", "body kit",
        "اگزوز", "exhaust", "ریمپ", "remap", "چیپ تیونینگ",
        "رینگ", "wheel", "رینگ اسپرت", "لاستیک", "tire",
        "تعویض روغن", "oil change", "سرویس ترمز", "brake service", "تنظیم فرمان", "wheel alignment",
        "تعمیر گیربکس", "gearbox repair", "تعمیر موتور", "engine repair", "ریمپ ECU", "ECU remap",
        "تیونینگ", "tuning", "پرفورمنس پارت", "performance parts", "توربو", "turbo",
        "سوپرشارژر", "supercharger", "باتری خودرو", "car battery", "تعویض لاستیک", "tire change",
        "تعویض شمع", "spark plug change", "تعویض فیلتر", "filter change", "کارواش", "car wash",
        "دیتیلینگ", "detailing", "سیستم صوتی", "car audio", "کیت بدنه", "body kit",
        "سنسور پارک", "parking sensor", "دوربین دنده عقب", "rear camera", "شارژ ای وی", "EV charging",
        "ای وی قطعات", "EV parts", "شارژ سریع", "fast charging"
        "سیستم صوتی", "car audio", "ساب ووفر", "subwoofer"
    ],
    
    'cooking_recipes': [
        # آشپزی و دستور پخت
        "دستور پخت", "recipe", "آموزش آشپزی", "cooking tutorial",
        "غذای ایرانی", "Persian food", "غذای خانگی", "homemade",
        "فست فود", "fast food", "غذای رژیمی", "diet food",
        "غذای گیاهی", "vegetarian", "وگان", "vegan",
        "بدون گلوتن", "gluten free", "کم کالری", "low calorie",
        "کتوژنیک", "keto", "پروتئین بالا", "high protein",
        
        # انواع غذا
        "پلو", "rice", "خورشت", "stew", "کباب", "kebab",
        "سوپ", "soup", "سالاد", "salad", "پیش غذا", "appetizer",
        "دسر", "dessert", "شیرینی", "pastry", "کیک", "cake",
        "نان", "bread", "پاستا", "pasta", "پیتزا", "pizza",
        "ساندویچ", "sandwich", "برگر", "burger", "سوشی", "sushi",
        
        # تکنیک‌های پخت
        "سرخ کردن", "frying", "کبابی", "grilling", "بخارپز", "steaming",
        "فر", "oven", "مایکروویو", "microwave", "ایر فرایر", "air fryer",
        "اسلو کوکر", "slow cooker", "زودپز", "pressure cooker",
        
        # مواد اولیه
        "گوشت", "meat", "مرغ", "chicken", "ماهی", "fish",
        "سبزیجات", "vegetables", "میوه", "fruits", "حبوبات", "legumes",
        "ادویه", "spices", "سس", "sauce", "روغن", "oil"
    ],
    
    'parenting_family': [
        # والدین و فرزندپروری
        "فرزندپروری", "parenting", "مادر", "mother", "پدر", "father",
        "والدین", "parents", "بارداری", "pregnancy", "نوزاد", "newborn",
        "شیردهی", "breastfeeding", "تغذیه نوزاد", "baby feeding",
        "خواب کودک", "baby sleep", "رشد کودک", "child development",
        "بازی کودک", "child play", "آموزش کودک", "child education",
        "تربیت", "discipline", "رفتار کودک", "child behavior",
        
        # سنین مختلف
        "نوزاد", "infant", "نوپا", "toddler", "پیش دبستانی", "preschool",
        "دبستانی", "elementary", "نوجوان", "teenager", "بلوغ", "puberty",
        
        # سلامت کودک
        "واکسن", "vaccine", "اطفال", "pediatric", "دندان شیری",
        "رشد قد", "height growth", "وزن کودک", "child weight",
        "بیماری کودک", "child illness", "آلرژی", "allergy",
        
        # لوازم کودک
        "سیسمونی", "baby gear", "کالسکه", "stroller", "صندلی ماشین",
        "تخت کودک", "crib", "پوشک", "diaper", "شیشه شیر", "bottle",
        "لباس بچه", "baby clothes", "اسباب بازی", "toys",
        # +50 parenting & family keywords
        "گروه مادران", "moms group", "گروه پدران", "dads group", "پس از زایمان", "postpartum",
        "تغذیه نوزاد", "baby nutrition", "شیرخوار", "infant care", "قطعه حرف", "weaning",
        "مشاوره شیردهی", "breastfeeding support", "آموزش والدین", "parenting class", "مهارت های فرزندپروری", "parenting skills",
        "آموزش نوپا", "toddler education", "مهد کودک مجازی", "virtual preschool", "گروه نوزادان", "newborn group",
        "سلامت روان والدین", "parent mental health", "همراهی والدین", "parent support", "مرکز والدین", "parent hub",
        "تبادل تجربه والدین", "parent experience exchange", "پیشنهاد بازی", "play ideas", "بازی های آموزشی", "educational games",
        "مدرسه والدین", "parent school", "برنامه رشد کودک", "child development program", "مشاوره تربیتی", "parent counseling"
    ],
    
    'beauty_skincare': [
        # مراقبت پوست
        "مراقبت پوست", "skincare", "پوست", "skin", "روتین پوست",
        "پاک کننده", "cleanser", "تونر", "toner", "سرم", "serum",
        "مرطوب کننده", "moisturizer", "ضد آفتاب", "sunscreen", "SPF",
        "آنتی ایجینگ", "anti aging", "ضد چروک", "anti wrinkle",
        "روشن کننده", "brightening", "ضد لک", "dark spot",
        "منافذ", "pores", "جوش", "acne", "ضد جوش", "anti acne",
        "پوست چرب", "oily skin", "پوست خشک", "dry skin",
        "پوست حساس", "sensitive skin", "پوست مختلط", "combination",
        
        # مراقبت مو
        "مراقبت مو", "hair care", "شامپو", "shampoo", "نرم کننده", "conditioner",
        "ماسک مو", "hair mask", "روغن مو", "hair oil", "سرم مو", "hair serum",
        "ریزش مو", "hair loss", "رشد مو", "hair growth", "موی خشک", "dry hair",
        "موی چرب", "oily hair", "شوره", "dandruff", "رنگ مو", "hair color",
        
        # آرایش
        "آرایش", "makeup", "فوندیشن", "foundation", "کانسیلر", "concealer",
        "پودر", "powder", "رژ لب", "lipstick", "رژ گونه", "blush",
        "سایه چشم", "eyeshadow", "خط چشم", "eyeliner", "ریمل", "mascara",
        "ابرو", "eyebrow", "هایلایتر", "highlighter", "کانتور", "contour",
        
        # برندها
        "اوردینری", "The Ordinary", "سراوی", "CeraVe", "لاروش", "La Roche",
        "نوتروژینا", "Neutrogena", "نیوآ", "Nivea", "بیودرما", "Bioderma",
        "مک", "MAC", "میبلین", "Maybelline", "لورال", "L'Oreal",
        "نارس", "NARS", "شارلوت تیلبوری", "Charlotte Tilbury",
        # +50 beauty & skincare keywords
        "مراقبت از پوست حساس", "sensitive skincare", "روتین شب", "night routine", "روتین صبح", "morning routine",
        "ماسک صورت", "face mask", "اسکراب", "scrub", "پاکسازی", "deep clean",
        "سرم ضدلک", "brightening serum", "ضد جوش", "acne treatment", "پاک کننده آرایش", "makeup remover",
        "پوست چرب", "oily skin", "پوست خشک", "dry skin", "مدیریت چربی", "oil control",
        "میکاپ روزانه", "daily makeup", "میکاپ محترس", "glam makeup", "برس آرایشی", "makeup brushes",
        "تخفیف آرایشی", "beauty sale", "لنز رنگی", "colored contacts", "ریمل ضدآب", "waterproof mascara",
        "ترند زیبایی", "beauty trends", "محصولات وگان", "vegan beauty", "حساسیتی", "hypoallergenic",
        "مراقبت از ناخن", "nail care", "مانیکور در خانه", "home manicure", "لاک ژل", "gel polish"
    ],
    
    'legal_finance': [
        # حقوقی
        "وکیل", "lawyer", "وکالت", "legal", "دادگاه", "court",
        "دادخواست", "lawsuit", "شکایت", "complaint", "مشاوره حقوقی",
        "قرارداد", "contract", "طلاق", "divorce", "مهریه", "dowry",
        "ارث", "inheritance", "وصیت", "will", "ملکی", "property law",
        "کیفری", "criminal", "حقوقی", "civil", "تجاری", "commercial",
        "کار", "labor law", "مالیاتی", "tax law", "بیمه", "insurance law",
        
        # مالی و بانکی
        "بانک", "bank", "حساب", "account", "کارت", "card",
        "انتقال وجه", "transfer", "پرداخت", "payment", "برداشت", "withdrawal",
        "وام", "loan", "اقساط", "installment", "سود", "interest",
        "سپرده", "deposit", "سرمایه گذاری", "investment",
        "صندوق", "fund", "سهام", "stock", "اوراق", "bond",
        
        # بیمه
        "بیمه", "insurance", "بیمه عمر", "life insurance",
        "بیمه سلامت", "health insurance", "بیمه خودرو", "car insurance",
        "بیمه آتش سوزی", "fire insurance", "بیمه مسافرتی", "travel insurance",
        "خسارت", "claim", "فرانشیز", "deductible", "حق بیمه", "premium",
        # +50 legal & finance keywords
        "قانون شرکتها", "corporate law", "قوانین مالیاتی", "tax regulations", "Securities", "بورس قوانين",
        "رفع سوءاحتمال", "anti money laundering", "AML", "KYC", "Compliance", "رعایت مقررات",
        "دعوای حقوقی", "litigation", "داوری", "arbitration", "حل اختلاف", "dispute resolution",
        "ورشکستگی", "insolvency", "اعسار", "bankruptcy", "حفظ دارایی", "asset protection",
        "سپرده سرمایه", "investment funds", "ETF", "صندوق سرمایه گذاری", "سرمایه گذاری خصوصی", "private equity",
        "حسابرسی", "audit", "حسابرسی داخلی", "internal audit", "صورت های مالی", "financial statements",
        "سرمایه گذاری خطرپذیر", "venture capital", "مالیات شرکتی", "corporate tax", "مالیات بر ارزش افزوده", "VAT"
    ],
    
    'pets_animals': [
        # حیوانات خانگی
        "سگ", "dog", "گربه", "cat", "پرنده", "bird", "ماهی", "fish",
        "خرگوش", "rabbit", "همستر", "hamster", "خوکچه هندی", "guinea pig",
        "طوطی", "parrot", "قناری", "canary", "مرغ عشق", "lovebird",
        "سگ نگهبان", "guard dog", "سگ آپارتمانی", "apartment dog",
        "گربه پرشین", "Persian cat", "گربه بریتیش", "British cat",
        
        # نگهداری
        "غذای حیوانات", "pet food", "غذای سگ", "dog food",
        "غذای گربه", "cat food", "غذای ماهی", "fish food",
        "لوازم حیوانات", "pet supplies", "قفس", "cage", "آکواریوم", "aquarium",
        "خاک گربه", "cat litter", "قلاده", "collar", "بند", "leash",
        "اسباب بازی حیوانات", "pet toys", "تشک", "pet bed",
        
        # بهداشت و سلامت
        "دامپزشک", "vet", "واکسن", "vaccination", "عقیم سازی", "neutering",
        "پشم", "fur", "ناخن", "nail", "حمام", "grooming",
        "انگل", "parasite", "کک", "flea", "کنه", "tick",
        
        # نژادها
        "ژرمن شپرد", "German Shepherd", "گلدن", "Golden Retriever",
        "هاسکی", "Husky", "پودل", "Poodle", "شیتزو", "Shih Tzu",
        "پاگ", "Pug", "بولداگ", "Bulldog", "چیهواهوا", "Chihuahua",
        # +50 pets & animals additions
        "پذیرش حیوانات", "pet adoption", "پناهگاه حیوانات", "animal shelter", "نجات حیوانات", "animal rescue",
        "پرورش سگ", "dog breeding", "تربیت توله", "puppy training", "آموزش سگ", "dog training",
        "باشگاه آموزش", "training club", "اسپیش", "spa for pets", "ترمیم و زیبایی حیوانات", "pet grooming",
        "عطاری حیوان", "pet herbs", "آکواریوم گیفت", "aquarium hobby", "نمایشگاه حیوانات", "pet show",
        "پرنده باز", "bird lovers", "ماهی آکواریومی", "aquatic fish", "مرغ عشق‌دارها", "lovebird fans",
        "اسب سواری", "horse riding", "باشگاه اسب", "equestrian club", "اسب نگهداری", "equine care",
        "تجهیزات حیوان", "pet equipment", "اسباب بازی های حیوان", "pet toys", "مناسبت حیوان", "pet events",
        "نژاد خاص", "rare breeds", "گروه حمایت", "support group", "سگ نگهبان", "guard dog club"
    ],
    
    'diy_crafts': [
        # کاردستی و دست سازه
        "کاردستی", "DIY", "دست ساز", "handmade", "هنر دستی", "craft",
        "خیاطی", "sewing", "بافتنی", "knitting", "قلاب بافی", "crochet",
        "گلدوزی", "embroidery", "چرم دوزی", "leather craft",
        "ساخت جواهر", "jewelry making", "مهره بافی", "beading",
        "رزین", "resin art", "اپوکسی", "epoxy", "شمع سازی", "candle making",
        "صابون سازی", "soap making", "عطرسازی", "perfume making",
        
        # نقاشی و هنر
        "نقاشی", "painting", "آبرنگ", "watercolor", "رنگ روغن", "oil painting",
        "اکریلیک", "acrylic", "پاستل", "pastel", "مداد رنگی", "colored pencil",
        "طراحی", "drawing", "اسکیس", "sketch", "کاریکاتور", "caricature",
        "خوشنویسی", "calligraphy", "لترینگ", "lettering",
        
        # چوب و فلز
        "نجاری", "woodworking", "چوب", "wood", "فلزکاری", "metalwork",
        "جوشکاری هنری", "art welding", "مجسمه سازی", "sculpture",
        "سفال", "pottery", "سرامیک", "ceramics", "چرخ سفالگری",
        
        # بازیافت و اپسایکل
        "بازیافت", "recycling", "اپسایکل", "upcycle", "دوباره سازی",
        # +50 DIY & craft keywords
        "فروش آثار", "sell handmade", "بازار هنری", "craft market", "اتسی", "Etsy",
        "فروشگاه دست ساز", "handmade shop", "کارگاه ساخت", "make workshop", "راهنما ساخت", "how-to guide",
        "آموزش خیاطی", "sewing tutorial", "کارگاه نجاری", "woodworking class", "طراحی جواهر", "jewelry design",
        "نقاشی کودک", "kids painting", "هنر خلاق", "creative art", "پتینه کاری", "patina technique",
        "گره زدن", "macrame", "ساخت زیورآلات", "bead jewelry", "رزین اپوکسی", "epoxy resin",
        "تزیینات دست", "handmade decor", "ساخت شمع", "candle craft", "صابون سازی", "soap craft",
        "آموزش کاردستی", "crafts for kids", "فروش آنلاین آثار", "online handmade sell", "گرامیداشت هنر", "art tribute",
        "دوره تخصصی صنایع دستی", "handicraft course", "بازار نمایشگاهی", "exhibition market"
    ],
    
    'nature_outdoors': [
        # طبیعت گردی
        "طبیعت", "nature", "طبیعت گردی", "hiking", "کوهنوردی", "mountaineering",
        "کمپینگ", "camping", "چادر", "tent", "ساک خواب", "sleeping bag",
        "کوله پشتی", "backpack", "پیاده روی", "trekking", "صخره نوردی", "climbing",
        
        # مکان‌ها
        "جنگل", "forest", "کوه", "mountain", "دریا", "sea", "ساحل", "beach",
        "رودخانه", "river", "آبشار", "waterfall", "دریاچه", "lake",
        "کویر", "desert", "غار", "cave", "چشمه", "spring",
        "پارک ملی", "national park", "منطقه حفاظت شده",
        
        # فعالیت‌ها
        "ماهیگیری", "fishing", "شکار", "hunting", "پرنده نگری", "birdwatching",
        "عکاسی طبیعت", "nature photography", "ستاره شناسی آماتور",
        "دوچرخه سواری کوهستان", "mountain biking", "رفتینگ", "rafting",
        "کایاک", "kayaking", "قایق سواری", "boating",
        
        # گیاهان
        "گل", "flower", "گیاه", "plant", "درخت", "tree", "باغبانی", "gardening",
        "گیاهان آپارتمانی", "houseplants", "کاکتوس", "cactus", "ساکولنت", "succulent",
        # +50 nature/outdoors keywords
        "ستاره شناسی", "astronomy", "ستاره ها", "stargazing", "عکاسی نجومی", "astrophotography",
        "حیات وحش", "wildlife", "مشاهده حیوانات", "animal watching", "پرنده نگری پیشرفته", "advanced birdwatching",
        "کمپینگ خانوادگی", "family camping", "کمپینگ لوکس", "glamping", "بقای در طبیعت", "survival skills",
        "مسیرهای طبیعت", "nature trails", "نگهداری مسیر", "trail maintenance", "پاکسازی طبیعت", "nature cleanup",
        "مستندسازی طبیعت", "nature documentary", "اکوتوریسم", "ecotourism", "پانسیون های کوهستانی", "mountain huts",
        "کوهنوردی فنی", "technical climbing", "طناب کشی", "rope climbing", "راهنمای کوه", "mountain guide",
        "ماجراجویان", "adventurers", "برنامه سفر طبیعت", "nature travel plan", "قاب عکس طبیعت", "nature photography contest"
    ],
    
    'events_celebrations': [
        # مناسبت‌ها
        "عید", "Eid", "نوروز", "Nowruz", "عید نوروز", "سال نو",
        "یلدا", "Yalda", "چهارشنبه سوری", "سیزده بدر",
        "عید فطر", "عید قربان", "محرم", "تاسوعا", "عاشورا",
        "کریسمس", "Christmas", "سال نو میلادی", "New Year",
        "ولنتاین", "Valentine", "روز مادر", "Mother's Day",
        "روز پدر", "Father's Day", "روز معلم", "Teacher's Day",
        
        # جشن‌ها
        "تولد", "birthday", "جشن تولد", "birthday party",
        "عروسی", "wedding", "نامزدی", "engagement", "سالگرد", "anniversary",
        "فارغ التحصیلی", "graduation", "ترفیع", "promotion",
        "خانه جدید", "housewarming", "بچه دار شدن", "baby shower",
        
        # تزیینات و برنامه‌ریزی
        "تزیین", "decoration", "بادکنک", "balloon", "گل آرایی", "flower arrangement",
        "کیک", "cake", "شمع", "candle", "کارت دعوت", "invitation",
        "هدیه", "gift", "کادو", "present", "بسته بندی", "gift wrapping",
        "تشریفات", "ceremony", "DJ", "موزیک مجلس", "catering",
        # +50 events & celebrations additions
          ],
    
    # 🔥 کلمات کلیدی بسیار محبوب و تضمینی (+300 کلمه)
    # این کلمات 100% توسط انسان‌ها برای نام گروه استفاده می‌شوند
    'guaranteed_popular': [
        # کلمات فارسی پرکاربرد در نام گروه‌ها
        "چت", "گپ", "دوستی", "آشنایی", "رفاقت",
        "پاتوق", "همدل", "یاران", "دوستان", "رفقا",
        "شب", "روز", "صبح", "عصر", "شبانه",
        "خنده", "شادی", "طنز", "جوک", "سرگرمی",
        "عشق", "دل", "قلب", "احساس", "رمانتیک",
        "موسیقی", "آهنگ", "موزیک", "ترانه", "صدا",
        "فیلم", "سینما", "سریال", "کلیپ", "ویدیو",
        "کتاب", "شعر", "ادبیات", "داستان", "رمان",
        "ورزش", "فوتبال", "بدنسازی", "فیتنس", "سلامت",
        "آشپزی", "غذا", "کیک", "شیرینی", "دسر",
        "مد", "فشن", "استایل", "لباس", "زیبایی",
        "سفر", "گردش", "تور", "طبیعت", "کوه",
        "عکس", "عکاسی", "فتو", "تصویر", "گالری",
        "هنر", "نقاشی", "طراحی", "گرافیک", "خلاقیت",
        "آموزش", "یادگیری", "درس", "کنکور", "زبان",
        "کار", "شغل", "استخدام", "درآمد", "پول",
        "خرید", "فروش", "بازار", "تخفیف", "حراج",
        "تکنولوژی", "موبایل", "کامپیوتر", "لپ تاپ", "گجت",
        "بازی", "گیم", "پلی", "آنلاین", "سرور",
        "خبر", "اخبار", "روز", "جدید", "آپدیت",
        
        # ترکیبات فوق محبوب
        "چت روم", "گروه چت", "پاتوق دوستان",
        "یاران همدل", "دوستی و آشنایی",
        "شب زنده داران", "شب نشینی", "پاتوق شبانه",
        "خنده و شادی", "طنز و جوک", "میم و خنده",
        "عاشقانه ها", "دل نوشته", "احساسی ها",
        "موزیک باز", "موسیقی دوستان", "آهنگ جدید",
        "فیلم و سریال", "سینما دوستان", "نقد فیلم",
        "کتاب خوان", "شعر و ادب", "داستان کوتاه",
        "ورزش و سلامت", "بدنسازی حرفه ای", "فوتبالی ها",
        "آشپزی خانگی", "دستور پخت", "کیک و شیرینی",
        "مد و زیبایی", "استایل روز", "فشن ایران",
        "سفر و گردشگری", "طبیعت گردی", "عکس طبیعت",
        "هنر و خلاقیت", "طراحی گرافیک", "نقاشی دیجیتال",
        "آموزش زبان", "انگلیسی", "آیلتس و تافل",
        "کار و استخدام", "فرصت شغلی", "کسب درآمد",
        "خرید و فروش", "بازار آنلاین", "تخفیف ویژه",
        "تکنو", "گجت جدید", "بررسی موبایل",
        "گیمر ها", "بازی آنلاین", "پابجی موبایل",
        "اخبار روز", "خبر فوری", "آپدیت روزانه",
        
        # کلمات انگلیسی تضمینی
        "chat", "group", "club", "team", "family",
        "friends", "love", "life", "fun", "happy",
        "music", "movie", "game", "sport", "food",
        "fashion", "beauty", "travel", "photo", "art",
        "tech", "news", "free", "VIP", "pro",
        "Iran", "Persian", "Tehran", "official", "original",
        
        # اسامی شهرهای ایران (همه گروه دارند)
        "تهران", "مشهد", "اصفهان", "شیراز", "تبریز",
        "کرج", "اهواز", "قم", "رشت", "کرمانشاه",
        "ارومیه", "زاهدان", "کرمان", "همدان", "یزد",
        "اردبیل", "بندرعباس", "ساری", "قزوین", "زنجان",
        "گیلان", "مازندران", "خراسان", "آذربایجان", "کردستان",
        
        # ایموجی‌های رایج در نام گروه (به صورت متنی)
        "گروه رسمی", "کانال اصلی", "تیم", "انجمن",
        "باشگاه", "آکادمی", "مدرسه", "آموزشگاه",
        "فروشگاه", "شاپ", "مارکت", "استور",
        "نیوز", "مگ", "مجله", "رسانه",
        "پادکست", "رادیو", "تی وی", "مدیا",
        
        # کلمات کوتاه و ساده (بسیار رایج)
        "ما", "من", "تو", "ایران", "فارسی",
        "نو", "کلاب", "هاب", "زون", "نت",
        "پلاس", "گلد", "وی آی پی", "پرو",
        "تاپ", "بست", "فرست", "وان", "نامبر وان",
        
        # عبارات دعوت‌کننده
        "بیا تو", "جوین شو", "عضو شو", "همراه ما",
        "با ما باش", "کنار ما", "در کنار هم",
        "همه با هم", "یکی برای همه", "خانواده",
        
        # موضوعات داغ و ترند
        "کریپتو", "بیت کوین", "ترید", "سیگنال",
        "ایردراپ", "NFT", "هوش مصنوعی", "AI",
        "چت جی پی تی", "میدجرنی", "فریلنس", "دورکاری",
        "استارتاپ", "کارآفرینی", "سرمایه گذاری",
        "یوتیوب", "اینستاگرام", "تیک تاک", "پادکست",
        
        # کلمات عاطفی و انگیزشی
        "امید", "آرزو", "رویا", "موفقیت", "پیروزی",
        "انگیزه", "انرژی", "مثبت", "شاد", "خوشحال",
        "زندگی", "لایف", "هدف", "رشد", "پیشرفت",
        "توسعه", "بهتر", "برتر", "عالی", "فوق العاده",
        
        # کلمات گروهی
        "گروه", "تیم", "جمع", "انجمن", "اتحادیه",
        # +50 حرفا/کلمات محبوب دیگر
        "همکاران", "coworkers", "همکلاسی ها", "classmates", "الومی", "alumni",
        "مادران", "mothers", "پدران", "fathers", "والدین", "parents",
        "همسایه ها", "neighbors", "دوستداران قهوه", "coffee lovers", "قهوه دوستان",
        "کتاب دوستان", "book lovers", "باشگاه کتاب", "book club", "نقد کتاب", "book review",
        "تماشاچیان", "viewers", "فیلم دوستان", "movie lovers", "سینما کلاب", "cinema club",
        "نویسندگان", "writers", "وبلاگ نویس ها", "bloggers", "کپی رایتر", "copywriters",
        "گروه موسیقی", "band", "موزیک دوستان", "music lovers", "پادکست سازها", "podcasters",
        "عکاسان", "photographers", "عکاسی حرفه ای", "pro photographers", "عکاسی آماتور", "photo hobby",
        "کوهنوردان", "mountaineers", "کمپینگ دوستان", "campers", "ماهیگیران", "fishermen",
        "دوستداران حیوانات", "pet lovers", "سگ باز", "dog lovers", "گربه باز", "cat lovers",
        "بازاریابان", "marketers", "طراحان گرافیک", "graphic designers", "برنامه نویسان", "developers",
        "طراحان وب", "web designers", "استارتاپ ها", "startups", "نوآوران", "innovators",
        "تیم تحقیقاتی", "research team", "علوم", "science club", "آموزش آنلاین", "online courses"
        "شبکه", "کامیونیتی", "فروم", "چنل", "کانال"
    ]
}


# ═══════════════════════════════════════════════════════════
# 🚂 Railway: محدود کردن BASE_KEYWORDS در حالت eco
# ═══════════════════════════════════════════════════════════
def _optimize_base_keywords():
    """محدود کردن تعداد کلمات کلیدی برای صرفه‌جویی در RAM"""
    global BASE_KEYWORDS
    
    if RAILWAY_MODE == 'eco':
        optimized = {}
        for category, keywords in BASE_KEYWORDS.items():
            if len(keywords) > MAX_KEYWORDS_PER_CATEGORY:
                # انتخاب تصادفی کلمات
                optimized[category] = random.sample(keywords, MAX_KEYWORDS_PER_CATEGORY)
            else:
                optimized[category] = keywords
        BASE_KEYWORDS = optimized
        # اجرای garbage collection برای آزادسازی RAM
        gc.collect()

# اجرای بهینه‌سازی در صورت نیاز
_optimize_base_keywords()


# ═══════════════════════════════════════════════════════════════════════════════
# 🎯🎯🎯 سیستم جستجوی هدفمند با اولویت‌بندی سه‌لایه (PRIORITY-BASED SEARCH) 🎯🎯🎯
# ═══════════════════════════════════════════════════════════════════════════════
# 
# 🥇 اولویت 1: ترید، کریپتو، رمزارز، پراپ تریدینگ، بیتکوین (70% تمرکز)
# 🥈 اولویت 2: مهاجرت، اقامت، ویزا، ایرانیان خارج از کشور (25% تمرکز)
# 🥉 اولویت 3: متفرقه و عمومی (5% تمرکز)
#
# ═══════════════════════════════════════════════════════════════════════════════

# 🎚️ تنظیمات توزیع جستجو بر اساس اولویت
SEARCH_PRIORITY_WEIGHTS = {
    'crypto_trading': 0.35,    # 35% جستجوها برای ترید/کریپتو
    'medical': 0.25,           # 25% جستجوها برای پزشکی/دارو/تجهیزات (🆕 اولویت بالا)
    'immigration': 0.20,       # 20% جستجوها برای مهاجرت
    'general': 0.20            # 20% جستجوها برای عمومی (افزایش برای یافتن گروه‌های بیشتر)
}

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀🚀🚀 کلمات کلیدی فوق‌العاده ساده و مؤثر برای یافتن 500+ گروه 🚀🚀🚀
# ═══════════════════════════════════════════════════════════════════════════════
# این کلمات ساده ولی بسیار مؤثر هستند و تضمین می‌کنند صدها گروه پیدا شود
# ═══════════════════════════════════════════════════════════════════════════════

SUPER_EFFECTIVE_KEYWORDS = [
    # ══════════════════════════════════════════════════════════════════════════
    # 🇮🇷 کلمات ایرانی ساده و پرکاربرد (تضمین نتیجه)
    # ══════════════════════════════════════════════════════════════════════════
    "ایران", "ایرانی", "فارسی", "تهران", "پارسی", "ایرانیان",
    "گروه ایرانی", "چت ایرانی", "گپ فارسی", "ایرانی ها",
    "فارسی زبان", "فارسی زبانان", "پرشین", "persian",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🏙️ شهرهای بزرگ ایران و استان‌ها
    # ══════════════════════════════════════════════════════════════════════════
    "تهران", "مشهد", "اصفهان", "شیراز", "تبریز", "کرج", "اهواز",
    "قم", "کرمان", "ارومیه", "رشت", "زاهدان", "همدان", "یزد",
    "کرمانشاه", "اردبیل", "بندرعباس", "ساری", "قزوین", "زنجان",
    "سنندج", "گرگان", "خرم‌آباد", "بجنورد", "بیرجند", "شهرکرد",
    "بوشهر", "ایلام", "سمنان", "اراک", "یاسوج", "کیش", "قشم",
    "گیلان", "مازندران", "خراسان", "آذربایجان", "کردستان", "خوزستان",
    "فارس", "هرمزگان", "سیستان", "بلوچستان", "لرستان", "گلستان",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🌍 ایرانیان خارج از کشور (گروه‌های بزرگ)
    # ══════════════════════════════════════════════════════════════════════════
    "ایرانیان ترکیه", "ایرانیان استانبول", "ایرانیان آنکارا", "ایرانیان ازمیر",
    "ایرانیان امارات", "ایرانیان دبی", "ایرانیان ابوظبی", "ایرانیان شارجه",
    "ایرانیان کانادا", "ایرانیان تورنتو", "ایرانیان ونکوور", "ایرانیان مونترال",
    "ایرانیان آلمان", "ایرانیان برلین", "ایرانیان مونیخ", "ایرانیان هامبورگ",
    "ایرانیان انگلیس", "ایرانیان لندن", "ایرانیان منچستر", "ایرانیان بریتانیا",
    "ایرانیان هلند", "ایرانیان آمستردام", "ایرانیان روتردام",
    "ایرانیان سوئد", "ایرانیان استکهلم", "ایرانیان مالمو",
    "ایرانیان فرانسه", "ایرانیان پاریس", "ایرانیان لیون",
    "ایرانیان اتریش", "ایرانیان وین", "ایرانیان سوئیس", "ایرانیان زوریخ",
    "ایرانیان استرالیا", "ایرانیان سیدنی", "ایرانیان ملبورن", "ایرانیان پرث",
    "ایرانیان آمریکا", "ایرانیان لس آنجلس", "ایرانیان نیویورک", "ایرانیان کالیفرنیا",
    "ایرانیان گرجستان", "ایرانیان تفلیس", "ایرانیان باتومی",
    "ایرانیان ارمنستان", "ایرانیان ایروان", "ایرانیان روسیه", "ایرانیان مسکو",
    "ایرانیان مالزی", "ایرانیان کوالالامپور", "ایرانیان تایلند", "ایرانیان بانکوک",
    "ایرانیان قبرس", "ایرانیان یونان", "ایرانیان اسپانیا", "ایرانیان ایتالیا",
    "ایرانیان بلژیک", "ایرانیان دانمارک", "ایرانیان نروژ", "ایرانیان فنلاند",
    "ایرانیان عمان", "ایرانیان قطر", "ایرانیان کویت", "ایرانیان عراق",
    "ایرانیان هند", "ایرانیان ژاپن", "ایرانیان چین", "ایرانیان کره",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 💼 شغل و کار
    # ══════════════════════════════════════════════════════════════════════════
    "کار", "استخدام", "نیازمندی", "کاریابی", "فرصت شغلی", "آگهی استخدام",
    "کار در منزل", "کار اینترنتی", "کار پاره وقت", "کار تمام وقت",
    "فریلنسر", "دورکاری", "همکاری", "مشارکت", "شغل", "حقوق",
    "درآمد", "درآمدزایی", "کسب درآمد", "پول درآوردن", "کار آنلاین",
    "استخدامی", "نیروی کار", "بازار کار", "کارجو", "کارفرما",
    "رزومه", "مصاحبه", "قرارداد", "حقوق و دستمزد", "بیمه کار",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🛒 خرید و فروش
    # ══════════════════════════════════════════════════════════════════════════
    "خرید", "فروش", "تبادل", "تجارت", "بازار", "فروشگاه",
    "دیوار", "شیپور", "آگهی", "نیازمندی ها", "دست دوم",
    "خرید و فروش", "بازرگانی", "تجاری", "واردات", "صادرات",
    "عمده فروشی", "خرده فروشی", "پخش", "توزیع", "تامین کننده",
    "کالا", "محصول", "قیمت", "ارزان", "تخفیف", "حراج",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🏠 ملک و مسکن
    # ══════════════════════════════════════════════════════════════════════════
    "ملک", "مسکن", "خانه", "آپارتمان", "اجاره", "رهن",
    "خرید خانه", "فروش ملک", "اجاره خانه", "مشاور املاک",
    "سرمایه گذاری ملک", "ساختمان", "مستغلات", "زمین",
    "ویلا", "باغ", "مغازه", "تجاری", "اداری", "انبار",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 💰 مالی و اقتصادی
    # ══════════════════════════════════════════════════════════════════════════
    "بورس", "سهام", "سرمایه گذاری", "پول", "بانک", "وام",
    "صرافی", "دلار", "ارز", "طلا", "سکه", "بیمه",
    "مالی", "اقتصاد", "کسب و کار", "استارتاپ", "کارآفرینی",
    "سرمایه", "ثروت", "پس انداز", "بازنشستگی", "سود",
    "اوراق", "صندوق", "ETF", "سبد سهام", "پرتفوی",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 💻 تکنولوژی و IT
    # ══════════════════════════════════════════════════════════════════════════
    "برنامه نویسی", "کامپیوتر", "موبایل", "اپلیکیشن", "وب",
    "هوش مصنوعی", "فناوری", "تکنولوژی", "دیجیتال", "اینترنت",
    "سئو", "دیجیتال مارکتینگ", "فروشگاه آنلاین", "اینستاگرام",
    "پایتون", "جاوا", "فرانت اند", "بک اند", "فول استک",
    "لینوکس", "ویندوز", "اندروید", "iOS", "اپل",
    "هاستینگ", "سرور", "دامنه", "وردپرس", "طراحی سایت",
    "گرافیک", "فتوشاپ", "ادوبی", "UI", "UX", "طراحی",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🎓 آموزش و تحصیل
    # ══════════════════════════════════════════════════════════════════════════
    "دانشگاه", "دانشجو", "تحصیل", "آموزش", "کنکور", "مدرسه",
    "زبان", "آیلتس", "تافل", "بورسیه", "لیسانس", "ارشد", "دکتری",
    "آموزشگاه", "کلاس", "دوره", "مدرک", "گواهینامه", "سرتیفیکیت",
    "تدریس", "معلم", "استاد", "دانش آموز", "درس", "امتحان",
    "المپیاد", "پژوهش", "تحقیق", "مقاله", "پایان نامه",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🚗 خودرو
    # ══════════════════════════════════════════════════════════════════════════
    "ماشین", "خودرو", "اتومبیل", "موتور", "سیکلت",
    "خرید ماشین", "فروش خودرو", "ماشین دست دوم", "لوازم یدکی",
    "تعمیرگاه", "مکانیکی", "صافکاری", "نقاشی", "اسپرت",
    "پراید", "پژو", "سمند", "تیبا", "کوییک", "دنا",
    
    # ══════════════════════════════════════════════════════════════════════════
    # ✈️ سفر و گردشگری
    # ══════════════════════════════════════════════════════════════════════════
    "سفر", "گردشگری", "توریست", "تور", "هتل", "پرواز",
    "بلیط", "ویزا", "پاسپورت", "سفارت", "کنسولگری",
    "اقامت", "مهاجرت", "پناهندگی", "گرین کارت", "اکسپرس انتری",
    "لاتاری", "DV", "تور گردشگری", "راهنمای سفر",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🩺 سلامت و پزشکی
    # ══════════════════════════════════════════════════════════════════════════
    "سلامت", "پزشکی", "دکتر", "بیمارستان", "دارو", "درمان",
    "پزشک", "کلینیک", "آزمایشگاه", "داروخانه", "تناسب اندام",
    "رژیم", "لاغری", "چاقی", "تغذیه", "ورزش", "فیتنس",
    "روانشناسی", "مشاوره", "روان درمانی", "اعصاب و روان",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🎨 سرگرمی و تفریح
    # ══════════════════════════════════════════════════════════════════════════
    "موسیقی", "فیلم", "سینما", "ورزش", "فوتبال", "بازی",
    "گیم", "سرگرمی", "تفریح", "کتاب", "هنر", "عکاسی",
    "استقلال", "پرسپولیس", "والیبال", "بسکتبال", "تنیس",
    "پلی استیشن", "ایکس باکس", "PC", "موبایل گیم",
    "رمان", "شعر", "ادبیات", "داستان", "نویسندگی",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 💑 اجتماعی
    # ══════════════════════════════════════════════════════════════════════════
    "دوستیابی", "آشنایی", "ازدواج", "همسریابی", "دوست",
    "چت", "گپ", "صحبت", "گفتگو", "انجمن", "گروه",
    "جامعه", "اجتماعی", "خیریه", "کمک", "داوطلب",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🍽️ غذا و رستوران
    # ══════════════════════════════════════════════════════════════════════════
    "غذا", "رستوران", "کافه", "آشپزی", "دسر", "شیرینی",
    "فست فود", "پیتزا", "برگر", "کباب", "نان", "آشپز",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🔧 خدمات
    # ══════════════════════════════════════════════════════════════════════════
    "تعمیرات", "نصب", "خدمات", "تاسیسات", "برق", "لوله کشی",
    "نظافت", "حمل و نقل", "باربری", "پیک", "اسباب کشی",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 📱 شبکه‌های اجتماعی
    # ══════════════════════════════════════════════════════════════════════════
    "تلگرام", "اینستاگرام", "واتساپ", "یوتیوب", "توییتر",
    "تیک تاک", "لینکدین", "فیسبوک", "پینترست", "اسنپ چت",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 💹 ترید و کریپتو (فارسی)
    # ══════════════════════════════════════════════════════════════════════════
    "ترید", "تریدر", "سیگنال", "کریپتو", "بیتکوین", "اتریوم",
    "رمزارز", "ارز دیجیتال", "فارکس", "فیوچرز", "اسپات",
    "بایننس", "صرافی ارز", "نوبیتکس", "والکس", "بیت پین",
    "آموزش ترید", "سیگنال رایگان", "تحلیل تکنیکال",
    "پراپ", "فاندینگ", "چالش ترید", "FTMO", "پراپ فرم",
    "ایردراپ", "ماینینگ", "استیکینگ", "NFT", "دیفای",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🔤 کلمات انگلیسی ساده و مؤثر
    # ══════════════════════════════════════════════════════════════════════════
    "iran", "iranian", "persian", "farsi", "tehran", "persia",
    "crypto", "bitcoin", "btc", "ethereum", "eth", "binance",
    "trade", "trader", "trading", "signal", "forex", "fx",
    "immigration", "visa", "turkey", "istanbul", "dubai", "uae",
    "canada", "toronto", "vancouver", "germany", "berlin",
    "london", "uk", "usa", "australia", "sydney",
    "job", "work", "business", "money", "investment", "income",
    "group", "chat", "community", "club", "forum", "channel",
    "airdrop", "nft", "defi", "web3", "blockchain", "altcoin",
    "prop", "funded", "ftmo", "challenge", "evaluation",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🆕 کلمات ترکیبی مؤثر برای یافتن گروه‌های بیشتر
    # ══════════════════════════════════════════════════════════════════════════
    "گروه ترید", "گروه سیگنال", "گروه کریپتو", "گروه فارکس",
    "گروه ایرانیان", "گروه فارسی", "کانال سیگنال", "سیگنال VIP",
    "آموزش رایگان", "دوره رایگان", "کسب درآمد آنلاین",
    "خرید و فروش ارز", "تبادل ارز", "نرخ ارز", "قیمت دلار",
    "خرید بیتکوین", "فروش تتر", "USDT", "صرافی ارز دیجیتال",
    "ایرانی خارج", "خارج از کشور", "مقیم خارج",
    "turkish lira", "dubai property", "canada immigration",
    
    # ══════════════════════════════════════════════════════════════════════════
    # �🆕 رمزارزهای خاص و ترکیبات جستجوی هوشمند رمزارز
    # ══════════════════════════════════════════════════════════════════════════
    # تون و اکوسیستم تلگرام
    "تون", "تون کوین", "TON", "تون نتورک", "نات کوین", "NOT", "notcoin",
    "همستر کامبت", "hamster kombat", "تپ سواپ", "tapswap", "داگز", "dogs",
    "بلوم", "blum", "ممفای", "memefi", "ماژور", "major", "کتیزن", "catizen",
    "ایردراپ تون", "ایردراپ تلگرام", "بازی تلگرام", "telegram game",
    "سیگنال تون", "TON signal", "گروه تون", "TON group", "ton airdrop",
    
    # ترون
    "ترون", "TRON", "TRX", "ترونیکس", "شبکه ترون", "TRC20",
    "سیگنال ترون", "گروه ترون", "tron signal", "tron group",
    
    # سولانا و اکوسیستم
    "سولانا", "SOL", "solana", "ریدیوم", "raydium", "جوپیتر", "jupiter",
    "سیگنال سولانا", "SOL signal", "solana airdrop", "ایردراپ سولانا",
    
    # BNB و بایننس اسمارت چین
    "بی ان بی", "BNB", "بایننس کوین", "BSC", "بایننس اسمارت چین",
    "پنکیک سواپ", "pancakeswap", "CAKE", "سیگنال BNB",
    
    # آربیتروم و لایه دو
    "آربیتروم", "ARB", "arbitrum", "اوپتیمیزم", "OP", "optimism",
    "بیس", "BASE", "base chain", "لایر دو", "layer 2", "L2",
    "zkSync", "StarkNet", "اسکرول", "scroll", "منتل", "mantle",
    
    # دوج و میم کوین‌ها
    "دوج کوین", "DOGE", "doge", "شیبا", "SHIB", "shiba",
    "پپه", "PEPE", "فلوکی", "FLOKI", "بانک", "BONK",
    "ویف", "WIF", "میم کوین", "meme coin", "memecoin",
    
    # هوش مصنوعی و کریپتو
    "فچ", "FET", "fetch ai", "رندر", "RNDR", "render",
    "ورلد کوین", "WLD", "worldcoin", "بیت تنسور", "TAO",
    "اوشن", "OCEAN", "آکاش", "AKT", "akash", "near ai",
    
    # DeFi اصلی
    "یونی سواپ", "UNI", "uniswap", "آوه", "AAVE", "aave",
    "لیدو", "LDO", "lido", "میکر", "MKR", "maker",
    "پندل", "PENDLE", "pendle", "ایگن لایر", "EIGEN", "eigenlayer",
    "کرو", "CRV", "curve", "وان اینچ", "1INCH", "1inch",
    
    # صرافی‌های معروف
    "بایننس", "binance", "بای بیت", "bybit", "اوکی ایکس", "OKX",
    "کوکوین", "kucoin", "گیت", "gate.io", "بیت گت", "bitget",
    "نوبیتکس", "nobitex", "والکس", "wallex", "رمزینکس", "ramzinex",
    "تبدیل", "tabdeal", "اکسیر", "exir", "آبان تتر", "aban tether",
    
    # ترکیبات جستجوی هوشمند رمزارز
    "سیگنال رایگان", "سیگنال VIP", "سیگنال فیوچرز", "سیگنال اسپات",
    "تحلیل بیتکوین", "تحلیل اتریوم", "تحلیل سولانا", "تحلیل تون",
    "آموزش ترید فارسی", "آموزش فیوچرز", "آموزش پرایس اکشن",
    "ICT فارسی", "SMC فارسی", "smart money", "price action",
    "bitcoin signal", "ethereum signal", "crypto signal persian",
    "free signal", "vip signal", "futures signal", "spot signal",
    
    # ══════════════════════════════════════════════════════════════════════════
    # �🏥 پزشکی، دارو و تجهیزات (برای یافتن گروه‌های پزشکی)
    # ══════════════════════════════════════════════════════════════════════════
    "دارو", "داروخانه", "پزشکی", "دکتر", "بیمارستان", "درمان",
    "سلامت", "کلینیک", "آزمایشگاه", "تجهیزات پزشکی", "لوازم پزشکی",
    "داروسازی", "نسخه", "مکمل", "ویتامین", "دندانپزشکی", "ایمپلنت",
    "فیزیوتراپی", "روانشناسی", "تغذیه", "رژیم", "زیبایی",
    "داروی نایاب", "داروی کمیاب", "تبادل دارو", "خرید دارو", "فروش دارو",
    "گروه پزشکی", "گروه داروسازی", "انجمن پزشکان", "کانال پزشکی",
    "گروه دندانپزشکی", "گروه آزمایشگاه", "تجهیزات بیمارستانی",
    "واکسن", "آنتی بیوتیک", "سرم", "قرص", "کپسول", "آمپول",
    "pharmacy", "medical", "drug", "medicine", "health", "clinic",
    "hospital", "dental", "lab", "equipment", "supplement",
    "doctor", "nurse", "pharma", "prescription", "vaccine",
    "گروه دارویی", "کانال دارو", "انجمن داروسازان",
    "بیماران دیابت", "بیماران سرطان", "بیماران MS",
    "گروه سلامت", "سلامتی", "بهداشت", "بهزیستی",
    "لوازم آرایشی بهداشتی", "محصولات بهداشتی", "سان لایت",
    "پزشکی تهران", "دارو تهران", "کلینیک تهران", "بیمارستان تهران",
    "پزشکی مشهد", "پزشکی اصفهان", "پزشکی شیراز",
    "دارو مشهد", "داروخانه اصفهان", "کلینیک شیراز",
    "تجهیزات پزشکی تهران", "لوازم پزشکی مشهد",
    "medical iran", "pharmacy iran", "dental iran", "clinic iran",
    "iranian medical", "persian pharmacy", "iran health",
    "drug exchange", "medical equipment iran", "dental implant iran",
    
    # ══════════════════════════════════════════════════════════════════════════
    # 🆕🆕 ترکیبات هوشمند بین‌دسته‌ای (Cross-Category Smart Combos)
    # ══════════════════════════════════════════════════════════════════════════
    # کریپتو + شهرهای خارجی (یافتن گروه‌های محلی کریپتو)
    "ترید استانبول", "کریپتو دبی", "ترید تورنتو", "crypto dubai",
    "bitcoin istanbul", "crypto toronto", "trade london", "forex dubai",
    "ترید تهران", "کریپتو مشهد", "ترید اصفهان", "بورس تهران",
    
    # مهاجرت + خدمات (یافتن گروه‌های خدماتی دیاسپورا)
    "صرافی استانبول", "صرافی دبی", "حواله ایران ترکیه", "حواله دبی",
    "وکیل ایرانی استانبول", "دکتر ایرانی دبی", "باربری استانبول",
    "آرایشگاه ایرانی", "رستوران ایرانی استانبول", "سوپر ایرانی",
    
    # عمومی + شهرها (جستجوی گسترده‌تر)
    "گروه تهران", "گروه مشهد", "گروه اصفهان", "گروه شیراز",
    "گروه تبریز", "گروه کرج", "گروه اهواز", "گروه قم",
    "group tehran", "group mashhad", "group isfahan",
    
    # شغل + موقعیت (یافتن گروه‌های کاری)
    "کار تهران", "استخدام مشهد", "کار دبی", "کار استانبول",
    "فریلنسر ایرانی", "دورکاری ایران", "job iran", "work dubai",
    
    # مالی + ایرانی
    "بورس تهران", "سهام ایران", "صندوق سرمایه گذاری", "بازار سرمایه",
    "طلای آبشده", "سکه بهار", "قیمت طلا", "قیمت دلار",
    "stock iran", "tehran stock", "gold iran", "dollar iran",
]

# 🔄 شمارنده سیکل جستجو برای مدیریت اولویت‌ها
search_cycle_counter = {
    'total': 0,
    'crypto_done': 0,
    'medical_done': 0,
    'immigration_done': 0,
    'general_done': 0,
    'last_category': None
}

# ═══════════════════════════════════════════════════════════════════════════════
# 🥇🥇🥇 اولویت 1: کلمات کلیدی جامع ترید و رمزارز 🥇🥇🥇
# ═══════════════════════════════════════════════════════════════════════════════

CRYPTO_TRADING_KEYWORDS = {
    # 📊 ترید و معاملات (فارسی) - گسترش یافته +150 کلمه
    'trading_fa': [
        "ترید", "تریدر", "تریدینگ", "معامله", "معاملات", "معامله‌گر", "معامله گر",
        "تحلیل تکنیکال", "تحلیل فاندامنتال", "تحلیلگر", "نمودار", "چارت",
        "کندل", "کندل استیک", "اسکالپ", "اسکالپینگ", "سوینگ ترید", "دی ترید",
        "پوزیشن ترید", "ستاپ معاملاتی", "سیگنال", "سیگنال ترید", "سیگنال رایگان",
        "سیگنال VIP", "سیگنال ویژه", "کانال سیگنال", "گروه سیگنال",
        "استراتژی", "استراتژی معاملاتی", "مدیریت سرمایه", "مدیریت ریسک",
        "حد ضرر", "استاپ لاس", "حد سود", "تیک پرافیت", "ریسک به ریوارد",
        "لوریج", "اهرم", "مارجین", "فیوچرز", "اسپات", "پرپچوال",
        "لانگ", "شورت", "لیکویید", "لیکوییدیشن", "PNL", "سود و ضرر",
        "پولبک", "بریک اوت", "ریتست", "حمایت", "مقاومت", "ساپورت", "رزیستنس",
        "روند", "ترند", "ترندلاین", "فیبوناچی", "RSI", "MACD", "EMA", "SMA",
        "اندیکاتور", "اوسیلاتور", "دایورجنس", "واگرایی", "همگرایی",
        "ICT", "SMC", "اسمارت مانی", "پرایس اکشن", "لیکوییدیتی",
        "آموزش ترید", "دوره ترید", "کلاس ترید", "مربی ترید", "کوچینگ ترید",
        # 🆕 گسترش جدید - تکنیک‌های پیشرفته
        "الیوت", "موج الیوت", "هارمونیک", "الگوی هارمونیک", "گارتلی", "باترفلای",
        "خفاش", "کرب", "سایفر", "شارک", "ABCD", "تری درایو", "وولف ویو",
        "اوردر بلاک", "فیر ولیو گپ", "FVG", "BOS", "CHoCH", "اوردر فلو",
        "وایکوف", "سم سایدن", "ال بروکس", "نایل فولر", "لنس بگز",
        "ایچیموکو", "بولینجر", "ATR", "ADX", "استوکاستیک", "CCI", "MFI",
        "حجم", "ولوم", "OBV", "VWAP", "پروفایل ولوم", "مارکت پروفایل",
        "تایم فریم", "مولتی تایم", "HTF", "LTF", "چنج آف کاراکتر",
        "برک آف استراکچر", "اینداسمنت", "میتیگیشن", "ری‌بالانس",
        # 🆕 سبک‌های معاملاتی
        "نوسان گیری", "نوسانگیر", "سیستم معاملاتی", "ستاپ روزانه",
        "معامله‌گر حرفه‌ای", "تریدر موفق", "درآمد از ترید", "زندگی از ترید",
        "فول تایم ترید", "پارت تایم ترید", "ترید شبانه", "ترید آسیا",
        "سشن لندن", "سشن نیویورک", "سشن توکیو", "کیل زون",
        # 🆕 روانشناسی ترید
        "روانشناسی ترید", "مدیریت احساسات", "ترس و طمع", "FOMO", "فومو",
        "ذهنیت تریدر", "انضباط معاملاتی", "ژورنال نویسی", "ژورنال ترید",
        "بک تست", "فوروارد تست", "دمو ترید", "لایو ترید", "ریل ترید",
    ],
    
    # 📊 ترید و معاملات (انگلیسی) - گسترش یافته +100 کلمه
    'trading_en': [
        "trade", "trader", "trading", "scalping", "scalp", "swing trade", "day trade",
        "position trade", "signal", "signals", "crypto signal", "trading signal",
        "VIP signal", "free signal", "technical analysis", "fundamental analysis",
        "chart", "candlestick", "setup", "strategy", "risk management",
        "stop loss", "take profit", "leverage", "margin", "futures", "spot",
        "perpetual", "long", "short", "liquidation", "PNL", "profit loss",
        "support", "resistance", "trend", "trendline", "fibonacci", "indicator",
        "RSI", "MACD", "EMA", "SMA", "divergence", "ICT", "SMC", "smart money",
        "price action", "liquidity", "trading course", "trading education",
        # 🆕 گسترش جدید
        "order block", "fair value gap", "break of structure", "change of character",
        "mitigation", "inducement", "imbalance", "wyckoff", "volume profile",
        "market structure", "elliott wave", "harmonic pattern", "supply demand",
        "orderflow", "footprint", "delta", "cumulative delta", "market profile",
        "session", "killzone", "london session", "new york session", "asian session",
        "backtest", "forward test", "demo trade", "live trade", "paper trading",
        "trading journal", "trading psychology", "fear greed", "discipline",
        "risk reward", "win rate", "expectancy", "drawdown", "equity curve",
        "prop trader", "funded account", "evaluation", "challenge pass",
        "trading bot", "algo trading", "automated trading", "trading robot",
        "copy trading", "social trading", "signal provider", "signal service",
    ],
    
    # 💰 رمزارز و کریپتو (فارسی) - گسترش یافته +200 کلمه
    'crypto_fa': [
        "کریپتو", "کریپتوکارنسی", "رمزارز", "رمز ارز", "ارز دیجیتال",
        "بیتکوین", "بیت کوین", "اتریوم", "اتر", "بایننس کوین", "بی ان بی",
        "تتر", "یو اس دی تی", "سولانا", "کاردانو", "ریپل", "دوج کوین",
        "شیبا", "پولکادات", "آوالانچ", "پالیگان", "ماتیک", "آپتوس", "آربیتروم",
        "لایت کوین", "چین لینک", "یونی سواپ", "آوه", "سند باکس", "دی سنترالند",
        "متاورس", "وب 3", "وب سه", "دیفای", "NFT", "توکن", "کوین", "آلت کوین",
        "صرافی", "صرافی ارز دیجیتال", "صرافی غیرمتمرکز", "DEX", "CEX",
        "بایننس", "کوکوین", "کوینکس", "بیت پین", "نوبیتکس", "والکس",
        "کیف پول", "والت", "متامسک", "تراست والت", "لجر", "کلد ولت",
        "هات ولت", "استیکینگ", "استیک", "فارمینگ", "ایردراپ", "ایر دراپ",
        "IDO", "ICO", "IEO", "لانچ پد", "پریسیل", "پیش فروش",
        "هودل", "HODL", "هولد", "DCA", "میانگین گیری", "بای دیپ",
        "پامپ", "دامپ", "بول ران", "بیر مارکت", "هاوینگ", "آنچین",
        "بلاکچین", "بلاک چین", "ماینینگ", "ماینر", "استخراج", "هش ریت",
        "گس فی", "کارمزد", "ترنزکشن", "تراکنش", "اسمارت کانترکت",
        # 🆕 کوین‌های جدید و ترندینگ
        "سویی", "SUI", "تون کوین", "TON", "پپه", "PEPE", "فلوکی", "FLOKI",
        "بونک", "BONK", "ورلد کوین", "WLD", "رندر", "RENDER", "فچ", "FET",
        "جوپیتر", "JUP", "پیت", "PYTH", "انجین", "ENJ", "گالا", "GALA",
        "آکسی", "AXS", "استپن", "GMT", "ایمیوتابل", "IMX", "فلو", "FLOW",
        "نیر", "NEAR", "اینجکتیو", "INJ", "سی", "SEI", "تیا", "TIA",
        "سلستیا", "CELESTIA", "مانتا", "MANTA", "جیتو", "JITO", "دایم", "DYMENSION",
        # 🆕 صرافی‌های بیشتر
        "بای بیت", "اوکی ایکس", "OKX", "هیوبی", "گیت آیو", "GATE",
        "مکس", "MEXC", "ایران بیتکس", "تبدیل", "رمزینکس", "اکسیر",
        "بیت 24", "آبان تتر", "تترلند", "پی ام پی", "پی تو پی", "P2P",
        # 🆕 مفاهیم DeFi
        "لیکوییدیتی پول", "LP", "ایلد فارمینگ", "لندینگ", "باروینگ",
        "کولترال", "لیکوییدیشن", "فلش لون", "آربیتراژ", "ساندویچ",
        "اسلیپیج", "ایمپرمننت لاس", "APY", "APR", "TVL", "توتال ولیو",
        "بریج", "کراس چین", "لایر 2", "L2", "رول آپ", "اپتیمیستیک",
        "ZK", "زیرو نالج", "پروف آف استیک", "POS", "پروف آف ورک", "POW",
        # 🆕 NFT و متاورس
        "ان اف تی", "کالکشن", "مینت", "ویت لیست", "اوپن سی", "OpenSea",
        "بلور", "BLUR", "مجیک ایدن", "تنسور", "آردوینو", "ORDINALS",
        "بی آر سی 20", "BRC20", "رون", "RUNE", "اینسکریپشن",
        # 🆕 ممکوین و ترندها
        "میم کوین", "شت کوین", "جم", "آلفا", "بتا", "پامپامنتال",
        "x100", "صد ایکس", "x1000", "ایکس هزار", "مون", "لامبو",
        # 🆕🆕 رمزارزهای بیشتر - تان/ترون/بیت‌کوین کش و ...
        "تون", "تون کوین", "تلگرام کوین", "TON", "تن نتورک", "جت تون", "JETTON",
        "ترون", "TRX", "ترونیکس", "TRON", "جاست", "JST", "سان", "SUN", "بی تی تی", "BTT",
        "بیت کوین کش", "BCH", "بیت کوین اس وی", "BSV", "بیت کوین گلد", "BTG",
        "مونرو", "XMR", "زی کش", "ZEC", "دش", "DASH", "لایت‌کوین", "LTC",
        "استلار", "XLM", "لومن", "STELLAR", "کازماس", "ATOM", "COSMOS",
        "الگوراند", "ALGO", "هدرا", "HBAR", "هدرا هش‌گراف", "ویچین", "VET",
        "آیوتا", "IOTA", "میوتا", "دسنترالایزد", "EOS", "ایاس", "نئو", "NEO",
        "وان", "ONE", "هارمونی", "HARMONY", "فانتوم", "FTM", "FANTOM",
        "زیلیکا", "ZIL", "ZILLIQA", "تزوس", "XTZ", "TEZOS",
        "کادنا", "KDA", "KADENA", "مالتیورس ایکس", "EGLD", "MultiversX",
        "هلیوم", "HNT", "HELIUM", "تتا", "THETA", "تتا نتورک",
        "کانفلاکس", "CFX", "CONFLUX", "استکز", "STX", "STACKS",
        "آپت", "APT", "APTOS", "آربیتروم", "ARB", "ARBITRUM",
        "اوپتیمیزم", "OP", "OPTIMISM", "بیس", "BASE", "بیس چین",
        "منتل", "MANTLE", "MNT", "اسکرول", "SCROLL", "لینیا", "LINEA",
        "زتا چین", "ZETA", "لایر زیرو", "LayerZero", "ZRO",
        "گراف", "GRT", "THE GRAPH", "فایل کوین", "FIL", "FILECOIN",
        "آر ویو", "AR", "ARWEAVE", "آکالا", "ACA", "ACALA",
        "ایپ کوین", "APE", "APECOIN", "لوپرینگ", "LRC", "LOOPRING",
        "دی وای دی ایکس", "DYDX", "پنکیک سواپ", "CAKE", "سوشی سواپ", "SUSHI",
        "وان اینچ", "1INCH", "کرو", "CRV", "CURVE", "بالانسر", "BAL",
        "یرن فایننس", "YFI", "YEARN", "کامپوند", "COMP", "COMPOUND",
        "میکر", "MKR", "MAKER", "DAI", "دای", "سینتتیکس", "SNX",
        "پنل", "PENDLE", "ایگن لایر", "EIGEN", "اتر فای", "ETHERFI",
        "رستیکینگ", "لیکویید استیکینگ", "لیدو", "LIDO", "LDO",
        "راکت پول", "RPL", "ROCKETPOOL", "فراکس", "FXS", "FRAX",
        # 🆕🆕 صرافی‌های غیرمتمرکز بیشتر  
        "ریدیوم", "RAY", "RAYDIUM", "اورکا", "ORCA",
        "تریدرجو", "JOE", "TRADERJOE", "کملات", "CAMELOT",
        "وی لاس", "VELAS", "سوئی", "SUI", "آپتوس", "APT",
        # 🆕🆕 بازی و GameFi
        "پیکس", "PIXELS", "بیگ تایم", "BIGTIME", "نوت کوین", "NOT", "NOTCOIN",
        "همستر کامبت", "HAMSTER", "تپ سواپ", "TAPSWAP", "کتیزن", "CATIZEN",
        "داگز", "DOGS", "Blum", "بلوم", "ممفای", "MEMEFI", "یسکوین", "YESCOIN",
        "ماژور", "MAJOR", "توماتو",
        # 🆕🆕 رمزارزهای حریم خصوصی
        "مونرو", "XMR", "زی‌کش", "ZEC", "دش", "DASH", "سکرت", "SCRT",
        "اوآسیس", "ROSE", "OASIS", "نایت‌فال", "NIGHTFALL",
        # 🆕🆕 هوش مصنوعی و کریپتو
        "فچ ای آی", "FET", "FETCH", "اوشن", "OCEAN", "سینگولاریتی نت", "AGIX",
        "رندر", "RNDR", "RENDER", "اکویتس", "AKT", "AKASH", "بیت‌تنسور", "TAO",
        "نیر ای آی", "NEAR AI", "ورلد کوین", "WLD", "WORLDCOIN",
    ],
    
    # 💰 رمزارز و کریپتو (انگلیسی) - گسترش یافته +150 کلمه
    'crypto_en': [
        "crypto", "cryptocurrency", "bitcoin", "btc", "ethereum", "eth",
        "binance", "bnb", "tether", "usdt", "usdc", "solana", "sol",
        "cardano", "ada", "ripple", "xrp", "dogecoin", "doge", "shiba",
        "polkadot", "dot", "avalanche", "avax", "polygon", "matic",
        "aptos", "apt", "arbitrum", "arb", "optimism", "op", "litecoin", "ltc",
        "chainlink", "link", "uniswap", "uni", "aave", "sandbox", "mana",
        "metaverse", "web3", "defi", "nft", "token", "coin", "altcoin",
        "exchange", "dex", "cex", "binance", "kucoin", "coinex", "bybit",
        "wallet", "metamask", "trust wallet", "ledger", "cold wallet",
        "staking", "stake", "farming", "airdrop", "ido", "ico", "ieo",
        "launchpad", "presale", "hodl", "hold", "dca", "buy dip",
        "pump", "dump", "bull run", "bear market", "halving", "on chain",
        "blockchain", "mining", "miner", "hash rate", "gas fee", "transaction",
        "smart contract",
        # 🆕 Trending coins
        "sui", "ton", "toncoin", "pepe", "floki", "bonk", "wld", "worldcoin",
        "render", "fet", "jupiter", "pyth", "enj", "gala", "axs", "axie",
        "gmt", "stepn", "imx", "flow", "near", "inj", "injective", "sei",
        "tia", "celestia", "manta", "jito", "dym", "dymension",
        # 🆕 Exchanges
        "okx", "huobi", "htx", "gate", "gateio", "mexc", "bitget", "kraken",
        "coinbase", "gemini", "bitstamp", "ftx", "crypto.com", "p2p",
        # 🆕 DeFi concepts
        "liquidity pool", "lp token", "yield farming", "lending", "borrowing",
        "collateral", "flash loan", "arbitrage", "slippage", "impermanent loss",
        "apy", "apr", "tvl", "bridge", "cross chain", "layer 2", "l2",
        "rollup", "optimistic", "zk rollup", "zero knowledge", "pos", "pow",
        # 🆕 NFT
        "opensea", "blur", "magic eden", "tensor", "ordinals", "brc20", "rune",
        "inscription", "collection", "mint", "whitelist", "wl", "pfp",
        # 🆕 Meme & trends
        "memecoin", "meme coin", "gem", "alpha", "x100", "x1000", "moon",
        "ape", "degen", "fud", "ngmi", "wagmi", "gm", "lfg",
        # 🆕🆕 More altcoins
        "ton", "toncoin", "telegram coin", "tron", "trx", "tronics", "btt",
        "bch", "bitcoin cash", "bsv", "btg", "monero", "xmr", "zcash", "zec",
        "dash", "stellar", "xlm", "cosmos", "atom", "algorand", "algo",
        "hedera", "hbar", "vechain", "vet", "iota", "eos", "neo",
        "harmony", "one", "fantom", "ftm", "zilliqa", "zil", "tezos", "xtz",
        "kadena", "kda", "multiversx", "egld", "helium", "hnt", "theta",
        "conflux", "cfx", "stacks", "stx", "base", "mantle", "mnt",
        "scroll", "linea", "zeta", "layerzero", "zro",
        "graph", "grt", "filecoin", "fil", "arweave", "ar",
        "ape", "apecoin", "loopring", "lrc", "dydx",
        "pancakeswap", "cake", "sushiswap", "sushi", "1inch",
        "curve", "crv", "balancer", "bal", "yearn", "yfi",
        "compound", "comp", "maker", "mkr", "dai", "synthetix", "snx",
        "pendle", "eigenlayer", "eigen", "etherfi", "restaking",
        "lido", "ldo", "rocketpool", "rpl", "frax", "fxs",
        "raydium", "ray", "orca", "traderjoe", "joe", "camelot",
        # 🆕🆕 GameFi & Telegram games
        "pixels", "bigtime", "notcoin", "not", "hamster kombat", "hamster",
        "tapswap", "catizen", "dogs", "blum", "memefi", "yescoin", "major",
        # 🆕🆕 AI crypto
        "fetch", "ocean", "singularity", "agix", "render", "rndr",
        "akash", "akt", "bittensor", "tao", "worldcoin", "wld",
        # 🆕🆕 Privacy
        "monero", "secret", "scrt", "oasis", "rose",
    ],
    
    # 💼 پراپ تریدینگ (فارسی)
    'prop_fa': [
        "پراپ", "پراپ فرم", "پراپ تریدینگ", "فاندینگ", "فاندد", "فاند شده",
        "چالش پراپ", "ارزیابی پراپ", "فیس پراپ", "اف تی ام او", "FTMO",
        "فاست فاند", "مای فورکس فاندز", "اینستنت فاندینگ", "فست پراپ",
        "اکانت پراپ", "حساب پراپ", "درصد سود", "پروفیت اسپلیت",
        "حداکثر دراداون", "دراداون روزانه", "دراداون کلی", "حد ضرر روزانه",
        "قوانین پراپ", "شرایط پراپ", "بهترین پراپ", "مقایسه پراپ",
        "کپی ترید", "کپی تریدینگ", "سوشال ترید", "سیگنال پراپ",
    ],
    
    # 💼 پراپ تریدینگ (انگلیسی)
    'prop_en': [
        "prop", "prop firm", "prop trading", "funded", "funded trader",
        "prop challenge", "evaluation", "ftmo", "my forex funds", "fast fund",
        "instant funding", "prop account", "profit split", "max drawdown",
        "daily drawdown", "prop rules", "best prop firm", "copy trade",
        "copy trading", "social trading",
    ],
    
    # 📈 فارکس (فارسی)
    'forex_fa': [
        "فارکس", "بازار فارکس", "جفت ارز", "یورو دلار", "پوند دلار",
        "دلار ین", "طلا", "اونس طلا", "نقره", "نفت", "برنت", "بازار جهانی",
        "بروکر", "بروکر فارکس", "متاتریدر", "MT4", "MT5", "تریدینگ ویو",
        "لات", "پیپ", "اسپرد", "سواپ", "رولاور", "اوردر", "پندینگ اوردر",
        "مارکت اوردر", "لیمیت", "استاپ", "تریلینگ استاپ",
    ],
    
    # 📈 فارکس (انگلیسی)
    'forex_en': [
        "forex", "fx", "currency pair", "eurusd", "gbpusd", "usdjpy",
        "gold", "xauusd", "silver", "oil", "brent", "broker", "metatrader",
        "mt4", "mt5", "tradingview", "lot", "pip", "spread", "swap",
        "pending order", "market order", "limit order", "stop order",
    ],
    
    # 💎 سرمایه‌گذاری و ثروت (فارسی)
    'investment_fa': [
        "سرمایه گذاری", "سرمایه‌گذاری", "درآمد", "درآمدزایی", "کسب درآمد",
        "درآمد آنلاین", "درآمد اینترنتی", "درآمد دلاری", "ثروت", "ثروتمند شدن",
        "استقلال مالی", "آزادی مالی", "پول", "سود", "بازگشت سرمایه",
        "بورس", "بورس تهران", "بورس ایران", "سهام", "شاخص", "سبدگردانی",
        "صندوق سرمایه گذاری", "طلا", "سکه", "دلار", "ارز", "نقدینگی",
    ],
    
    # 💎 سرمایه‌گذاری و ثروت (انگلیسی)
    'investment_en': [
        "investment", "investing", "income", "online income", "passive income",
        "wealth", "financial freedom", "money", "profit", "roi", "return",
        "stock", "stock market", "portfolio", "fund", "asset", "capital",
    ],
    
    # 🎮 ترکیبات و گروه‌های ترید (فارسی)
    'trading_groups_fa': [
        "گروه ترید", "گروه تریدر", "گروه کریپتو", "گروه بیتکوین",
        "گروه سیگنال", "گروه فارکس", "گروه پراپ", "انجمن ترید",
        "انجمن تریدرها", "کانال ترید", "کانال سیگنال", "کانال کریپتو",
        "جامعه تریدرها", "تریدرهای ایرانی", "کریپتوکارنسی ایران",
        "ارز دیجیتال ایران", "بیتکوین فارسی", "آموزش رایگان ترید",
        "تریدرهای موفق", "مستر ترید", "پادشاه ترید", "ترید حرفه‌ای",
    ],
    
    # 🎮 ترکیبات و گروه‌های ترید (انگلیسی)
    'trading_groups_en': [
        "trading group", "trader group", "crypto group", "bitcoin group",
        "signal group", "forex group", "prop group", "trading community",
        "trader community", "crypto community", "iran trader", "persian trader",
        "iranian crypto", "farsi crypto", "free signal group",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# 🥈🥈🥈 اولویت 2: کلمات کلیدی جامع مهاجرت و اقامت 🥈🥈🥈
# ═══════════════════════════════════════════════════════════════════════════════

IMMIGRATION_KEYWORDS = {
    # 🛫 مهاجرت عمومی (فارسی) - گسترش یافته +50 کلمه
    'immigration_fa': [
        "مهاجرت", "مهاجر", "مهاجرین", "مهاجران", "اقامت", "ویزا", "گذرنامه",
        "پاسپورت", "پناهندگی", "پناهنده", "پناهجو", "لجوء", "سفارت", "کنسولگری",
        "خروج از کشور", "زندگی خارج", "ایرانیان خارج", "ایرانیان مقیم",
        "فارسی زبان", "پارسی زبان", "همگام", "هموطن", "ایران",
        # 🆕 گسترش جدید
        "مهاجرت کاری", "مهاجرت تحصیلی", "مهاجرت سرمایه گذاری", "مهاجرت خانوادگی",
        "اقامت دائم", "اقامت موقت", "ریجکت ویزا", "اپلای ویزا", "وقت سفارت",
        "ترجمه مدارک", "مدارک مهاجرت", "پرونده مهاجرتی", "وکیل مهاجرتی",
        "مشاور مهاجرت", "موسسه مهاجرت", "اپوینتمنت", "بیومتریک", "فینگرپرینت",
        "کانادایی شدن", "شهروند", "تابعیت", "ملیت", "دو تابعیتی",
        "خارج نشین", "غربت", "دوری وطن", "دلتنگ ایران", "ایرانی غریب",
    ],
    
    # 🛫 مهاجرت عمومی (انگلیسی) - گسترش یافته
    'immigration_en': [
        "immigration", "immigrant", "visa", "passport", "refugee", "asylum",
        "expat", "expatriate", "abroad", "overseas", "iranian", "persian",
        "farsi", "iran", "living abroad",
        # 🆕 گسترش جدید
        "pr visa", "permanent residence", "work permit", "study permit",
        "visa application", "embassy appointment", "biometric", "fingerprint",
        "document translation", "immigration lawyer", "immigration consultant",
        "green card", "citizenship", "nationality", "dual citizenship",
        "expat life", "iranian expat", "persian community", "farsi speaker",
    ],
    
    # 🇹🇷 ترکیه (فارسی) - گسترش یافته +100 کلمه
    'turkey_fa': [
        "ترکیه", "ترک", "استانبول", "آنکارا", "ازمیر", "آنتالیا", "بورسا",
        "آدانا", "مرسین", "کوجاالی", "ساکاریا", "ترابزون", "قونیه", "گازی آنتپ",
        "تکسیم", "فاتح", "لاله لی", "اسنیورت", "باشاک شهیر", "بیلیک دوزو",
        "کادیکوی", "بشیکتاش", "شیشلی", "اتیلر", "بهچه شهیر", "آکسارای",
        "اقامت ترکیه", "کار در ترکیه", "زندگی استانبول", "خانه استانبول",
        "اجاره ترکیه", "ملک ترکیه", "کیملیک", "اقامت توریستی", "اقامت کار",
        "ایرانیان ترکیه", "فارسی ترکیه", "گروه استانبول", "گروه ازمیر",
        "تحصیل ترکیه", "دانشگاه ترکیه", "بورسیه ترکیه",
        # 🆕 محله‌های بیشتر استانبول
        "آوجیلار", "کوچوکچکمجه", "بویوکچکمجه", "سلطان غازی", "گازیوس من پاشا",
        "اسکودار", "مالتپه", "پندیک", "کارتال", "آتاشهیر", "عمرانیه",
        "ساریر", "بیکوز", "چکمه کوی", "سیلیوری", "آرناوت کوی", "چاتالجا",
        "زیتین بورنو", "باکرکوی", "گونگورن", "باغجلار", "کوجوک چکمجه",
        # 🆕 شهرهای بیشتر ترکیه
        "اسکیشهر", "دنیزلی", "کایسری", "سامسون", "بالیکسیر", "آیدین",
        "موغلا", "بدروم", "مارماریس", "فتحیه", "آلانیا", "سیده", "کمر",
        "مانیسا", "اورفا", "دیاربکر", "وان", "ارزروم", "ریزه", "گیرسون",
        # 🆕 خدمات و نیازها
        "وکیل ترکیه", "حسابدار ترکیه", "دکتر ایرانی استانبول", "بیمارستان ترکیه",
        "مدرسه ایرانی", "زبان ترکی", "کلاس ترکی", "تومر", "ترکی یاد بگیر",
        "حمل اثاثیه", "باربری استانبول", "جابجایی ترکیه", "کانتینر ایران",
        "صرافی استانبول", "حواله ایران", "پول ترکیه", "لیر", "دلار استانبول",
        "رستوران ایرانی", "غذای ایرانی", "سوپرمارکت ایرانی", "محصولات ایرانی",
        "آرایشگاه ایرانی", "آرایشگر فارسی", "خیاط ایرانی", "پارچه استانبول",
        "طلا استانبول", "بازار استانبول", "گرند بازار", "خرید استانبول",
    ],
    
    # 🇹🇷 ترکیه (انگلیسی) - گسترش یافته
    'turkey_en': [
        "turkey", "istanbul", "ankara", "izmir", "antalya", "bursa",
        "turkish visa", "kimlik", "turkish residence", "living in turkey",
        "work in turkey", "study turkey", "iranian istanbul", "persian turkey",
        # 🆕 گسترش جدید
        "esenyurt", "fatih", "kadikoy", "besiktas", "sisli", "atashehir",
        "taksim", "aksaray", "laleli", "basaksehir", "bahcesehir", "beylikduzu",
        "istanbul apartment", "istanbul rent", "istanbul job", "turkish lira",
        "iranian restaurant istanbul", "persian community turkey",
    ],
    
    # 🇦🇪 امارات و دبی (فارسی) - گسترش یافته +80 کلمه
    'uae_fa': [
        "امارات", "دبی", "ابوظبی", "ابوذبی", "شارجه", "عجمان", "راس الخیمه",
        "ام القیوین", "الفجیره", "دیره", "بر دبی", "جمیرا", "مارینا",
        "برج خلیفه", "مال امارات", "دبی مال", "پالم جمیرا", "داون تاون",
        "اقامت دبی", "ویزای امارات", "کار در دبی", "زندگی دبی",
        "تجارت دبی", "شرکت دبی", "فری زون", "منطقه آزاد",
        "امارات آیدی", "ویزای کار", "ویزای سرمایه گذاری",
        "ایرانیان دبی", "ایرانیان امارات", "فارسی دبی", "گروه دبی",
        "خرید دبی", "ملک دبی", "اجاره دبی",
        # 🆕 مناطق بیشتر
        "جی بی آر", "JBR", "بیزنس بی", "جمیرا لیک", "الخلیج", "القصیص",
        "الوارقا", "مردف", "البرشا", "المنخول", "الکرامه", "النهده",
        "عود متعنا", "واحه السیلیکون", "موتور سیتی", "اسپورتس سیتی",
        "دبی هیلز", "داماک هیلز", "عربین رنچز", "تاون سکویر",
        "الریم آیلند", "یاس آیلند", "سعدیات", "مصفح", "موسی فاح",
        # 🆕 خدمات
        "لایسنس دبی", "مجوز کار", "لیبر کارت", "ویزای گلدن",
        "حساب بانکی امارات", "بانک دبی", "راک بانک", "مشرق بانک",
        "درهم", "تبدیل درهم", "صرافی دبی", "حواله ایران دبی",
        "گروه تجار دبی", "بیزینسمن دبی", "کارآفرین دبی",
    ],
    
    # 🇦🇪 امارات و دبی (انگلیسی) - گسترش یافته
    'uae_en': [
        "uae", "dubai", "abu dhabi", "sharjah", "ajman", "rak",
        "dubai visa", "emirates id", "dubai work", "living dubai",
        "dubai business", "free zone", "iranian dubai", "persian uae",
        # 🆕 گسترش جدید
        "jbr", "marina", "downtown dubai", "jumeirah", "business bay",
        "deira", "bur dubai", "al barsha", "jlt", "silicon oasis",
        "golden visa", "dubai license", "dubai apartment", "dubai rent",
        "iranian business dubai", "persian community dubai",
    ],
    
    # 🇮🇶 عراق (فارسی) - گسترش یافته
    'iraq_fa': [
        "عراق", "بغداد", "اربیل", "سلیمانیه", "کربلا", "نجف", "بصره",
        "کردستان عراق", "اقلیم کردستان", "زیارت", "عتبات",
        "ایرانیان عراق", "فارسی عراق", "کار عراق", "تجارت عراق",
        # 🆕 گسترش جدید
        "سماوه", "ناصریه", "کوفه", "دیوانیه", "حله", "کاظمین", "سامرا",
        "موصل", "کرکوک", "دهوک", "زاخو", "عمادیه", "رانیه",
        "ویزای عراق", "زیارت عتبات", "اربعین", "زائر", "کاروان زیارتی",
        "تجار عراق", "صادرات عراق", "واردات عراق", "تجارت ایران عراق",
        # 🆕 گسترش بیشتر - شهرها و مناطق
        "تکریت", "بعقوبه", "رمادی", "فلوجه", "عماره", "کوت", "بابل",
        "هیت", "حدیثه", "تلعفر", "سیدکان", "شقلاوه", "پیرانشهر عراق",
        "منطقه سبز بغداد", "کاظمیه", "اعظمیه", "منصور بغداد", "کراده",
        # 🆕 تجارت و کسب و کار
        "بازرگانی عراق", "تجارت مرزی", "مرز شلمچه", "مرز مهران", "مرز خسروی",
        "مرز پرویزخان", "مرز حاج عمران", "مرز چزابه", "گمرک عراق",
        "صادرات به عراق", "واردات از عراق", "کالای ایرانی عراق",
        "نمایشگاه بغداد", "نمایشگاه اربیل", "نمایشگاه بصره",
        "شرکت ایرانی عراق", "پیمانکار عراق", "ساخت و ساز عراق",
        "حواله عراق", "دینار عراق", "صرافی عراق", "تبدیل دینار",
        # 🆕 زیارت و مذهبی
        "زیارت کربلا", "زیارت نجف", "حرم امام حسین", "حرم حضرت عباس",
        "حرم امام علی", "زیارت سامرا", "زیارت کاظمین",
        "اربعین حسینی", "پیاده روی اربعین", "موکب", "موکب اربعین",
        "هتل کربلا", "هتل نجف", "اقامت نزدیک حرم", "سوئیت کربلا",
        # 🆕 خدمات
        "بیمارستان ایرانی عراق", "دکتر ایرانی اربیل", "کلینیک ایرانی بغداد",
        "رستوران ایرانی اربیل", "هتل ایرانی عراق", "حمل بار عراق",
        "آموزش عربی", "زبان کردی", "مترجم عراق", "وکیل عراق",
    ],
    
    # 🇮🇶 عراق (انگلیسی)
    'iraq_en': [
        "iraq", "baghdad", "erbil", "sulaymaniyah", "karbala", "najaf",
        "kurdistan iraq", "iranian iraq", "work iraq",
        "basra", "mosul", "kirkuk", "duhok", "arbaeen", "pilgrimage",
        # 🆕 expanded
        "iraq business", "iraq trade", "iran iraq trade", "export iraq",
        "import iraq", "erbil business", "baghdad business", "basra port",
        "iraqi dinar", "iraq visa", "iraq construction", "iraq contractor",
        "iranian erbil", "persian iraq", "karbala hotel", "najaf hotel",
        "arbaeen walk", "pilgrimage iraq", "ziyarat iraq",
        "iranian community iraq", "farsi iraq", "persian community erbil",
    ],
    
    # 🇪🇺 اروپا عمومی (فارسی) - گسترش یافته
    'europe_fa': [
        "اروپا", "اتحادیه اروپا", "شنگن", "ویزای شنگن", "اقامت اروپا",
        "مهاجرت اروپا", "کار اروپا", "تحصیل اروپا", "زندگی اروپا",
        "پناهندگی اروپا", "ایرانیان اروپا",
        # 🆕 گسترش جدید
        "ویزای توریستی اروپا", "تور اروپا", "سفر اروپا", "گشت اروپا",
        "بلیط اروپا", "هتل اروپا", "اجاره ماشین اروپا", "رانندگی اروپا",
        "کار اتحادیه اروپا", "بلوکارت اروپا", "اقامت کار اروپا",
        "ایرانیان مقیم اروپا", "فارسی زبانان اروپا", "انجمن ایرانیان اروپا",
    ],
    
    # 🇩🇪 آلمان (فارسی) - گسترش یافته +50 کلمه
    'germany_fa': [
        "آلمان", "برلین", "مونیخ", "فرانکفورت", "هامبورگ", "کلن",
        "دوسلدورف", "اشتوتگارت", "هانوفر", "نورنبرگ",
        "آسبیلدونگ", "اوسبیلدونگ", "بلوکارت", "ویزای آلمان",
        "تحصیل آلمان", "کار آلمان", "زندگی آلمان", "اقامت آلمان",
        "ایرانیان آلمان", "فارسی آلمان", "گروه برلین",
        # 🆕 شهرهای بیشتر
        "درسدن", "لایپزیک", "بن", "وین", "بوخوم", "وپرتال", "بیله فلد",
        "مونستر", "کارلسروهه", "ماینتس", "آخن", "فرایبورگ", "هایدلبرگ",
        # 🆕 موضوعات
        "زبان آلمانی", "کلاس آلمانی", "گوته", "تلک", "دویچ لرنن",
        "آپارتمان آلمان", "WG", "وونگ گماینشافت", "اجاره برلین",
        "شغل آلمان", "جاب آلمان", "کار آی تی آلمان", "مهندس آلمان",
        "بورسیه آلمان", "DAAD", "دانشگاه آلمان", "TU", "ماستر آلمان",
        "انجمن ایرانیان آلمان", "ایرانیان برلین", "ایرانیان مونیخ",
    ],
    
    # 🇬🇧 انگلستان (فارسی) - گسترش یافته
    'uk_fa': [
        "انگلیس", "انگلستان", "لندن", "منچستر", "بیرمنگام", "لیورپول",
        "بریتانیا", "ویزای انگلیس", "تحصیل انگلیس", "کار لندن",
        "زندگی لندن", "ایرانیان لندن", "فارسی انگلیس",
        # 🆕 گسترش جدید
        "لیدز", "شفیلد", "بریستول", "ناتینگهام", "لسترlLeicester",
        "نیوکاسل", "ساوتهمپتون", "کاردیف", "ادینبورگ", "گلاسکو",
        "ویزای تحصیلی انگلیس", "ویزای کار انگلیس", "ویزای خانوادگی",
        "اجاره لندن", "خانه لندن", "کار لندن", "شغل لندن", "جاب انگلیس",
        "دانشگاه لندن", "آکسفورد", "کمبریج", "امپریال", "UCL", "LSE",
        "ایرانیان انگلستان", "پرشین لندن", "انجمن ایرانیان انگلیس",
    ],
    
    # 🇨🇦 کانادا (فارسی) - گسترش یافته +60 کلمه
    'canada_fa': [
        "کانادا", "تورنتو", "ونکوور", "مونترال", "اتاوا", "کلگری",
        "ادمونتون", "کبک", "اکسپرس انتری", "پی آر کانادا",
        "استودی پرمیت", "ورک پرمیت", "شهروندی کانادا",
        "مهاجرت کانادا", "ویزای کانادا", "تحصیل کانادا",
        "ایرانیان کانادا", "ایرانیان تورنتو", "فارسی ونکوور",
        # 🆕 شهرها و استان‌ها
        "مونترال", "میسیساگا", "برمپتون", "مارکام", "ریچموندهیل",
        "نورث یورک", "اسکاربرو", "برنابی", "سری", "ریچموند",
        "ویکتوریا", "هالیفکس", "وینیپگ", "ساسکاتون", "رجاینا",
        "انتاریو", "بریتیش کلمبیا", "آلبرتا", "کبک", "منیتوبا",
        # 🆕 برنامه‌های مهاجرتی
        "PNP", "پی ان پی", "فدرال اسکیلد", "CEC", "FSW", "FST",
        "LMIA", "العمیا", "پوینت کانادا", "امتیازبندی", "CRS",
        "آیلتس کانادا", "CLB", "ایتا", "اکسپرس انتری دراو",
        # 🆕 زندگی کانادا
        "اجاره تورنتو", "خانه ونکوور", "کار کانادا", "شغل کانادا",
        "حقوق کانادا", "مالیات کانادا", "بیمه کانادا", "OHIP",
        "رانندگی کانادا", "گواهینامه کانادا", "ماشین کانادا",
        "ایرانیان مونترال", "پرشین تورنتو", "نورث تهران",
    ],
    
    # 🇦🇺 استرالیا (فارسی) - گسترش یافته
    'australia_fa': [
        "استرالیا", "سیدنی", "ملبورن", "بریزبن", "پرث", "آدلاید",
        "ویزای استرالیا", "پی آر استرالیا", "مهاجرت استرالیا",
        "تحصیل استرالیا", "کار استرالیا", "ایرانیان استرالیا",
        # 🆕 گسترش جدید
        "کانبرا", "گلد کوست", "نیوکاسل", "ووولونگونگ", "هوبارت",
        "ویزای ساب کلاس", "189", "190", "491", "482", "500", "485",
        "اسکیل سلکت", "EOI", "نامینیشن", "اسپانسر استرالیا",
        "اجاره سیدنی", "خانه ملبورن", "کار استرالیا", "شغل سیدنی",
        "ایرانیان سیدنی", "ایرانیان ملبورن", "پرشین استرالیا",
    ],
    
    # 🌍 سایر کشورها (فارسی) - گسترش یافته +80 کلمه
    'other_countries_fa': [
        # فرانسه
        "فرانسه", "پاریس", "لیون", "مارسی", "نیس", "تولوز", "بوردو",
        "ویزای فرانسه", "اقامت فرانسه", "ایرانیان پاریس",
        # هلند
        "هلند", "آمستردام", "روتردام", "لاهه", "اوترخت", "آیندهوون",
        "ویزای هلند", "KNM", "MVV", "ایرانیان هلند",
        # سوئد
        "سوئد", "استکهلم", "گوتنبرگ", "مالمو", "اوپسالا",
        "ویزای سوئد", "پناهندگی سوئد", "ایرانیان سوئد",
        # نروژ
        "نروژ", "اسلو", "برگن", "تروندهایم", "ویزای نروژ", "ایرانیان نروژ",
        # سوئیس
        "سوئیس", "زوریخ", "ژنو", "برن", "بازل", "لوزان",
        "ویزای سوئیس", "کار سوئیس", "ایرانیان سوئیس",
        # اتریش
        "اتریش", "وین", "گراتس", "سالزبورگ", "ویزای اتریش", "ایرانیان وین",
        # ایتالیا
        "ایتالیا", "رم", "میلان", "فلورانس", "ونیز", "ناپل", "تورین",
        "ویزای ایتالیا", "ایرانیان ایتالیا",
        # اسپانیا
        "اسپانیا", "مادرید", "بارسلونا", "والنسیا", "سویا", "مالاگا",
        "ویزای اسپانیا", "گلدن ویزا", "ایرانیان اسپانیا",
        # آمریکا
        "آمریکا", "نیویورک", "لس آنجلس", "شیکاگو", "هیوستون", "فینیکس",
        "سن فرانسیسکو", "سن دیگو", "سیاتل", "بوستون", "میامی", "واشنگتن",
        "گرین کارت", "لاتری", "ویزای B1", "ویزای B2", "ویزای F1",
        "ایرانیان آمریکا", "ایرانیان لس آنجلس", "تهران جلز",
        # ارمنستان
        "ارمنستان", "ایروان", "گیومری", "ویزای ارمنستان", "ایرانیان ارمنستان",
        # گرجستان
        "گرجستان", "تفلیس", "باتومی", "کوتایسی", "ویزای گرجستان", "ایرانیان گرجستان",
        # مالزی
        "مالزی", "کوالالامپور", "پنانگ", "لنکاوی", "ویزای مالزی", "ایرانیان مالزی",
        # تایلند
        "تایلند", "بانکوک", "پاتایا", "پوکت", "چیانگ مای", "ویزای تایلند",
        # قبرس
        "قبرس", "نیکوزیا", "لارناکا", "لیماسول", "قبرس شمالی", "ایرانیان قبرس",
        # یونان
        "یونان", "آتن", "تسالونیکی", "ویزای یونان", "ایرانیان یونان",
        # پرتغال
        "پرتغال", "لیسبون", "پورتو", "گلدن ویزای پرتغال", "ایرانیان پرتغال",
        # عمان
        "عمان", "مسقط", "صلاله", "ویزای عمان", "کار عمان", "ایرانیان عمان",
        # قطر
        "قطر", "دوحه", "ویزای قطر", "کار قطر", "ایرانیان قطر",
    ],
    
    # 📋 موضوعات مهاجرتی (فارسی) - گسترش یافته +40 کلمه
    'immigration_topics_fa': [
        "ویزای تحصیلی", "ویزای کار", "ویزای سرمایه گذاری", "ویزای توریستی",
        "اقامت کار", "اقامت تحصیلی", "اقامت دائم", "شهروندی", "پاسپورت دوم",
        "تحصیل خارج", "بورسیه", "اکسپت", "ریجکت", "مشاوره مهاجرت",
        "وکیل مهاجرت", "موسسه مهاجرت", "لاتری", "قرعه کشی آمریکا",
        "اجاره خانه", "رنت", "ملک", "مستغلات", "حمل اثاث", "باربری",
        "کار خارج", "استخدام خارج", "درآمد دلاری", "ارز خارجی",
        "حواله", "صرافی", "انتقال پول", "وسترن یونیون",
        # 🆕 گسترش جدید
        "آپلای کردن", "اپلیکیشن", "فرم ویزا", "DS160", "چک لیست مدارک",
        "آفر لتر", "CAS", "LOA", "I20", "نامه پذیرش", "فاند پروف",
        "بانک استیتمنت", "اسپانسر", "گارانتور", "تعهدنامه",
        "مصاحبه ویزا", "سوالات مصاحبه", "تمکن مالی", "تراز حساب",
        "ترجمه رسمی", "ترجمه دادگستری", "آپوستیل", "لگالایز",
        "بیمه مسافرتی", "بلیط هواپیما", "هتل رزرو", "آیتینرری",
        "کد رهگیری", "ترک", "شماره پرونده", "وضعیت پرونده",
    ],
    
    # 👥 گروه‌های مهاجرتی - گسترش یافته
    'immigration_groups_fa': [
        "گروه مهاجرت", "گروه ایرانیان", "گروه فارسی زبان", "انجمن ایرانیان",
        "جامعه ایرانی", "همیاری", "کمک به مهاجرین", "راهنمای مهاجرت",
        "تجربه مهاجرت", "مشکلات مهاجرت", "سوال مهاجرت",
        # 🆕 گسترش جدید
        "گروه ویزا", "گروه اقامت", "گروه کار خارج", "گروه تحصیل خارج",
        "اطلاعات مهاجرت", "مشاوره رایگان مهاجرت", "تجربیات مهاجرت",
        "چت مهاجرت", "پرسش و پاسخ مهاجرت", "انجمن مهاجرین",
        "ایرانیان دور از وطن", "هموطنان خارج", "ایرانی‌های خارج",
    ],
    
    # 🆕 انگلیسی گسترش یافته کشورها
    'countries_en': [
        "germany", "berlin", "munich", "frankfurt", "hamburg",
        "uk", "london", "manchester", "birmingham", "edinburgh",
        "canada", "toronto", "vancouver", "montreal", "calgary",
        "australia", "sydney", "melbourne", "brisbane", "perth",
        "france", "paris", "lyon", "marseille", "nice",
        "netherlands", "amsterdam", "rotterdam", "the hague",
        "sweden", "stockholm", "gothenburg", "malmo",
        "norway", "oslo", "bergen", "trondheim",
        "switzerland", "zurich", "geneva", "bern", "basel",
        "austria", "vienna", "graz", "salzburg",
        "italy", "rome", "milan", "florence", "venice",
        "spain", "madrid", "barcelona", "valencia", "seville",
        "usa", "new york", "los angeles", "chicago", "houston",
        "iran visa", "persian abroad", "farsi community", "iranian group",
        # 🆕🆕 کشورهای بیشتر انگلیسی
        "armenia", "yerevan", "georgia", "tbilisi", "batumi",
        "malaysia", "kuala lumpur", "thailand", "bangkok",
        "cyprus", "nicosia", "limassol", "greece", "athens",
        "portugal", "lisbon", "porto", "oman", "muscat", "qatar", "doha",
        "new zealand", "auckland", "wellington", "ireland", "dublin",
        "belgium", "brussels", "denmark", "copenhagen", "finland", "helsinki",
        "czech", "prague", "poland", "warsaw", "hungary", "budapest",
        "iranian community", "persian expat", "farsi speaking",
        "iranian diaspora", "persian diaspora", "iran abroad",
        "pr application", "immigration program", "skilled worker",
        "family reunification", "visitor visa", "tourist visa",
        "settlement visa", "tier 2 visa", "h1b visa", "eb3 visa",
        "express entry", "provincial nominee", "australian pr",
        "blue card europe", "freelancer visa germany",
        "golden visa uae", "golden visa portugal", "golden visa spain",
        "digital nomad visa", "startup visa", "investor visa",
    ],
    
    # 🆕🆕 دیاسپورا و اجتماعات ایرانی (فارسی)
    'diaspora_fa': [
        "ایرانیان خارج از کشور", "ایرانیان مقیم خارج", "هموطن خارج نشین",
        "جامعه ایرانی", "جامعه فارسی زبان", "انجمن ایرانیان",
        "ایرانیان دور از وطن", "غربت نشین", "مهاجر ایرانی",
        "دیاسپورا ایرانی", "فرهنگ ایرانی خارج", "نوروز خارج",
        "شب یلدا خارج", "چهارشنبه سوری", "جشن ایرانی",
        "رستوران ایرانی", "سوپرمارکت ایرانی", "نانوایی ایرانی",
        "کتابفروشی فارسی", "مدرسه فارسی", "کلاس فارسی",
        "پادکست فارسی", "رادیو فارسی", "تلویزیون فارسی",
        "موسیقی ایرانی", "کنسرت ایرانی", "فیلم ایرانی",
        "آرایشگاه ایرانی", "دکتر ایرانی", "وکیل ایرانی",
        "حسابدار ایرانی", "مکانیک ایرانی", "راننده ایرانی",
        "بچه محل", "گروه شهری", "همشهری", "هم استانی",
        "گروه تهرانی‌ها", "گروه اصفهانی‌ها", "گروه شیرازی‌ها",
        "گروه مشهدی‌ها", "گروه تبریزی‌ها", "گروه اهوازی‌ها",
        "گروه کرمانی‌ها", "گروه گیلانی‌ها", "گروه مازندرانی‌ها",
        "ایرانیان آنکارا", "ایرانیان ازمیر", "ایرانیان آنتالیا",
        "ایرانیان بورسا", "ایرانیان مرسین", "ایرانیان ترابزون",
        "ایرانیان شارجه", "ایرانیان ابوظبی", "ایرانیان عجمان",
        "ایرانیان بریزبن", "ایرانیان پرث", "ایرانیان آدلاید",
        "ایرانیان ادمونتون", "ایرانیان کلگری", "ایرانیان اتاوا",
        "ایرانیان هامبورگ", "ایرانیان فرانکفورت", "ایرانیان کلن",
        "ایرانیان پاریس", "ایرانیان لیون", "ایرانیان آمستردام",
        "ایرانیان استکهلم", "ایرانیان اسلو", "ایرانیان وین",
        "ایرانیان رم", "ایرانیان میلان", "ایرانیان مادرید",
        "ایرانیان بارسلونا", "ایرانیان لیسبون", "ایرانیان دوبلین",
        "ایرانیان بروکسل", "ایرانیان کپنهاگ", "ایرانیان هلسینکی",
        "ایرانیان پراگ", "ایرانیان ورشو", "ایرانیان بوداپست",
        "ایرانیان نیویورک", "ایرانیان واشنگتن", "ایرانیان شیکاگو",
        "ایرانیان هیوستون", "ایرانیان سن فرانسیسکو", "ایرانیان سیاتل",
        "ایرانیان آکلند", "ایرانیان ولینگتون", "ایرانیان کوالالامپور",
    ],
    
    # 🆕🆕 دیاسپورا (انگلیسی)
    'diaspora_en': [
        "iranian community", "persian community", "iranian diaspora",
        "persian diaspora", "farsi speaking community",
        "iranian association", "persian association", "iranian society",
        "iranian cultural center", "persian cultural event",
        "nowruz celebration", "yalda night", "iranian new year",
        "iranian restaurant", "persian restaurant", "persian grocery",
        "iranian supermarket", "persian food", "iranian bakery",
        "iranian doctor", "iranian lawyer", "iranian dentist",
        "iranian accountant", "iranian realtor", "iranian business",
        "persian music event", "iranian concert", "iranian film festival",
        "persian school", "farsi class", "persian language",
        "tehrangeles", "persian square", "westwood persian",
        "iranian students", "persian student association",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# 🥉🥉🥉 اولویت 3: کلمات کلیدی عمومی و متفرقه 🥉🥉🥉
# ═══════════════════════════════════════════════════════════════════════════════

GENERAL_KEYWORDS = {
    'general_fa': [
        # کلمات عمومی فارسی - گسترش یافته
        "گروه", "گپ", "چت", "دوستی", "ایران", "ایرانی", "تهران", "نایاب", "کرج",
        "مشهد", "اصفهان", "شیراز", "تبریز", "خرید", "یافتن", "کمیاب", "فروش", "تخفیف",
        "استخدام", "کار", "آموزش", "تبادل", "دارو", "دوره", "کلاس", "سلامت", "پزشکی",
        # خرید و فروش عمومی
        "بازار", "فروشگاه", "آنلاین", "اینترنتی", "دست دوم", "نو", "کارکرده",
        "واردات", "صادرات", "عمده", "خرده", "پخش", "توزیع", "تامین",
        "قیمت", "ارزان", "حراج", "تخفیف ویژه", "فروش فوری", "مزایده",
        # شغل و کار
        "استخدامی", "نیروی کار", "بازار کار", "کاریابی", "فرصت شغلی",
        "فریلنسر", "دورکاری", "پاره وقت", "تمام وقت", "کار آنلاین",
        "درآمد", "کسب درآمد", "درآمدزایی", "همکاری", "مشارکت",
        # اجتماعی و ارتباطات
        "انجمن", "جامعه", "اجتماعی", "خیریه", "کمک", "داوطلب",
        "دوستیابی", "آشنایی", "ازدواج", "همسریابی",
        "چت روم", "گپ فارسی", "گفتگو", "صحبت", "مکالمه",
        # آموزش و تحصیل
        "دانشگاه", "دانشجو", "کنکور", "مدرسه", "معلم", "تدریس",
        "زبان", "آیلتس", "تافل", "بورسیه", "لیسانس", "ارشد", "دکتری",
        "آموزشگاه", "مدرک", "گواهینامه", "سرتیفیکیت",
        # سرگرمی و تفریح
        "موسیقی", "فیلم", "سینما", "ورزش", "فوتبال", "بازی", "گیم",
        "سرگرمی", "تفریح", "کتاب", "هنر", "عکاسی", "نقاشی",
        # خودرو و حمل و نقل
        "ماشین", "خودرو", "موتور", "لوازم یدکی", "تعمیرگاه",
        "بلیط", "پرواز", "هتل", "رزرو", "سفر", "گردشگری",
        # ملک و مسکن
        "ملک", "خانه", "آپارتمان", "اجاره", "رهن", "ویلا", "زمین",
        "مشاور املاک", "خرید خانه", "فروش ملک",
        # تکنولوژی
        "موبایل", "کامپیوتر", "لپ تاپ", "تبلت", "گوشی",
        "اپلیکیشن", "نرم افزار", "بازی موبایل", "اینترنت",
        # غذا و رستوران
        "غذا", "رستوران", "کافه", "آشپزی", "شیرینی", "کیک",
        # خدمات
        "تعمیرات", "خدمات", "نصب", "برق", "لوله کشی", "نظافت",
        # مالی
        "بورس", "سهام", "سرمایه گذاری", "بانک", "وام", "بیمه",
        "دلار", "طلا", "سکه", "ارز",
        # دارو و سلامت (تکمیلی)
        "داروخانه", "مکمل", "ویتامین", "رژیم", "لاغری", "تناسب اندام",
        "کلینیک", "بیمارستان", "آزمایشگاه", "تجهیزات پزشکی",
        "دندانپزشکی", "روانشناسی", "فیزیوتراپی", "چشم پزشکی",
    ],
    'general_en': [
        "group", "chat", "iran", "iranian", "persian", "farsi", "tehran",
        "buy", "sell", "job", "work", "training", "course", "health",
        "pharmacy", "medical", "dental", "clinic", "hospital", "drug",
        "equipment", "laboratory", "supplement", "vitamin",
        "immigration", "visa", "travel", "tour", "hotel", "flight",
        "real estate", "apartment", "rent", "car", "motor",
        "technology", "mobile", "computer", "app", "software",
        "food", "restaurant", "cafe", "cooking",
        "education", "university", "language", "IELTS",
        "music", "movie", "sport", "game", "book", "art",
        "business", "invest", "stock", "forex", "crypto",
        "freelance", "remote", "online", "digital",
        "community", "social", "charity", "marriage", "friendship",
    ],
    
    # 🆕🆕 زیر دسته‌های جدید عمومی
    'beauty_fa': [
        "آرایشگاه", "آرایشگر", "سالن زیبایی", "آرایش", "مو", "رنگ مو",
        "کراتین", "اکستنشن مو", "ناخن", "کاشت ناخن", "ابرو", "میکروبلیدینگ",
        "پاکسازی صورت", "فیشال", "لیزر", "لیزر موهای زائد", "جوانسازی",
        "بوتاکس", "فیلر", "تزریق ژل", "لیفت ابرو", "لیفت صورت",
        "عطر", "ادکلن", "لوازم آرایشی", "محصولات پوستی", "سان اسکرین",
        "مراقبت پوست", "اسکین کر", "درماتولوژی", "زیبایی صورت",
    ],
    
    'legal_fa': [
        "وکیل", "حقوقی", "قانون", "دادگاه", "قاضی", "دادسرا",
        "مشاوره حقوقی", "وکیل پایه یک", "وکیل مهاجرت", "وکیل ملکی",
        "قرارداد", "سند", "محضر", "ثبت", "ثبت شرکت", "ثبت برند",
        "طلاق", "مهریه", "نفقه", "حضانت", "ارث", "وصیت",
        "حقوق کار", "بیمه تامین اجتماعی", "شکایت", "دیه", "جرم",
    ],
    
    'finance_fa': [
        "بورس تهران", "بورس کالا", "فرابورس", "بازار سرمایه", "صندوق سرمایه",
        "سبدگردان", "صندوق ETF", "صندوق درآمد ثابت", "اوراق مشارکت",
        "تحلیل بنیادی بورس", "تحلیل تکنیکال بورس", "سهام عدالت",
        "عرضه اولیه", "IPO", "حق تقدم", "افزایش سرمایه",
        "بیمه عمر", "بیمه ماشین", "بیمه مسافرتی", "بیمه تکمیلی",
        "وام مسکن", "وام ازدواج", "وام کسب و کار", "وام فرزندآوری",
        "مالیات", "اظهارنامه", "حسابداری", "حسابرسی", "دفتر اسناد",
    ],
    
    'pet_fa': [
        "حیوانات خانگی", "سگ", "گربه", "پرنده", "آکواریوم", "ماهی",
        "دامپزشکی", "دامپزشک", "واکسن حیوانات", "غذای حیوانات",
        "پت شاپ", "فروش سگ", "فروش گربه", "خرگوش", "همستر",
        "آموزش سگ", "نگهداری گربه", "پرورش سگ", "پانسیون حیوانات",
    ],
    
    'digital_marketing_fa': [
        "دیجیتال مارکتینگ", "بازاریابی دیجیتال", "بازاریابی اینترنتی",
        "سئو", "SEO", "تبلیغات گوگل", "گوگل ادز", "تبلیغات کلیکی",
        "اینستاگرام مارکتینگ", "تلگرام مارکتینگ", "تبلیغات تلگرام",
        "افزایش فالور", "افزایش ممبر", "فالور اینستا", "لایک اینستا",
        "ادمین اینستاگرام", "ادمین تلگرام", "تولید محتوا", "کپی رایتینگ",
        "ایمیل مارکتینگ", "فانل فروش", "لندینگ پیج", "سایت فروشگاهی",
        "درآمد اینترنتی", "کسب و کار آنلاین", "استارتاپ ایرانی",
        "بازاریابی شبکه ای", "نتورک مارکتینگ", "MLM", "بازاریابی",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# �🏥🏥 اولویت ویژه: کلمات کلیدی جامع پزشکی، دارویی و تجهیزات 🏥🏥🏥
# ═══════════════════════════════════════════════════════════════════════════════

MEDICAL_KEYWORDS = {
    # 💊 دارو و داروخانه - جامع
    'pharma_fa': [
        "دارو", "داروخانه", "داروسازی", "داروساز", "فارماکولوژی", "فارماسیوتیکال",
        "نسخه", "نسخه نویسی", "نسخه خوانی", "نسخه الکترونیک", "نسخه پزشکی",
        "داروی ژنریک", "داروی برند", "داروی OTC", "داروی نایاب", "داروی کمیاب",
        "داروی خاص", "بیماران خاص", "داروی ضروری", "داروی حیاتی",
        "تبادل دارو", "خرید دارو", "فروش دارو", "تامین دارو", "پخش دارو",
        "عمده دارو", "واردات دارو", "صادرات دارو", "ترخیص دارو",
        "شرکت دارویی", "کارخانه دارویی", "انبار دارویی", "زنجیره تامین دارو",
        "داروخانه آنلاین", "داروخانه شبانه روزی", "داروخانه بیمارستانی",
        "فهرست دارویی", "دارونامه", "فارماکوپه", "WHO", "FDA", "NAFDAC",
        "اثربخشی دارو", "عوارض جانبی", "تداخل دارویی", "دوز دارو", "مقدار مصرف",
        "آنتی بیوتیک", "مسکن", "ضد التهاب", "ضد درد", "استروئید",
        "داروی قلبی", "داروی فشار", "داروی قند", "داروی تیروئید", "داروی روانپزشکی",
        "داروی ضد سرطان", "شیمی درمانی", "هورمون درمانی", "ایمونوتراپی",
        "داروی بیولوژیک", "بیوسیمیلار", "مونوکلونال آنتی بادی",
        "واکسن", "واکسیناسیون", "سرم", "آنتی سرم", "ایمونوگلوبولین",
        "مکمل غذایی", "ویتامین", "پروبیوتیک", "امگا", "آهن", "کلسیم",
        "گیاه دارویی", "طب سنتی", "عطاری", "عرقیات", "دمنوش", "عصاره",
        "فرآورده آرایشی بهداشتی", "لوازم بهداشتی", "سلولز", "پانسمان",
        "بیمه دارو", "تعرفه دارو", "قیمت دارو", "یارانه دارو",
        "انجمن داروسازان", "نظام دارویی", "سازمان غذا و دارو",
    ],
    
    'pharma_en': [
        "pharmacy", "pharmaceutical", "pharmacology", "drug", "medication",
        "prescription", "OTC", "generic drug", "brand drug", "medicine",
        "drug supply", "drug distribution", "wholesale drug", "import drug",
        "pharma company", "drug manufacturer", "drug warehouse",
        "online pharmacy", "hospital pharmacy", "clinical pharmacy",
        "antibiotic", "painkiller", "anti-inflammatory", "steroid",
        "vaccine", "immunization", "supplement", "vitamin", "probiotic",
        "herbal medicine", "traditional medicine", "natural remedy",
        "drug interaction", "side effect", "dosage", "FDA approved",
        "biologic drug", "biosimilar", "monoclonal antibody",
        "chemotherapy", "immunotherapy", "hormone therapy",
    ],

    # 🦷 دندانپزشکی - جامع
    'dental_fa': [
        "دندانپزشکی", "دندانپزشک", "دندان", "دهان", "لثه", "فک",
        "ایمپلنت", "ایمپلنت دندان", "کاشت دندان", "پیوند استخوان فک",
        "ارتودنسی", "سیم کشی دندان", "براکت", "ارتودنسی نامرئی", "اینویزالاین",
        "لمینیت", "لمینت دندان", "ونیر", "کامپوزیت", "سرامیک دندان",
        "روکش دندان", "کراون", "بریج", "پروتز دندان", "دندان مصنوعی",
        "عصب کشی", "اندو", "درمان ریشه", "پالپکتومی", "پالپوتومی",
        "جرمگیری", "بروساژ", "پالیش", "بلیچینگ", "سفید کردن دندان",
        "پرکردن دندان", "کشیدن دندان", "جراحی دندان عقل", "جراحی فک",
        "رادیولوژی دندان", "OPG", "CBCT", "رادیوگرافی دندان",
        "بیهوشی دندانپزشکی", "بی حسی موضعی", "سدیشن",
        "دندانپزشکی کودکان", "پدودنتیکس", "دندانپزشکی ترمیمی",
        "پریو", "پریودنتیکس", "جراحی لثه", "پیوند لثه",
        "دندانپزشکی زیبایی", "طراحی لبخند", "اسمایل دیزاین",
        "لابراتوار دندانسازی", "تکنسین دندانسازی", "قالب دندان",
        "یونیت دندانپزشکی", "توربین", "آنگل", "کنتراانگل", "میکروموتور",
        "مواد دندانپزشکی", "آمالگام", "گلاس آینومر", "رزین کامپوزیت",
        "نخ دندان", "خمیر دندان", "دهانشویه", "مسواک", "واترجت",
        "بهداشت دهان و دندان", "پیشگیری پوسیدگی", "فلوراید تراپی",
        "TMJ", "مفصل فکی گیجگاهی", "دندان قروچه", "بروکسیسم",
        "عفونت دندان", "آبسه دندان", "پوسیدگی", "تحلیل لثه",
    ],
    
    'dental_en': [
        "dental", "dentist", "dentistry", "tooth", "teeth", "gum",
        "implant", "dental implant", "orthodontics", "braces", "invisalign",
        "veneer", "laminate", "crown", "bridge", "denture", "prosthesis",
        "root canal", "endodontics", "pulpectomy", "extraction",
        "scaling", "polishing", "bleaching", "whitening",
        "periodontics", "gum surgery", "bone graft",
        "cosmetic dentistry", "smile design", "pediatric dentistry",
        "dental lab", "dental unit", "dental materials",
        "dental radiology", "OPG", "CBCT", "dental X-ray",
    ],

    # 🔬 آزمایشگاه و تشخیص - جامع
    'laboratory_fa': [
        "آزمایشگاه", "لابراتوار", "آزمایش", "تست", "نمونه گیری",
        "آزمایشگاه تشخیص طبی", "آزمایشگاه پاتولوژی", "آزمایشگاه ژنتیک",
        "آزمایشگاه میکروبیولوژی", "آزمایشگاه بیوشیمی", "آزمایشگاه هماتولوژی",
        "آزمایشگاه هورمون", "آزمایشگاه ایمونولوژی", "آزمایشگاه سرولوژی",
        "آزمایش خون", "CBC", "آزمایش ادرار", "آزمایش مدفوع",
        "آزمایش قند خون", "FBS", "HbA1C", "GTT", "تست تحمل گلوکز",
        "آزمایش کبد", "تست عملکرد کبدی", "ALT", "AST", "ALP", "بیلی روبین",
        "آزمایش کلیه", "BUN", "کراتینین", "GFR", "تست عملکرد کلیوی",
        "آزمایش تیروئید", "TSH", "T3", "T4", "آنتی TPO",
        "آزمایش هورمون", "FSH", "LH", "استرادیول", "پروژسترون", "تستوسترون",
        "آزمایش چربی خون", "کلسترول", "تری گلیسیرید", "HDL", "LDL",
        "آزمایش ویتامین", "ویتامین D", "ویتامین B12", "فریتین", "آهن سرم",
        "PSA", "تومور مارکر", "CA125", "CA15-3", "CEA", "AFP",
        "PCR", "آزمایش COVID", "آنتی بادی", "آنتی ژن", "الایزا", "ELISA",
        "کشت خون", "کشت ادرار", "آنتی بیوگرام", "حساسیت دارویی",
        "سیتولوژی", "پاپ اسمیر", "بیوپسی", "نمونه برداری",
        "پاتولوژی", "هیستوپاتولوژی", "سیتوپاتولوژی", "فروزن سکشن",
        "بانک خون", "گروه خون", "کراس مچ", "آنتی بادی خون",
        "اسپرم آنالیز", "اسپرموگرام", "آنالیز مایع منی",
        "ژنتیک", "کاریوتایپ", "تست ژنتیک", "مشاوره ژنتیک", "NGS",
        "تجهیزات آزمایشگاهی", "سانتریفیوژ", "میکروسکوپ", "اسپکتروفتومتر",
        "اتوآنالایزر", "هماتولوژی آنالایزر", "بیوشیمی آنالایزر",
        "سمپلر", "پیپت", "میکروپیپت", "لوله آزمایش", "لام", "لامل",
        "محیط کشت", "انکوباتور", "اتوکلاو", "استریلایزر", "هود لامینار",
        "کیت آزمایشگاهی", "کنترل کیفی", "کالیبراسیون", "QC", "QA",
    ],
    
    'laboratory_en': [
        "laboratory", "lab", "test", "analysis", "sample", "specimen",
        "clinical lab", "pathology lab", "genetics lab", "microbiology lab",
        "blood test", "CBC", "urine test", "stool test",
        "blood sugar", "FBS", "HbA1C", "liver function", "kidney function",
        "thyroid test", "TSH", "hormone test", "lipid profile",
        "PSA", "tumor marker", "PCR", "ELISA", "antibody", "antigen",
        "culture", "sensitivity", "biopsy", "cytology", "histopathology",
        "blood bank", "genetics", "karyotype", "NGS",
        "lab equipment", "centrifuge", "microscope", "analyzer",
        "autoclave", "incubator", "laminar hood", "pipette",
    ],

    # 🏥 تجهیزات پزشکی و بیمارستانی - جامع
    'equipment_fa': [
        "تجهیزات پزشکی", "تجهیزات بیمارستانی", "لوازم پزشکی",
        "دستگاه پزشکی", "ابزار جراحی", "تجهیزات اتاق عمل",
        "مانیتور بیمار", "ونتیلاتور", "دستگاه تنفس مصنوعی", "NICU",
        "الکتروشوک", "دفیبریلاتور", "AED", "پمپ سرنگ", "پمپ انفوزیون",
        "تخت بیمارستان", "تخت ICU", "برانکارد", "ترالی احیا", "ترالی دارو",
        "لارنگوسکوپ", "اتوسکوپ", "افتالموسکوپ", "درماتوسکوپ",
        "الکتروکوتر", "ساکشن جراحی", "چراغ اتاق عمل", "میز جراحی",
        "ESU", "الکتروسرجری", "رادیوفرکانسی", "لیزر جراحی",
        "سونوگرافی", "اکوکاردیوگرافی", "داپلر", "پروب سونو",
        "MRI", "CT اسکن", "رادیولوژی", "فلوروسکوپی", "آنژیوگرافی",
        "ماموگرافی", "دنسیتومتری", "DEXA", "اسکن استخوان",
        "آندوسکوپی", "کولونوسکوپی", "لاپاروسکوپی", "آرتروسکوپی",
        "دیالیز", "دستگاه دیالیز", "اسمز معکوس", "آب دیالیز",
        "اکسیژن ساز", "کنسانتراتور اکسیژن", "CPAP", "BiPAP", "ونتیلاتور",
        "قیچی جراحی", "پنس", "فورسپس", "کلمپ", "رترکتور", "هموستات",
        "ست جراحی", "ست بخیه", "نخ بخیه", "سوزن بخیه", "استپلر جراحی",
        "کاتتر", "سوند", "کانولا", "سرنگ", "سر سوزن", "آنژیوکت",
        "گاز استریل", "باند", "چسب جراحی", "درین", "لوله", "کانکتور",
        "استریل کردن", "اتوکلاو", "پلاسما استریل", "EO استریل",
        "تجهیزات فیزیوتراپی", "لیزر فیزیوتراپی", "شاک ویو", "TENS",
        "التراسوند تراپی", "دیاترمی", "مگنت تراپی", "تراکشن",
        "واردات تجهیزات پزشکی", "شرکت تجهیزات پزشکی", "نمایندگی تجهیزات",
        "تعمیر تجهیزات پزشکی", "کالیبراسیون تجهیزات", "PM تجهیزات",
        "مصرفی پزشکی", "یکبار مصرف", "دستکش", "ماسک", "سرنگ", "گان",
    ],
    
    'equipment_en': [
        "medical equipment", "hospital equipment", "medical device",
        "surgical instrument", "patient monitor", "ventilator", "defibrillator",
        "hospital bed", "ICU bed", "stretcher", "infusion pump", "syringe pump",
        "laryngoscope", "otoscope", "ophthalmoscope", "dermatoscope",
        "electrosurgery", "ESU", "surgical laser", "RF device",
        "ultrasound", "echocardiography", "doppler", "MRI", "CT scan",
        "X-ray", "mammography", "densitometry", "DEXA",
        "endoscopy", "colonoscopy", "laparoscopy", "arthroscopy",
        "dialysis machine", "oxygen concentrator", "CPAP", "BiPAP",
        "surgical scissors", "forceps", "clamp", "retractor", "suture",
        "catheter", "cannula", "syringe", "needle", "drain",
        "sterilization", "autoclave", "physiotherapy equipment",
        "medical supplies", "disposable", "import medical equipment",
    ],

    # 🧪 داروسازی صنعتی و تحقیقاتی
    'pharma_industry_fa': [
        "داروسازی صنعتی", "تولید دارو", "فرمولاسیون", "GMP",
        "کنترل کیفیت دارو", "QC دارویی", "QA دارویی", "تضمین کیفیت",
        "ثبت دارو", "IRC", "پروانه بهداشتی", "مجوز تولید",
        "مواد اولیه دارویی", "API", "اکسیپیان", "مواد جانبی",
        "بسته بندی دارویی", "بلیستر", "ویال", "آمپول", "قوطی دارو",
        "لیوفیلیزاسیون", "خشک کن انجمادی", "گرانولاسیون", "کوتینگ",
        "تحقیق و توسعه دارویی", "R&D", "کارآزمایی بالینی", "فاز بالینی",
        "فارماکوکینتیک", "فارماکودینامیک", "بیواکوئیوالنسی",
        "پایداری دارو", "تاریخ انقضا", "شرایط نگهداری", "زنجیره سرد",
        "توزیع دارو", "لجستیک دارویی", "سرد خانه دارویی",
        "بازرسی دارویی", "GMP inspection", "HACCP", "ISO 13485",
    ],

    # 👨‍⚕️ کادر درمان و پزشکان
    'medical_staff_fa': [
        "پزشک", "دکتر", "متخصص", "فوق تخصص", "جراح", "دستیار",
        "رزیدنت", "اینترن", "پرستار", "ماما", "تکنسین", "بهیار",
        "پزشک عمومی", "پزشک خانواده", "پزشک متخصص", "پزشک فوق تخصص",
        "نظام پزشکی", "سازمان نظام پزشکی", "پروانه طبابت",
        "بیمارستان", "درمانگاه", "کلینیک", "مطب", "اورژانس",
        "بیمارستان دولتی", "بیمارستان خصوصی", "بیمارستان آموزشی",
        "ICU", "CCU", "NICU", "اورژانس", "اتاق عمل", "بخش بستری",
        "نوبت دهی", "نوبت آنلاین", "نوبت دکتر", "ویزیت", "مشاوره پزشکی",
        "تعرفه پزشکی", "بیمه درمان", "بیمه تکمیلی", "تامین اجتماعی",
        "کارنامه سلامت", "پرونده الکترونیک", "HIS", "سامانه سلامت",
        "طرح تحول سلامت", "بسته خدمتی", "DRG", "بیمه سلامت",
        "کنگره پزشکی", "همایش پزشکی", "سمپوزیوم", "ژورنال کلاب",
        "CME", "بازآموزی", "آموزش مداوم", "امتیاز بازآموزی",
    ],

    # 🌡️ بیماری‌ها و گروه‌های بیمارخاص
    'patient_groups_fa': [
        "گروه بیماران", "انجمن بیماران", "حمایت بیماران",
        "بیماران دیابتی", "انجمن دیابت", "گروه دیابت",
        "بیماران سرطانی", "انجمن سرطان", "حمایت سرطان",
        "بیماران MS", "انجمن ام اس", "گروه ام اس",
        "بیماران قلبی", "گروه قلب", "انجمن قلب",
        "بیماران کلیوی", "گروه دیالیزی", "انجمن کلیه",
        "بیماران تالاسمی", "انجمن هموفیلی", "بیماران خونی",
        "بیماران پوستی", "انجمن پسوریازیس", "گروه اگزما",
        "بیماران روانی", "انجمن افسردگی", "گروه اضطراب",
        "بیماران آسم", "انجمن آلرژی", "گروه آسم",
        "ناباروری", "IVF", "گروه ناباروری", "انجمن ناباروری",
        "کودکان اوتیسم", "انجمن اوتیسم", "گروه ADHD",
        "بیماران پارکینسون", "انجمن آلزایمر", "گروه صرع",
        "بیماران ارتوپدی", "گروه کمردرد", "انجمن آرتروز",
        "رژیم درمانی", "تغذیه درمانی", "رژیم کتو", "رژیم دیابتی",
        "ورزش درمانی", "تناسب اندام", "فیتنس", "یوگا", "پیلاتس",
    ],

    # 🔬 تحقیقات پزشکی و علمی
    'medical_research_fa': [
        "تحقیقات پزشکی", "مقاله پزشکی", "ژورنال پزشکی",
        "پابمد", "PubMed", "اسکوپوس", "Scopus", "ISI",
        "متاآنالیز", "ری ویو سیستماتیک", "RCT", "کارآزمایی بالینی",
        "اتیک", "کمیته اخلاق", "رضایت آگاهانه", "IRB",
        "بیوتکنولوژی", "نانوتکنولوژی", "مهندسی بافت", "سلول بنیادی",
        "ژن درمانی", "CRISPR", "مهندسی ژنتیک", "بیوانفورماتیک",
        "هوش مصنوعی پزشکی", "تله مدیسین", "سلامت دیجیتال", "mHealth",
        "رباتیک جراحی", "داوینچی", "جراحی رباتیک", "جراحی از راه دور",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# �🎯 تابع انتخاب هوشمند کلمات کلیدی بر اساس اولویت
# ═══════════════════════════════════════════════════════════════════════════════

def get_priority_based_keywords(count=50, force_category=None):
    """
    تولید کلمات کلیدی بر اساس سیستم اولویت‌بندی چهارلایه
    
    Args:
        count: تعداد کلمات مورد نیاز
        force_category: اجبار انتخاب از یک دسته خاص ('crypto_trading', 'medical', 'immigration', 'general')
    
    Returns:
        list: لیست کلمات کلیدی با اولویت‌بندی
    """
    global search_cycle_counter
    
    keywords = []
    
    # اگر دسته خاصی اجبار شده
    if force_category:
        category = force_category
    else:
        # انتخاب دسته بر اساس وزن‌ها و چرخش
        search_cycle_counter['total'] += 1
        cycle = search_cycle_counter['total']
        
        # هر 20 سیکل: 7 کریپتو، 5 پزشکی، 4 مهاجرت، 4 عمومی
        cycle_position = cycle % 20
        
        if cycle_position < 7:  # 0-6: کریپتو (35%)
            category = 'crypto_trading'
        elif cycle_position < 12:  # 7-11: پزشکی/دارو (25%)
            category = 'medical'
        elif cycle_position < 16:  # 12-15: مهاجرت (20%)
            category = 'immigration'
        else:  # 16-19: عمومی (20%)
            category = 'general'
    
    search_cycle_counter['last_category'] = category
    
    # جمع‌آوری کلمات از دسته انتخاب شده
    if category == 'crypto_trading':
        search_cycle_counter['crypto_done'] += 1
        all_words = []
        for subcat in CRYPTO_TRADING_KEYWORDS.values():
            all_words.extend(subcat)
        keywords = random.sample(all_words, min(count, len(all_words)))
    
    elif category == 'medical':
        search_cycle_counter['medical_done'] += 1
        all_words = []
        for subcat in MEDICAL_KEYWORDS.values():
            all_words.extend(subcat)
        keywords = random.sample(all_words, min(count, len(all_words)))
        
    elif category == 'immigration':
        search_cycle_counter['immigration_done'] += 1
        all_words = []
        for subcat in IMMIGRATION_KEYWORDS.values():
            all_words.extend(subcat)
        keywords = random.sample(all_words, min(count, len(all_words)))
        
    else:  # general
        search_cycle_counter['general_done'] += 1
        all_words = []
        for subcat in GENERAL_KEYWORDS.values():
            all_words.extend(subcat)
        keywords = random.sample(all_words, min(count, len(all_words)))
    
    # ترکیبات هوشمند اضافه کن
    combos = generate_smart_combos(category, count // 4)
    keywords.extend(combos)
    
    # حذف تکراری و shuffle
    keywords = list(set(keywords))
    random.shuffle(keywords)
    
    return keywords[:count]


def generate_smart_combos(category, count=10):
    """
    🧠 تولید ترکیبات هوشمند کلمات کلیدی - نسخه پیشرفته
    این تابع ترکیبات موثر برای یافتن گروه‌های هدف تولید می‌کند
    """
    combos = []
    
    if category == 'crypto_trading':
        # 🥇 ترکیبات پیشرفته برای ترید و کریپتو
        prefixes = [
            "گروه", "کانال", "سیگنال", "آموزش", "رایگان", "VIP", "پریمیوم",
            "group", "signal", "free", "vip", "premium", "channel"
        ]
        mains = [
            "ترید", "تریدر", "کریپتو", "بیتکوین", "فارکس", "پراپ", "سیگنال",
            "اتریوم", "سولانا", "فیوچرز", "اسپات", "ایردراپ", "NFT",
            "trade", "trader", "crypto", "bitcoin", "forex", "prop", "signal",
            "ethereum", "solana", "futures", "spot", "airdrop", "btc", "eth",
            # 🆕 رمزارزهای بیشتر
            "تون", "TON", "ترون", "TRX", "دوج", "DOGE", "BNB", "شیبا",
            "آربیتروم", "ARB", "اوپتیمیزم", "OP", "پپه", "PEPE",
            "سوئی", "SUI", "آپتوس", "APT", "نات کوین", "NOT",
            "همستر", "hamster", "تپ سواپ", "بلوم", "داگز", "dogs",
        ]
        suffixes = [
            "فارسی", "ایرانی", "رایگان", "VIP", "حرفه‌ای", "پیشرفته",
            "persian", "iranian", "free", "vip", "pro", "premium"
        ]
        
        # تولید ترکیبات متنوع - بیشتر
        for _ in range(count // 2):
            # prefix + main
            combos.append(f"{random.choice(prefixes)} {random.choice(mains)}")
            # main + suffix
            combos.append(f"{random.choice(mains)} {random.choice(suffixes)}")
            # prefix + main + suffix (سه‌کلمه‌ای)
            combos.append(f"{random.choice(prefixes)} {random.choice(mains)} {random.choice(suffixes)}")
        
        # ترکیبات خاص و موثر - گسترش یافته
        special_combos = [
            "سیگنال رایگان کریپتو", "سیگنال VIP بیتکوین", "گروه ترید ایرانی",
            "کانال سیگنال فارسی", "آموزش ترید رایگان", "پراپ تریدینگ ایرانی",
            "free crypto signal", "bitcoin signal group", "forex persian",
            "crypto trading iranian", "prop firm persian", "trading community",
            "سیگنال فیوچرز", "اسپات سیگنال", "تحلیل تکنیکال", "پرایس اکشن",
            "ICT فارسی", "SMC ایرانی", "smart money concept",
            "ایردراپ رایگان", "ایردراپ جدید", "airdrop free", "new airdrop",
            "صرافی ایرانی", "نوبیتکس", "والکس", "بایننس فارسی",
            # 🆕 ترکیبات جدید رمزارزها
            "سیگنال تون", "سیگنال ترون", "سیگنال سولانا", "سیگنال دوج",
            "تحلیل تون", "تحلیل ترون", "تحلیل BNB", "تحلیل سوئی",
            "ایردراپ تون", "ایردراپ سولانا", "ton airdrop", "sol airdrop",
            "گروه نات کوین", "گروه همستر کامبت", "notcoin group", "hamster group",
            "بازی تلگرام", "telegram game", "تپ تو ارن", "tap to earn",
            "لایر دو", "layer 2", "L2 signal", "آربیتروم سیگنال",
            "میم کوین سیگنال", "meme coin signal", "پپه سیگنال", "شیبا سیگنال",
            "هوش مصنوعی کریپتو", "AI crypto", "رندر سیگنال", "فچ ای آی",
            # 🆕 صرافی‌ها و پلتفرم‌ها
            "bybit persian", "okx farsi", "kucoin iranian", "bitget signal",
            "سیگنال بای بیت", "گروه اوکی اکس", "کوکوین فارسی",
        ]
        combos.extend(random.sample(special_combos, min(len(special_combos), count)))
        
    elif category == 'immigration':
        # 🥈 ترکیبات پیشرفته برای مهاجرت
        prefixes = [
            "گروه", "انجمن", "ایرانیان", "فارسی", "مهاجرت", "اقامت", "ویزا",
            "iranian", "persian", "farsi", "immigration", "expat"
        ]
        cities = [
            "استانبول", "دبی", "تورنتو", "ونکوور", "برلین", "لندن", "سیدنی",
            "پاریس", "آمستردام", "وین", "ازمیر", "آنتالیا", "بورسا",
            "istanbul", "dubai", "toronto", "vancouver", "berlin", "london",
            "sydney", "paris", "amsterdam", "izmir", "antalya"
        ]
        countries = [
            "ترکیه", "امارات", "کانادا", "آلمان", "انگلیس", "استرالیا",
            "فرانسه", "هلند", "سوئد", "نروژ", "آمریکا", "اتریش",
            "turkey", "uae", "canada", "germany", "uk", "australia"
        ]
        topics = [
            "کار", "اجاره", "خانه", "ملک", "تحصیل", "ویزا", "اقامت",
            "work", "rent", "house", "property", "study", "visa", "residence"
        ]
        
        for _ in range(count // 4):
            # prefix + city
            combos.append(f"{random.choice(prefixes)} {random.choice(cities)}")
            # prefix + country
            combos.append(f"{random.choice(prefixes)} {random.choice(countries)}")
            # topic + city
            combos.append(f"{random.choice(topics)} {random.choice(cities)}")
            # city + topic
            combos.append(f"{random.choice(cities)} {random.choice(topics)}")
        
        # ترکیبات خاص و موثر
        special_combos = [
            "ایرانیان استانبول", "ایرانیان دبی", "ایرانیان تورنتو",
            "گروه ترکیه", "گروه امارات", "گروه کانادا", "گروه آلمان",
            "اجاره استانبول", "اجاره دبی", "کار ترکیه", "کار دبی",
            "مهاجرت کانادا", "مهاجرت آلمان", "اکسپرس انتری",
            "iranian istanbul", "iranian dubai", "persian toronto",
            "turkey group", "uae group", "canada immigration",
            "فارسی استانبول", "فارسی زبان دبی", "ایرانیان مقیم",
            "کیملیک", "ویزای ترکیه", "اقامت دبی", "ویزای کار",
            "باربری استانبول", "حواله ایران", "صرافی استانبول"
        ]
        combos.extend(random.sample(special_combos, min(len(special_combos), count // 2)))
    
    elif category == 'medical':
        # 🏥 ترکیبات پیشرفته برای پزشکی و دارو
        prefixes = [
            "گروه", "کانال", "انجمن", "فروش", "خرید", "تبادل", "تامین",
            "group", "channel", "buy", "sell", "supply", "exchange"
        ]
        mains = [
            "دارو", "داروخانه", "پزشکی", "تجهیزات پزشکی", "لوازم پزشکی",
            "آزمایشگاه", "دندانپزشکی", "ایمپلنت", "مکمل", "ویتامین",
            "pharmacy", "medical", "drug", "dental", "lab", "equipment",
            "supplement", "vitamin", "health", "clinic"
        ]
        suffixes = [
            "ایران", "تهران", "فارسی", "ایرانی", "نایاب", "کمیاب",
            "iran", "persian", "tehran", "online", "wholesale"
        ]
        cities = [
            "تهران", "مشهد", "اصفهان", "شیراز", "تبریز", "کرج",
            "اهواز", "قم", "کرمان", "رشت", "یزد"
        ]
        
        for _ in range(count // 3):
            combos.append(f"{random.choice(prefixes)} {random.choice(mains)}")
            combos.append(f"{random.choice(mains)} {random.choice(suffixes)}")
            combos.append(f"{random.choice(mains)} {random.choice(cities)}")
        
        special_combos = [
            "گروه دارویی", "کانال دارو", "انجمن داروسازان", "تبادل دارو",
            "داروی نایاب", "داروی کمیاب", "خرید دارو", "فروش دارو",
            "تجهیزات پزشکی تهران", "لوازم پزشکی مشهد", "آزمایشگاه ایران",
            "گروه پزشکان", "انجمن پزشکی", "گروه دندانپزشکی",
            "pharmacy iran", "medical equipment", "drug exchange",
            "dental implant", "iranian pharmacy", "persian medical",
            "گروه داروسازی", "کانال پزشکی", "انجمن پرستاری",
            "تجهیزات بیمارستانی", "لوازم آزمایشگاهی", "مکمل غذایی",
            "واکسن ایران", "بیمارستان تهران", "کلینیک تهران",
            "گروه بیماران دیابتی", "انجمن سرطان", "گروه MS",
            "داروخانه آنلاین", "نسخه الکترونیک", "بیمه درمان",
            "دارو تهران", "دارو مشهد", "دارو اصفهان",
        ]
        combos.extend(random.sample(special_combos, min(len(special_combos), count // 2)))
        
    else:
        # 🥉 ترکیبات عمومی - گسترش‌یافته
        prefixes = ["گروه", "چت", "انجمن", "group", "chat", "community", "کانال", "channel"]
        mains = ["ایران", "ایرانی", "فارسی", "تهران", "iran", "persian", "farsi",
                 "مشهد", "اصفهان", "شیراز", "تبریز", "کرج"]
        topics = ["دوستی", "گپ", "آشنایی", "friendship", "chat", "social",
                  "خرید", "فروش", "کار", "استخدام", "آموزش", "ورزش",
                  "buy", "sell", "job", "work", "education", "sport"]
        
        for _ in range(count // 2):
            combos.append(f"{random.choice(prefixes)} {random.choice(mains)}")
            combos.append(f"{random.choice(mains)} {random.choice(topics)}")
        
        special_combos = [
            "گروه تهران", "گروه مشهد", "گروه اصفهان", "گروه شیراز",
            "خرید فروش تهران", "استخدام تهران", "اجاره تهران",
            "بورس تهران", "سهام ایران", "فریلنسر ایرانی",
            "آموزش آنلاین", "کسب درآمد", "دورکاری",
            "دوستیابی", "ازدواج ایران", "همسریابی",
            "group iran", "chat persian", "iranian community",
            "فوتبال ایران", "پرسپولیس", "استقلال", "بدنسازی",
            "آشپزی ایرانی", "رستوران تهران", "کافه تهران",
        ]
        combos.extend(random.sample(special_combos, min(len(special_combos), count)))
    
    # حذف تکراری و shuffle
    combos = list(set(combos))
    random.shuffle(combos)
    
    return combos[:count]


# 🆕 تابع جدید: تولید کلمات کلیدی ترکیبی هوشمند برای جستجوی موثرتر
def generate_effective_search_queries(category='crypto_trading', count=30):
    """
    🚀 تولید کوئری‌های جستجوی فوق‌مؤثر برای یافتن گروه‌های هدف
    پشتیبانی از 4 دسته: crypto_trading, immigration, medical, general
    """
    queries = []
    
    if category == 'crypto_trading':
        # کوئری‌های موثر برای ترید
        base_queries = [
            "سیگنال", "ترید", "کریپتو", "بیتکوین", "signal", "trade", "crypto",
        ]
        modifiers_fa = ["رایگان", "VIP", "فارسی", "ایرانی", "گروه", "کانال"]
        modifiers_en = ["free", "vip", "persian", "iranian", "group", "channel"]
        
        for base in base_queries[:4]:
            for mod in modifiers_fa[:3]:
                queries.append(f"{mod} {base}")
                queries.append(f"{base} {mod}")
        
        for base in base_queries[4:]:
            for mod in modifiers_en[:3]:
                queries.append(f"{mod} {base}")
        
        # 🆕 کوئری‌های مستقیم موثر - گسترش یافته
        direct_queries = [
            "ترید ایران", "کریپتو فارسی", "بیتکوین ایرانی", "فارکس فارسی",
            "سیگنال بیتکوین", "سیگنال اتریوم", "ایردراپ", "airdrop",
            "پراپ ایرانی", "prop persian", "فیوچرز سیگنال", "futures signal",
            # 🆕 رمزارزهای خاص
            "سیگنال تون", "سیگنال ترون", "سیگنال سولانا", "سیگنال دوج",
            "TON signal", "TRON signal", "SOL signal", "BNB signal",
            "گروه ترید فارسی", "کانال سیگنال", "آموزش ترید",
            "تحلیل تکنیکال", "تحلیل فاندامنتال", "technical analysis",
            "گروه فیوچرز", "گروه اسپات", "ترید روزانه",
            "سیگنال فیوچرز", "لوریج ترید", "اسکالپ", "scalp",
            "صرافی ایرانی", "نوبیتکس", "والکس", "رمزینکس",
            "بایننس فارسی", "binance persian", "bybit farsi",
            "پامپ", "pump", "ایردراپ تون", "ایردراپ سولانا",
            "نات کوین", "notcoin", "همستر کامبت", "hamster kombat",
            "تپ سواپ", "داگز", "dogs", "بلوم", "blum", "ممفای",
            "پراپ FTMO", "پراپ فاندنکست", "prop challenge",
            "فارکس ایرانی", "forex iranian", "متاتریدر", "metatrader",
        ]
        queries.extend(direct_queries)
        
    elif category == 'immigration':
        # کوئری‌های موثر برای مهاجرت
        cities = ["استانبول", "دبی", "تورنتو", "istanbul", "dubai", "toronto",
                  "آنکارا", "ونکوور", "مونترال", "لندن", "برلین", "سیدنی"]
        prefixes = ["ایرانیان", "گروه", "iranian", "persian", "فارسی"]
        
        for city in cities:
            for prefix in prefixes[:3]:
                queries.append(f"{prefix} {city}")
        
        direct_queries = [
            "مهاجرت ترکیه", "مهاجرت کانادا", "ایرانیان ترکیه", "ایرانیان امارات",
            "کار استانبول", "اجاره دبی", "ویزا ترکیه", "اقامت دبی",
            # 🆕 بیشتر
            "مهاجرت آلمان", "مهاجرت انگلیس", "مهاجرت استرالیا",
            "اکسپرس انتری", "express entry", "ویزای کار", "work permit",
            "پناهندگی اروپا", "ایرانیان آلمان", "ایرانیان لندن",
            "ایرانیان پاریس", "ایرانیان آمستردام", "ایرانیان وین",
            "کیملیک ترکیه", "اجاره استانبول", "خانه دبی",
            "زندگی ترکیه", "زندگی کانادا", "living istanbul",
            "iranian community", "persian community",
            "گروه مهاجرت", "مشاوره مهاجرت", "وکیل مهاجرت",
            "بلوکارت آلمان", "ویزای تحصیلی", "بورسیه",
            "ایرانیان نیویورک", "ایرانیان لس آنجلس", "tehrangeles",
            "ایرانیان اربیل", "تجارت عراق", "زیارت کربلا",
            "ایرانیان ارمنستان", "ایرانیان گرجستان", "ایرانیان قبرس",
        ]
        queries.extend(direct_queries)
    
    elif category == 'medical':
        # 🆕 کوئری‌های موثر برای پزشکی و دارو
        base_medical = ["دارو", "داروخانه", "پزشکی", "تجهیزات پزشکی", "مکمل"]
        modifiers = ["گروه", "کانال", "فروش", "خرید", "تامین", "آنلاین"]
        cities = ["تهران", "مشهد", "اصفهان", "شیراز", "تبریز"]
        
        for base in base_medical:
            for mod in modifiers[:3]:
                queries.append(f"{mod} {base}")
        
        for base in base_medical[:3]:
            for city in cities[:3]:
                queries.append(f"{base} {city}")
        
        direct_queries = [
            "گروه دارویی", "کانال دارو", "انجمن داروسازان", "تبادل دارو",
            "داروی نایاب", "داروی کمیاب", "خرید دارو", "فروش دارو",
            "تجهیزات پزشکی تهران", "آزمایشگاه ایران", "لوازم پزشکی",
            "گروه پزشکان", "انجمن پزشکی", "دندانپزشکی ایران",
            "pharmacy iran", "medical equipment", "drug exchange",
            "dental implant", "iranian pharmacy", "persian medical",
            "داروخانه آنلاین", "مکمل غذایی", "ویتامین ایران",
            "بیمارستان تهران", "کلینیک تهران", "دکتر ایرانی",
            "لنز طبی", "سمعک", "عینک طبی", "ارتوپد", "فیزیوتراپی",
            "بیمه درمان", "نسخه الکترونیک", "پرستاری ایران",
            "گروه بیماران", "انجمن سرطان", "دیابت ایران",
            "لوازم آزمایشگاهی", "تجهیزات بیمارستانی", "واکسن",
        ]
        queries.extend(direct_queries)
    
    elif category == 'general':
        # 🆕 کوئری‌های عمومی
        direct_queries = [
            "گروه ایرانی", "چت فارسی", "گپ ایرانی", "انجمن ایرانیان",
            "group iran", "persian chat", "farsi group",
            "خرید فروش", "بازار ایران", "استخدام ایران", "کار تهران",
            "موبایل فروش", "لپ تاپ فروش", "خودرو فروش",
            "ملک تهران", "اجاره تهران", "فروش آپارتمان",
            "دوستیابی", "آشنایی", "ازدواج", "همسریابی",
            "آموزش آنلاین", "فریلنسر ایرانی", "دورکاری",
            "درآمد اینترنتی", "کسب درآمد", "بازاریابی",
            "گروه تهران", "گروه مشهد", "گروه اصفهان", "گروه شیراز",
            "گروه تبریز", "گروه کرج", "گروه اهواز",
            "ورزش ایران", "فوتبال ایران", "بدنسازی",
            "آشپزی ایرانی", "غذای ایرانی", "رستوران",
        ]
        queries.extend(direct_queries)
    
    # حذف تکراری
    queries = list(set(queries))
    random.shuffle(queries)
    
    return queries[:count]


def get_all_crypto_keywords():
    """دریافت تمام کلمات کلیدی ترید و کریپتو"""
    all_words = []
    for subcat in CRYPTO_TRADING_KEYWORDS.values():
        all_words.extend(subcat)
    return list(set(all_words))


def get_all_immigration_keywords():
    """دریافت تمام کلمات کلیدی مهاجرت"""
    all_words = []
    for subcat in IMMIGRATION_KEYWORDS.values():
        all_words.extend(subcat)
    return list(set(all_words))


def get_all_medical_keywords():
    """دریافت تمام کلمات کلیدی پزشکی و دارو"""
    all_words = []
    for subcat in MEDICAL_KEYWORDS.values():
        all_words.extend(subcat)
    return list(set(all_words))


def get_search_statistics():
    """دریافت آمار جستجوها بر اساس دسته"""
    total = search_cycle_counter['total'] or 1
    return {
        'total_searches': total,
        'crypto_searches': search_cycle_counter['crypto_done'],
        'crypto_percent': f"{(search_cycle_counter['crypto_done'] / total) * 100:.1f}%",
        'medical_searches': search_cycle_counter['medical_done'],
        'medical_percent': f"{(search_cycle_counter['medical_done'] / total) * 100:.1f}%",
        'immigration_searches': search_cycle_counter['immigration_done'],
        'immigration_percent': f"{(search_cycle_counter['immigration_done'] / total) * 100:.1f}%",
        'general_searches': search_cycle_counter['general_done'],
        'general_percent': f"{(search_cycle_counter['general_done'] / total) * 100:.1f}%",
        'last_category': search_cycle_counter['last_category'],
    }

# ═══════════════════════════════════════════════════════════════════════════════

# SEARCH_KEYWORDS حذف شد - از سیستم اولویت‌بندی استفاده می‌شود
SEARCH_KEYWORDS = []

# حافظه یادگیری (کلماتی که نتیجه خوب دادند)
learned_keywords = {
    'successful': {},  # {keyword: success_count}
    'failed': {},      # {keyword: fail_count}
    'extracted': set() # کلمات استخراج شده از گروه‌ها
}

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠 Stub ساده برای سازگاری (بدون بار پردازشی)
# ═══════════════════════════════════════════════════════════════════════════════

class DummyRLAgent:
    """Stub ساده - بدون یادگیری تقویتی (سبک برای سرور)"""
    def __init__(self):
        self.q_table = {}
        self.exploration_rate = 0.2
        self.state_visits = {}
        self.action_history = []
        self.reward_history = []
    
    def get_state(self): return "default"
    def get_top_keywords(self, n=20): return []
    def record_reward(self, state, keyword, reward): pass
    def get_statistics(self): return {'total_actions': 0, 'total_reward': 0, 'avg_reward': 0}

# نمونه global (سبک)
rl_agent = DummyRLAgent()

# ═══════════════════════════════════════════════════════════════════════════════
# 🌐🌐🌐 سیستم کشف شبکه‌ای گروه‌ها (NETWORK-BASED DISCOVERY) 🌐🌐🌐
# ═══════════════════════════════════════════════════════════════════════════════

class NetworkGroupDiscovery:
    """
    کشف گروه‌های جدید از طریق شبکه اجتماعی
    - از گروه‌های فعلی، گروه‌های مرتبط پیدا می‌کنه
    - از اعضای مشترک استفاده می‌کنه
    """
    
    def __init__(self):
        self.group_members = {}  # {group_id: set(user_ids)}
        self.user_groups = {}  # {user_id: set(group_ids)}
        self.group_similarity = {}  # {(g1, g2): similarity_score}
        self.discovered_groups = set()  # گروه‌های کشف شده
        self.high_value_keywords = set()  # کلمات با ارزش بالا
        
    def record_group_member(self, group_id, user_id):
        """ثبت عضویت کاربر در گروه"""
        if group_id not in self.group_members:
            self.group_members[group_id] = set()
        self.group_members[group_id].add(user_id)
        
        if user_id not in self.user_groups:
            self.user_groups[user_id] = set()
        self.user_groups[user_id].add(group_id)
    
    def calculate_similarity(self, group1_id, group2_id):
        """محاسبه شباهت دو گروه بر اساس اعضای مشترک (Jaccard Similarity)"""
        members1 = self.group_members.get(group1_id, set())
        members2 = self.group_members.get(group2_id, set())
        
        if not members1 or not members2:
            return 0.0
        
        intersection = len(members1 & members2)
        union = len(members1 | members2)
        
        return intersection / union if union > 0 else 0.0
    
    def find_similar_groups(self, group_id, min_similarity=0.1, top_n=10):
        """پیدا کردن گروه‌های مشابه"""
        similarities = []
        
        for other_id in self.group_members.keys():
            if other_id == group_id:
                continue
            
            sim = self.calculate_similarity(group_id, other_id)
            if sim >= min_similarity:
                similarities.append((other_id, sim))
        
        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:top_n]
    
    def suggest_keywords_from_titles(self, group_titles):
        """استخراج کلمات کلیدی از عناوین گروه‌های موفق"""
        import re
        
        keyword_freq = {}
        
        for title in group_titles:
            if not title:
                continue
            
            # تمیز کردن عنوان
            clean = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', title.lower())
            words = clean.split()
            
            # کلمات با طول مناسب
            for word in words:
                if len(word) >= 2:
                    keyword_freq[word] = keyword_freq.get(word, 0) + 1
            
            # ترکیبات دوتایی
            for i in range(len(words) - 1):
                combo = f"{words[i]} {words[i+1]}"
                keyword_freq[combo] = keyword_freq.get(combo, 0) + 1
        
        # مرتب‌سازی بر اساس فراوانی
        sorted_keywords = sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True)
        
        # فیلتر کلمات با فراوانی حداقل 2
        high_value = [kw for kw, freq in sorted_keywords if freq >= 2]
        self.high_value_keywords.update(high_value[:50])
        
        return high_value[:30]
    
    def get_exploration_keywords(self, successful_groups_info):
        """تولید کلمات کلیدی برای اکتشاف بر اساس گروه‌های موفق"""
        # استخراج از عناوین
        titles = [info.get('title', '') for info in successful_groups_info]
        title_keywords = self.suggest_keywords_from_titles(titles)
        
        # ترکیب با کلمات با ارزش بالا
        all_keywords = list(self.high_value_keywords)
        all_keywords.extend(title_keywords)
        
        return list(set(all_keywords))[:100]

# نمونه‌های global سیستم‌های هوشمند
network_discovery = NetworkGroupDiscovery()
# member_inviter خواهد شد smart_inviter بعد از تعریف کلاس SmartMemberInviter

# ═══════════════════════════════════════════════════════════════════════════════
# 📈📈📈 سیستم پیش‌بینی کیفیت گروه (GROUP QUALITY PREDICTOR) 📈📈📈
# ═══════════════════════════════════════════════════════════════════════════════

class GroupQualityPredictor:
    """
    پیش‌بینی کیفیت گروه قبل از join کردن
    از ویژگی‌های گروه برای امتیازدهی استفاده می‌کنه
    """
    
    def __init__(self):
        self.feature_weights = {
            'member_count': 0.25,
            'has_positive_keywords': 0.30,
            'has_location_keyword': 0.20,
            'has_iranian_keyword': 0.15,
            'title_length': 0.05,
            'is_verified': 0.05
        }
        self.quality_history = []  # [(features, actual_quality)]
        
    def extract_features(self, chat):
        """استخراج ویژگی‌ها از گروه"""
        features = {}
        
        title = getattr(chat, 'title', '').lower() if hasattr(chat, 'title') else ''
        members = getattr(chat, 'participants_count', 0)
        
        # تعداد اعضا (نرمال‌سازی)
        if members >= 1000:
            features['member_count'] = 1.0
        elif members >= 100:
            features['member_count'] = 0.7
        elif members >= 10:
            features['member_count'] = 0.4
        else:
            features['member_count'] = 0.1
        
        # کلمات مثبت
        positive_words = ['iran', 'persian', 'farsi', 'ایران', 'فارسی', 'ایرانی', 
                         'مهاجر', 'expat', 'community', 'group', 'گروه', 'جامعه']
        features['has_positive_keywords'] = 1.0 if any(w in title for w in positive_words) else 0.0
        
        # کلمات مکان
        location_words = ['istanbul', 'dubai', 'toronto', 'berlin', 'london', 'استانبول', 
                         'دبی', 'تورنتو', 'برلین', 'لندن', 'ترکیه', 'امارات', 'کانادا']
        features['has_location_keyword'] = 1.0 if any(w in title for w in location_words) else 0.0
        
        # کلمات ایرانی
        iranian_words = ['iran', 'persian', 'ایران', 'پرشین', 'فارسی', 'ایرانی']
        features['has_iranian_keyword'] = 1.0 if any(w in title for w in iranian_words) else 0.0
        
        # طول عنوان
        features['title_length'] = min(1.0, len(title) / 50)
        
        # تایید شده
        features['is_verified'] = 1.0 if getattr(chat, 'verified', False) else 0.0
        
        return features
    
    def predict_quality(self, chat):
        """پیش‌بینی امتیاز کیفیت (0-1)"""
        features = self.extract_features(chat)
        
        score = 0.0
        for feature_name, value in features.items():
            weight = self.feature_weights.get(feature_name, 0)
            score += weight * value
        
        return score
    
    def should_join(self, chat, threshold=0.3):
        """آیا باید به این گروه join کنیم؟"""
        quality = self.predict_quality(chat)
        return quality >= threshold
    
    def record_feedback(self, chat, was_successful):
        """ثبت بازخورد برای یادگیری"""
        features = self.extract_features(chat)
        actual_quality = 1.0 if was_successful else 0.0
        self.quality_history.append((features, actual_quality))
        
        # به‌روزرسانی وزن‌ها (هر 100 نمونه)
        if len(self.quality_history) % 100 == 0:
            self._update_weights()
    
    def _update_weights(self):
        """به‌روزرسانی وزن‌ها با gradient descent ساده"""
        if len(self.quality_history) < 50:
            return
        
        recent = self.quality_history[-100:]
        
        for feature_name in self.feature_weights:
            gradient = 0.0
            for features, actual in recent:
                predicted = sum(self.feature_weights[f] * features.get(f, 0) 
                               for f in self.feature_weights)
                error = actual - predicted
                gradient += error * features.get(feature_name, 0)
            
            # به‌روزرسانی با learning rate کوچک
            self.feature_weights[feature_name] += 0.01 * gradient / len(recent)
            
            # نرمال‌سازی وزن‌ها
            self.feature_weights[feature_name] = max(0, min(1, self.feature_weights[feature_name]))

# نمونه global
quality_predictor = GroupQualityPredictor()

# ═══════════════════════════════════════════════════════════════════════════════
# 🎯🎯🎯 سیستم انتخاب هوشمند کلمات (SMART KEYWORD SELECTOR) 🎯🎯🎯
# ═══════════════════════════════════════════════════════════════════════════════

class SmartKeywordSelector:
    """
    انتخاب هوشمند کلمات کلیدی با ترکیب چند روش:
    1. RL Agent
    2. Network Discovery
    3. Historical Performance
    4. Time-based Optimization
    """
    
    def __init__(self):
        self.time_performance = {}  # {hour: {keyword: success_rate}}
        self.category_weights = {}  # {category: weight}
        self.shopbination_rules = []  # قوانین ترکیب
        
    def get_optimal_keywords(self, count=50):
        """دریافت بهترین کلمات کلیدی"""
        current_hour = datetime.now().hour
        
        keywords = set()
        
        # 1️⃣ از RL Agent
        rl_keywords = rl_agent.get_top_keywords(count // 3)
        keywords.update(rl_keywords)
        
        # 2️⃣ از Network Discovery
        if hasattr(network_discovery, 'high_value_keywords'):
            network_keywords = list(network_discovery.high_value_keywords)[:count // 3]
            keywords.update(network_keywords)
        
        # 3️⃣ از Historical Performance
        successful = learned_keywords.get('successful', {})
        sorted_successful = sorted(successful.items(), key=lambda x: x[1], reverse=True)
        historical_keywords = [kw for kw, _ in sorted_successful[:count // 3]]
        keywords.update(historical_keywords)
        
        # 4️⃣ Time-based: کلمات مناسب زمان فعلی
        time_keywords = self._get_time_based_keywords(current_hour)
        keywords.update(time_keywords[:10])
        
        # 5️⃣ Random exploration برای تنوع
        base_keywords = self._get_base_keywords()
        import random
        random_keywords = random.sample(base_keywords, min(10, len(base_keywords)))
        keywords.update(random_keywords)
        
        result = list(keywords)
        random.shuffle(result)
        
        return result[:count]
    
    def _get_time_based_keywords(self, hour):
        """کلمات مناسب بر اساس زمان روز"""
        if 6 <= hour < 12:  # صبح
            return ["صبح", "کار", "استخدام", "job", "work", "hiring", "morning"]
        elif 12 <= hour < 18:  # ظهر/عصر
            return ["غذا", "رستوران", "ناهار", "کافه", "food", "restaurant", "cafe"]
        elif 18 <= hour < 23:  # شب
            return ["سرگرمی", "دوستی", "چت", "گپ", "fun", "chat", "friends"]
        else:  # نیمه شب
            return ["خواب", "آرامش", "موسیقی", "music", "relax", "night"]
    
    def _get_base_keywords(self):
        """کلمات پایه همیشه مفید"""
        return [
            # ترکیه
            "ایرانی استانبول", "ایرانیان ترکیه", "فارسی استانبول", "تهرانی استانبول",
            "iranian istanbul", "persian turkey", "farsi istanbul",
            "کار استانبول", "اجاره استانبول", "خانه استانبول", "مهاجرت ترکیه",
            "فاتح", "تکسیم", "اسنیورت", "کادیکوی", "باشاک شهیر",
            # دبی/امارات
            "ایرانی دبی", "ایرانیان امارات", "فارسی دبی", "persian dubai",
            "iranian uae", "کار دبی", "اجاره دبی", "مهاجرت امارات",
            "جمیرا", "مارینا", "بردبی", "دیره",
            # کانادا
            "ایرانی تورنتو", "ایرانیان کانادا", "مهاجرت کانادا",
            "iranian toronto", "persian canada", "pr canada",
            # آلمان
            "ایرانی برلین", "ایرانیان آلمان", "iranian germany", "persian berlin",
            # عمومی
            "مهاجرت", "ویزا", "اقامت", "کار", "تحصیل", "دانشگاه",
            "immigration", "visa", "job", "study", "university"
        ]
    
    def record_result(self, keyword, success):
        """ثبت نتیجه جستجو برای یادگیری"""
        hour = datetime.now().hour
        
        if hour not in self.time_performance:
            self.time_performance[hour] = {}
        
        if keyword not in self.time_performance[hour]:
            self.time_performance[hour][keyword] = {'success': 0, 'total': 0}
        
        self.time_performance[hour][keyword]['total'] += 1
        if success:
            self.time_performance[hour][keyword]['success'] += 1
        
        # به‌روزرسانی RL Agent
        state = rl_agent.get_state()
        reward = 1.0 if success else -0.5
        rl_agent.record_reward(state, keyword, reward)

# نمونه global
smart_selector = SmartKeywordSelector()

# ═══════════════════════════════════════════════════════════════════════════════
# 👥👥👥 سیستم هوشمند دعوت اعضا (SMART MEMBER INVITER) 👥👥👥
# ═══════════════════════════════════════════════════════════════════════════════

class SmartMemberInviter:
    """
    سیستم فوق‌پیشرفته برای اضافه کردن اعضا به گروه هدف
    
    ویژگی‌ها:
    1. امتیازدهی هوشمند به کاربران (کدام کاربر احتمال موفقیت بیشتری دارد)
    2. یادگیری از موفقیت‌ها و شکست‌ها
    3. مدیریت هوشمند FloodWait
    4. چند استراتژی دعوت (Add مستقیم، PM، لینک)
    5. زمان‌بندی هوشمند (بهترین ساعات)
    6. تشخیص کاربران فعال و مرتبط
    """
    
    def __init__(self):
        # امتیازات کاربران
        self.user_scores = {}  # {user_id: {'score': float, 'features': dict}}
        
        # آمار عملکرد
        self.invite_stats = {
            'direct_add_success': 0,
            'direct_add_fail': 0,
            'pm_success': 0,
            'pm_fail': 0,
            'total_attempts': 0,
            'flood_waits': 0,
            'best_hours': {},  # {hour: success_rate}
            'source_group_performance': {}  # {group_id: success_rate}
        }
        
        # وزن ویژگی‌ها برای امتیازدهی - همه کاربران یکسان هستند
        self.feature_weights = {
            'has_username': 0.25,  # داشتن username مهم است
            'is_recent_active': 0.30,  # فعالیت اخیر
            'has_profile_photo': 0.15,  # داشتن عکس پروفایل
            'is_not_bot': 0.15,  # ربات نباشد
            'source_group_quality': 0.15,  # کیفیت گروه منبع
        }
        
        # تاریخچه FloodWait برای پیش‌بینی
        self.flood_history = []  # [(timestamp, wait_seconds)]
        
        # حالت فعلی سیستم
        self.current_mode = 'normal'  # normal, cautious, aggressive
        self.consecutive_success = 0
        self.consecutive_fail = 0
        
        # صف اولویت‌دار کاربران
        self.priority_queue = []  # [(score, user_id)]
        
    def calculate_user_score(self, user_id, user_info, source_group_id=None):
        """محاسبه امتیاز کاربر برای اولویت‌بندی دعوت"""
        score = 0.5  # پایه
        features = {}
        
        # 1. داشتن username
        has_username = bool(user_info.get('username'))
        features['has_username'] = has_username
        score += self.feature_weights['has_username'] if has_username else 0
        
        # 2. فعالیت اخیر (بر اساس timestamp)
        scraped_time = user_info.get('timestamp', 0)
        hours_ago = (time.time() - scraped_time) / 3600
        is_recent = hours_ago < 24
        features['is_recent_active'] = is_recent
        score += self.feature_weights['is_recent_active'] if is_recent else 0
        
        # 3. داشتن عکس پروفایل (اگر موجود باشد)
        has_photo = user_info.get('has_photo', True)  # پیش‌فرض True
        features['has_profile_photo'] = has_photo
        score += self.feature_weights['has_profile_photo'] if has_photo else 0
        
        # 4. ربات نبودن
        is_bot = user_info.get('is_bot', False)
        features['is_not_bot'] = not is_bot
        score += self.feature_weights['is_not_bot'] if not is_bot else 0
        
        # 5. کیفیت گروه منبع
        if source_group_id:
            group_perf = self.invite_stats['source_group_performance'].get(source_group_id, {})
            group_success_rate = group_perf.get('success', 0) / max(group_perf.get('total', 1), 1)
            features['source_group_quality'] = group_success_rate
            score += self.feature_weights['source_group_quality'] * group_success_rate
        
        # ذخیره
        self.user_scores[user_id] = {'score': score, 'features': features}
        
        return score
    
    def get_prioritized_users(self, available_users, user_db, limit=50):
        """دریافت لیست کاربران با اولویت‌بندی هوشمند"""
        scored_users = []
        
        for user_id in available_users:
            user_info = user_db.get(user_id, {})
            source_group = user_info.get('scraped_from')
            
            score = self.calculate_user_score(user_id, user_info, source_group)
            scored_users.append((score, user_id))
        
        # مرتب‌سازی نزولی بر اساس امتیاز
        scored_users.sort(key=lambda x: x[0], reverse=True)
        
        # اضافه کردن تنوع: 80% بهترین‌ها، 20% تصادفی
        top_count = int(limit * 0.8)
        random_count = limit - top_count
        
        result = [uid for _, uid in scored_users[:top_count]]
        
        remaining = [uid for _, uid in scored_users[top_count:]]
        if remaining and random_count > 0:
            import random
            random_picks = random.sample(remaining, min(random_count, len(remaining)))
            result.extend(random_picks)
        
        return result
    
    def record_invite_result(self, user_id, method, success, source_group_id=None):
        """ثبت نتیجه دعوت برای یادگیری"""
        hour = datetime.now().hour
        
        self.invite_stats['total_attempts'] += 1
        
        if method == 'direct':
            if success:
                self.invite_stats['direct_add_success'] += 1
                self.consecutive_success += 1
                self.consecutive_fail = 0
            else:
                self.invite_stats['direct_add_fail'] += 1
                self.consecutive_fail += 1
                self.consecutive_success = 0
        elif method == 'pm':
            if success:
                self.invite_stats['pm_success'] += 1
            else:
                self.invite_stats['pm_fail'] += 1
        
        # به‌روزرسانی عملکرد ساعتی
        if hour not in self.invite_stats['best_hours']:
            self.invite_stats['best_hours'][hour] = {'success': 0, 'total': 0}
        self.invite_stats['best_hours'][hour]['total'] += 1
        if success:
            self.invite_stats['best_hours'][hour]['success'] += 1
        
        # به‌روزرسانی عملکرد گروه منبع
        if source_group_id:
            if source_group_id not in self.invite_stats['source_group_performance']:
                self.invite_stats['source_group_performance'][source_group_id] = {'success': 0, 'total': 0}
            self.invite_stats['source_group_performance'][source_group_id]['total'] += 1
            if success:
                self.invite_stats['source_group_performance'][source_group_id]['success'] += 1
        
        # به‌روزرسانی وزن‌ها بر اساس موفقیت
        self._update_weights(user_id, success)
        
        # تنظیم حالت سیستم
        self._adjust_mode()
    
    def _update_weights(self, user_id, success):
        """به‌روزرسانی وزن ویژگی‌ها"""
        if user_id not in self.user_scores:
            return
        
        features = self.user_scores[user_id].get('features', {})
        learning_rate = 0.01
        
        for feature_name, feature_value in features.items():
            if feature_name in self.feature_weights:
                # اگر موفق بود و ویژگی True بود، وزن را افزایش بده
                if success and feature_value:
                    self.feature_weights[feature_name] += learning_rate
                # اگر شکست خورد و ویژگی True بود، کمی کاهش بده
                elif not success and feature_value:
                    self.feature_weights[feature_name] -= learning_rate * 0.5
                
                # محدود کردن به [0.05, 0.5]
                self.feature_weights[feature_name] = max(0.05, min(0.5, self.feature_weights[feature_name]))
    
    def _adjust_mode(self):
        """تنظیم حالت سیستم بر اساس عملکرد"""
        if self.consecutive_success >= 10:
            self.current_mode = 'aggressive'  # سریع‌تر
        elif self.consecutive_fail >= 5:
            self.current_mode = 'cautious'  # محتاط‌تر
        else:
            self.current_mode = 'normal'
    
    def get_optimal_delay(self):
        """محاسبه تاخیر بهینه بر اساس حالت و تاریخچه"""
        base_delays = {
            'aggressive': (30, 60),
            'normal': (60, 120),
            'cautious': (120, 180)
        }
        
        min_delay, max_delay = base_delays.get(self.current_mode, (60, 120))
        
        # اگر FloodWait اخیر داشتیم، تاخیر بیشتر
        recent_floods = [f for f in self.flood_history if time.time() - f[0] < 3600]
        if recent_floods:
            multiplier = 1 + (len(recent_floods) * 0.3)
            min_delay = int(min_delay * multiplier)
            max_delay = int(max_delay * multiplier)
        
        return random.randint(min_delay, max_delay)
    
    def record_flood_wait(self, wait_seconds):
        """ثبت FloodWait"""
        self.flood_history.append((time.time(), wait_seconds))
        self.invite_stats['flood_waits'] += 1
        self.current_mode = 'cautious'
        
        # پاکسازی تاریخچه قدیمی (بیشتر از 6 ساعت)
        self.flood_history = [f for f in self.flood_history if time.time() - f[0] < 21600]
    
    def is_good_time_to_invite(self):
        """آیا الان زمان مناسبی برای دعوت است؟"""
        hour = datetime.now().hour
        
        # ساعات خوب: 7-12 صبح و 15-22 عصر
        good_hours = list(range(7, 13)) + list(range(15, 23))
        
        # اگر آمار داریم، از آن استفاده کن
        if self.invite_stats['best_hours']:
            hour_stats = self.invite_stats['best_hours'].get(hour, {})
            if hour_stats.get('total', 0) >= 10:
                success_rate = hour_stats['success'] / hour_stats['total']
                return success_rate > 0.2  # حداقل 20% موفقیت
        
        return hour in good_hours
    
    def get_statistics(self):
        """آمار کامل سیستم"""
        total_direct = self.invite_stats['direct_add_success'] + self.invite_stats['direct_add_fail']
        total_pm = self.invite_stats['pm_success'] + self.invite_stats['pm_fail']
        
        direct_rate = self.invite_stats['direct_add_success'] / max(total_direct, 1)
        pm_rate = self.invite_stats['pm_success'] / max(total_pm, 1)
        
        # بهترین ساعات
        best_hours = []
        for hour, stats in self.invite_stats['best_hours'].items():
            if stats['total'] >= 5:
                rate = stats['success'] / stats['total']
                best_hours.append((hour, rate))
        best_hours.sort(key=lambda x: x[1], reverse=True)
        
        return {
            'total_attempts': self.invite_stats['total_attempts'],
            'direct_add_success': self.invite_stats['direct_add_success'],
            'direct_add_rate': f"{direct_rate:.1%}",
            'pm_success': self.invite_stats['pm_success'],
            'pm_rate': f"{pm_rate:.1%}",
            'flood_waits': self.invite_stats['flood_waits'],
            'current_mode': self.current_mode,
            'best_hours': best_hours[:3],
            'feature_weights': self.feature_weights
        }

# نمونه global
smart_inviter = SmartMemberInviter()

# ═══════════════════════════════════════════════════════════════════════════════
# 📨📨📨 سیستم پیام‌های شخصی‌سازی شده (PERSONALIZED PM SYSTEM) 📨📨📨
# ═══════════════════════════════════════════════════════════════════════════════

class PersonalizedPMSystem:
    """
    سیستم ارسال پیام‌های شخصی‌سازی شده برای دعوت
    """
    
    def __init__(self):
        # قالب‌های مختلف پیام
        self.message_templates = [
            # قالب 1: رسمی
            """سلام {name} عزیز 👋

ما یک گروه تخصصی در زمینه داروسازی و پزشکی داریم که ممکنه براتون مفید باشه:

🔗 {link}

📌 مزایای گروه:
• اطلاعات دارویی به‌روز
• مشاوره رایگان
• شبکه‌سازی با متخصصین

منتظرتون هستیم! 🙏""",
            
            # قالب 2: دوستانه
            """سلام {name} 😊

یه گروه خوب در حوزه دارو و سلامت پیدا کردم، فکر کردم شاید بدردتون بخوره:

{link}

بیاید آشنا بشیم! ✌️""",
            
            # قالب 3: کوتاه
            """سلام {name}
گروه داروسازی 👇
{link}
خوشحال میشیم ببینیمتون 🌹""",
            
            # قالب 4: حرفه‌ای
            """با سلام و احترام {name} عزیز

دعوت به عضویت در گروه تخصصی PharmaWeb:
{link}

• جامعه متخصصین دارویی
• آخرین اخبار صنعت
• فرصت‌های شغلی

با احترام 🙏"""
        ]
        
        # آمار هر قالب
        self.template_stats = {i: {'sent': 0, 'success': 0} for i in range(len(self.message_templates))}
        
        # آخرین قالب استفاده شده (برای تنوع)
        self.last_template_index = -1
    
    def get_personalized_message(self, user_info):
        """تولید پیام شخصی‌سازی شده"""
        # انتخاب هوشمند قالب
        template_index = self._select_best_template()
        template = self.message_templates[template_index]
        
        # شخصی‌سازی
        name = user_info.get('first_name', 'دوست')
        if not name or name == 'Unknown':
            name = 'دوست'
        
        message = template.format(
            name=name,
            link=GROUP_LINK
        )
        
        self.last_template_index = template_index
        self.template_stats[template_index]['sent'] += 1
        
        return message, template_index
    
    def _select_best_template(self):
        """انتخاب بهترین قالب بر اساس عملکرد"""
        # محاسبه نرخ موفقیت هر قالب
        rates = []
        for i, stats in self.template_stats.items():
            if stats['sent'] >= 10:
                rate = stats['success'] / stats['sent']
            else:
                rate = 0.5  # پیش‌فرض برای قالب‌های کم‌استفاده
            rates.append((i, rate))
        
        # 70% بهترین، 30% تصادفی (برای تنوع و اکتشاف)
        if random.random() < 0.7 and rates:
            rates.sort(key=lambda x: x[1], reverse=True)
            return rates[0][0]
        else:
            # انتخاب تصادفی با اولویت به قالب‌های کم‌استفاده
            least_used = min(self.template_stats.items(), key=lambda x: x[1]['sent'])[0]
            return least_used if random.random() < 0.5 else random.randint(0, len(self.message_templates) - 1)
    
    def record_result(self, template_index, success):
        """ثبت نتیجه برای یادگیری"""
        if success:
            self.template_stats[template_index]['success'] += 1

# نمونه global
pm_system = PersonalizedPMSystem()

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀🚀🚀 سیستم‌های تبلیغاتی پیشرفته (ADVANCED MARKETING SYSTEMS) 🚀🚀🚀
# ═══════════════════════════════════════════════════════════════════════════════

class ViralMarketingEngine:
    """
    سیستم بازاریابی ویروسی - ایجاد رشد نمایی از طریق اعضا
    
    استراتژی‌ها:
    1. پیام‌های قابل اشتراک‌گذاری
    2. محتوای ارزشمند رایگان
    3. چالش‌ها و مسابقات
    4. سیستم ارجاع (Referral)
    """
    
    def __init__(self):
        # محتوای ویروسی
        self.viral_contents = {
            'valuable_info': [
                "🔥 اطلاعیه مهم دارویی:\n\n📋 لیست داروهای کمیاب موجود:\n{drug_list}\n\n⚡ این لیست هر روز به‌روز می‌شود\n\n📢 اگر کسی رو می‌شناسید که نیاز داره، این پیام رو بهش بفرستید 🙏\n\n🔗 گروه تخصصی: {link}",
                "💊 راهنمای رایگان:\n\nچطور داروهای کمیاب رو پیدا کنیم؟\n\n1️⃣ عضویت در گروه‌های تخصصی\n2️⃣ ارتباط با تأمین‌کنندگان معتبر\n3️⃣ استفاده از شبکه‌های مطمئن\n\n📌 گروه ما: {link}\n\n🔄 این پیام رو برای دوستانتون بفرستید",
                "⚠️ هشدار مهم:\n\nمراقب داروهای تقلبی باشید!\n\n✅ نشانه‌های داروی اصل:\n• بسته‌بندی سالم\n• هولوگرام معتبر\n• تاریخ انقضا واضح\n\n🏥 برای مشاوره رایگان:\n{link}",
            ],
            'engagement_triggers': [
                "❓ سوال روز:\n\nکدوم دارو رو سخت‌تر پیدا می‌کنید؟\n\n1️⃣ داروهای اعصاب\n2️⃣ داروهای قلبی\n3️⃣ داروهای سرطان\n4️⃣ داروهای دیابت\n\n💬 نظرتون رو بگید!\n\n🔗 {link}",
                "📊 نظرسنجی:\n\nاز 1 تا 10 چقدر به دسترسی دارویی راضی هستید؟\n\n💬 عددتون رو کامنت کنید\n\n🔗 {link}",
            ],
            'shareable_tips': [
                "💡 نکته کاربردی #{tip_num}:\n\n{tip_content}\n\n📤 این نکته رو با دوستانتون به اشتراک بذارید\n\n🔗 نکات بیشتر: {link}",
            ]
        }
        
        # نکات کاربردی برای اشتراک
        self.tips = [
            "داروها رو در دمای مناسب نگهداری کنید - معمولاً زیر 25 درجه",
            "قبل از خرید دارو، تاریخ انقضا رو حتماً چک کنید",
            "داروهای مشابه رو با هم مصرف نکنید بدون مشورت پزشک",
            "لیست داروهاتون رو همیشه به‌روز نگه دارید",
            "برای داروهای خاص، از منابع معتبر خرید کنید",
            "عوارض جانبی داروها رو حتماً مطالعه کنید",
            "داروهای کنترل‌شده نیاز به نسخه معتبر دارند",
            "ذخیره‌سازی صحیح دارو = اثربخشی بیشتر",
        ]
        
        # آمار ویروسی
        self.viral_stats = {
            'shares_estimated': 0,
            'engagement_rate': 0.0,
            'best_content_type': None,
            'content_performance': {}
        }
        
        self.tip_counter = 0
    
    def generate_viral_content(self, content_type='valuable_info'):
        """تولید محتوای ویروسی"""
        if content_type == 'shareable_tips':
            self.tip_counter = (self.tip_counter + 1) % len(self.tips)
            template = random.choice(self.viral_contents['shareable_tips'])
            content = template.format(
                tip_num=self.tip_counter + 1,
                tip_content=self.tips[self.tip_counter],
                link=GROUP_LINK
            )
        else:
            templates = self.viral_contents.get(content_type, self.viral_contents['valuable_info'])
            template = random.choice(templates)
            
            # انتخاب لیست دارو کوتاه برای محتوای ویروسی
            short_drug_list = "• آدرال\n• اوزمپیک\n• کونسرتا\n• ریتالین\n• مودافینیل"
            
            content = template.format(
                drug_list=short_drug_list,
                link=GROUP_LINK
            )
        
        return content
    
    def get_referral_message(self, referrer_name=""):
        """پیام ارجاع برای دعوت دوستان"""
        messages = [
            f"👋 سلام!\n\n{referrer_name} شما رو به گروه تخصصی دارو دعوت کرده:\n\n🔗 {GROUP_LINK}\n\n✨ مزایا:\n• دسترسی به داروهای کمیاب\n• مشاوره رایگان\n• قیمت مناسب",
            f"💊 دعوت‌نامه از طرف {referrer_name}\n\nعضو گروه داروسازی شوید:\n{GROUP_LINK}\n\n🎁 عضویت رایگان",
        ]
        return random.choice(messages)


class ContentMarketingEngine:
    """
    موتور بازاریابی محتوایی - تولید محتوای جذاب خودکار
    
    انواع محتوا:
    1. اطلاعات آموزشی
    2. اخبار دارویی
    3. نکات سلامتی
    4. معرفی محصولات
    """
    
    def __init__(self):
        # الگوهای محتوا
        self.content_templates = {
            'educational': [
                "📚 آموزش:\n\n{title}\n\n{content}\n\n💡 برای اطلاعات بیشتر:\n{link}",
                "🎓 دانستنی‌های دارویی:\n\n{title}\n\n{content}\n\n📌 منبع: {link}",
            ],
            'news': [
                "📰 خبر جدید:\n\n{title}\n\n{content}\n\n🔗 جزئیات: {link}",
                "⚡ به‌روزرسانی:\n\n{title}\n\n{content}\n\n📢 {link}",
            ],
            'tips': [
                "💊 نکته سلامتی:\n\n{content}\n\n🏥 {link}",
                "✨ توصیه روز:\n\n{content}\n\n📌 {link}",
            ],
            'product': [
                "🆕 موجود شد:\n\n{title}\n\n✅ اصل و تضمینی\n📦 ارسال سریع\n\n🛒 سفارش: {link}",
            ]
        }
        
        # بانک محتوا
        self.content_bank = {
            'educational_titles': [
                "نحوه صحیح مصرف داروهای اعصاب",
                "تداخلات دارویی مهم",
                "نگهداری صحیح انسولین",
                "تفاوت داروی اصل و تقلبی",
                "داروهای OTC و نسخه‌ای",
            ],
            'educational_content': [
                "داروهای اعصاب باید دقیقاً طبق دستور پزشک مصرف شوند. قطع ناگهانی می‌تواند خطرناک باشد.",
                "قبل از مصرف هر داروی جدید، تداخل با داروهای فعلی را بررسی کنید.",
                "انسولین باید در یخچال نگهداری شود اما قبل از تزریق به دمای اتاق برسد.",
                "داروی اصل دارای هولوگرام، بارکد معتبر و بسته‌بندی سالم است.",
                "داروهای OTC بدون نسخه قابل خرید هستند اما داروهای نسخه‌ای نیاز به تجویز پزشک دارند.",
            ],
            'news_titles': [
                "داروی جدید وارد بازار شد",
                "تغییرات قیمت داروها",
                "موجودی جدید داروهای کمیاب",
                "راه‌اندازی سرویس جدید",
            ],
            'tips_content': [
                "داروها را دور از دسترس کودکان نگهداری کنید.",
                "هرگز داروی دیگران را مصرف نکنید.",
                "تاریخ انقضای داروها را مرتب چک کنید.",
                "داروهای مایع بعد از باز کردن عمر کمتری دارند.",
                "برای یادآوری مصرف دارو از آلارم استفاده کنید.",
            ]
        }
        
        # آمار محتوا
        self.content_stats = {
            'generated': 0,
            'by_type': {},
            'engagement': {}
        }
    
    def generate_content(self, content_type='educational'):
        """تولید محتوای خودکار"""
        templates = self.content_templates.get(content_type, self.content_templates['tips'])
        template = random.choice(templates)
        
        if content_type == 'educational':
            idx = random.randint(0, len(self.content_bank['educational_titles']) - 1)
            content = template.format(
                title=self.content_bank['educational_titles'][idx],
                content=self.content_bank['educational_content'][idx],
                link=GROUP_LINK
            )
        elif content_type == 'news':
            title = random.choice(self.content_bank['news_titles'])
            content = template.format(
                title=title,
                content="برای اطلاعات کامل به گروه مراجعه کنید.",
                link=GROUP_LINK
            )
        elif content_type == 'tips':
            tip = random.choice(self.content_bank['tips_content'])
            content = template.format(
                content=tip,
                link=GROUP_LINK
            )
        else:
            content = template.format(
                title="محصول جدید",
                link=GROUP_LINK
            )
        
        self.content_stats['generated'] += 1
        self.content_stats['by_type'][content_type] = self.content_stats['by_type'].get(content_type, 0) + 1
        
        return content


class TimeOptimizationEngine:
    """
    موتور بهینه‌سازی زمان - تحلیل بهترین زمان‌ها برای فعالیت
    
    تحلیل‌ها:
    1. بهترین ساعات روز
    2. بهترین روزهای هفته
    3. الگوهای فعالیت مخاطب
    """
    
    def __init__(self):
        # آمار ساعتی
        self.hourly_stats = {h: {'actions': 0, 'success': 0, 'engagement': 0} for h in range(24)}
        
        # آمار روزانه (0=شنبه، 6=جمعه)
        self.daily_stats = {d: {'actions': 0, 'success': 0} for d in range(7)}
        
        # ساعات طلایی پیش‌فرض (ایران)
        self.golden_hours = {
            'morning': list(range(8, 12)),    # 8-12 صبح
            'afternoon': list(range(14, 17)),  # 14-17 بعدازظهر
            'evening': list(range(20, 24)),    # 20-24 شب
        }
        
        # ضرایب زمانی
        self.time_multipliers = {}
        self._calculate_initial_multipliers()
    
    def _calculate_initial_multipliers(self):
        """محاسبه ضرایب اولیه"""
        for hour in range(24):
            if hour in self.golden_hours['evening']:
                self.time_multipliers[hour] = 1.5  # بالاترین
            elif hour in self.golden_hours['morning']:
                self.time_multipliers[hour] = 1.3
            elif hour in self.golden_hours['afternoon']:
                self.time_multipliers[hour] = 1.2
            elif 0 <= hour < 6:
                self.time_multipliers[hour] = 0.3  # شب
            else:
                self.time_multipliers[hour] = 1.0
    
    def record_action(self, success=True, engagement=0):
        """ثبت فعالیت"""
        now = datetime.now()
        hour = now.hour
        day = now.weekday()
        
        self.hourly_stats[hour]['actions'] += 1
        if success:
            self.hourly_stats[hour]['success'] += 1
        self.hourly_stats[hour]['engagement'] += engagement
        
        self.daily_stats[day]['actions'] += 1
        if success:
            self.daily_stats[day]['success'] += 1
        
        # به‌روزرسانی ضرایب
        self._update_multipliers()
    
    def _update_multipliers(self):
        """به‌روزرسانی ضرایب بر اساس داده‌های واقعی"""
        for hour, stats in self.hourly_stats.items():
            if stats['actions'] >= 20:  # حداقل داده کافی
                success_rate = stats['success'] / stats['actions']
                # ترکیب با ضریب پیش‌فرض
                self.time_multipliers[hour] = (self.time_multipliers[hour] + success_rate * 2) / 2
    
    def get_current_multiplier(self):
        """دریافت ضریب فعلی"""
        hour = datetime.now().hour
        return self.time_multipliers.get(hour, 1.0)
    
    def is_optimal_time(self):
        """آیا الان زمان بهینه است؟"""
        return self.get_current_multiplier() >= 1.2
    
    def get_next_optimal_time(self):
        """زمان بهینه بعدی"""
        current_hour = datetime.now().hour
        
        for offset in range(1, 25):
            check_hour = (current_hour + offset) % 24
            if self.time_multipliers.get(check_hour, 0) >= 1.2:
                return check_hour
        
        return current_hour + 1
    
    def get_recommended_delay(self, base_delay):
        """تاخیر پیشنهادی بر اساس زمان"""
        multiplier = self.get_current_multiplier()
        
        if multiplier >= 1.3:
            return int(base_delay * 0.7)  # سریع‌تر در ساعات خوب
        elif multiplier <= 0.5:
            return int(base_delay * 1.5)  # کندتر در ساعات بد
        else:
            return base_delay


class ABTestingFramework:
    """
    فریم‌ورک تست A/B - بهینه‌سازی پیام‌ها و استراتژی‌ها
    
    قابلیت‌ها:
    1. تست همزمان چند نسخه
    2. تحلیل آماری
    3. انتخاب خودکار برنده
    """
    
    def __init__(self):
        # آزمایش‌های فعال
        self.active_tests = {}
        
        # نتایج
        self.test_results = {}
        
        # حداقل نمونه برای نتیجه‌گیری
        self.min_sample_size = 30
    
    def create_test(self, test_name, variants):
        """ایجاد آزمایش جدید"""
        self.active_tests[test_name] = {
            'variants': {v: {'shown': 0, 'success': 0} for v in variants},
            'created': time.time(),
            'status': 'running'
        }
    
    def get_variant(self, test_name):
        """انتخاب variant برای نمایش"""
        if test_name not in self.active_tests:
            return None
        
        test = self.active_tests[test_name]
        variants = test['variants']
        
        # اگر یکی خیلی بهتره، بیشتر اون رو نشون بده
        total_shown = sum(v['shown'] for v in variants.values())
        
        if total_shown >= self.min_sample_size * len(variants):
            # محاسبه نرخ موفقیت
            rates = {}
            for name, data in variants.items():
                if data['shown'] > 0:
                    rates[name] = data['success'] / data['shown']
                else:
                    rates[name] = 0
            
            # 80% بهترین، 20% تصادفی (برای اکتشاف)
            if random.random() < 0.8:
                best = max(rates.items(), key=lambda x: x[1])[0]
                return best
        
        # انتخاب تصادفی با اولویت کم‌نمایش
        least_shown = min(variants.items(), key=lambda x: x[1]['shown'])[0]
        return least_shown if random.random() < 0.4 else random.choice(list(variants.keys()))
    
    def record_result(self, test_name, variant, success):
        """ثبت نتیجه"""
        if test_name in self.active_tests:
            if variant in self.active_tests[test_name]['variants']:
                self.active_tests[test_name]['variants'][variant]['shown'] += 1
                if success:
                    self.active_tests[test_name]['variants'][variant]['success'] += 1
    
    def get_winner(self, test_name):
        """دریافت برنده آزمایش"""
        if test_name not in self.active_tests:
            return None
        
        variants = self.active_tests[test_name]['variants']
        
        # بررسی کافی بودن نمونه
        for data in variants.values():
            if data['shown'] < self.min_sample_size:
                return None  # هنوز زوده
        
        # محاسبه نرخ‌ها
        rates = {}
        for name, data in variants.items():
            rates[name] = data['success'] / data['shown'] if data['shown'] > 0 else 0
        
        winner = max(rates.items(), key=lambda x: x[1])
        return {'variant': winner[0], 'rate': winner[1], 'rates': rates}


class EngagementBooster:
    """
    افزایش‌دهنده تعامل - استراتژی‌های افزایش engagement
    
    تکنیک‌ها:
    1. سوالات تعاملی
    2. نظرسنجی
    3. محتوای تعاملی
    4. پاسخ به کامنت‌ها
    """
    
    def __init__(self):
        # الگوهای تعاملی
        self.engagement_templates = {
            'questions': [
                "❓ نظرتون چیه؟\n\n{topic}\n\n💬 کامنت بذارید",
                "🤔 شما چی فکر می‌کنید؟\n\n{topic}",
                "📊 کدوم رو ترجیح میدید؟\n\n{options}",
            ],
            'polls': [
                "🗳 رای بدید:\n\n{question}\n\n{options}",
            ],
            'challenges': [
                "🎯 چالش روز:\n\n{challenge}\n\n🏆 برنده جایزه می‌گیره!",
            ]
        }
        
        # موضوعات تعاملی
        self.topics = [
            "بهترین راه برای پیدا کردن داروهای کمیاب چیه؟",
            "تجربه خریدتون از داروخانه‌های آنلاین چطور بوده؟",
            "کدوم داروها رو سخت‌تر پیدا می‌کنید؟",
            "نظرتون درباره قیمت داروها چیه؟",
        ]
        
        # گزینه‌های نظرسنجی
        self.poll_options = [
            "1️⃣ عالی\n2️⃣ خوب\n3️⃣ متوسط\n4️⃣ ضعیف",
            "👍 موافقم\n👎 مخالفم\n🤷 نظری ندارم",
        ]
        
        # آمار engagement
        self.engagement_stats = {
            'posts': 0,
            'responses': 0,
            'avg_engagement': 0.0
        }
    
    def generate_engagement_content(self, content_type='questions'):
        """تولید محتوای تعاملی"""
        templates = self.engagement_templates.get(content_type, self.engagement_templates['questions'])
        template = random.choice(templates)
        
        content = template.format(
            topic=random.choice(self.topics),
            question=random.choice(self.topics),
            options=random.choice(self.poll_options),
            challenge="یک نکته دارویی مفید به اشتراک بذارید"
        )
        
        self.engagement_stats['posts'] += 1
        return content
    
    def record_engagement(self, responses=0):
        """ثبت engagement"""
        self.engagement_stats['responses'] += responses
        if self.engagement_stats['posts'] > 0:
            self.engagement_stats['avg_engagement'] = (
                self.engagement_stats['responses'] / self.engagement_stats['posts']
            )


class FunnelAnalytics:
    """
    تحلیل قیف تبدیل - ردیابی مسیر کاربر
    
    مراحل قیف:
    1. Awareness (دیده شدن)
    2. Interest (علاقه)
    3. Consideration (بررسی)
    4. Conversion (تبدیل)
    5. Retention (حفظ)
    """
    
    def __init__(self):
        # مراحل قیف
        self.funnel_stages = {
            'awareness': {'count': 0, 'users': set()},      # پیام دیده شد
            'interest': {'count': 0, 'users': set()},       # کلیک روی لینک
            'consideration': {'count': 0, 'users': set()},  # ورود به گروه
            'conversion': {'count': 0, 'users': set()},     # عضو شد
            'retention': {'count': 0, 'users': set()},      # فعال ماند
        }
        
        # نرخ تبدیل بین مراحل
        self.conversion_rates = {}
        
        # تاریخچه
        self.history = []
    
    def record_stage(self, user_id, stage):
        """ثبت ورود کاربر به مرحله"""
        if stage in self.funnel_stages:
            self.funnel_stages[stage]['count'] += 1
            self.funnel_stages[stage]['users'].add(user_id)
            
            self.history.append({
                'user_id': user_id,
                'stage': stage,
                'timestamp': time.time()
            })
            
            self._calculate_conversion_rates()
    
    def _calculate_conversion_rates(self):
        """محاسبه نرخ تبدیل"""
        stages = ['awareness', 'interest', 'consideration', 'conversion', 'retention']
        
        for i in range(len(stages) - 1):
            current = stages[i]
            next_stage = stages[i + 1]
            
            current_count = self.funnel_stages[current]['count']
            next_count = self.funnel_stages[next_stage]['count']
            
            if current_count > 0:
                rate = next_count / current_count
                self.conversion_rates[f"{current}_to_{next_stage}"] = rate
    
    def get_funnel_report(self):
        """گزارش قیف"""
        report = {
            'stages': {},
            'conversion_rates': self.conversion_rates,
            'total_conversions': self.funnel_stages['conversion']['count'],
            'retention_rate': 0.0
        }
        
        for stage, data in self.funnel_stages.items():
            report['stages'][stage] = data['count']
        
        if self.funnel_stages['conversion']['count'] > 0:
            report['retention_rate'] = (
                self.funnel_stages['retention']['count'] / 
                self.funnel_stages['conversion']['count']
            )
        
        return report
    
    def get_bottleneck(self):
        """شناسایی گلوگاه"""
        if not self.conversion_rates:
            return None
        
        # کمترین نرخ تبدیل = گلوگاه
        bottleneck = min(self.conversion_rates.items(), key=lambda x: x[1])
        return {
            'stage': bottleneck[0],
            'rate': bottleneck[1],
            'recommendation': self._get_recommendation(bottleneck[0])
        }
    
    def _get_recommendation(self, bottleneck_stage):
        """پیشنهاد برای رفع گلوگاه"""
        recommendations = {
            'awareness_to_interest': "پیام‌های جذاب‌تر با CTA واضح‌تر استفاده کنید",
            'interest_to_consideration': "محتوای ارزشمندتر و اعتمادسازی بیشتر",
            'consideration_to_conversion': "فرآیند عضویت را ساده‌تر کنید",
            'conversion_to_retention': "محتوای منظم و ارزشمند ارائه دهید",
        }
        return recommendations.get(bottleneck_stage, "استراتژی را بازبینی کنید")


class CrossPromotionSystem:
    """
    سیستم تبلیغات متقابل - همکاری با گروه‌های مرتبط
    
    استراتژی‌ها:
    1. شناسایی گروه‌های مرتبط
    2. پیشنهاد همکاری
    3. تبادل تبلیغات
    """
    
    def __init__(self):
        # گروه‌های همکار
        self.partner_groups = {}  # {group_id: {'name': str, 'type': str, 'active': bool}}
        
        # تاریخچه همکاری
        self.collaboration_history = []
        
        # پیام‌های همکاری
        self.collab_messages = [
            "سلام ادمین عزیز 👋\n\nما یک گروه تخصصی در زمینه دارو و پزشکی هستیم.\n\nآیا امکان همکاری تبلیغاتی وجود داره؟\n\n🔗 {our_link}\n\nممنون 🙏",
            "با سلام\n\nگروه {our_name} پیشنهاد تبادل تبلیغات رو داره.\n\nشما معرفی ما، ما معرفی شما.\n\nنظرتون چیه؟",
        ]
    
    def identify_potential_partners(self, group_info):
        """شناسایی گروه‌های مناسب برای همکاری"""
        # کلمات کلیدی مرتبط
        related_keywords = ['دارو', 'پزشکی', 'سلامت', 'بهداشت', 'pharmacy', 'medical', 'health']
        
        title = (group_info.get('title') or '').lower()
        about = (group_info.get('about') or '').lower()
        
        text = f"{title} {about}"
        
        # بررسی ارتباط
        relevance_score = sum(1 for kw in related_keywords if kw in text)
        
        return relevance_score >= 2  # حداقل 2 کلمه کلیدی
    
    def generate_collab_request(self):
        """تولید پیام درخواست همکاری"""
        template = random.choice(self.collab_messages)
        return template.format(
            our_link=GROUP_LINK,
            our_name="PharmaWebGp"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# نمونه‌های Global از سیستم‌های تبلیغاتی
# ═══════════════════════════════════════════════════════════════════════════════

viral_engine = ViralMarketingEngine()
content_engine = ContentMarketingEngine()
time_optimizer = TimeOptimizationEngine()
ab_testing = ABTestingFramework()
engagement_booster = EngagementBooster()
funnel_analytics = FunnelAnalytics()
cross_promotion = CrossPromotionSystem()


# ═══════════════════════════════════════════════════════════════════════════════
# 🛡️🛡️🛡️ سیستم محافظت پیشرفته (ADVANCED PROTECTION SYSTEM) 🛡️🛡️🛡️
# ═══════════════════════════════════════════════════════════════════════════════

class AntiSpamProtection:
    """
    سیستم محافظت در برابر بن شدن و تشخیص spam
    
    ویژگی‌ها:
    1. تشخیص الگوهای خطرناک
    2. مدیریت هوشمند rate
    3. استراحت خودکار
    4. پیش‌بینی FloodWait
    """
    
    def __init__(self):
        # تاریخچه خطاها
        self.error_history = []  # [(timestamp, error_type)]
        
        # شمارنده‌های هشدار
        self.warning_counts = {
            'flood_wait': 0,
            'peer_flood': 0,
            'user_banned': 0,
            'privacy_restricted': 0,
            'chat_write_forbidden': 0
        }
        
        # وضعیت سلامت
        self.health_score = 100  # 0-100
        
        # حالت فعلی - شروع با حالت cautious برای امنیت ⚠️
        self.current_mode = 'cautious'  # 🔒 تغییر از normal به cautious
        
        # زمان آخرین استراحت
        self.last_rest_time = 0
        
        # تنظیمات - افزایش حساسیت برای کاهش ریسک بن ⚠️
        self.rest_threshold = 60  # 🔒 افزایش از 30 به 60 - استراحت زودتر
        self.emergency_threshold = 30  # 🔒 افزایش از 10 به 30 - توقف زودتر
        
        # آمار روزانه
        self.daily_stats = {
            'actions': 0,
            'errors': 0,
            'flood_waits_total': 0
        }
        
        # زمان ریست روزانه
        self.daily_reset_time = time.time()
    
    def record_error(self, error_type, severity=1):
        """ثبت خطا و به‌روزرسانی سلامت"""
        self.error_history.append((time.time(), error_type))
        
        # به‌روزرسانی شمارنده
        if error_type in self.warning_counts:
            self.warning_counts[error_type] += 1
        
        # کاهش سلامت
        health_penalty = {
            'flood_wait': 15,
            'peer_flood': 25,
            'user_banned': 30,
            'privacy_restricted': 5,
            'chat_write_forbidden': 10
        }
        
        self.health_score -= health_penalty.get(error_type, 5) * severity
        self.health_score = max(0, self.health_score)
        
        self.daily_stats['errors'] += 1
        
        # به‌روزرسانی حالت
        self._update_mode()
        
        # پاکسازی تاریخچه قدیمی (بیش از 1 ساعت)
        cutoff = time.time() - 3600
        self.error_history = [e for e in self.error_history if e[0] > cutoff]
    
    def record_success(self):
        """ثبت موفقیت و بهبود سلامت"""
        self.health_score = min(100, self.health_score + 0.5)
        self.daily_stats['actions'] += 1
        self._update_mode()
    
    def record_flood_wait(self, seconds):
        """ثبت FloodWait"""
        self.record_error('flood_wait', severity=1 + (seconds / 60))
        self.daily_stats['flood_waits_total'] += seconds
    
    def _update_mode(self):
        """به‌روزرسانی حالت بر اساس سلامت"""
        if self.health_score < self.emergency_threshold:
            self.current_mode = 'emergency'
        elif self.health_score < self.rest_threshold:
            self.current_mode = 'rest'
        elif self.health_score < 50:
            self.current_mode = 'cautious'
        else:
            self.current_mode = 'normal'
    
    def should_rest(self):
        """آیا باید استراحت کنیم؟"""
        if self.current_mode in ['rest', 'emergency']:
            return True
        
        # اگر 1 ساعت از آخرین استراحت گذشته و سلامت زیر 50 است
        if time.time() - self.last_rest_time > 3600 and self.health_score < 50:
            return True
        
        return False
    
    def get_rest_duration(self):
        """مدت زمان استراحت پیشنهادی"""
        if self.current_mode == 'emergency':
            return 1800  # 30 دقیقه
        elif self.current_mode == 'rest':
            return 600  # 10 دقیقه
        else:
            return 120  # 2 دقیقه
    
    def mark_rested(self):
        """ثبت استراحت"""
        self.last_rest_time = time.time()
        self.health_score = min(100, self.health_score + 20)
        self._update_mode()
    
    def get_rate_multiplier(self):
        """ضریب سرعت بر اساس سلامت"""
        if self.current_mode == 'emergency':
            return 0.1  # خیلی کند
        elif self.current_mode == 'rest':
            return 0.3
        elif self.current_mode == 'cautious':
            return 0.6
        else:
            return 1.0 + (self.health_score - 70) / 100  # بیش از 70 = سریع‌تر
    
    def predict_flood_risk(self):
        """پیش‌بینی ریسک FloodWait"""
        recent_floods = sum(1 for t, e in self.error_history 
                          if e == 'flood_wait' and time.time() - t < 600)
        
        if recent_floods >= 3:
            return 'high'
        elif recent_floods >= 1:
            return 'medium'
        else:
            return 'low'
    
    def get_status_report(self):
        """گزارش وضعیت"""
        return {
            'health_score': self.health_score,
            'mode': self.current_mode,
            'flood_risk': self.predict_flood_risk(),
            'daily_actions': self.daily_stats['actions'],
            'daily_errors': self.daily_stats['errors'],
            'error_rate': self.daily_stats['errors'] / max(self.daily_stats['actions'], 1),
            'warning_counts': self.warning_counts
        }
    
    def reset_daily_stats(self):
        """ریست آمار روزانه"""
        if time.time() - self.daily_reset_time > 86400:  # 24 ساعت
            self.daily_stats = {'actions': 0, 'errors': 0, 'flood_waits_total': 0}
            self.daily_reset_time = time.time()
            # بونوس سلامت برای روز جدید
            self.health_score = min(100, self.health_score + 10)


class SmartContentRotation:
    """
    چرخش هوشمند محتوا برای جلوگیری از تکرار و افزایش اثربخشی
    
    ویژگی‌ها:
    1. تنوع محتوا
    2. تشخیص محتوای موثر
    3. جلوگیری از تکرار
    4. شخصی‌سازی بر اساس گروه
    """
    
    def __init__(self):
        # تاریخچه محتوا برای هر گروه
        self.group_content_history = {}  # {group_id: [(content_hash, timestamp)]}
        
        # امتیاز محتوا
        self.content_scores = {}  # {content_hash: {'shown': int, 'engagement': float}}
        
        # کش محتوای تولید شده
        self.content_cache = []
        
        # تنظیمات
        self.min_repeat_interval = 3600  # حداقل 1 ساعت بین تکرار
        self.max_history_per_group = 50
    
    def get_content_hash(self, content):
        """هش محتوا برای شناسایی"""
        import hashlib
        return hashlib.md5(content[:100].encode()).hexdigest()[:8]
    
    def should_send_content(self, group_id, content):
        """آیا این محتوا باید به این گروه ارسال شود؟"""
        content_hash = self.get_content_hash(content)
        
        if group_id not in self.group_content_history:
            return True
        
        # بررسی تکرار اخیر
        for sent_hash, sent_time in self.group_content_history[group_id]:
            if sent_hash == content_hash:
                if time.time() - sent_time < self.min_repeat_interval:
                    return False
        
        return True
    
    def record_sent(self, group_id, content):
        """ثبت ارسال محتوا"""
        content_hash = self.get_content_hash(content)
        
        if group_id not in self.group_content_history:
            self.group_content_history[group_id] = []
        
        self.group_content_history[group_id].append((content_hash, time.time()))
        
        # محدود کردن تاریخچه
        if len(self.group_content_history[group_id]) > self.max_history_per_group:
            self.group_content_history[group_id] = self.group_content_history[group_id][-self.max_history_per_group:]
        
        # به‌روزرسانی امتیاز
        if content_hash not in self.content_scores:
            self.content_scores[content_hash] = {'shown': 0, 'engagement': 0.0}
        self.content_scores[content_hash]['shown'] += 1
    
    def record_engagement(self, content, engagement_score):
        """ثبت engagement برای محتوا"""
        content_hash = self.get_content_hash(content)
        if content_hash in self.content_scores:
            # میانگین متحرک
            old = self.content_scores[content_hash]['engagement']
            self.content_scores[content_hash]['engagement'] = (old * 0.7) + (engagement_score * 0.3)
    
    def get_best_content_for_group(self, group_id, available_contents):
        """انتخاب بهترین محتوا برای گروه"""
        valid_contents = []
        
        for content in available_contents:
            if self.should_send_content(group_id, content):
                content_hash = self.get_content_hash(content)
                score = self.content_scores.get(content_hash, {}).get('engagement', 0.5)
                valid_contents.append((score, content))
        
        if not valid_contents:
            # همه محتواها اخیراً ارسال شده‌اند
            return random.choice(available_contents) if available_contents else None
        
        # 70% بهترین، 30% تصادفی
        if random.random() < 0.7:
            valid_contents.sort(key=lambda x: x[0], reverse=True)
            return valid_contents[0][1]
        else:
            return random.choice(valid_contents)[1]


class GroupPerformanceTracker:
    """
    ردیاب عملکرد گروه‌ها برای بهینه‌سازی هدف‌گیری
    
    ویژگی‌ها:
    1. امتیازدهی گروه‌ها
    2. تشخیص گروه‌های فعال
    3. اولویت‌بندی گروه‌ها
    """
    
    def __init__(self):
        # آمار هر گروه
        self.group_stats = {}  # {group_id: {metrics}}
        
        # لیست سیاه موقت
        self.temp_blacklist = {}  # {group_id: until_timestamp}
    
    def init_group(self, group_id):
        """مقداردهی اولیه گروه"""
        if group_id not in self.group_stats:
            self.group_stats[group_id] = {
                'messages_sent': 0,
                'messages_deleted': 0,  # پیام‌های حذف شده توسط ادمین
                'errors': 0,
                'last_success': 0,
                'engagement': 0,
                'quality_score': 50,  # 0-100
                'category': 'unknown'
            }
    
    def record_success(self, group_id, engagement=0):
        """ثبت موفقیت"""
        self.init_group(group_id)
        self.group_stats[group_id]['messages_sent'] += 1
        self.group_stats[group_id]['last_success'] = time.time()
        self.group_stats[group_id]['engagement'] += engagement
        self.group_stats[group_id]['quality_score'] = min(100, 
            self.group_stats[group_id]['quality_score'] + 1)
    
    def record_error(self, group_id, error_type='generic'):
        """ثبت خطا"""
        self.init_group(group_id)
        self.group_stats[group_id]['errors'] += 1
        self.group_stats[group_id]['quality_score'] = max(0,
            self.group_stats[group_id]['quality_score'] - 5)
        
        # blacklist موقت برای خطاهای شدید
        if error_type in ['banned', 'forbidden']:
            self.temp_blacklist[group_id] = time.time() + 3600  # 1 ساعت
    
    def record_message_deleted(self, group_id):
        """ثبت حذف پیام توسط ادمین"""
        self.init_group(group_id)
        self.group_stats[group_id]['messages_deleted'] += 1
        self.group_stats[group_id]['quality_score'] = max(0,
            self.group_stats[group_id]['quality_score'] - 10)
    
    def is_blacklisted(self, group_id):
        """آیا گروه در blacklist است؟"""
        if group_id in self.temp_blacklist:
            if time.time() < self.temp_blacklist[group_id]:
                return True
            else:
                del self.temp_blacklist[group_id]
        return False
    
    def get_priority_groups(self, group_ids, limit=None):
        """گروه‌ها به ترتیب اولویت"""
        scored_groups = []
        
        for gid in group_ids:
            if self.is_blacklisted(gid):
                continue
            
            self.init_group(gid)
            score = self.group_stats[gid]['quality_score']
            
            # بونوس برای گروه‌های با engagement بالا
            if self.group_stats[gid]['messages_sent'] > 0:
                engagement_rate = self.group_stats[gid]['engagement'] / self.group_stats[gid]['messages_sent']
                score += engagement_rate * 10
            
            # جریمه برای حذف پیام
            delete_rate = self.group_stats[gid]['messages_deleted'] / max(self.group_stats[gid]['messages_sent'], 1)
            score -= delete_rate * 20
            
            scored_groups.append((score, gid))
        
        scored_groups.sort(key=lambda x: x[0], reverse=True)
        
        result = [gid for _, gid in scored_groups]
        return result[:limit] if limit else result
    
    def get_group_score(self, group_id):
        """امتیاز گروه"""
        self.init_group(group_id)
        return self.group_stats[group_id]['quality_score']


class MessageTemplateOptimizer:
    """
    بهینه‌ساز قالب پیام‌ها با یادگیری خودکار
    """
    
    def __init__(self):
        # قالب‌های پیام با تنوع
        self.templates = {
            'intro': [
                "💊 {drug_list}",
                "🏥 لیست داروهای موجود:\n\n{drug_list}",
                "✨ داروهای اصل و تضمینی:\n\n{drug_list}",
                "📋 موجودی امروز:\n\n{drug_list}",
            ],
            'footer': [
                "\n\n📦 ارسال سراسری\n💬 @PharmaWebAd",
                "\n\n🚚 ارسال فوری\n📞 @PharmaWebAd",
                "\n\n✅ تضمین اصالت\n🛒 @PharmaWebAd",
            ],
            'cta': [
                "💬 برای سفارش پیام دهید",
                "📩 جهت خرید DM کنید",
                "🛒 سفارش: @PharmaWebAd",
            ]
        }
        
        # امتیاز هر ترکیب
        self.shopbination_scores = {}
    
    def generate_message(self, drug_list):
        """تولید پیام با قالب تصادفی"""
        intro = random.choice(self.templates['intro'])
        footer = random.choice(self.templates['footer'])
        
        message = intro.format(drug_list=drug_list) + footer
        
        return message
    
    def record_performance(self, message_hash, engagement):
        """ثبت عملکرد پیام"""
        if message_hash not in self.shopbination_scores:
            self.shopbination_scores[message_hash] = {'count': 0, 'total_engagement': 0}
        
        self.shopbination_scores[message_hash]['count'] += 1
        self.shopbination_scores[message_hash]['total_engagement'] += engagement


# نمونه‌های Global از سیستم‌های محافظت
anti_spam = AntiSpamProtection()
content_rotation = SmartContentRotation()
group_tracker = GroupPerformanceTracker()
message_optimizer = MessageTemplateOptimizer()


# ═══════════════════════════════════════════════════════════════════════════════
# ⚔️⚔️⚔️ سیستم جنگجوی تهاجمی (WARRIOR MODE) ⚔️⚔️⚔️
# ═══════════════════════════════════════════════════════════════════════════════

class WarriorGroupJoiner:
    """
    سیستم تهاجمی برای عضویت حداکثری در گروه‌ها
    
    🎯 استراتژی اولویت‌بندی سه‌لایه:
    1. 🥇 اولویت 1 (70%): ترید، کریپتو، رمزارز، پراپ، فارکس
    2. 🥈 اولویت 2 (25%): مهاجرت، اقامت، ویزا، ایرانیان خارج
    3. 🥉 اولویت 3 (5%): عمومی و متفرقه
    
    ویژگی‌ها:
    - حمله سریع و متوالی
    - تنوع در کلمات جستجو با اولویت‌بندی
    - مقاومت در برابر محدودیت‌ها
    - بازیابی سریع پس از FloodWait
    """
    
    def __init__(self):
        # 🎯 کلمات کلیدی اولویت 1: ترید و کریپتو (70%)
        self.priority1_keywords = [
            # ترید فارسی
            "ترید", "تریدر", "تریدینگ", "معامله", "معاملات", "سیگنال", "سیگنال ترید",
            "تحلیل تکنیکال", "تحلیل فاندامنتال", "استراتژی", "پرایس اکشن",
            "فیوچرز", "اسپات", "لانگ", "شورت", "لوریج", "مارجین",
            # کریپتو فارسی
            "کریپتو", "رمزارز", "ارز دیجیتال", "بیتکوین", "بیت کوین", "اتریوم",
            "تتر", "بایننس", "صرافی", "ایردراپ", "استیکینگ", "دیفای", "NFT",
            "سولانا", "کاردانو", "پولکادات", "آربیتروم", "هولد", "HODL",
            # پراپ فارسی
            "پراپ", "پراپ فرم", "پراپ تریدینگ", "فاندینگ", "چالش پراپ", "FTMO",
            "درصد سود", "دراداون", "کپی ترید",
            # فارکس فارسی
            "فارکس", "جفت ارز", "طلا", "اونس", "بروکر", "متاتریدر",
            # سرمایه‌گذاری
            "سرمایه گذاری", "درآمد", "درآمد دلاری", "بورس", "سهام",
            # انگلیسی
            "trade", "trader", "trading", "signal", "crypto", "cryptocurrency",
            "bitcoin", "btc", "ethereum", "eth", "binance", "forex", "fx",
            "prop", "prop firm", "funded", "defi", "nft", "airdrop", "staking",
            "scalping", "swing", "futures", "spot", "leverage", "margin",
        ]
        
        # 🎯 کلمات کلیدی اولویت 2: مهاجرت و اقامت (25%)
        self.priority2_keywords = [
            # مهاجرت فارسی
            "مهاجرت", "مهاجر", "اقامت", "ویزا", "پناهندگی", "پاسپورت",
            "ایرانیان خارج", "فارسی زبان", "سفارت", "کنسولگری",
            # ترکیه
            "ترکیه", "استانبول", "آنکارا", "ازمیر", "کیملیک", "اقامت ترکیه",
            "ایرانیان استانبول", "ایرانیان ترکیه",
            # امارات
            "امارات", "دبی", "ابوظبی", "شارجه", "ایرانیان دبی", "ویزای دبی",
            # عراق
            "عراق", "اربیل", "سلیمانیه", "بغداد",
            # اروپا
            "آلمان", "برلین", "انگلیس", "لندن", "کانادا", "تورنتو", "ونکوور",
            "استرالیا", "سیدنی", "فرانسه", "هلند", "سوئد",
            # موضوعات
            "ویزای کار", "ویزای تحصیلی", "اقامت کار", "شهروندی",
            # انگلیسی
            "immigration", "visa", "expat", "refugee", "asylum",
            "turkey", "istanbul", "dubai", "uae", "canada", "germany",
            "iranian", "persian", "farsi",
        ]
        
        # 🎯 کلمات کلیدی اولویت 3: عمومی (5%)
        self.priority3_keywords = [
            "چت", "گپ", "دوستی", "گروه", "ایرانی", "فارسی",
            "تهران", "مشهد", "اصفهان", "شیراز",
            "خرید", "فروش", "استخدام", "کار",
            "group", "chat", "iran", "persian",
        ]
        
        # ترکیب برای سازگاری
        self.universal_keywords = self.priority1_keywords + self.priority2_keywords + self.priority3_keywords
        
        # آمار عملکرد
        self.stats = {
            'total_searches': 0,
            'total_joins': 0,
            'flood_waits': 0,
            'total_flood_time': 0,
            'keywords_tried': set(),
            'successful_keywords': {},
            'failed_keywords': {},
            'hourly_joins': {},
            'best_hour': None
        }
        
        # وضعیت جنگجو - شروع با حالت defensive برای امنیت ⚠️
        self.mode = 'defensive'  # 🔒 تغییر از aggressive به defensive
        self.consecutive_success = 0
        self.consecutive_fail = 0
        self.last_flood_time = 0
        
        # تنظیمات سرعت - بهینه‌سازی شده برای کاهش ریسک بن ⚠️
        self.speed_settings = {
            'aggressive': {
                'search_delay': 10,   # 🔒 افزایش به 10 ثانیه (از 1.5)
                'join_delay': 5,      # 🔒 افزایش به 5 ثانیه (از 0.5)
                'batch_size': 5,      # 🔒 کاهش به 5 (از 15)
                'parallel_searches': 3  # 🔒 کاهش به 3 (از 8)
            },
            'balanced': {
                'search_delay': 20,   # 🔒 افزایش به 20 ثانیه (از 3)
                'join_delay': 10,     # 🔒 افزایش به 10 ثانیه (از 1)
                'batch_size': 3,      # 🔒 کاهش به 3 (از 10)
                'parallel_searches': 2  # 🔒 کاهش به 2 (از 5)
            },
            'defensive': {
                'search_delay': 45,   # 🔒 افزایش به 45 ثانیه (از 8)
                'join_delay': 30,     # 🔒 افزایش به 30 ثانیه (از 3)
                'batch_size': 2,      # 🔒 کاهش به 2 (از 3)
                'parallel_searches': 1  # 🔒 کاهش به 1 (از 2)
            }
        }
    
    def get_search_keywords(self, count=50):
        """
        تولید کلمات جستجوی متنوع با سیستم اولویت‌بندی چهارلایه
        
        🥇 35% از اولویت 1: ترید/کریپتو
        🏥 25% از اولویت 2: پزشکی/دارو/تجهیزات
        🥈 20% از اولویت 3: مهاجرت
        🥉 10% از اولویت 4: عمومی
        """
        keywords = []
        
        # 🥇 35% از اولویت 1: ترید و کریپتو
        priority1_count = int(count * 0.35)
        
        # از کلمات موفق قبلی در این حوزه
        successful_crypto = []
        for kw, score in self.stats['successful_keywords'].items():
            if any(crypto in kw.lower() for crypto in ['ترید', 'کریپتو', 'بیتکوین', 'trade', 'crypto', 'bitcoin', 'forex']):
                successful_crypto.append((kw, score))
        successful_crypto.sort(key=lambda x: x[1], reverse=True)
        keywords.extend([kw for kw, _ in successful_crypto[:priority1_count // 3]])
        
        # بقیه از لیست اولویت 1
        remaining1 = priority1_count - len(keywords)
        if remaining1 > 0:
            sample1 = random.sample(self.priority1_keywords, min(remaining1, len(self.priority1_keywords)))
            keywords.extend(sample1)
        
        # 🏥 25% از اولویت 2: پزشکی و دارو
        medical_count = int(count * 0.25)
        
        # از کلمات موفق قبلی در این حوزه
        successful_med = []
        for kw, score in self.stats['successful_keywords'].items():
            if any(med in kw.lower() for med in ['دارو', 'پزشک', 'تجهیزات', 'آزمایشگاه', 'pharmacy', 'medical', 'drug', 'dental', 'lab']):
                successful_med.append((kw, score))
        successful_med.sort(key=lambda x: x[1], reverse=True)
        keywords.extend([kw for kw, _ in successful_med[:medical_count // 3]])
        
        # بقیه از MEDICAL_KEYWORDS
        remaining_med = medical_count - len([kw for kw, _ in successful_med[:medical_count // 3]])
        if remaining_med > 0:
            all_med = get_all_medical_keywords()
            if all_med:
                sample_med = random.sample(all_med, min(remaining_med, len(all_med)))
                keywords.extend(sample_med)
        
        # 🥈 20% از اولویت 3: مهاجرت
        priority2_count = int(count * 0.20)
        
        # از کلمات موفق قبلی در این حوزه
        successful_imm = []
        for kw, score in self.stats['successful_keywords'].items():
            if any(imm in kw.lower() for imm in ['مهاجرت', 'استانبول', 'دبی', 'ترکیه', 'immigration', 'istanbul', 'dubai']):
                successful_imm.append((kw, score))
        successful_imm.sort(key=lambda x: x[1], reverse=True)
        keywords.extend([kw for kw, _ in successful_imm[:priority2_count // 3]])
        
        # بقیه از لیست اولویت 2
        remaining2 = priority2_count - len([kw for kw, _ in successful_imm[:priority2_count // 3]])
        if remaining2 > 0:
            sample2 = random.sample(self.priority2_keywords, min(remaining2, len(self.priority2_keywords)))
            keywords.extend(sample2)
        
        # 🥉 10% از اولویت 4: عمومی
        priority3_count = int(count * 0.10)
        if priority3_count > 0:
            sample3 = random.sample(self.priority3_keywords, min(priority3_count, len(self.priority3_keywords)))
            keywords.extend(sample3)
        
        # ترکیبات هوشمند (10% اضافی)
        combos = self._generate_combos(count // 10)
        keywords.extend(combos)
        
        # حذف تکراری و shuffle
        keywords = list(set(keywords))
        random.shuffle(keywords)
        
        return keywords[:count]
    
    def _generate_combos(self, count):
        """تولید ترکیبات هوشمند با اولویت ترید/کریپتو و پزشکی - گسترش‌یافته"""
        combos = []
        
        # ترکیبات ترید/کریپتو (اولویت بالا) - گسترش‌یافته
        crypto_prefixes = ["گروه", "سیگنال", "کانال", "آموزش", "group", "signal", "free", "VIP", "رایگان"]
        crypto_mains = [
            "ترید", "کریپتو", "بیتکوین", "فارکس", "پراپ", "trade", "crypto", "bitcoin", "forex",
            "تون", "TON", "ترون", "TRX", "سولانا", "SOL", "دوج", "DOGE", "BNB",
            "فیوچرز", "اسپات", "ایردراپ", "NFT", "دیفای", "futures", "airdrop",
            "آربیتروم", "اوپتیمیزم", "نات کوین", "همستر", "بلوم",
        ]
        
        for _ in range(max(count // 3, 3)):
            prefix = random.choice(crypto_prefixes)
            main = random.choice(crypto_mains)
            combos.append(f"{prefix} {main}")
        
        # ترکیبات پزشکی/دارو (اولویت بالا)
        med_prefixes = ["گروه", "کانال", "انجمن", "خرید", "فروش", "تبادل", "group", "channel"]
        med_mains = ["دارو", "داروخانه", "پزشکی", "تجهیزات پزشکی", "آزمایشگاه", "دندانپزشکی",
                     "pharmacy", "medical", "drug", "dental", "lab", "health",
                     "مکمل", "ویتامین", "لوازم پزشکی", "کلینیک"]
        
        for _ in range(max(count // 3, 3)):
            prefix = random.choice(med_prefixes)
            main = random.choice(med_mains)
            combos.append(f"{prefix} {main}")
        
        # ترکیبات مهاجرت - گسترش‌یافته
        imm_prefixes = ["ایرانیان", "فارسی", "گروه", "iranian", "persian", "مهاجرت"]
        imm_mains = [
            "استانبول", "دبی", "ترکیه", "امارات", "istanbul", "dubai", "turkey",
            "تورنتو", "ونکوور", "لندن", "برلین", "سیدنی", "پاریس",
            "toronto", "vancouver", "london", "berlin", "sydney",
            "کانادا", "آلمان", "انگلیس", "استرالیا",
        ]
        
        for _ in range(max(count // 3, 3)):
            prefix = random.choice(imm_prefixes)
            main = random.choice(imm_mains)
            combos.append(f"{prefix} {main}")
        
        return combos
    
    def record_join(self, keyword, success):
        """ثبت نتیجه عضویت"""
        self.stats['total_searches'] += 1
        self.stats['keywords_tried'].add(keyword)
        
        if success:
            self.stats['total_joins'] += 1
            self.stats['successful_keywords'][keyword] = \
                self.stats['successful_keywords'].get(keyword, 0) + 1
            self.consecutive_success += 1
            self.consecutive_fail = 0
            
            # ثبت ساعت
            hour = datetime.now().hour
            self.stats['hourly_joins'][hour] = \
                self.stats['hourly_joins'].get(hour, 0) + 1
        else:
            self.stats['failed_keywords'][keyword] = \
                self.stats['failed_keywords'].get(keyword, 0) + 1
            self.consecutive_fail += 1
            self.consecutive_success = 0
        
        # تنظیم حالت
        self._adjust_mode()
    
    def record_flood_wait(self, seconds):
        """ثبت FloodWait"""
        self.stats['flood_waits'] += 1
        self.stats['total_flood_time'] += seconds
        self.last_flood_time = time.time()
        self.consecutive_fail += 3  # FloodWait = 3 شکست
        self._adjust_mode()
    
    def _adjust_mode(self):
        """تنظیم حالت بر اساس عملکرد"""
        # اگر FloodWait اخیر داشتیم
        if time.time() - self.last_flood_time < 300:  # 5 دقیقه
            self.mode = 'defensive'
            return
        
        # بر اساس موفقیت‌های متوالی
        if self.consecutive_success >= 20:
            self.mode = 'aggressive'
        elif self.consecutive_fail >= 5:
            self.mode = 'defensive'
        else:
            self.mode = 'balanced'
    
    def get_settings(self):
        """دریافت تنظیمات فعلی"""
        return self.speed_settings[self.mode]
    
    def get_optimal_time(self):
        """بهترین ساعت برای عضویت"""
        if not self.stats['hourly_joins']:
            return None
        
        best = max(self.stats['hourly_joins'].items(), key=lambda x: x[1])
        return best[0]


class AggressiveMemberAdder:
    """
    سیستم تهاجمی برای اضافه کردن اعضا به گروه هدف
    
    استراتژی:
    1. اضافه مستقیم (ترجیحی)
    2. PM هوشمند (بکاپ)
    3. چرخش بین روش‌ها
    4. یادگیری از نتایج
    """
    
    def __init__(self):
        # آمار روش‌ها
        self.method_stats = {
            'direct_add': {'success': 0, 'fail': 0, 'flood_waits': 0},
            'pm_invite': {'success': 0, 'fail': 0, 'flood_waits': 0}
        }
        
        # صف کاربران
        self.user_queue = []  # [(user_id, user_info, priority)]
        
        # کاربران پردازش شده
        self.processed_users = set()
        
        # blacklist موقت
        self.temp_blacklist = {}  # {user_id: until_timestamp}
        
        # تنظیمات
        self.settings = {
            'direct_add_ratio': 0.7,  # 70% اضافه مستقیم
            'pm_ratio': 0.3,  # 30% PM
            'batch_size': 10,
            'delay_between_adds': 3,  # ثانیه
            'delay_after_flood': 60,  # ثانیه
            'max_retries': 2
        }
        
        # وضعیت
        self.current_method = 'direct_add'
        self.consecutive_direct_fails = 0
    
    def add_to_queue(self, users):
        """اضافه کردن کاربران به صف"""
        for user_id, user_info in users.items():
            if user_id in self.processed_users:
                continue
            if user_id in self.temp_blacklist:
                if time.time() < self.temp_blacklist[user_id]:
                    continue
                else:
                    del self.temp_blacklist[user_id]
            
            # محاسبه اولویت
            priority = self._calculate_priority(user_info)
            self.user_queue.append((user_id, user_info, priority))
        
        # مرتب‌سازی بر اساس اولویت
        self.user_queue.sort(key=lambda x: x[2], reverse=True)
    
    def _calculate_priority(self, user_info):
        """محاسبه اولویت کاربر - همه کاربران یکسان هستند"""
        priority = 50
        
        # داشتن username = +20
        if user_info.get('username'):
            priority += 20
        
        # فعالیت اخیر = +15
        if user_info.get('is_recent'):
            priority += 15
        
        # داشتن عکس پروفایل = +10
        if user_info.get('has_photo'):
            priority += 10
        
        # 🔒 اولویت پریمیوم حذف شد - همه کاربران یکسان
        # if user_info.get('is_premium'):
        #     priority += 15
        
        return priority
    
    def get_next_batch(self, size=None):
        """دریافت دسته بعدی کاربران"""
        if size is None:
            size = self.settings['batch_size']
        
        batch = []
        while len(batch) < size and self.user_queue:
            user_id, user_info, priority = self.user_queue.pop(0)
            if user_id not in self.processed_users:
                batch.append((user_id, user_info))
        
        return batch
    
    def select_method(self):
        """انتخاب روش اضافه کردن"""
        # اگر اضافه مستقیم خیلی fail داره، به PM برو
        if self.consecutive_direct_fails >= 5:
            self.current_method = 'pm_invite'
            self.consecutive_direct_fails = 0
            return 'pm_invite'
        
        # محاسبه نرخ موفقیت
        direct = self.method_stats['direct_add']
        pm = self.method_stats['pm_invite']
        
        direct_rate = direct['success'] / max(direct['success'] + direct['fail'], 1)
        pm_rate = pm['success'] / max(pm['success'] + pm['fail'], 1)
        
        # انتخاب بر اساس نرخ موفقیت
        if direct_rate > pm_rate * 1.2:  # اگر مستقیم 20% بهتره
            return 'direct_add'
        elif pm_rate > direct_rate * 1.2:
            return 'pm_invite'
        else:
            # تصادفی با وزن
            return 'direct_add' if random.random() < self.settings['direct_add_ratio'] else 'pm_invite'
    
    def record_result(self, user_id, method, success, error_type=None):
        """ثبت نتیجه"""
        self.processed_users.add(user_id)
        
        if success:
            self.method_stats[method]['success'] += 1
            self.consecutive_direct_fails = 0
        else:
            self.method_stats[method]['fail'] += 1
            if method == 'direct_add':
                self.consecutive_direct_fails += 1
            
            # blacklist برای خطاهای خاص
            if error_type in ['privacy', 'blocked']:
                self.temp_blacklist[user_id] = time.time() + 86400  # 24 ساعت
    
    def record_flood_wait(self, method, seconds):
        """ثبت FloodWait"""
        self.method_stats[method]['flood_waits'] += 1
        # سوئیچ به روش دیگه
        self.current_method = 'pm_invite' if method == 'direct_add' else 'direct_add'
    
    def get_delay(self):
        """محاسبه تاخیر"""
        # اگر FloodWait اخیر داشتیم
        total_floods = sum(s['flood_waits'] for s in self.method_stats.values())
        if total_floods > 0:
            base = self.settings['delay_between_adds']
            return base * (1 + total_floods * 0.5)
        
        return self.settings['delay_between_adds']
    
    def get_statistics(self):
        """آمار کامل"""
        direct = self.method_stats['direct_add']
        pm = self.method_stats['pm_invite']
        
        return {
            'queue_size': len(self.user_queue),
            'processed': len(self.processed_users),
            'direct_add': {
                'success': direct['success'],
                'fail': direct['fail'],
                'rate': f"{direct['success'] / max(direct['success'] + direct['fail'], 1):.1%}"
            },
            'pm_invite': {
                'success': pm['success'],
                'fail': pm['fail'],
                'rate': f"{pm['success'] / max(pm['success'] + pm['fail'], 1):.1%}"
            },
            'current_method': self.current_method
        }


# نمونه‌های Global
warrior_joiner = WarriorGroupJoiner()
aggressive_adder = AggressiveMemberAdder()

# 🧠 سیستم INTELLIGENT-ADAPTIVE: سازگاری هوشمند با محدودیت‌ها
EDIT_DELAY_MINUTES = 180
MIRROR_EDIT_DELAY = 10
MIRROR_BLOCK_DURATION = 900
BROADCAST_INTERVAL = 600  # 🔒 فاصله 5 دقیقه بین broadcast ها (افزایش از 60)
MESSAGE_DELAY_MIN = 600  # 🔒 حداقل 10 دقیقه فاصله بین پیام‌ها (افزایش از 180)
MESSAGE_DELAY_MAX = 1800  # 🔒 حداکثر 20 دقیقه فاصله بین پیام‌ها (افزایش از 300)

# 🎯 سیستم Adaptive Rate Limiting - بهینه‌سازی شده برای کاهش ریسک بن ⚠️
SEARCH_INTERVAL = 25  # 🔒 25 ثانیه بین جستجوها (کاهش برای جستجوی سریعتر)
SEARCH_INTERVAL_MIN = 15  # 🔒 حداقل 15 ثانیه
SEARCH_INTERVAL_MAX = 50  # 🔒 حداکثر 50 ثانیه (کاهش برای جستجوی بیشتر)
JOIN_LIMIT_PER_CYCLE = 15  # 🔒 15 عضویت در هر سیکل (افزایش)
JOIN_LIMIT_MIN = 8  # 🔒 حداقل 8 (افزایش)
JOIN_LIMIT_MAX = 30  # 🔒 حداکثر 30 (افزایش)
PARALLEL_SEARCHES = 8  # 🔒 8 جستجوی موازی (افزایش برای سرعت بیشتر)
MIN_DELAY = 1.5  # 🔒 تاخیر 1.5 ثانیه (کاهش)
MAX_DELAY = 6.0  # 🔒 تاخیر 6 ثانیه (کاهش)
SEARCH_LIMIT = 80  # 🔒 80 نتیجه (افزایش شدید)
# ⚠️ MIN_GROUP_MEMBERS حذف شد - از مقدار 500 در تنظیمات بالا استفاده می‌شود
KEYWORD_GENERATION_COUNT = 250  # 🔒 250 کلمه کلیدی (افزایش شدید برای یافتن 1000+ گروه)

# ═══════════════════════════════════════════════════════════════════════════════
# 🧠🧠🧠 سیستم هوش مصنوعی پیشرفته برای تولید کلمات کلیدی 🧠🧠🧠
# ═══════════════════════════════════════════════════════════════════════════════

# 📊 سیستم یادگیری و امتیازدهی
KEYWORD_SCORES = {}  # {keyword: {'score': float, 'success': int, 'fail': int, 'last_used': timestamp}}
KEYWORD_LEARNING_FILE = "keyword_learning.json"
SUCCESSFUL_PATTERNS = []  # الگوهای موفق
FAILED_PATTERNS = []  # الگوهای ناموفق

# 🎯 ماتریس وزن‌دهی هوشمند برای ترکیبات (به‌روزرسانی شده)
WEIGHT_MATRIX = {
    'crypto_trade': 5.0,    # 🥇 ترید/کریپتو (وزن بالاترین)
    'city_topic': 3.0,      # شهر + موضوع (وزن بالا)
    'country_action': 2.8,  # کشور + فعل
    'iranian_city': 2.5,    # ایرانی + شهر
    'service_city': 2.3,    # خدمات + شهر
    'profession_topic': 2.0, # حرفه + موضوع
    'single_keyword': 1.0,  # کلمه تک
    'random_combo': 0.5,    # ترکیب تصادفی
}

# 🌐 دسته‌بندی هوشمند با اولویت (بازطراحی شده)
SMART_CATEGORIES = {
    # ═══════════════════════════════════════════════════════════════════
    # 🥇 اولویت 1: ترید و رمزارز (وزن بالاترین)
    # ═══════════════════════════════════════════════════════════════════
    'crypto_trading': {
        'weight': 15,  # بالاترین وزن
        'items': {
            'fa': ["ترید", "تریدر", "کریپتو", "رمزارز", "ارز دیجیتال", "بیتکوین", "اتریوم",
                   "سیگنال", "فارکس", "پراپ", "فیوچرز", "اسپات", "بایننس", "صرافی",
                   "تحلیل تکنیکال", "پرایس اکشن", "استیکینگ", "ایردراپ", "دیفای", "NFT",
                   "معامله", "معاملات", "سرمایه گذاری", "درآمد دلاری", "بورس"],
            'en': ["trade", "trader", "trading", "crypto", "cryptocurrency", "bitcoin", "btc",
                   "ethereum", "eth", "signal", "forex", "prop", "futures", "spot", "binance",
                   "exchange", "technical analysis", "price action", "staking", "airdrop", "defi", "nft"]
        }
    },
    
    'crypto_coins': {
        'weight': 14,
        'items': {
            'fa': ["بیتکوین", "اتریوم", "تتر", "سولانا", "کاردانو", "ریپل", "دوج کوین",
                   "شیبا", "پولکادات", "آوالانچ", "آربیتروم", "اپتیمیزم"],
            'en': ["bitcoin", "btc", "ethereum", "eth", "tether", "usdt", "solana", "sol",
                   "cardano", "ada", "ripple", "xrp", "dogecoin", "doge", "shiba",
                   "polkadot", "dot", "avalanche", "avax", "arbitrum", "arb", "optimism", "op"]
        }
    },
    
    'prop_forex': {
        'weight': 13,
        'items': {
            'fa': ["پراپ", "پراپ فرم", "فاندینگ", "FTMO", "چالش پراپ", "فارکس", "طلا", "اونس",
                   "بروکر", "متاتریدر", "لوریج", "مارجین"],
            'en': ["prop", "prop firm", "funded", "ftmo", "funding", "forex", "fx", "gold", "xauusd",
                   "broker", "metatrader", "mt4", "mt5", "leverage", "margin"]
        }
    },
    
    # ═══════════════════════════════════════════════════════════════════
    # 🥈 اولویت 2: مهاجرت و اقامت (وزن متوسط)
    # ═══════════════════════════════════════════════════════════════════
    'priority_cities': {
        'weight': 10,
        'items': {
            'fa': ["استانبول", "دبی", "تورنتو", "برلین", "لندن", "پاریس", "سیدنی", "آمستردام",
                   "آنکارا", "ازمیر", "ابوظبی", "شارجه", "ونکوور", "مونترال", "مونیخ", "فرانکفورت"],
            'en': ["istanbul", "dubai", "toronto", "berlin", "london", "paris", "sydney", "amsterdam",
                   "ankara", "izmir", "abu dhabi", "sharjah", "vancouver", "montreal", "munich", "frankfurt"]
        }
    },
    
    'priority_countries': {
        'weight': 9,
        'items': {
            'fa': ["ترکیه", "امارات", "کانادا", "آلمان", "انگلیس", "فرانسه", "استرالیا", "هلند", "سوئد", "نروژ"],
            'en': ["turkey", "uae", "canada", "germany", "uk", "france", "australia", "netherlands", "sweden", "norway"]
        }
    },
    
    'hot_topics': {
        'weight': 8,
        'items': {
            'fa': ["مهاجرت", "ویزا", "اقامت", "کار", "تحصیل", "خانه", "اجاره", "استخدام", "دانشگاه"],
            'en': ["immigration", "visa", "residence", "job", "work", "study", "house", "rent", "hiring", "university"]
        }
    },
    
    'iranian_identity': {
        'weight': 7,
        'items': {
            'fa': ["ایرانی", "ایرانیان", "فارسی", "پارسی", "فارسی زبان", "ایرانی‌ها", "همشهری", "هموطن"],
            'en': ["iranian", "persian", "farsi", "irani", "persian speaking"]
        }
    },
    
    # ═══════════════════════════════════════════════════════════════════
    # 🥉 اولویت 3: خدمات و متفرقه (وزن پایین)
    # ═══════════════════════════════════════════════════════════════════
    'key_services': {
        'weight': 6,
        'items': {
            'fa': ["دکتر", "وکیل", "مشاور", "بیمارستان", "بانک", "صرافی", "آژانس", "مدرسه", "کلاس", "آموزش"],
            'en': ["doctor", "lawyer", "consultant", "hospital", "bank", "exchange", "agency", "school", "class", "education"]
        }
    },
    
    'action_verbs': {
        'weight': 5,
        'items': {
            'fa': ["زندگی در", "کار در", "تحصیل در", "مهاجرت به", "اجاره در", "خرید در", "فروش در"],
            'en': ["living in", "working in", "studying in", "moving to", "renting in", "buying in", "selling in"]
        }
    },
    
    'istanbul_districts': {
        'weight': 8,
        'items': {
            'fa': ["تکسیم", "فاتح", "لاله‌لی", "اسنیورت", "باشاک‌شهیر", "کادیکوی", "بشیکتاش", "شیشلی", 
                   "اتیلر", "اومرانیه", "مالتپه", "پندیک", "کارتال", "آتاشهیر"],
            'en': ["taksim", "fatih", "laleli", "esenyurt", "basaksehir", "kadikoy", "besiktas", "sisli",
                   "etiler", "umraniye", "maltepe", "pendik", "kartal", "atasehir"]
        }
    },
    
    'dubai_areas': {
        'weight': 8,
        'items': {
            'fa': ["دیره", "بردبی", "جمیرا", "مارینا", "داون‌تاون", "بیزنس‌بی", "البرشا", "جی‌بی‌آر"],
            'en': ["deira", "bur dubai", "jumeirah", "marina", "downtown", "business bay", "al barsha", "jbr"]
        }
    }
}

# 🔄 الگوریتم Markov Chain برای تولید ترکیبات هوشمند
MARKOV_TRANSITIONS = {
    # از شهر به چه چیزی برویم؟
    'city': ['topic', 'service', 'iranian', 'action'],
    # از کشور به چه چیزی برویم؟
    'country': ['action', 'topic', 'iranian'],
    # از موضوع به چه چیزی برویم؟
    'topic': ['city', 'country'],
    # از ایرانی به چه چیزی برویم؟
    'iranian': ['city', 'country', 'service'],
    # از خدمات به چه چیزی برویم؟
    'service': ['city', 'iranian'],
    # از فعل به چه چیزی برویم؟
    'action': ['country', 'city']
}

# 📈 تابع محاسبه امتیاز TF-IDF مانند
def calculate_keyword_tfidf(keyword, success_count, total_uses, total_keywords):
    """محاسبه امتیاز کلمه کلیدی با الگوریتم TF-IDF مانند"""
    if total_uses == 0:
        return 1.0
    
    # Term Frequency: چند بار موفق بوده؟
    tf = success_count / max(total_uses, 1)
    
    # Inverse Document Frequency: چقدر نادر است؟
    import math
    idf = math.log(total_keywords / max(total_uses, 1) + 1)
    
    return tf * idf

# 🎲 الگوریتم Thompson Sampling برای انتخاب بهینه
def thompson_sampling_select(keywords_with_scores, n_select):
    """انتخاب کلمات کلیدی با الگوریتم Thompson Sampling"""
    import random
    
    selected = []
    for keyword, data in keywords_with_scores.items():
        success = data.get('success', 1)
        fail = data.get('fail', 1)
        
        # نمونه‌گیری از توزیع Beta
        # Beta(success + 1, fail + 1)
        sample = random.betavariate(success + 1, fail + 1)
        selected.append((keyword, sample))
    
    # مرتب‌سازی و انتخاب بهترین‌ها
    selected.sort(key=lambda x: x[1], reverse=True)
    return [kw for kw, score in selected[:n_select]]

# 🧬 نسخه ساده‌شده انتخابگر کلمات کلیدی (بهینه‌سازی شده)
class GeneticKeywordEvolver:
    """سیستم ساده انتخاب کلمات کلیدی - نسخه سبک"""
    
    def __init__(self, population_size=50, mutation_rate=0.15):
        self.population_size = population_size
        self.mutation_rate = mutation_rate
    
    def create_chromosome(self, categories):
        """ایجاد یک لیست ساده از کلمات"""
        import random
        result = []
        for cat_name, cat_data in categories.items():
            items = cat_data.get('items', {})
            lang = random.choice(['fa', 'en'])
            if lang in items and items[lang]:
                selected = random.sample(items[lang], min(2, len(items[lang])))
                result.extend(selected)
        return result
    
    def evolve_generation(self, categories, all_keywords):
        """انتخاب ساده کلمات کلیدی"""
        return self.create_chromosome(categories)

# 🔮 نسخه ساده‌شده پیش‌بینی (بهینه‌سازی شده)
class KeywordSuccessPredictor:
    """پیش‌بینی ساده موفقیت کلمه کلیدی - نسخه سبک"""
    
    def __init__(self):
        pass
    
    def predict_success(self, keyword):
        """پیش‌بینی ساده بر اساس سابقه"""
        if keyword in KEYWORD_SCORES:
            data = KEYWORD_SCORES[keyword]
            success = data.get('success', 0)
            fail = data.get('fail', 0)
            if success + fail > 0:
                return success / (success + fail)
        return 0.5  # پیش‌فرض

# نمونه‌های global
genetic_evolver = GeneticKeywordEvolver(population_size=30, mutation_rate=0.15)
success_predictor = KeywordSuccessPredictor()

# 🔢 محدودیت‌های تلگرام - بهینه‌شده
MAX_GROUPS_LIMIT = 400  # حداکثر 400 گروه (کاهش یافته)
GROUP_CLEANUP_THRESHOLD = 380  # شروع پاکسازی
GROUP_LEAVE_BATCH = 40  # خروج از 40 گروه قدیمی

# 🧠 Intelligent Backoff System - بهینه‌شده
FLOOD_WAIT_MULTIPLIER = 1.5  # ضریب بیشتر برای امنیت
FLOOD_WAIT_RESET_TIME = 900  # بازگشت به حالت عادی بعد از 15 دقیقه
CONSECUTIVE_FAILS_THRESHOLD = 3  # کاهش به 3 خطا
HEALTH_CHECK_INTERVAL = 600  # بررسی سلامت هر 10 دقیقه

# ایجاد کلاینت — StringSession از env var اگر موجود باشد (Railway)
# Importing bot.py in tests must not open my_session.session (that kicks Railway off Telegram).
_session_string = os.environ.get('TELETHON_SESSION_STRING', '').strip()
_live = bool(_session_string) or bool(os.environ.get('RAILWAY_ENVIRONMENT')) or __name__ == '__main__'
if _session_string:
    from telethon.sessions import StringSession as _StringSession
    _session = _StringSession(_session_string)
    print("✅ Using StringSession from TELETHON_SESSION_STRING env var", flush=True)
elif _live:
    _session = session_name
    print("⚠️  No TELETHON_SESSION_STRING — using file session (may break on Railway redeploy)", flush=True)
else:
    from telethon.sessions import MemorySession as _MemorySession
    _session = _MemorySession()

client = TelegramClient(
    _session,
    api_id,
    api_hash,
    connection_retries=10,
    retry_delay=5,
    timeout=30,
    request_retries=3,
    flood_sleep_threshold=60
)

# دیکشنری‌ها
groups = []
pm_responded = set()
mirror_users = {}  # {group_id: {user_id: expiration_time}}
group_ai_last_response = {}  # {group_id: timestamp} - زمان آخرین پاسخ AI در هر گروه
sent_messages = {}  # {group_id: [(message_id, timestamp)]}
joined_groups = set()  # گروه‌هایی که عضو شدیم
search_offset = 0  # برای صفحه‌بندی جستجو
last_message_time = {}  # {group_id: timestamp} - زمان آخرین پیام ارسالی در هر گروه
group_retry_count = {}  # {group_id: retry_count} - تعداد تلاش‌های ناموفق هر گروه

# ═══════════════════════════════════════════════════════════════════════════════
# 🚫 سیستم Blacklist دائمی (PERMANENT BLACKLIST)
# ═══════════════════════════════════════════════════════════════════════════════
# گروه‌هایی که به دلایل مختلف نباید دیگر عضو شویم
PERMANENT_BLACKLIST_FILE = "permanent_blacklist.json"
permanent_blacklist = set()  # {group_id, ...}
permanent_blacklist_reasons = {}  # {group_id: {'reason': str, 'timestamp': float, 'username': str}}

# دلایل اضافه شدن به blacklist دائمی
BLACKLIST_REASONS = {
    'low_members': 'تعداد اعضا کمتر از حد مجاز',
    'no_write_access': 'امکان ارسال پیام وجود ندارد',
    'banned': 'بن شده در گروه',
    'restricted': 'دسترسی محدود شده',
    'private_channel': 'کانال خصوصی شده',
    'broadcast_only': 'فقط ادمین می‌تواند پیام بفرستد',
    'manual': 'اضافه شده به صورت دستی'
}

def load_permanent_blacklist():
    """بارگذاری لیست سیاه دائمی از فایل"""
    global permanent_blacklist, permanent_blacklist_reasons
    try:
        if os.path.exists(PERMANENT_BLACKLIST_FILE):
            with open(PERMANENT_BLACKLIST_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                permanent_blacklist = set(data.get('blacklist', []))
                permanent_blacklist_reasons = data.get('reasons', {})
                # تبدیل کلیدها به int
                permanent_blacklist_reasons = {int(k): v for k, v in permanent_blacklist_reasons.items()}
                slog(f"✅ {len(permanent_blacklist)} گروه در blacklist دائمی بارگذاری شد")
    except Exception as e:
        slog(f"⚠️ خطا در بارگذاری blacklist: {e}")
        permanent_blacklist = set()
        permanent_blacklist_reasons = {}

def save_permanent_blacklist():
    """ذخیره لیست سیاه دائمی در فایل"""
    try:
        data = {
            'blacklist': list(permanent_blacklist),
            'reasons': {str(k): v for k, v in permanent_blacklist_reasons.items()},
            'last_updated': time.time(),
            'count': len(permanent_blacklist)
        }
        with open(PERMANENT_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        slog(f"⚠️ خطا در ذخیره blacklist: {e}")

def add_to_permanent_blacklist(group_id, reason='manual', username=None, title=None):
    """اضافه کردن گروه به blacklist دائمی"""
    global permanent_blacklist, permanent_blacklist_reasons
    
    if group_id not in permanent_blacklist:
        permanent_blacklist.add(group_id)
        permanent_blacklist_reasons[group_id] = {
            'reason': reason,
            'timestamp': time.time(),
            'username': username or 'نامشخص',
            'title': title or 'نامشخص'
        }
        
        # ذخیره هر 10 مورد یا اگر مهم باشد
        if len(permanent_blacklist) % 10 == 0:
            save_permanent_blacklist()
        
        slog(f"🚫 Blacklist دائمی: {title or group_id} ({BLACKLIST_REASONS.get(reason, reason)})")
        return True
    return False

def is_permanently_blacklisted(group_id):
    """آیا گروه در blacklist دائمی است؟"""
    return group_id in permanent_blacklist

def remove_from_permanent_blacklist(group_id):
    """حذف از blacklist دائمی (برای موارد استثنایی)"""
    if group_id in permanent_blacklist:
        permanent_blacklist.discard(group_id)
        if group_id in permanent_blacklist_reasons:
            del permanent_blacklist_reasons[group_id]
        save_permanent_blacklist()
        return True
    return False

# بارگذاری blacklist در شروع
load_permanent_blacklist()

# حافظه اعضا (بارگذاری می‌شود)
members_db = {
    'scraped_users': {},  # {user_id: {username, first_name, access_hash, scraped_from, timestamp}}
    'invited_users': set(),  # user_id های دعوت شده موفق
    'failed_users': set(),  # user_id های ناموفق
    'sent_pm': set(),  # user_id هایی که PM دریافت کرده‌اند
    'checked_groups': set(),  # group_id های چک شده
    'our_group_members': set(),  # اعضای گروه خودمان @PharmaWebGp - نباید به آنها پیام بفرستیم
    'contacted_users': set()  # 🆕 همه کاربرانی که قبلاً با آنها ارتباط داشته‌ایم (هر نوع)
}

# شناسه گروه خودمان (بعداً پر می‌شود)
our_group_id = None

# فایل ذخیره کلمات یادگرفته شده
KEYWORDS_DB_FILE = "learned_keywords.json"


# ═══════════════════════════════════════════════════════════════════════════════
# 🔒 سیستم جلوگیری از ارسال پیام تکراری (NO DUPLICATE MESSAGE SYSTEM)
# ═══════════════════════════════════════════════════════════════════════════════
# ⚠️ مهم: به هر کاربر فقط 1 پیام ارسال می‌شود - بدون استثنا!
# ═══════════════════════════════════════════════════════════════════════════════

def has_previous_contact(user_id):
    """
    بررسی آیا قبلاً با این کاربر ارتباط داشته‌ایم
    
    این تابع بررسی می‌کند:
    1. آیا قبلاً PM ارسال شده؟
    2. آیا قبلاً دعوت شده؟
    3. آیا در لیست contacted_users است؟
    4. آیا در لیست failed_users است؟
    
    Returns:
        bool: True اگر قبلاً ارتباط داشته‌ایم، False در غیر این صورت
    """
    user_id_str = str(user_id)
    
    # بررسی در sent_pm
    if user_id_str in members_db.get('sent_pm', set()):
        return True
    if user_id in members_db.get('sent_pm', set()):
        return True
    
    # بررسی در invited_users
    if user_id_str in members_db.get('invited_users', set()):
        return True
    if user_id in members_db.get('invited_users', set()):
        return True
    
    # بررسی در contacted_users
    if user_id_str in members_db.get('contacted_users', set()):
        return True
    if user_id in members_db.get('contacted_users', set()):
        return True
    
    # بررسی در failed_users (یعنی قبلاً تلاش شده)
    if user_id_str in members_db.get('failed_users', set()):
        return True
    if user_id in members_db.get('failed_users', set()):
        return True
    
    # بررسی در pm_responded (کاربرانی که پاسخ دادند)
    if user_id_str in pm_responded:
        return True
    if user_id in pm_responded:
        return True
    
    return False


def mark_user_contacted(user_id, contact_type='pm'):
    """
    ثبت ارتباط با کاربر
    
    Args:
        user_id: شناسه کاربر
        contact_type: نوع ارتباط ('pm', 'invite', 'failed')
    """
    user_id_str = str(user_id)
    
    # اضافه به contacted_users (لیست اصلی)
    if 'contacted_users' not in members_db:
        members_db['contacted_users'] = set()
    members_db['contacted_users'].add(user_id_str)
    
    # اضافه به لیست مربوطه
    if contact_type == 'pm':
        members_db['sent_pm'].add(user_id_str)
    elif contact_type == 'invite':
        members_db['invited_users'].add(user_id_str)
    elif contact_type == 'failed':
        members_db['failed_users'].add(user_id_str)


def can_send_pm_to_user(user_id):
    """
    آیا می‌توان به این کاربر پیام فرستاد؟
    
    ⚠️ قانون اصلی: فقط 1 پیام به هر کاربر - بدون استثنا!
    
    Returns:
        tuple: (can_send: bool, reason: str)
    """
    user_id_str = str(user_id)
    
    # 1. بررسی ارتباط قبلی
    if has_previous_contact(user_id):
        return (False, "قبلاً ارتباط داشته‌ایم")
    
    # 2. بررسی اینکه عضو گروه ما نباشد
    if is_our_group_member(user_id_str):
        return (False, "عضو گروه خودمان است")
    
    return (True, "OK")


async def check_existing_chat_with_user(user_id):
    """
    بررسی آیا قبلاً چتی با این کاربر باز شده است
    
    ⚠️ این تابع چک می‌کند:
    1. آیا دیالوگ/چتی با این کاربر وجود دارد
    2. آیا قبلاً پیامی به این کاربر ارسال شده
    
    Args:
        user_id: شناسه کاربر
        
    Returns:
        bool: True اگر چت قبلی وجود دارد
    """
    try:
        # بررسی در حافظه محلی (سریع‌تر)
        if has_previous_contact(user_id):
            return True
        
        # بررسی دیالوگ‌های موجود (اگر نیاز باشد)
        # این بخش فقط در صورت نیاز فعال می‌شود چون API call دارد
        # try:
        #     dialogs = await client.get_dialogs(limit=100)
        #     for dialog in dialogs:
        #         if hasattr(dialog.entity, 'id') and str(dialog.entity.id) == str(user_id):
        #             mark_user_contacted(user_id, 'pm')
        #             return True
        # except:
        #     pass
        
        return False
    except Exception:
        return False


# آمار عملکرد
stats = {
    'messages_sent': 0,
    'messages_edited': 0,
    'groups_joined': 0,
    'searches_done': 0,
    'start_time': None,
    'groups_cleaned': 0,
    'memory_cleaned': 0,
    'members_scraped': 0,
    'members_invited': 0,
    'invite_success': 0,
    'invite_failed': 0,
    'pm_sent': 0,
    'pm_failed': 0,
    'groups_left': 0,
    'flood_waits': 0,
    'consecutive_fails': 0,
    'last_success_time': None,
    'health_status': 'excellent'  # excellent, good, fair, poor, critical
}

# 🧠 Adaptive Rate Control
adaptive_control = {
    'current_search_interval': SEARCH_INTERVAL,
    'current_join_limit': JOIN_LIMIT_PER_CYCLE,
    'last_flood_wait': 0,
    'flood_wait_count': 0,
    'consecutive_success': 0,
    'speed_mode': 'normal'  # slow, normal, fast, turbo
}

# 🧠 توابع Intelligent Management
def calculate_health_status():
    """محاسبه وضعیت سلامت سیستم"""
    try:
        # بررسی نرخ موفقیت
        if stats['searches_done'] > 0:
            success_rate = (stats['groups_joined'] / stats['searches_done']) * 100
        else:
            success_rate = 0
        
        # بررسی FloodWait
        flood_ratio = stats['flood_waits'] / max(stats['groups_joined'], 1)
        
        # بررسی خطاهای متوالی
        consecutive_fails = stats['consecutive_fails']
        
        # تعیین وضعیت
        if consecutive_fails >= 10 or flood_ratio > 0.5:
            return 'critical'
        elif consecutive_fails >= 7 or flood_ratio > 0.3:
            return 'poor'
        elif consecutive_fails >= 5 or flood_ratio > 0.2:
            return 'fair'
        elif success_rate > 5:
            return 'excellent'
        else:
            return 'good'
    except:
        return 'good'

def adjust_adaptive_speed():
    """تنظیم هوشمند سرعت بر اساس وضعیت"""
    health = calculate_health_status()
    stats['health_status'] = health
    
    if health == 'critical':
        adaptive_control['current_search_interval'] = SEARCH_INTERVAL_MAX
        adaptive_control['current_join_limit'] = JOIN_LIMIT_MIN
        adaptive_control['speed_mode'] = 'slow'
    elif health == 'poor':
        adaptive_control['current_search_interval'] = SEARCH_INTERVAL * 2
        adaptive_control['current_join_limit'] = JOIN_LIMIT_MIN + 5
        adaptive_control['speed_mode'] = 'slow'
    elif health == 'fair':
        adaptive_control['current_search_interval'] = SEARCH_INTERVAL * 1.5
        adaptive_control['current_join_limit'] = int(JOIN_LIMIT_PER_CYCLE * 0.7)
        adaptive_control['speed_mode'] = 'normal'
    elif health == 'good':
        adaptive_control['current_search_interval'] = SEARCH_INTERVAL
        adaptive_control['current_join_limit'] = JOIN_LIMIT_PER_CYCLE
        adaptive_control['speed_mode'] = 'normal'
    else:  # excellent
        adaptive_control['current_search_interval'] = max(SEARCH_INTERVAL_MIN, SEARCH_INTERVAL - 1)
        adaptive_control['current_join_limit'] = min(JOIN_LIMIT_MAX, JOIN_LIMIT_PER_CYCLE + 10)
        adaptive_control['speed_mode'] = 'turbo'

async def smart_leave_old_groups():
    """خروج هوشمند از گروه‌های قدیمی برای فضای بیشتر"""
    if len(groups) >= GROUP_CLEANUP_THRESHOLD:
        logger.info(f"🔄 شروع پاکسازی هوشمند: {len(groups)} گروه")
        
        # خروج از گروه‌های قدیمی
        groups_to_leave = groups[:GROUP_LEAVE_BATCH]
        left_count = 0
        
        for group_id in groups_to_leave:
            try:
                entity = await client.get_entity(group_id)
                await client.delete_dialog(entity)
                remove_group_completely(group_id)
                left_count += 1
                stats['groups_left'] += 1
                await asyncio.sleep(0.5)
            except:
                remove_group_completely(group_id)
        
        logger.info(f"✅ {left_count} گروه قدیمی حذف شد - فضا برای گروه‌های جدید")
        return left_count
    return 0

# تابع حذف گروه با پاکسازی کامل
def remove_group_completely(group_id):
    """حذف کامل یک گروه از تمام دیکشنری‌ها"""
    removed = False
    
    if group_id in groups:
        groups.remove(group_id)
        removed = True
    
    if group_id in last_message_time:
        del last_message_time[group_id]
    
    if group_id in sent_messages:
        del sent_messages[group_id]
    
    if group_id in group_retry_count:
        del group_retry_count[group_id]
    
    if group_id in mirror_users:
        del mirror_users[group_id]
    
    if removed:
        stats['groups_cleaned'] += 1
    
    return removed

# تابع محاسبه تاخیر exponential backoff
def get_retry_delay(retry_count):
    """محاسبه تاخیر با exponential backoff"""
    return INITIAL_RETRY_DELAY * (2 ** retry_count)

# 🚀 بارگذاری کلمات اضافی از فایل JSON (اختیاری - اگر نباشه ربات بدون مشکل کار می‌کنه)
def load_extended_keywords():
    """بارگذاری کلمات اضافی از فایل extended_keywords.json (OPTIONAL)"""
    try:
        import json
        from pathlib import Path
        
        json_path = Path(__file__).parent / "extended_keywords.json"
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                extended = json.load(f)
                
            # اضافه کردن به BASE_KEYWORDS
            count = 0
            for category, words in extended.items():
                if category not in BASE_KEYWORDS:
                    BASE_KEYWORDS[category] = []
                BASE_KEYWORDS[category].extend(words)
                count += len(words)
            
            return count  # تعداد کلمات اضافه شده
        else:
            # فایل نیست - اشکالی نداره، با کلمات پایه کار می‌کنیم
            return 0
    except Exception as e:
        # هر خطایی - ربات با کلمات پایه ادامه میده
        pass
        return 0

# 🌍 کلمات عمومی و پرکاربرد برای جستجوی فراگیر (بدون محدودیت موضوعی)
UNIVERSAL_TERMS = [
    # کلمات فارسی پرجستجو
    "گروه", "گپ", "چت", "دوستی", "دوستان", "دورهمی", "پاتوق", "لینک", "لینکدونی",
    "تهران", "ایران", "کرج", "مشهد", "اصفهان", "شیراز", "تبریز", "اهواز",
    "دختر", "پسر", "عاشقانه", "رمانتیک", "همسریابی", "ازدواج", "صیغه",
    "دانشجو", "دانشگاه", "مدرسه", "کنکور", "درس", "جزوه", "استاد",
    "فیلم", "سریال", "موزیک", "آهنگ", "عکس", "کلیپ", "طنز", "جوک", "خنده",
    "بازی", "گیم", "گیمر", "پابجی", "کالاف", "کلش", "فیفا",
    "خبر", "اخبار", "ورزش", "فوتبال", "استقلال", "پرسپولیس",
    "خرید", "فروش", "دیوار", "شیپور", "بازار", "ارز", "دلار", "طلا", "سکه",
    "بورس", "ترید", "کریپتو", "بیتکوین", "اتریوم", "تتر",
    "کار", "استخدام", "کاریابی", "درآمد", "پول", "ثروت",
    "ماشین", "خودرو", "موتور", "املاک", "خانه", "آپارتمان",
    "آشپزی", "غذا", "سلامت", "پزشکی", "زیبایی", "آرایش",
    "مذهبی", "قرآن", "نماز", "هیئت", "خدا",
    "آزاد", "بحث", "گفتگو", "نقد", "سیاسی", "اجتماعی",
    
    # کلمات انگلیسی پرکاربرد
    "group", "chat", "gap", "gp", "pv", "link",
    "tehran", "iran", "karaj", "mashhad", "shiraz",
    "love", "dating", "friend", "friends", "friendly",
    "girl", "boy", "girls", "boys",
    "music", "movie", "film", "song", "video",
    "game", "gamer", "pubg", "cod", "clash",
    "crypto", "bitcoin", "btc", "eth", "tether", "usdt",
    "trade", "forex", "money", "business",
    "news", "sport", "football", "soccer",
    "shop", "store", "market", "buy", "sell",
    
    # حروف و ترکیبات کوتاه
    "گروه چت", "گروه دوستی", "گپ تهران", "چت روم", "گپ دوستانه",
    "chat group", "iran chat", "tehran gp", "crypto group"
]

# 🌍 تولید جستجوهای هدفمند با سیستم اولویت‌بندی سه‌لایه
def generate_smart_keywords(count=100):
    """
    🎯 تولید کلمات کلیدی هوشمند با اولویت‌بندی چهارلایه:
    
    🥇 اولویت 1 (30%): ترید، کریپتو، رمزارز، پراپ، فارکس
    🏥 اولویت 2 (25%): پزشکی، دارو، تجهیزات، آزمایشگاه، دندانپزشکی
    🥈 اولویت 3 (20%): مهاجرت، اقامت، ویزا، ایرانیان خارج
    🥉 اولویت 4 (15%): عمومی و SUPER_EFFECTIVE_KEYWORDS
    
    این تابع به صورت هوشمند کلمات را از هر دسته انتخاب می‌کند.
    """
    generated = set()
    
    try:
        # ═══════════════════════════════════════════════════════════════════════
        # 🚀🚀🚀 کلمات سوپر مؤثر - همیشه اول اضافه می‌شوند! 🚀🚀🚀
        # ═══════════════════════════════════════════════════════════════════════
        super_effective_count = int(count * 0.20)  # 20% از کل کلمات
        selected_super = random.sample(SUPER_EFFECTIVE_KEYWORDS, min(super_effective_count, len(SUPER_EFFECTIVE_KEYWORDS)))
        generated.update(selected_super)
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🥇 اولویت 1: ترید و رمزارز (30% از کلمات)
        # ═══════════════════════════════════════════════════════════════════════
        crypto_count = int(count * 0.30)
        
        all_crypto_words = []
        for subcat in CRYPTO_TRADING_KEYWORDS.values():
            all_crypto_words.extend(subcat)
        
        selected_crypto = random.sample(all_crypto_words, min(crypto_count, len(all_crypto_words)))
        generated.update(selected_crypto)
        
        crypto_prefixes = ["گروه", "کانال", "سیگنال", "آموزش", "رایگان", "VIP", "group", "signal", "free", "تحلیل", "analysis"]
        crypto_mains = ["ترید", "کریپتو", "بیتکوین", "فارکس", "پراپ", "فیوچرز", "اسپات", 
                        "trade", "crypto", "bitcoin", "forex", "futures", "spot",
                        "تون", "TON", "ترون", "TRX", "سولانا", "SOL", "BNB", "دوج", "DOGE",
                        "آربیتروم", "ARB", "اوپتیمیزم", "OP", "میم کوین", "meme coin",
                        "نات کوین", "NOT", "همستر", "hamster", "بازی تلگرام",
                        "ایردراپ", "airdrop", "NFT", "دیفای", "defi"]
        
        for _ in range(crypto_count // 4):
            prefix = random.choice(crypto_prefixes)
            main = random.choice(crypto_mains)
            generated.add(f"{prefix} {main}")
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🏥 اولویت 2: پزشکی و دارو (25% از کلمات) - 🆕 جدید
        # ═══════════════════════════════════════════════════════════════════════
        medical_count = int(count * 0.25)
        
        all_medical_words = []
        for subcat in MEDICAL_KEYWORDS.values():
            all_medical_words.extend(subcat)
        
        selected_medical = random.sample(all_medical_words, min(medical_count, len(all_medical_words)))
        generated.update(selected_medical)
        
        # ترکیبات ویژه پزشکی/دارو
        med_prefixes = ["گروه", "کانال", "انجمن", "فروش", "خرید", "تبادل", "group", "channel"]
        med_mains = ["دارو", "داروخانه", "پزشکی", "تجهیزات پزشکی", "آزمایشگاه", "دندانپزشکی",
                     "pharmacy", "medical", "drug", "dental", "lab", "equipment", "health"]
        med_cities = ["تهران", "مشهد", "اصفهان", "شیراز", "ایران", "iran", "persian"]
        
        for _ in range(medical_count // 4):
            prefix = random.choice(med_prefixes)
            main = random.choice(med_mains)
            generated.add(f"{prefix} {main}")
            city = random.choice(med_cities)
            generated.add(f"{main} {city}")
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🥈 اولویت 3: مهاجرت و اقامت (20% از کلمات)
        # ═══════════════════════════════════════════════════════════════════════
        immigration_count = int(count * 0.20)
        
        # جمع‌آوری همه کلمات مهاجرت
        all_immigration_words = []
        for subcat in IMMIGRATION_KEYWORDS.values():
            all_immigration_words.extend(subcat)
        
        # انتخاب تصادفی
        selected_immigration = random.sample(all_immigration_words, min(immigration_count, len(all_immigration_words)))
        generated.update(selected_immigration)
        
        # ترکیبات ویژه مهاجرت - گسترش‌یافته
        imm_prefixes = ["ایرانیان", "فارسی", "گروه", "مهاجرت", "iranian", "persian", "اقامت", "ویزا"]
        imm_cities = ["استانبول", "دبی", "ترکیه", "امارات", "کانادا", "آلمان", "لندن",
                      "istanbul", "dubai", "turkey", "canada", "germany", "london",
                      "تورنتو", "ونکوور", "برلین", "سیدنی", "پاریس", "آمستردام",
                      "toronto", "vancouver", "berlin", "sydney", "paris",
                      "اربیل", "ایروان", "تفلیس", "باتومی", "مونترال"]
        
        for _ in range(immigration_count // 4):
            prefix = random.choice(imm_prefixes)
            city = random.choice(imm_cities)
            generated.add(f"{prefix} {city}")
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🥉 اولویت 4: عمومی (15% از کلمات)
        # ═══════════════════════════════════════════════════════════════════════
        general_count = int(count * 0.15)
        
        # جمع‌آوری کلمات عمومی
        all_general_words = []
        for subcat in GENERAL_KEYWORDS.values():
            all_general_words.extend(subcat)
        
        # انتخاب تصادفی
        if all_general_words:
            selected_general = random.sample(all_general_words, min(general_count, len(all_general_words)))
            generated.update(selected_general)
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🧠 ترکیب با سیستم‌های یادگیری (bonus)
        # ═══════════════════════════════════════════════════════════════════════
        
        # کلمات موفق از تاریخچه
        try:
            successful = learned_keywords.get('successful', {})
            sorted_successful = sorted(successful.items(), key=lambda x: x[1], reverse=True)
            top_successful = [kw for kw, _ in sorted_successful[:30]]
            generated.update(top_successful)
        except:
            pass
        
        # کلمات استخراج شده از گروه‌ها
        try:
            extracted = list(learned_keywords.get('extracted', set()))[:20]
            generated.update(extracted)
        except:
            pass
        
        # ═══════════════════════════════════════════════════════════════════════
        # 🎲 نهایی‌سازی و مرتب‌سازی هوشمند
        # ═══════════════════════════════════════════════════════════════════════
        
        result = list(generated)
        random.shuffle(result)
        
        # مرتب‌سازی بر اساس موفقیت قبلی
        try:
            def keyword_priority(kw):
                success = learned_keywords.get('successful', {}).get(kw, 0)
                fail = learned_keywords.get('failed', {}).get(kw, 0)
                total = success + fail
                if total > 0:
                    return success / total + (success / 100)
                return 0.5
            
            result.sort(key=keyword_priority, reverse=True)
            
            # 30% تصادفی برای اکتشاف
            explore_count = len(result) // 3
            explore_part = result[explore_count:]
            random.shuffle(explore_part)
            result = result[:explore_count] + explore_part
        except:
            pass
        
        # محدود کردن به تعداد درخواستی
        final = result[:count] if len(result) > count else result
        
        # اطمینان از حداقل تعداد
        if len(final) < 50:
            fallback_keywords = [
                # ترید/کریپتو (اولویت 1)
                "ترید", "کریپتو", "بیتکوین", "سیگنال", "فارکس", "پراپ",
                "trade", "crypto", "bitcoin", "signal", "forex",
                # مهاجرت (اولویت 2)
                "ایرانی استانبول", "ایرانیان ترکیه", "فارسی دبی", "مهاجرت کانادا",
                "iranian istanbul", "persian dubai", "expat turkey",
            ]
            for bk in fallback_keywords:
                if len(final) >= count:
                    break
                if bk not in final:
                    final.append(bk)
        
        slog(f"🎯 {len(final)} کلمه کلیدی تولید شد (70% ترید | 25% مهاجرت | 5% عمومی)")
        return final
        
    except Exception as e:
        slog(f"❌ خطا در تولید جستجوها: {e}")
        # Fallback با اولویت ترید/کریپتو
        fallback = [
            "ترید", "تریدر", "کریپتو", "بیتکوین", "سیگنال", "فارکس", "پراپ",
            "trade", "trader", "crypto", "bitcoin", "signal", "forex", "prop",
            "ایرانی استانبول", "ایرانیان ترکیه", "مهاجرت", "دبی",
        ]
        return fallback[:count] if count <= len(fallback) else fallback


# 🎯 استخراج کلمات کلیدی از عنوان گروه (با فیلتر قوی)
def extract_keywords_from_title(title):
    """استخراج کلمات کلیدی مفید از عنوان گروه"""
    if not title or not isinstance(title, str):
        return []
    
    # حذف کاراکترهای خاص و ایموجی
    clean_title = re.sub(r'[^\w\s\u0600-\u06FF]', ' ', title.lower())
    words = clean_title.split()
    
    keywords = []
    
    # کلمات مرتبط با پزشکی
    medical_indicators = [
        'دارو', 'پزشک', 'درمان', 'سلامت', 'داروخانه', 'دکتر',
        'کلینیک', 'بیمارستان', 'طب', 'بهداشت', 'آزمایش', 'تشخیص',
        'مشاوره', 'ویزیت', 'نسخه', 'قرص', 'آمپول'
    ]
    
    # 💰 کلمات مرتبط با کریپتو و سرمایه‌گذاری
    crypto_indicators = [
        'بیتکوین', 'bitcoin', 'btc', 'اتریوم', 'ethereum', 'eth',
        'ارز', 'کریپتو', 'crypto', 'رمز', 'دیجیتال', 'بلاکچین',
        'ترید', 'trade', 'معامله', 'سیگنال', 'signal', 'ایردراپ',
        'airdrop', 'بایننس', 'binance', 'صرافی', 'تتر', 'usdt',
        'سرمایه', 'درآمد', 'بورس', 'فارکس', 'forex', 'طلا',
        'دلار', 'پامپ', 'pump', 'هولد', 'hodl', 'استیک', 'stake'
    ]
    
    # کلمات غیرمفید (فیلتر)
    stopwords = [
        'گروه', 'کانال', 'چنل', 'channel', 'group', 'chat', 'تلگرام',
        'telegram', 'رایگان', 'free', 'join', 'link', 'عضو', 'member',
        'تبلیغات', 'آگهی', 'اسپم', 'spam'
    ]
    
    # بررسی کلمات
    for word in words:
        word = word.strip()
        
        # فیلتر کلمات کوتاه و غیرمفید
        if len(word) < 3 or word in stopwords:
            continue
        
        # فقط فارسی یا انگلیسی
        if not re.match(r'^[\u0600-\u06FFa-zA-Z]+$', word):
            continue
        
        # اولویت به کلمات پزشکی
        if any(indicator in word for indicator in medical_indicators):
            if word not in keywords:
                keywords.append(word)
        # اولویت به کلمات کریپتو و سرمایه‌گذاری
        elif any(indicator in word for indicator in crypto_indicators):
            if word not in keywords:
                keywords.append(word)
        # یا کلمه بزرگ و مفید
        elif len(word) >= 4 and len(word) <= 15:
            if word not in keywords:
                keywords.append(word)
    
    return keywords[:5]  # حداکثر 5 کلمه

# 📊 رتبه‌بندی کلمات بر اساس موفقیت (بهبود یافته)
def rank_keywords_by_success():
    """مرتب‌سازی کلمات بر اساس نرخ موفقیت"""
    ranked = []
    
    try:
        for keyword, success in learned_keywords.get('successful', {}).items():
            fail = learned_keywords.get('failed', {}).get(keyword, 0)
            total = success + fail
            
            if total >= 3:  # حداقل 3 بار استفاده شده باشد
                success_rate = success / total
                
                # فرمول امتیازدهی پیشرفته:
                # 1. نرخ موفقیت (0-1)
                # 2. بونوس استفاده: کلماتی که بیشتر استفاده شدند
                # 3. پنالتی برای کلمات خیلی طولانی (> 30 کاراکتر)
                
                usage_bonus = min(total / 10, 2)  # حداکثر 2x
                length_penalty = 1.0
                
                if len(keyword) > 30:
                    length_penalty = 0.5  # کلمات خیلی طولانی نامطلوب هستند
                elif len(keyword) < 3:
                    length_penalty = 0.3  # کلمات خیلی کوتاه نامطلوب
                
                score = success_rate * (1 + usage_bonus) * length_penalty
                ranked.append((keyword, score, success, fail))
        
        # مرتب‌سازی نزولی
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked
        
    except Exception as e:
        logger.error(f"❌ خطا در rank_keywords_by_success: {e}")
        return []

# 💾 بارگذاری کلمات یادگرفته شده
def load_learned_keywords():
    """بارگذاری کلمات یادگرفته شده از فایل"""
    global learned_keywords
    try:
        if Path(KEYWORDS_DB_FILE).exists():
            with open(KEYWORDS_DB_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                learned_keywords['successful'] = data.get('successful', {})
                learned_keywords['failed'] = data.get('failed', {})
                learned_keywords['extracted'] = set(data.get('extracted', []))
                logger.info(f"✅ کلمات یادگرفته شده بارگذاری شد: {len(learned_keywords['successful'])} موفق")
    except Exception as e:
        logger.error(f"❌ خطا در بارگذاری کلمات: {e}")

# 💾 ذخیره کلمات یادگرفته شده
def save_learned_keywords():
    """ذخیره کلمات یادگرفته شده در فایل"""
    try:
        data = {
            'successful': learned_keywords['successful'],
            'failed': learned_keywords['failed'],
            'extracted': list(learned_keywords['extracted'])
        }
        with open(KEYWORDS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره کلمات: {e}")

# 🔄 به‌روزرسانی کلمات بر اساس نتایج (با مدیریت حافظه)
def update_keyword_performance(keyword, found_groups_count):
    """به‌روزرسانی عملکرد کلمه بر اساس نتایج"""
    try:
        if found_groups_count > 0:
            learned_keywords['successful'][keyword] = learned_keywords['successful'].get(keyword, 0) + found_groups_count
        else:
            learned_keywords['failed'][keyword] = learned_keywords['failed'].get(keyword, 0) + 1
        
        # 🧹 پاکسازی کلمات ضعیف (نرخ موفقیت < 10% و حداقل 20 تلاش)
        total = learned_keywords['successful'].get(keyword, 0) + learned_keywords['failed'].get(keyword, 0)
        if total >= 20:
            success_rate = (learned_keywords['successful'].get(keyword, 0) / total) * 100
            if success_rate < 10:
                # حذف کلمه ضعیف
                learned_keywords['failed'].pop(keyword, None)
                learned_keywords['successful'].pop(keyword, None)
                if keyword in learned_keywords['extracted']:
                    learned_keywords['extracted'].remove(keyword)
                logger.info(f"🗑️ حذف کلمه ضعیف: '{keyword}' (نرخ: {success_rate:.0f}%)")
        
        # 🧹 محدود کردن حافظه (حداکثر 500 کلمه)
        if len(learned_keywords['successful']) + len(learned_keywords['failed']) > 500:
            # حذف کلمات با کمترین استفاده
            all_kws = {**learned_keywords['successful'], **learned_keywords['failed']}
            sorted_kws = sorted(all_kws.items(), key=lambda x: x[1])
            
            # حذف 100 کلمه ضعیف
            for kw, _ in sorted_kws[:100]:
                learned_keywords['successful'].pop(kw, None)
                learned_keywords['failed'].pop(kw, None)
            
            logger.info("🧹 پاکسازی حافظه: 100 کلمه ضعیف حذف شد")
        
        # ذخیره هر 10 جستجو یکبار
        if (learned_keywords['successful'].get(keyword, 0) + learned_keywords['failed'].get(keyword, 0)) % 10 == 0:
            save_learned_keywords()
            
    except Exception as e:
        logger.error(f"❌ خطا در update_keyword_performance: {e}")

# تابع بارگذاری حافظه
def load_members_db():
    """بارگذاری دیتابیس اعضا از فایل - بهینه برای Railway"""
    global members_db
    try:
        if Path(MEMBERS_DB_FILE).exists():
            with open(MEMBERS_DB_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # تبدیل list به set برای invited و failed
                members_db['scraped_users'] = data.get('scraped_users', {})
                members_db['invited_users'] = set(data.get('invited_users', []))
                members_db['failed_users'] = set(data.get('failed_users', []))
                members_db['sent_pm'] = set(data.get('sent_pm', []))
                members_db['checked_groups'] = set(data.get('checked_groups', []))
                members_db['our_group_members'] = set(data.get('our_group_members', []))
                members_db['contacted_users'] = set(data.get('contacted_users', []))  # 🆕
                
                # 🚂 Railway: محدود کردن اندازه در بارگذاری
                if RAILWAY_MODE == 'eco':
                    if len(members_db['scraped_users']) > MAX_SCRAPED_USERS:
                        # حفظ فقط کاربران جدیدتر
                        sorted_users = sorted(
                            members_db['scraped_users'].items(),
                            key=lambda x: x[1].get('timestamp', 0),
                            reverse=True
                        )
                        members_db['scraped_users'] = dict(sorted_users[:MAX_SCRAPED_USERS])
                    
                    # محدود کردن set ها
                    for set_name in ['invited_users', 'failed_users', 'sent_pm', 'checked_groups']:
                        railway_manager.limit_set_size(members_db[set_name], MAX_MEMORY_ITEMS)
                
                logger.info(f"✅ حافظه بارگذاری شد: {len(members_db['scraped_users'])} کاربر")
        else:
            # 🚂 Railway: فایل وجود ندارد - شروع با حافظه خالی
            logger.info("📝 Railway: شروع با حافظه خالی (فایل وجود ندارد)")
    except json.JSONDecodeError:
        # 🚂 Railway: فایل خراب - شروع با حافظه خالی
        logger.warning("⚠️ Railway: فایل JSON خراب - شروع با حافظه خالی")
    except Exception as e:
        logger.error(f"❌ خطا در بارگذاری حافظه: {e}")

# تابع ذخیره حافظه
def save_members_db():
    """ذخیره دیتابیس اعضا در فایل"""
    try:
        data = {
            'scraped_users': members_db['scraped_users'],
            'invited_users': list(members_db['invited_users']),
            'failed_users': list(members_db['failed_users']),
            'sent_pm': list(members_db['sent_pm']),
            'checked_groups': list(members_db['checked_groups']),
            'our_group_members': list(members_db['our_group_members']),
            'contacted_users': list(members_db.get('contacted_users', set()))  # 🆕
        }
        with open(MEMBERS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره حافظه: {e}")

# تابع بارگذاری اعضای گروه خودمان
async def load_our_group_members():
    """بارگذاری اعضای گروه @PharmaWebGp برای عدم ارسال پیام به آنها"""
    global our_group_id
    try:
        # دریافت entity گروه خودمان
        target_entity = await client.get_entity(TARGET_GROUP)
        our_group_id = target_entity.id
        
        logger.info(f"🏠 بارگذاری اعضای گروه خودمان: {target_entity.title}")
        
        # دریافت اعضای گروه
        participants = await client(GetParticipantsRequest(
            channel=target_entity,
            filter=ChannelParticipantsRecent(),
            offset=0,
            limit=500,  # حداکثر 500 عضو اخیر
            hash=0
        ))
        
        new_members_count = 0
        for user in participants.users:
            if not user.bot:
                user_id = str(user.id)
                if user_id not in members_db['our_group_members']:
                    members_db['our_group_members'].add(user_id)
                    new_members_count += 1
        
        logger.info(f"✅ {len(members_db['our_group_members'])} عضو گروه ما شناسایی شد ({new_members_count} جدید)")
        save_members_db()
        
    except Exception as e:
        logger.error(f"❌ خطا در بارگذاری اعضای گروه ما: {e}")

# تابع بررسی آیا کاربر عضو گروه ماست
def is_our_group_member(user_id):
    """بررسی آیا کاربر عضو گروه @PharmaWebGp است"""
    return str(user_id) in members_db['our_group_members']

# ═══════════════════════════════════════════════════════════════════════════════
# 💾💾💾 سیستم ذخیره و بارگذاری هوش مصنوعی 💾💾💾
# ═══════════════════════════════════════════════════════════════════════════════

AI_STATE_FILE = "ai_learning_state.json"

def save_ai_state():
    """ذخیره وضعیت سیستم‌های هوشمند"""
    try:
        state = {
            # RL Agent state
            'rl_agent': {
                'q_table': rl_agent.q_table,
                'exploration_rate': rl_agent.exploration_rate,
                'state_visits': rl_agent.state_visits,
                'total_actions': len(rl_agent.action_history),
                'total_reward': sum(rl_agent.reward_history) if rl_agent.reward_history else 0
            },
            
            # Network Discovery state
            'network_discovery': {
                'high_value_keywords': list(network_discovery.high_value_keywords),
                'group_count': len(network_discovery.group_members)
            },
            
            # Quality Predictor state
            'quality_predictor': {
                'feature_weights': quality_predictor.feature_weights,
                'history_count': len(quality_predictor.quality_history)
            },
            
            # Smart Selector state
            'smart_selector': {
                'time_performance': {
                    str(h): {
                        kw: {'success': d['success'], 'total': d['total']}
                        for kw, d in kws.items()
                    }
                    for h, kws in smart_selector.time_performance.items()
                }
            },
            
            # Metadata
            'saved_at': time.time(),
            'total_groups_joined': stats.get('groups_joined', 0)
        }
        
        with open(AI_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 وضعیت AI ذخیره شد (Q-table: {len(rl_agent.q_table)} ورودی)")
        
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره وضعیت AI: {e}")

def load_ai_state():
    """بارگذاری وضعیت سیستم‌های هوشمند"""
    try:
        if not Path(AI_STATE_FILE).exists():
            logger.info("🆕 فایل وضعیت AI وجود ندارد - شروع از صفر")
            return
        
        with open(AI_STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        
        # بارگذاری RL Agent
        if 'rl_agent' in state:
            rl_data = state['rl_agent']
            rl_agent.q_table = rl_data.get('q_table', {})
            rl_agent.exploration_rate = rl_data.get('exploration_rate', 0.2)
            rl_agent.state_visits = rl_data.get('state_visits', {})
            logger.info(f"🤖 RL Agent بارگذاری شد: Q-table={len(rl_agent.q_table)}")
        
        # بارگذاری Network Discovery
        if 'network_discovery' in state:
            net_data = state['network_discovery']
            network_discovery.high_value_keywords = set(net_data.get('high_value_keywords', []))
            logger.info(f"🌐 Network Discovery بارگذاری شد: {len(network_discovery.high_value_keywords)} کلمه")
        
        # بارگذاری Quality Predictor
        if 'quality_predictor' in state:
            qp_data = state['quality_predictor']
            quality_predictor.feature_weights = qp_data.get('feature_weights', quality_predictor.feature_weights)
            logger.info(f"📈 Quality Predictor بارگذاری شد")
        
        # بارگذاری Smart Selector
        if 'smart_selector' in state:
            ss_data = state['smart_selector']
            time_perf = ss_data.get('time_performance', {})
            smart_selector.time_performance = {
                int(h): kws for h, kws in time_perf.items()
            }
            logger.info(f"🎯 Smart Selector بارگذاری شد: {len(smart_selector.time_performance)} ساعت")
        
        logger.info("✅ وضعیت AI با موفقیت بارگذاری شد!")
        
    except Exception as e:
        logger.error(f"❌ خطا در بارگذاری وضعیت AI: {e}")

# پیام دعوت برای PM
INVITE_PM_MESSAGE = f"""
سلام 👋

شما را به گروه **تبادل و یافتن داروهای خاص و کمیاب** دعوت می‌کنیم:

🔗 {GROUP_LINK}

در این گروه می‌توانید:
✅ داروهای کمیاب را پیدا کنید
✅ داروهای اضافی خود را تبادل کنید
✅ از تخفیف‌های ویژه داروخانه‌ها استفاده کنید

منتظر حضور شما هستیم! 💊
"""

# تابع ارسال PM دعوت
async def send_invite_pm(user_id, user_info):
    """ارسال پیام خصوصی دعوت هوشمند به کاربر"""
    try:
        # 🔑 گرفتن entity کاربر برای ارسال PM
        try:
            user_entity = await client.get_entity(int(user_id))
        except Exception as entity_err:
            # اگر نتونستیم entity بگیریم، با username امتحان کن
            if user_info.get('username'):
                try:
                    user_entity = await client.get_entity(user_info['username'])
                except:
                    raise Exception(f"نمی‌تونیم entity کاربر رو پیدا کنیم (ID: {user_id})")
            else:
                raise Exception(f"نمی‌تونیم entity کاربر رو پیدا کنیم (ID: {user_id}, بدون username)")
        
        # 🎯 ساخت پیام شخصی‌سازی شده
        personalized_message = smart_inviter.generate_personalized_message(user_info)
        
        # ارسال PM
        await client.send_message(user_entity, personalized_message)
        members_db['sent_pm'].add(user_id)
        stats['pm_sent'] += 1
        logger.info(f"✅ PM ارسال شد به @{user_info.get('username', 'Unknown')}")
        save_members_db()
        return True
        
    except UserPrivacyRestrictedError:
        logger.warning(f"❌ PM غیرفعال: @{user_info.get('username', 'Unknown')}")
        members_db['failed_users'].add(user_id)
        stats['pm_failed'] += 1
        save_members_db()
        return False
    except UserIsBlockedError:
        logger.warning(f"🚫 بلاک شدیم توسط: @{user_info.get('username', 'Unknown')}")
        members_db['failed_users'].add(user_id)
        stats['pm_failed'] += 1
        save_members_db()
        return False
    except (UserIdInvalidError, InputUserDeactivatedError):
        logger.warning(f"⚠️ اکانت غیرفعال یا حذف شده: @{user_info.get('username', 'Unknown')}")
        members_db['failed_users'].add(user_id)
        stats['pm_failed'] += 1
        save_members_db()
        return False
    except FloodWaitError as e:
        logger.warning(f"⚠️ FloodWait در PM: {e.seconds} ثانیه")
        raise  # پاس به بالا برای مدیریت
    except Exception as e:
        error_msg = str(e)
        if "entity" in error_msg.lower():
            logger.warning(f"⚠️ نمی‌تونیم به @{user_info.get('username', 'Unknown')} PM بفرستیم (entity نامعتبر)")
        else:
            logger.error(f"❌ خطا در PM به @{user_info.get('username', 'Unknown')}: {error_msg[:100]}")
        members_db['failed_users'].add(user_id)
        stats['pm_failed'] += 1
        save_members_db()
        return False

# تابع فیلتر کاربران فعال - اصلاح شده
def is_active_user(user):
    """بررسی فعال بودن کاربر - نسخه بهینه"""
    if not isinstance(user, User):
        return False
    
    # چک کردن بات نباشد
    if user.bot:
        return False
    
    # بررسی access_hash معتبر - بدون آن نمی‌توانیم دعوت کنیم
    if not user.access_hash:
        return False
    
    # ترجیح کاربرانی با username (ولی اجباری نیست)
    # حذف شرط username برای افزایش تعداد کاربران
    
    # چک وضعیت آنلاین - قبول همه وضعیت‌ها به جز deleted
    status = user.status
    if isinstance(status, (UserStatusOnline, UserStatusRecently)):
        return True
    
    if isinstance(status, UserStatusLastWeek):
        return True
    
    # حتی کاربران بدون وضعیت مشخص هم قبول شوند
    # (بعضی کاربران حریم خصوصی دارند)
    if status is None:
        return True
    
    return True  # قبول بقیه وضعیت‌ها

# تابع پاکسازی حافظه
def cleanup_old_messages():
    """پاک کردن پیام‌های قدیمی‌تر از 24 ساعت از حافظه"""
    current_time = time.time()
    cleaned_count = 0
    
    for group_id in list(sent_messages.keys()):
        messages = sent_messages[group_id]
        # فیلتر پیام‌های جدیدتر از 24 ساعت
        new_messages = [(msg_id, timestamp) for msg_id, timestamp in messages 
                       if current_time - timestamp < MESSAGE_RETENTION]
        
        cleaned_count += len(messages) - len(new_messages)
        
        if new_messages:
            sent_messages[group_id] = new_messages
        else:
            del sent_messages[group_id]
    
    stats['memory_cleaned'] += cleaned_count
    return cleaned_count

# تابع نمایش آمار
async def show_stats():
    """نمایش آمار هر 10 دقیقه"""
    while True:
        await asyncio.sleep(600)  # هر 10 دقیقه
        
        if stats['start_time']:
            uptime = time.time() - stats['start_time']
            hours = int(uptime // 3600)
            minutes = int((uptime % 3600) // 60)
            
            logger.info("=" * 60)
            logger.info("📊 آمار عملکرد ربات:")
            logger.info(f"   ⏱️ زمان فعالیت: {hours} ساعت و {minutes} دقیقه")
            logger.info(f"   📱 تعداد گروه‌ها: {len(groups)}")
            logger.info(f"   ✉️ پیام‌های ارسالی: {stats['messages_sent']}")
            logger.info(f"   ✏️ پیام‌های ویرایش شده: {stats['messages_edited']}")
            logger.info(f"   ➕ گروه‌های جدید: {stats['groups_joined']}")
            logger.info(f"   🗑️ گروه‌های پاکسازی شده: {stats['groups_cleaned']}")
            logger.info(f"   💾 پیام‌های پاک شده از حافظه: {stats['memory_cleaned']}")
            logger.info(f"   🔍 جستجوهای انجام شده: {stats['searches_done']}")
            
            # 🎯 آمار دعوت اعضا (بهینه شده)
            total_scraped = len(members_db.get('scraped_users', {}))
            total_invited = len(members_db.get('invited_users', set()))
            total_failed = len(members_db.get('failed_users', set()))
            total_pm_sent = len(members_db.get('sent_pm', set()))
            pending_invites = total_scraped - total_invited - total_failed
            
            logger.info("-" * 60)
            logger.info("⚔️ آمار سیستم دعوت به @PharmaWebGp:")
            logger.info(f"   👥 کل Scrape شده: {total_scraped}")
            logger.info(f"   ⏳ در صف دعوت: {pending_invites}")
            logger.info(f"   ✅ موفق Add: {stats['invite_success']}")
            logger.info(f"   📨 PM ارسالی: {total_pm_sent}")
            logger.info(f"   ❌ ناموفق: {total_failed}")
            logger.info(f"   📊 نرخ موفقیت: {(stats['invite_success'] / max(total_invited, 1) * 100):.1f}%")
            logger.info(f"   🎯 پیشرفت روزانه: {stats['invite_success']}/{DAILY_INVITE_TARGET} ({(stats['invite_success']/DAILY_INVITE_TARGET*100):.1f}%)")
            logger.info(f"   ⚡ سرعت: {(stats['invite_success'] / max(uptime/3600, 0.1)):.1f} عضو/ساعت")
            
            # 🛡️ آمار سیستم محافظت
            protection_status = anti_spam.get_status_report()
            logger.info("-" * 40)
            logger.info("🛡️ وضعیت محافظت:")
            logger.info(f"   💚 سلامت سیستم: {protection_status['health_score']:.0f}/100")
            logger.info(f"   🔄 حالت: {protection_status['mode']}")
            logger.info(f"   ⚠️ ریسک FloodWait: {protection_status['flood_risk']}")
            logger.info(f"   📈 نرخ خطا: {protection_status['error_rate']:.1%}")
            
            # 📈 آمار سیستم‌های تبلیغاتی
            logger.info("-" * 40)
            logger.info("📈 سیستم‌های تبلیغاتی:")
            logger.info(f"   🌐 محتوای تولیدی: {content_engine.content_stats['generated']}")
            logger.info(f"   💬 تعاملات: {engagement_booster.engagement_stats['posts']}")
            logger.info(f"   ⏰ ضریب زمانی: {time_optimizer.get_current_multiplier():.2f}")
            
            # قیف تبدیل
            funnel = funnel_analytics.get_funnel_report()
            logger.info(f"   🎯 Awareness: {funnel['stages'].get('awareness', 0)}")
            logger.info(f"   🎯 Conversion: {funnel['stages'].get('conversion', 0)}")
            
            # 🚫 آمار Blacklist دائمی
            logger.info("-" * 40)
            logger.info("🚫 آمار Blacklist دائمی:")
            logger.info(f"   📊 کل گروه‌های blacklist: {len(permanent_blacklist)}")
            
            # شمارش دلایل
            reason_counts = {}
            for gid, info in permanent_blacklist_reasons.items():
                reason = info.get('reason', 'unknown')
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
            
            for reason, count in reason_counts.items():
                reason_text = BLACKLIST_REASONS.get(reason, reason)
                logger.info(f"   • {reason_text}: {count}")
            
            # محاسبه میانگین‌ها
            if hours > 0:
                avg_groups_per_hour = stats['groups_joined'] / hours
                avg_messages_per_hour = stats['messages_sent'] / hours
                logger.info("-" * 40)
                logger.info(f"   📈 میانگین عضویت در ساعت: {avg_groups_per_hour:.1f}")
                logger.info(f"   📈 میانگین پیام در ساعت: {avg_messages_per_hour:.1f}")
            
            # ریست آمار روزانه
            anti_spam.reset_daily_stats()
            
            logger.info("=" * 60)

# تسک پاکسازی گروه‌های مرده و غیرفعال
async def cleanup_dead_groups():
    """بررسی و حذف گروه‌های مرده/غیرفعال + پاکسازی حافظه"""
    while True:
        try:
            await asyncio.sleep(CLEANUP_INTERVAL)  # هر 30 دقیقه
            
            logger.info("🧹 شروع پاکسازی گروه‌های مرده و حافظه...")
            
            removed_count = 0
            checked_count = 0
            
            for group_id in list(groups):
                checked_count += 1
                try:
                    # تلاش برای دریافت اطلاعات گروه
                    entity = await client.get_entity(group_id)
                    
                    # چک کنیم که هنوز عضو هستیم
                    if isinstance(entity, Channel):
                        if not entity.left:
                            # گروه سالم است - ریست کردن retry count
                            if group_id in group_retry_count:
                                del group_retry_count[group_id]
                            continue
                    
                    # اگر از گروه خارج شدیم، حذفش کن
                    if remove_group_completely(group_id):
                        removed_count += 1
                        logger.info(f"🗑️ گروه {group_id} حذف شد (left)")
                    
                except Exception as e:
                    error_msg = str(e)
                    # اگر گروه پیدا نشد یا دسترسی نداریم
                    if any(err in error_msg for err in ["PEER_ID_INVALID", "CHANNEL_INVALID", "CHANNEL_PRIVATE"]):
                        if remove_group_completely(group_id):
                            removed_count += 1
                            logger.info(f"🗑️ گروه {group_id} حذف شد (invalid)")
                
                # تاخیر کوچک برای جلوگیری از FloodWait
                await asyncio.sleep(0.5)
            
            # پاکسازی حافظه - حذف پیام‌های قدیمی
            cleaned_messages = cleanup_old_messages()
            
            logger.info(f"✅ پاکسازی کامل شد:")
            logger.info(f"   🔍 بررسی شد: {checked_count} گروه")
            logger.info(f"   🗑️ حذف شد: {removed_count} گروه مرده")
            logger.info(f"   💾 پاک شد: {cleaned_messages} پیام قدیمی")
            logger.info(f"   📊 گروه‌های فعال: {len(groups)}")
            
        except Exception as e:
            logger.error(f"❌ خطا در cleanup: {e}")
            await asyncio.sleep(60)

# ═══════════════════════════════════════════════════════════════════════════════
# 🧹 تسک خروج از گروه‌های کم‌عضو (LOW MEMBER GROUP CLEANUP)
# ═══════════════════════════════════════════════════════════════════════════════
async def leave_low_member_groups():
    """
    بررسی و خروج از گروه‌هایی که تعداد اعضای کمی دارند
    
    ویژگی‌ها:
    - بررسی دوره‌ای تمام گروه‌ها (شامل همه dialogs)
    - پشتیبانی از هر دو نوع گروه: Channel (سوپرگروه) و Chat (گروه خصوصی)
    - خروج از گروه‌هایی که کمتر از MIN_GROUP_MEMBERS عضو دارند (500 عضو)
    - حفظ گروه خودمان (@PharmaWebGp)
    - اضافه کردن به blacklist دائمی برای جلوگیری از عضویت مجدد
    - تاخیر هوشمند برای جلوگیری از FloodWait
    """
    # تاخیر اولیه کوتاه برای اطمینان از بارگذاری کامل
    await asyncio.sleep(30)
    
    slog(f"🧹 [LOW_MEMBER] سیستم بررسی گروه‌های کم‌عضو شروع شد (حداقل: {MIN_GROUP_MEMBERS} عضو)")
    
    while True:
        try:
            # ⚠️ بررسی سوییچ
            if not ENABLE_LOW_MEMBER_LEAVE:
                await asyncio.sleep(60)
                continue
            
            slog(f"🧹 [LOW_MEMBER] شروع بررسی گروه‌های کم‌عضو...")
            
            left_count = 0
            checked_count = 0
            skipped_count = 0
            low_member_groups = []  # لیست گروه‌های کم عضو برای خروج
            
            # ✅ مهم: از dialogs مستقیم استفاده کن با limit بالا
            # این تضمین می‌کند که همه گروه‌هایی که عضوشان هستیم بررسی شوند
            try:
                all_dialogs = await client.get_dialogs(limit=500)
                all_groups = []
                
                for d in all_dialogs:
                    entity = d.entity
                    # ✅ پشتیبانی از سوپرگروه‌ها (Channel با megagroup=True)
                    if isinstance(entity, Channel) and entity.megagroup and not entity.broadcast:
                        all_groups.append({'id': d.id, 'entity': entity, 'type': 'supergroup', 'dialog': d})
                    # ✅ پشتیبانی از گروه‌های خصوصی/قدیمی (Chat)
                    elif isinstance(entity, Chat):
                        all_groups.append({'id': d.id, 'entity': entity, 'type': 'chat', 'dialog': d})
                
                slog(f"🔍 [LOW_MEMBER] تعداد کل گروه‌ها: {len(all_groups)}")
            except Exception as e:
                slog(f"❌ [LOW_MEMBER] خطا در دریافت dialogs: {str(e)[:50]}")
                await asyncio.sleep(300)
                continue
            
            # ابتدا همه گروه‌ها را بررسی کن و لیست کم‌عضوها را بساز
            for group_info in all_groups:
                try:
                    entity = group_info['entity']
                    group_id = group_info['id']
                    group_type = group_info['type']
                    
                    # رد کردن اگر گروه خودمان است
                    if our_group_id and group_id == our_group_id:
                        continue
                    
                    checked_count += 1
                    
                    # دریافت تعداد اعضا بر اساس نوع گروه
                    try:
                        if group_type == 'supergroup':
                            # برای سوپرگروه‌ها از GetFullChannelRequest استفاده کن
                            full_channel = await client(GetFullChannelRequest(channel=entity))
                            member_count = full_channel.full_chat.participants_count
                        else:
                            # برای Chat های معمولی از participants_count استفاده کن
                            member_count = getattr(entity, 'participants_count', 0) or 0
                    except Exception:
                        skipped_count += 1
                        continue
                    
                    title = getattr(entity, 'title', 'نامشخص')
                    
                    # اگر کم عضو است، به لیست اضافه کن
                    if member_count < MIN_GROUP_MEMBERS:
                        low_member_groups.append({
                            'id': group_id,
                            'entity': entity,
                            'title': title,
                            'members': member_count,
                            'type': group_type
                        })
                        slog(f"   ❌ کم‌عضو: {title[:35]} | 👥 {member_count} | {group_type}")
                    
                    # تاخیر کوچک بین بررسی‌ها
                    await asyncio.sleep(0.5)
                    
                except ChannelPrivateError:
                    if group_id in groups:
                        groups.remove(group_id)
                    if group_id in joined_groups:
                        joined_groups.discard(group_id)
                    continue
                except Exception:
                    continue
            
            slog(f"🔍 [LOW_MEMBER] بررسی شد: {checked_count} | کم‌عضو: {len(low_member_groups)}")
            
            # حالا از گروه‌های کم‌عضو خارج شو
            if low_member_groups:
                slog(f"🚪 [LOW_MEMBER] شروع خروج از {len(low_member_groups)} گروه کم‌عضو...")
            
            for group_info in low_member_groups[:MAX_LEAVES_PER_CYCLE]:
                try:
                    group_type = group_info.get('type', 'supergroup')
                    entity = group_info['entity']
                    group_id = group_info['id']
                    title = group_info['title']
                    members = group_info['members']
                    
                    slog(f"🚪 [LOW_MEMBER] خروج از: {title[:40]} ({members} عضو) [{group_type}]...")
                    
                    # خروج بر اساس نوع گروه
                    try:
                        if group_type == 'supergroup':
                            # برای سوپرگروه‌ها از LeaveChannelRequest استفاده کن
                            await client(LeaveChannelRequest(channel=entity))
                        else:
                            # برای Chat های معمولی - چند روش مختلف امتحان کن
                            try:
                                # روش 1: استفاده از delete_dialog
                                await client.delete_dialog(entity)
                            except Exception as e1:
                                try:
                                    # روش 2: استفاده از edit_folder برای آرشیو
                                    await client.edit_folder(entity, folder=1)
                                    slog(f"   📁 آرشیو شد: {title[:30]}")
                                except Exception as e2:
                                    # روش 3: فقط به blacklist اضافه کن
                                    slog(f"   ⚠️ نمی‌توان خارج شد، فقط blacklist: {title[:30]}")
                    except Exception as leave_err:
                        slog(f"   ⚠️ خطای خروج: {str(leave_err)[:40]}")
                    
                    if group_id in groups:
                        groups.remove(group_id)
                    if group_id in joined_groups:
                        joined_groups.discard(group_id)
                    
                    # ✅ اضافه کردن به blacklist دائمی - فوری ذخیره شود
                    username = getattr(entity, 'username', None)
                    add_to_permanent_blacklist(
                        group_id, 
                        reason='low_members',
                        username=username,
                        title=title
                    )
                    # ذخیره فوری blacklist بعد از هر خروج
                    save_permanent_blacklist()
                    
                    left_count += 1
                    stats['groups_left'] = stats.get('groups_left', 0) + 1
                    
                    slog(f"✅ [LOW_MEMBER] خارج شد + Blacklist: {group_info['title'][:30]} ({group_info['members']} عضو)")
                    
                    delay = random.uniform(LEAVE_GROUP_DELAY_MIN, LEAVE_GROUP_DELAY_MAX)
                    await asyncio.sleep(delay)
                    
                except FloodWaitError as e:
                    slog(f"⚠️ [LOW_MEMBER] FloodWait: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                    break
                except Exception as e:
                    slog(f"❌ [LOW_MEMBER] خطا در خروج: {str(e)[:50]}")
            
            slog(f"✅ [LOW_MEMBER] خروج از {left_count} گروه کم‌عضو | باقی‌مانده: {len(all_groups) - left_count}")
            
            # انتظار تا سیکل بعدی
            slog(f"⏰ [LOW_MEMBER] سیکل بعدی در {LOW_MEMBER_CHECK_INTERVAL // 60} دقیقه...")
            await asyncio.sleep(LOW_MEMBER_CHECK_INTERVAL)
            
        except Exception as e:
            slog(f"❌ [LOW_MEMBER] خطا: {str(e)[:100]}")
            await asyncio.sleep(300)


async def check_group_member_count(entity):
    """
    بررسی تعداد اعضای گروه قبل از عضویت
    
    Returns:
        int: تعداد اعضا یا -1 اگر نتوانست بررسی کند
    """
    try:
        if not isinstance(entity, Channel):
            return -1
        
        full_channel = await client(GetFullChannelRequest(channel=entity))
        return full_channel.full_chat.participants_count
    except Exception:
        return -1


def should_join_group_by_members(member_count):
    """
    بررسی آیا باید به گروه بر اساس تعداد اعضا ملحق شد
    
    Args:
        member_count: تعداد اعضای گروه
        
    Returns:
        bool: True اگر باید عضو شد، False اگر نباید
    """
    if not CHECK_MEMBERS_BEFORE_JOIN:
        return True
    
    if member_count < 0:  # نتوانستیم تعداد را بگیریم
        return True  # اجازه عضویت بده (بعداً بررسی می‌شود)
    
    return member_count >= MIN_GROUP_MEMBERS


async def check_group_write_access(entity):
    """
    بررسی آیا امکان ارسال پیام در گروه وجود دارد
    
    روش‌های بررسی:
    1. بررسی default_banned_rights گروه
    2. بررسی banned_rights کاربر در گروه
    3. بررسی broadcast بودن (کانال یک‌طرفه)
    
    Args:
        entity: موجودیت گروه/کانال
        
    Returns:
        tuple: (can_write: bool, reason: str)
    """
    try:
        if not isinstance(entity, Channel):
            return (True, "not_channel")
        
        # 1. کانال‌های broadcast (فقط ادمین می‌تونه بنویسه)
        if entity.broadcast and not entity.megagroup:
            return (False, "broadcast_channel")
        
        # 2. بررسی دسترسی‌های پیش‌فرض گروه
        if hasattr(entity, 'default_banned_rights') and entity.default_banned_rights:
            rights = entity.default_banned_rights
            
            # آیا ارسال پیام برای همه بسته است؟
            if hasattr(rights, 'send_messages') and rights.send_messages:
                return (False, "send_messages_banned")
            
            # آیا ارسال مدیا بسته است؟ (گاهی این هم مهمه)
            if hasattr(rights, 'send_media') and rights.send_media:
                # این اختیاری است - فقط پیام متنی می‌تونیم بفرستیم
                pass
        
        # 3. بررسی محدودیت‌های خاص کاربر (اگر بن شده باشیم)
        if hasattr(entity, 'banned_rights') and entity.banned_rights:
            rights = entity.banned_rights
            
            if hasattr(rights, 'send_messages') and rights.send_messages:
                return (False, "user_banned_from_sending")
            
            # بررسی تاریخ انقضای بن
            if hasattr(rights, 'until_date') and rights.until_date:
                # اگر until_date در آینده باشه، هنوز بن هستیم
                import datetime
                if rights.until_date > datetime.datetime.now(datetime.timezone.utc):
                    return (False, "user_temporarily_banned")
        
        # 4. بررسی left بودن از گروه
        # ⚠️ توجه: این بررسی فقط بعد از عضویت معنی دارد
        # قبل از عضویت، left=True طبیعی است
        # if hasattr(entity, 'left') and entity.left:
        #     return (False, "left_group")
        
        # 5. بررسی restricted بودن
        if hasattr(entity, 'restricted') and entity.restricted:
            # گاهی گروه‌ها restricted هستند ولی هنوز می‌شه نوشت
            pass
        
        return (True, "ok")
        
    except Exception as e:
        logger.debug(f"خطا در بررسی دسترسی: {e}")
        return (True, f"error: {str(e)}")  # در صورت خطا، فرض کن می‌شه نوشت


async def test_write_access_by_sending(entity):
    """
    تست واقعی دسترسی ارسال با ارسال و حذف سریع پیام
    
    ⚠️ این متد ممکن است کمی ریسک داشته باشد
    فقط در صورت نیاز استفاده شود
    
    Returns:
        tuple: (can_write: bool, reason: str)
    """
    try:
        # ارسال پیام تست (یک کاراکتر نامرئی)
        test_msg = await client.send_message(entity, "⁠")  # Zero-width space
        
        # حذف سریع
        await test_msg.delete()
        
        return (True, "test_passed")
        
    except ChatWriteForbiddenError:
        return (False, "write_forbidden")
    except UserBannedInChannelError:
        return (False, "user_banned")
    except Exception as e:
        error_str = str(e).lower()
        if "forbidden" in error_str or "banned" in error_str or "restricted" in error_str:
            return (False, f"error: {error_str[:50]}")
        return (True, f"unknown_error: {error_str[:50]}")


# ═══════════════════════════════════════════════════════════════════════════════
# 🔒 تسک خروج از گروه‌های بسته (RESTRICTED GROUP CLEANUP)
# ═══════════════════════════════════════════════════════════════════════════════
async def leave_restricted_groups():
    """
    بررسی و خروج از گروه‌هایی که امکان ارسال پیام در آنها وجود ندارد
    
    ویژگی‌ها:
    - تشخیص گروه‌های فقط خواندنی
    - تشخیص بن شدن توسط ادمین
    - تشخیص کانال‌های یک‌طرفه
    - حفظ گروه خودمان (@PharmaWebGp)
    """
    while True:
        try:
            # ⚠️ بررسی سوییچ
            if not ENABLE_RESTRICTED_GROUP_LEAVE:
                await asyncio.sleep(60)
                continue
            
            # تاخیر اولیه
            await asyncio.sleep(RESTRICTED_CHECK_INTERVAL)
            
            logger.info("🔒 شروع بررسی گروه‌های بسته/محدود...")
            
            left_count = 0
            checked_count = 0
            restricted_reasons = {}
            
            for group_id in list(groups):
                # محدودیت تعداد خروج
                if left_count >= MAX_LEAVES_PER_CYCLE:
                    logger.info(f"⏸️ به حداکثر خروج رسیدیم ({MAX_LEAVES_PER_CYCLE})")
                    break
                
                try:
                    entity = await client.get_entity(group_id)
                    
                    # رد کردن گروه خودمان
                    if our_group_id and group_id == our_group_id:
                        continue
                    
                    # فقط کانال‌ها و سوپرگروه‌ها
                    if not isinstance(entity, Channel):
                        continue
                    
                    checked_count += 1
                    
                    # بررسی دسترسی ارسال
                    can_write, reason = await check_group_write_access(entity)
                    
                    if not can_write:
                        # خروج از گروه
                        try:
                            await client(LeaveChannelRequest(channel=entity))
                            
                            # حذف از لیست‌ها
                            if group_id in groups:
                                groups.remove(group_id)
                            if group_id in joined_groups:
                                joined_groups.discard(group_id)
                            
                            # ✅ اضافه کردن به blacklist دائمی
                            username = getattr(entity, 'username', None)
                            group_title = getattr(entity, 'title', 'Unknown')[:30]
                            add_to_permanent_blacklist(
                                group_id,
                                reason='no_write_access',
                                username=username,
                                title=group_title
                            )
                            
                            left_count += 1
                            stats['groups_left'] = stats.get('groups_left', 0) + 1
                            
                            # ثبت دلیل
                            restricted_reasons[reason] = restricted_reasons.get(reason, 0) + 1
                            
                            logger.info(f"🔒 خروج + Blacklist: '{group_title}' (دلیل: {reason})")
                            
                            # تاخیر بین خروج‌ها
                            delay = random.uniform(LEAVE_GROUP_DELAY_MIN, LEAVE_GROUP_DELAY_MAX)
                            await asyncio.sleep(delay)
                            
                        except FloodWaitError as e:
                            logger.warning(f"⚠️ FloodWait: {e.seconds}s")
                            await asyncio.sleep(e.seconds)
                            break
                            
                        except Exception as e:
                            logger.error(f"❌ خطا در خروج: {e}")
                    
                    # تاخیر کوچک
                    await asyncio.sleep(0.5)
                    
                except ChannelPrivateError:
                    # گروه خصوصی شده
                    if group_id in groups:
                        groups.remove(group_id)
                    if group_id in joined_groups:
                        joined_groups.discard(group_id)
                    left_count += 1
                    restricted_reasons['private'] = restricted_reasons.get('private', 0) + 1
                    
                except Exception as e:
                    logger.debug(f"⚠️ خطا در بررسی {group_id}: {e}")
                    continue
            
            # گزارش
            logger.info(f"✅ بررسی گروه‌های بسته کامل شد:")
            logger.info(f"   🔍 بررسی شد: {checked_count}")
            logger.info(f"   🔒 خروج: {left_count}")
            if restricted_reasons:
                logger.info(f"   📊 دلایل: {dict(restricted_reasons)}")
            logger.info(f"   📊 گروه‌های باقی‌مانده: {len(groups)}")
            
        except Exception as e:
            logger.error(f"❌ خطا در leave_restricted_groups: {e}")
            await asyncio.sleep(300)


async def check_group_is_usable(entity, skip_member_check=False):
    """
    بررسی جامع آیا گروه قابل استفاده است
    
    ترکیب بررسی‌ها:
    - تعداد اعضا (اختیاری)
    - دسترسی ارسال پیام
    
    Args:
        entity: موجودیت گروه
        skip_member_check: اگر True باشد، بررسی تعداد اعضا انجام نمی‌شود
    
    Returns:
        tuple: (is_usable: bool, reason: str)
    """
    try:
        # 1. بررسی دسترسی ارسال (فقط اگر فعال باشد)
        if CHECK_WRITE_ACCESS_BEFORE_JOIN and ENABLE_RESTRICTED_GROUP_LEAVE:
            can_write, reason = await check_group_write_access(entity)
            if not can_write:
                return (False, f"no_write_access: {reason}")
        
        # 2. بررسی تعداد اعضا (فقط اگر درخواست شده و فعال باشد)
        if not skip_member_check and CHECK_MEMBERS_BEFORE_JOIN and ENABLE_LOW_MEMBER_LEAVE:
            member_count = await check_group_member_count(entity)
            if not should_join_group_by_members(member_count):
                return (False, f"low_members: {member_count}")
        
        return (True, "ok")
    except Exception as e:
        # در صورت خطا، اجازه عضویت بده (بعداً بررسی می‌شود)
        return (True, f"check_error: {str(e)}")


# 🌍 جستجوی بدون محدودیت: پذیرش همه گروه‌ها
async def fast_search_supergroups(keyword, limit=100, max_pages=5):
    """
    🎯 جستجوی هوشمند فوق‌سریع با سیستم اولویت‌بندی 4 لایه + صفحه‌بندی عمیق
    بهینه‌سازی: max_pages=5 (بجای 3)، شناسایی سریع اولویت‌ها
    """
    found_groups = []
    found_ids = set()
    
    # کلمات کلیدی اولویت 1: ترید و کریپتو (امتیاز: +15)
    priority1_keywords = [
        'ترید', 'تریدر', 'trade', 'trader', 'trading', 'سیگنال', 'signal',
        'کریپتو', 'crypto', 'بیتکوین', 'bitcoin', 'btc', 'اتریوم', 'ethereum',
        'فارکس', 'forex', 'بایننس', 'binance', 'صرافی', 'exchange',
        'تحلیل', 'analysis', 'چارت', 'chart', 'پراپ', 'prop', 'ftmo',
        'ایردراپ', 'airdrop', 'nft', 'defi', 'web3', 'فیوچرز', 'futures',
        # 🆕 رمزارزهای بیشتر برای شناسایی
        'تون', 'ton', 'toncoin', 'ترون', 'tron', 'trx', 'سولانا', 'solana', 'sol',
        'بایننس', 'bnb', 'دوج', 'doge', 'شیبا', 'shiba', 'پپه', 'pepe',
        'ریپل', 'xrp', 'کاردانو', 'ada', 'لایتکوین', 'ltc', 'آوالانچ', 'avax',
        'پالیگان', 'matic', 'لینک', 'chainlink', 'دات', 'dot', 'polkadot',
        'اسپات', 'spot', 'لوریج', 'leverage', 'مارجین', 'margin',
        'تریدینگ ویو', 'tradingview', 'متاتریدر', 'metatrader',
        'پامپ', 'pump', 'دامپ', 'dump', 'نات', 'notcoin', 'همستر', 'hamster',
        'بلاکچین', 'blockchain', 'والت', 'wallet', 'صرافی غیرمتمرکز', 'dex',
        # 🆕🆕 رمزارزها و پلتفرم‌های جدیدتر
        'آربیتروم', 'arbitrum', 'arb', 'اوپتیمیزم', 'optimism', 'op',
        'سوئی', 'sui', 'آپتوس', 'aptos', 'apt', 'بیس', 'base',
        'لایر دو', 'layer2', 'l2', 'zksync', 'starknet',
        'فچ', 'fetch', 'رندر', 'render', 'rndr', 'ورلد کوین', 'worldcoin',
        'بیت تنسور', 'bittensor', 'tao', 'اوشن', 'ocean',
        'لیدو', 'lido', 'ldo', 'یونی سواپ', 'uniswap', 'uni',
        'میکر', 'maker', 'mkr', 'آوه', 'aave', 'کرو', 'curve',
        'پنکیک', 'pancakeswap', 'cake', 'سوشی', 'sushi',
        'تپ سواپ', 'tapswap', 'بلوم', 'blum', 'داگز', 'dogs',
        'کتیزن', 'catizen', 'ممفای', 'memefi', 'ماژور', 'major',
        'بانک', 'bonk', 'ویف', 'wif', 'فلوکی', 'floki',
        'نوبیتکس', 'nobitex', 'والکس', 'wallex', 'رمزینکس', 'ramzinex',
        'بای بیت', 'bybit', 'اوکی اکس', 'okx', 'کوکوین', 'kucoin',
        'بیت گت', 'bitget', 'گیت', 'gate', 'مکسی', 'mexc',
        'استیکینگ', 'staking', 'ماینینگ', 'mining', 'یلد', 'yield',
    ]
    
    # کلمات کلیدی اولویت 2: پزشکی و دارو (امتیاز: +12)
    priority2_keywords = [
        'دارو', 'داروخانه', 'پزشکی', 'پزشک', 'دکتر', 'بیمارستان',
        'pharmacy', 'medical', 'drug', 'medicine', 'doctor', 'hospital',
        'آزمایشگاه', 'laboratory', 'lab', 'دندانپزشکی', 'dental',
        'مکمل', 'supplement', 'ویتامین', 'vitamin', 'تجهیزات پزشکی',
        'ایمپلنت', 'implant', 'لنز', 'lens', 'عینک', 'سمعک',
        'سلامت', 'health', 'درمان', 'treatment', 'بهداشت',
        'کلینیک', 'clinic', 'پرستاری', 'nursing', 'فیزیوتراپی',
        'نایاب', 'کمیاب', 'ارتوپدی', 'orthopedic', 'قلب', 'cardiac',
    ]
    
    # کلمات کلیدی اولویت 3: مهاجرت (امتیاز: +8)
    priority3_keywords = [
        'iran', 'iranian', 'ایران', 'ایرانی', 'فارسی', 'persian',
        'istanbul', 'turkey', 'ترکیه', 'استانبول', 'dubai', 'دبی',
        'canada', 'کانادا', 'germany', 'آلمان', 'مهاجرت', 'immigration',
        'ویزا', 'visa', 'اقامت', 'پناهندگی', 'سفارت', 'embassy',
        'امارات', 'uae', 'لندن', 'london', 'تورنتو', 'toronto',
        'ونکوور', 'vancouver', 'سیدنی', 'sydney', 'ملبورن', 'melbourne',
        'کیملیک', 'kimlik', 'پناهنده', 'refugee', 'اقامت دائم',
        'مهاجر', 'ایرانیان مقیم', 'هموطن', 'دیاسپورا', 'diaspora',
    ]
    
    # کلمات منفی
    negative_keywords = ['porn', 'sex', 'xxx', 'adult', '18+', 'casino', 'gambling', 'قمار']
    
    try:
        offset_rate = 0
        offset_id = 0
        offset_peer = InputPeerEmpty()
        
        for page in range(max_pages):
            try:
                result = await client(SearchGlobalRequest(
                    q=keyword,
                    filter=InputMessagesFilterEmpty(),
                    min_date=None,
                    max_date=None,
                    offset_rate=offset_rate,
                    offset_peer=offset_peer,
                    offset_id=offset_id,
                    limit=limit
                ))
                
                if not result.chats:
                    break
                
                for chat in result.chats:
                    # فیلترهای اصلی
                    if not isinstance(chat, Channel):
                        continue
                    if not chat.megagroup:
                        continue
                    if chat.broadcast:
                        continue
                    if getattr(chat, 'forum', False):
                        continue
                    if chat.id in found_ids:
                        continue
                    if is_permanently_blacklisted(chat.id):
                        continue
                    if chat.id in joined_groups:
                        continue
                    
                    # بررسی دسترسی ارسال پیام
                    can_send = True
                    if hasattr(chat, 'default_banned_rights') and chat.default_banned_rights:
                        if chat.default_banned_rights.send_messages:
                            can_send = False
                    
                    if not can_send:
                        username = getattr(chat, 'username', None)
                        title = getattr(chat, 'title', 'Unknown')
                        add_to_permanent_blacklist(chat.id, reason='no_write_access', username=username, title=title)
                        continue
                    
                    # بررسی تعداد اعضا
                    members = getattr(chat, 'participants_count', None)
                    if members is not None and members > 0 and members < MIN_GROUP_MEMBERS:
                        continue
                    
                    # بررسی عنوان
                    title = getattr(chat, 'title', '').lower()
                    
                    # چک کلمات منفی
                    if any(neg in title for neg in negative_keywords):
                        continue
                    
                    # محاسبه امتیاز - سیستم 4 لایه
                    quality_score = 0
                    category = 'general'
                    
                    if any(kw in title for kw in priority1_keywords):
                        quality_score += 15  # کریپتو بالاترین
                        category = 'crypto_trading'
                    elif any(kw in title for kw in priority2_keywords):
                        quality_score += 12  # پزشکی دوم
                        category = 'medical'
                    elif any(kw in title for kw in priority3_keywords):
                        quality_score += 8   # مهاجرت سوم
                        category = 'immigration'
                    else:
                        quality_score += 1   # عمومی
                    
                    # امتیاز اعضا
                    if members:
                        if members >= 10000:
                            quality_score += 5
                        elif members >= 5000:
                            quality_score += 4
                        elif members >= 2000:
                            quality_score += 3
                        elif members >= 1000:
                            quality_score += 2
                        elif members >= 500:
                            quality_score += 1
                    
                    chat._priority_score = quality_score
                    chat._category = category
                    found_groups.append(chat)
                    found_ids.add(chat.id)
                
                # pagination
                if result.messages:
                    last_msg = result.messages[-1]
                    offset_rate = getattr(last_msg, 'date', None)
                    if offset_rate:
                        offset_rate = int(offset_rate.timestamp())
                    else:
                        offset_rate = 0
                    offset_id = last_msg.id
                else:
                    break
                
                await asyncio.sleep(random.uniform(0.8, 1.8))
                
            except FloodWaitError as e:
                slog(f"⏳ FloodWait: {e.seconds}s")
                await asyncio.sleep(e.seconds + 1)
                break
            except:
                break
        
        found_groups.sort(key=lambda x: getattr(x, '_priority_score', 0), reverse=True)
        
        if found_groups:
            try:
                update_keyword_performance(keyword, len(found_groups))
                slog(f"🔍 '{keyword}' → {len(found_groups)} گروه")
            except:
                pass
        
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds)
    except:
        pass
    
    return found_groups

# ═══════════════════════════════════════════════════════════════════════════════
# 🆕 تابع جستجوی عمیق با pagination
# ═══════════════════════════════════════════════════════════════════════════════
async def deep_search_supergroups(keyword, limit=100, max_pages=8):
    """
    جستجوی عمیق با تا 8 صفحه برای یافتن نتایج بیشتر
    مناسب برای کلمات کلیدی پربازده
    """
    return await fast_search_supergroups(keyword, limit, max_pages)

# تابع جستجوی موازی برای سرعت بیشتر (اختیاری)
async def parallel_search_groups(keywords_batch, use_deep=False):
    """جستجوی موازی چند کلیدواژه به صورت همزمان - بهینه‌شده"""
    max_pages = 8 if use_deep else 5
    semaphore = asyncio.Semaphore(6)  # حداکثر 6 همزمان
    
    async def limited_search(kw):
        async with semaphore:
            return await fast_search_supergroups(kw, SEARCH_LIMIT, max_pages)
    
    tasks = [limited_search(kw) for kw in keywords_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_groups = []
    seen_ids = set()
    
    for result in results:
        if isinstance(result, list):
            for group in result:
                if group.id not in seen_ids:
                    all_groups.append(group)
                    seen_ids.add(group.id)
    
    return all_groups

# تسک برای پخش خودکار در گروه‌ها (کاملاً بازنویسی شده با ReliableBroadcastController)
async def broadcast_to_groups():
    """
    ارسال پیام‌های تبلیغاتی با کنترل‌کننده قابل اعتماد (تاخیرهای طولانی متغیر + تطبیقی).
    هرگز محتوای بی‌کیفیت ارسال نمی‌شود. الگوی انسانی + batch rest.
    """
    while True:
        try:
            if not ENABLE_BROADCAST:
                await asyncio.sleep(60)
                continue

            if anti_spam.should_rest():
                rest_duration = anti_spam.get_rest_duration()
                slog(f"😴 استراحت {rest_duration//60} دقیقه (سلامت: {anti_spam.health_score})")
                await asyncio.sleep(rest_duration)
                anti_spam.mark_rested()
                continue

            if not groups:
                slog("⏳ [BROADCAST] در انتظار گروه‌ها...")
                await asyncio.sleep(120)
                continue

            # کنترل‌کننده مرکزی
            can, reason = broadcast_controller.can_send_now(0)
            if not can:
                rest = random.randint(600, 1800)
                slog(f"⏸️ [BROADCAST] محدودیت: {reason}. استراحت {rest//60} دقیقه | {broadcast_controller.get_status()}")
                await asyncio.sleep(rest)
                continue

            priority_groups = group_tracker.get_priority_groups(groups)
            try:
                priority_groups = group_quality_scorer.get_top_groups(priority_groups, len(priority_groups))
            except:
                pass
            random.shuffle(priority_groups)

            sent_in_this_cycle = 0
            for group_id in priority_groups:
                try:
                    if is_permanently_blacklisted(group_id):
                        if group_id in groups:
                            groups.remove(group_id)
                        continue
                    if group_tracker.is_blacklisted(group_id):
                        continue

                    can_send, why = broadcast_controller.can_send_now(group_id)
                    if not can_send:
                        continue

                    # === CENTRAL + ENGAGER content + strict gate ===
                    fresh_ctx = await fetch_recent_group_context(client, group_id, limit=6)
                    text = None
                    try:
                        # Prefer engager-generated valuable content (highest intelligence)
                        text = await group_engager.generate_starter(group_id, fresh_ctx)
                    except:
                        pass
                    if not text or not is_high_quality_natural(text):
                        text = content_rotation.get_best_content_for_group(group_id, drug_lists)
                    if not text or not is_high_quality_natural(text):
                        text = await generate_natural_valuable_post()  # AI
                    if not text or not is_high_quality_natural(text):
                        continue  # ZERO low quality ever

                    # Extra anti-rep + time engine
                    if not content_rotation.should_send_content(group_id, text):
                        continue
                    try:
                        mult = time_optimizer.get_current_multiplier() if 'time_optimizer' in globals() else 1.0
                        if mult > 1.6:
                            continue  # too risky right now
                    except:
                        pass

                    if not content_rotation.should_send_content(group_id, text):
                        continue

                    # ارسال + شبیه‌سازی انسان
                    # Final ultra-strict gate for broadcast (never low quality or repetitive)
                    if _is_repetitive_or_similar(group_id, text):
                        continue
                    if not is_high_quality_natural(text):
                        continue

                    await simulate_read_and_type(client, group_id, len(text) if text else 40)
                    msg = await client.send_message(group_id, text)

                    # ثبت‌ها
                    last_message_time[group_id] = time.time()
                    if group_id not in sent_messages:
                        sent_messages[group_id] = []
                    sent_messages[group_id].append((msg.id, time.time()))
                    stats['messages_sent'] += 1

                    _record_bot_output(group_id, text)
                    anti_spam.record_success()
                    content_rotation.record_sent(group_id, text)
                    group_tracker.record_success(group_id)
                    try:
                        time_optimizer.record_action(success=True)
                        funnel_analytics.record_stage(group_id, 'awareness')
                    except:
                        pass

                    broadcast_controller.record_send(group_id, success=True)
                    slog(f"✅ [BROADCAST] به {group_id} | {broadcast_controller.get_status()}")

                    await safe_broadcast_delay(after_success=True)
                    await enforce_batch_rest_if_needed()

                    sent_in_this_cycle += 1
                    if sent_in_this_cycle >= 1:  # یک ارسال موفق در هر دور برای ایمنی
                        break

                except FloodWaitError as e:
                    anti_spam.record_flood_wait(e.seconds)
                    broadcast_controller.on_flood_wait(e.seconds)
                    flood_sleep = int(e.seconds * 1.8) + random.randint(120, 400)
                    slog(f"🛑 FloodWait {e.seconds}s → خواب {flood_sleep//60} دقیقه")
                    await asyncio.sleep(flood_sleep)
                    break
                except Exception as e:
                    err = str(e)
                    broadcast_controller.on_error(err, group_id)
                    group_retry_count[group_id] = group_retry_count.get(group_id, 0) + 1
                    if any(x in err for x in ["PEER_FLOOD", "USER_BANNED", "CHAT_WRITE_FORBIDDEN", "CHANNEL_PRIVATE"]):
                        add_to_permanent_blacklist(group_id, reason=err[:80])
                        try:
                            remove_group_completely(group_id)
                        except:
                            pass
                    await asyncio.sleep(random.randint(45, 120))
                    continue

            # استراحت کوتاه بین سیکل‌ها
            if random.random() < 0.3:
                slog(f"ℹ️ [BROADCAST] وضعیت: {broadcast_controller.get_status()}")
            await asyncio.sleep(random.randint(20, 50))

        except Exception as e:
            slog(f"❌ خطای کلی broadcast: {e}")
            await asyncio.sleep(40)

# تسک برای ویرایش نامحسوس پیام‌های قدیمی
async def edit_old_messages():
    """ویرایش پیام‌های قدیمی با مدیریت هوشمند FloodWait"""
    while True:
        try:
            # 📢 بررسی فعال بودن broadcast
            if not ENABLE_BROADCAST:
                await asyncio.sleep(60)
                continue
            
            current_time = time.time()
            edit_threshold = EDIT_DELAY_MINUTES * 60
            
            for group_id, messages in list(sent_messages.items()):
                for msg_id, send_time in list(messages):
                    if current_time - send_time >= edit_threshold:
                        try:
                            # دریافت پیام
                            msg = await client.get_messages(group_id, ids=msg_id)
                            if msg and msg.text:
                                # درج لینک در وسط متن (نامحسوس)
                                lines = msg.text.split('\n')
                                mid_point = len(lines) // 2
                                
                                # درج لینک به صورت طبیعی
                                link_text = random.choice([
                                    "🌐 medpharmaweb.shop",
                                    "💬 @PharmaWebAD",
                                    "📞 @PharmaWebAD \n  🌐 medpharmaweb.shop"
                                ])
                                lines.insert(mid_point, link_text)
                                
                                new_text = '\n'.join(lines)
                                await msg.edit(new_text)
                                
                                stats['messages_edited'] += 1
                                logger.info(f"✏️ پیام {msg_id} در گروه {group_id} ویرایش شد")
                                
                                # حذف از لیست
                                messages.remove((msg_id, send_time))
                                
                        except FloodWaitError as e:
                            # FloodWait فقط این ویرایش رو تحت تاثیر قرار میده
                            logger.warning(f"⚠️ FloodWait در ویرایش پیام {msg_id}: {e.seconds} ثانیه - ادامه با پیام بعدی")
                            # این پیام رو فعلاً رد می‌کنیم، بعداً دوباره تلاش میشه
                            continue
                            
                        except Exception as e:
                            error_msg = str(e)
                            if "MESSAGE_NOT_MODIFIED" not in error_msg:
                                logger.error(f"❌ خطا در ویرایش پیام {msg_id}: {e}")
                            # حذف از لیست حتی اگر خطا داشت
                            if (msg_id, send_time) in messages:
                                messages.remove((msg_id, send_time))
            
            await asyncio.sleep(60)  # هر 1 دقیقه چک کن
            
        except Exception as e:
            logger.error(f"❌ خطا کلی در edit_old_messages: {e}")
            await asyncio.sleep(30)

# 🚀 تابع جدید: جستجوی موازی چندین کلمه برای یافتن گروه‌های بیشتر
async def parallel_multi_search(keywords_list):
    """
    🚀 جستجوی موازی فوق‌سریع با Semaphore برای کنترل همزمانی
    حداکثر 6 جستجوی همزمان برای جلوگیری از FloodWait
    """
    all_found_groups = {}  # {group_id: (chat_object, keyword)}
    semaphore = asyncio.Semaphore(6)  # حداکثر 6 جستجوی همزمان
    
    async def single_search(keyword):
        async with semaphore:
            try:
                found = await fast_search_supergroups(keyword, SEARCH_LIMIT)
                return keyword, found
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds, 15))
                return keyword, []
            except Exception:
                return keyword, []
    
    # اجرای موازی جستجوها
    tasks = [single_search(kw) for kw in keywords_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # جمع‌آوری نتایج (بدون تکراری) با ترجیح گروه‌های با امتیاز بالاتر
    for result in results:
        if isinstance(result, tuple):
            keyword, found_groups = result
            if found_groups:
                for chat in found_groups:
                    existing = all_found_groups.get(chat.id)
                    if existing is None:
                        all_found_groups[chat.id] = (chat, keyword)
                    else:
                        # اگر گروه تکراری پیدا شد، نسخه با امتیاز بالاتر را نگه دار
                        old_score = getattr(existing[0], '_priority_score', 0)
                        new_score = getattr(chat, '_priority_score', 0)
                        if new_score > old_score:
                            all_found_groups[chat.id] = (chat, keyword)
    
    return all_found_groups

# 🧠 انتخاب هوشمند کلمات با اولویت موفقیت بالا
def get_smart_keyword_batch(keyword_index, batch_size=3):
    """انتخاب دسته کلمات با اولویت به کلمات موفق"""
    global SEARCH_KEYWORDS
    
    # اگر لیست تمام شد، shuffle و ریست
    if keyword_index >= len(SEARCH_KEYWORDS):
        keyword_index = 0
        random.shuffle(SEARCH_KEYWORDS)
    
    # رتبه‌بندی کلمات براساس موفقیت
    ranked = rank_keywords_by_success()
    
    # 70% از کلمات موفق، 30% تصادفی (برای کشف)
    batch = []
    
    # اضافه کردن کلمات موفق (70%)
    if ranked and len(ranked) >= 5:
        top_successful = [kw for kw, _, _, _ in ranked[:20]]  # 20 تا از بهترین‌ها
        successful_count = int(batch_size * 0.7)
        batch.extend(random.sample(top_successful, min(successful_count, len(top_successful))))
    
    # پر کردن با کلمات تصادفی (30%)
    remaining = batch_size - len(batch)
    if remaining > 0 and keyword_index < len(SEARCH_KEYWORDS):
        end_index = min(keyword_index + remaining, len(SEARCH_KEYWORDS))
        batch.extend(SEARCH_KEYWORDS[keyword_index:end_index])
        keyword_index = end_index
    
    # اگر هنوز کم داریم، از کلمات تصادفی استفاده کن
    while len(batch) < batch_size and len(SEARCH_KEYWORDS) > 0:
        batch.append(random.choice(SEARCH_KEYWORDS))
    
    return batch, keyword_index

# تسک هوشمند برای جستجو و عضویت با یادگیری AI - UPGRADED 🚀
async def search_and_join_groups():
    """⚔️ جستجو و عضویت تهاجمی با WARRIOR MODE - بهینه‌سازی شده ⚔️"""
    keyword_index = 0
    consecutive_empty = 0
    regenerate_cycle = 0
    total_keywords_used = set()
    search_wave = 0  # شمارنده موج جستجو
    
    # 🧠 تولید الگوها با سیستم هوشمند + جنگجو
    global SEARCH_KEYWORDS
    
    # ترکیب کلمات هوشمند با کلمات جنگجو - 200 تا جنگجو (بجای 100)
    smart_keywords = generate_smart_keywords(count=KEYWORD_GENERATION_COUNT)
    warrior_keywords = warrior_joiner.get_search_keywords(count=200)
    
    # 🆕 اضافه کردن کوئری‌های موثر مستقیم از هر 4 دسته
    effective_crypto = generate_effective_search_queries('crypto_trading', count=40)
    effective_immigration = generate_effective_search_queries('immigration', count=25)
    effective_medical = generate_effective_search_queries('medical', count=20)
    effective_general = generate_effective_search_queries('general', count=15)
    
    SEARCH_KEYWORDS = list(set(
        smart_keywords + warrior_keywords + 
        effective_crypto + effective_immigration + 
        effective_medical + effective_general
    ))
    random.shuffle(SEARCH_KEYWORDS)
    
    logger.info(f"⚔️ WARRIOR MODE فعال شد!")
    logger.info(f"🧠 {len(SEARCH_KEYWORDS)} الگو تولید شد (ترکیب 6 منبع)")
    logger.info(f"🎯 حالت: {warrior_joiner.mode.upper()}")
    settings = warrior_joiner.get_settings()
    logger.info(f"⚡ تنظیمات: batch={settings['batch_size']} | parallel={settings['parallel_searches']}")
    logger.info(f"🔢 محدودیت: حداکثر {MAX_GROUPS_LIMIT} گروه")
    
    while True:
        try:
            # ⚠️ بررسی سوییچ جستجوی گروه
            if not ENABLE_GROUP_SEARCH:
                await asyncio.sleep(60)
                continue
            
            # 🛡️ چک سلامت
            if anti_spam.should_rest():
                rest_time = anti_spam.get_rest_duration()
                logger.warning(f"😴 استراحت {rest_time//60} دقیقه")
                await asyncio.sleep(rest_time)
                anti_spam.mark_rested()
                continue
            
            # ⚔️ دریافت تنظیمات جنگجو
            settings = warrior_joiner.get_settings()
            search_wave += 1
            
            # 🧠 Health Check - هر 15 جستجو (بجای 20)
            if stats['searches_done'] > 0 and stats['searches_done'] % 15 == 0:
                adjust_adaptive_speed()
                logger.info(f"⚔️ Wave:{search_wave} Mode:{warrior_joiner.mode} | Health:{anti_spam.health_score:.0f} | Groups:{len(groups)}")
            
            # 🔄 تولید مجدد الگوها - هر 20 جستجو (بجای 30) برای تنوع بیشتر
            if stats['searches_done'] > 0 and stats['searches_done'] % 20 == 0:
                regenerate_cycle += 1
                smart_keywords = generate_smart_keywords(count=KEYWORD_GENERATION_COUNT)
                warrior_keywords = warrior_joiner.get_search_keywords(count=200)
                
                # هر 3 سیکل، کوئری‌های موثر را هم دوباره تولید کن
                extra_queries = []
                if regenerate_cycle % 3 == 0:
                    extra_queries = (
                        generate_effective_search_queries('crypto_trading', count=40) +
                        generate_effective_search_queries('immigration', count=25) +
                        generate_effective_search_queries('medical', count=20)
                    )
                
                SEARCH_KEYWORDS = list(set(smart_keywords + warrior_keywords + extra_queries))
                random.shuffle(SEARCH_KEYWORDS)
                keyword_index = 0
                logger.info(f"🔄 چرخه {regenerate_cycle} | {len(SEARCH_KEYWORDS)} الگو")
            
            # ♻️ مدیریت ظرفیت
            if len(groups) >= GROUP_CLEANUP_THRESHOLD:
                await smart_leave_old_groups()
            
            # 🚀 انتخاب الگوهای جستجو
            if keyword_index >= len(SEARCH_KEYWORDS):
                keyword_index = 0
                random.shuffle(SEARCH_KEYWORDS)
            
            # استفاده از تنظیمات جنگجو
            parallel_count = settings['parallel_searches']
            end_index = min(keyword_index + parallel_count, len(SEARCH_KEYWORDS))
            keyword_batch = SEARCH_KEYWORDS[keyword_index:end_index]
            keyword_index = end_index
            
            if not keyword_batch:
                keyword_batch = random.sample(SEARCH_KEYWORDS, min(parallel_count, len(SEARCH_KEYWORDS)))
            
            stats['searches_done'] += len(keyword_batch)
            total_keywords_used.update(keyword_batch)
            
            # 🚀 جستجوی موازی
            all_found_groups = await parallel_multi_search(keyword_batch)
            
            if not all_found_groups:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    random.shuffle(SEARCH_KEYWORDS)
                    consecutive_empty = 0
                await asyncio.sleep(5)
                continue
            
            consecutive_empty = 0
            joined_count = 0
            
            # شافل گروه‌ها
            groups_list = list(all_found_groups.items())
            random.shuffle(groups_list)
            
            # 🧠 پیش‌فیلتر سریع
            try:
                filtered_groups = []
                for group_id, (chat, source_keyword) in groups_list:
                    if quality_predictor.should_join(chat, threshold=0.2):
                        filtered_groups.append((group_id, chat, source_keyword))
                
                if len(filtered_groups) < len(groups_list) // 3:
                    filtered_groups = [(gid, chat, kw) for gid, (chat, kw) in groups_list]
                
                groups_list = filtered_groups
            except:
                groups_list = [(gid, chat, kw) for gid, (chat, kw) in all_found_groups.items()]
            
            # ⚔️ عضویت تهاجمی
            batch_size = settings['batch_size']
            join_delay = settings['join_delay']
            
            for item in groups_list:
                if joined_count >= batch_size:
                    break
                
                if len(item) == 3:
                    group_id, chat, source_keyword = item
                else:
                    group_id, (chat, source_keyword) = item
                
                # ✅ بررسی blacklist دائمی
                if is_permanently_blacklisted(group_id):
                    continue
                
                if group_id in joined_groups or group_id in groups:
                    continue
                
                try:
                    # 🚀 بررسی سریع - فقط چک کردن default_banned_rights (بدون API call)
                    # بررسی تعداد اعضا را رد می‌کنیم چون API call اضافی ایجاد می‌کند
                    if hasattr(chat, 'default_banned_rights') and chat.default_banned_rights:
                        if chat.default_banned_rights.send_messages:
                            # گروه دسترسی ارسال ندارد - به blacklist اضافه کن
                            username = getattr(chat, 'username', None)
                            title = getattr(chat, 'title', 'N/A')
                            add_to_permanent_blacklist(group_id, reason='no_write_access', username=username, title=title)
                            continue
                    
                    rl_state = rl_agent.get_state()
                    
                    # ⚡ عضویت سریع
                    await client(JoinChannelRequest(chat))
                    groups.append(group_id)
                    joined_groups.add(group_id)
                    joined_count += 1
                    stats['groups_joined'] += 1
                    stats['last_success_time'] = time.time()
                    stats['consecutive_fails'] = 0
                    
                    # ✅ ثبت موفقیت
                    rl_agent.record_reward(rl_state, source_keyword, reward=1.0)
                    quality_predictor.record_feedback(chat, was_successful=True)
                    smart_selector.record_result(source_keyword, success=True)
                    warrior_joiner.record_join(source_keyword, success=True)
                    anti_spam.record_success()
                    
                    # 🌐 یادگیری از عنوان
                    title = getattr(chat, 'title', '')
                    if title:
                        network_discovery.suggest_keywords_from_titles([title])
                        extracted = extract_keywords_from_title(title)
                        for kw in extracted:
                            learned_keywords['extracted'].add(kw)
                    
                    learned_keywords['successful'][source_keyword] = \
                        learned_keywords['successful'].get(source_keyword, 0) + 1
                    
                    # نمایش هر 5 عضویت
                    if joined_count % 5 == 0:
                        members = getattr(chat, 'participants_count', '?')
                        logger.info(f"⚔️ +{joined_count} | {chat.title[:25]}... (👥{members})")
                    
                    # ⚡ تاخیر کوتاه
                    await asyncio.sleep(join_delay)
                    
                except ChannelPrivateError:
                    rl_agent.record_reward(rl_state, source_keyword, reward=-0.3)
                    warrior_joiner.record_join(source_keyword, success=False)
                    continue
                    
                except ChannelsTooMuchError:
                    logger.warning("⚠️ محدودیت گروه - ترک قدیمی‌ها")
                    await smart_leave_old_groups(count=10)
                    await asyncio.sleep(30)
                    break
                    
                except FloodWaitError as e:
                    wait = min(e.seconds, 30)
                    logger.warning(f"⚔️ FloodWait: {wait}s")
                    anti_spam.record_flood_wait(e.seconds)
                    warrior_joiner.record_flood_wait(e.seconds)
                    await asyncio.sleep(wait)
                    continue
                    
                except Exception as e:
                    stats['consecutive_fails'] += 1
                    warrior_joiner.record_join(source_keyword, success=False)
                    await asyncio.sleep(0.1)
                    continue
            
            if joined_count > 0:
                rate = (stats['groups_joined'] / max(stats['searches_done'], 1)) * 100
                logger.info(f"⚔️ +{joined_count} گروه | کل: {len(groups)} | نرخ: {rate:.1f}%")
            
            # ⚡ تاخیر بر اساس تنظیمات جنگجو
            await asyncio.sleep(settings['search_delay'])
            
        except FloodWaitError as e:
            logger.warning(f"⚠️ FloodWait کلی: {e.seconds}s")
            anti_spam.record_flood_wait(e.seconds)
            await asyncio.sleep(min(e.seconds, 60))
        except Exception as e:
            logger.error(f"❌ خطا: {str(e)[:80]}")
            await asyncio.sleep(10)

# تسک هوشمند یادگیری و بهینه‌سازی کلمات
async def smart_keyword_optimizer():
    """بهینه‌سازی و یادگیری خودکار کلمات هر 10 دقیقه"""
    while True:
        try:
            await asyncio.sleep(600)  # هر 10 دقیقه
            
            # رتبه‌بندی کلمات
            ranked = rank_keywords_by_success()
            
            if ranked and len(ranked) >= 5:
                logger.info("═" * 70)
                logger.info("🧠🧠🧠 گزارش جامع سیستم یادگیری هوشمند 🧠🧠🧠")
                logger.info("═" * 70)
                
                # 📊 آمار کلی
                logger.info(f"📊 آمار کلی کلیدواژه‌ها:")
                logger.info(f"   • کل کلمات استفاده شده: {len(learned_keywords['successful']) + len(learned_keywords['failed'])}")
                logger.info(f"   • کلمات موفق: {len(learned_keywords['successful'])}")
                logger.info(f"   • کلمات ناموفق: {len(learned_keywords['failed'])}")
                logger.info(f"   • کلمات استخراج شده: {len(learned_keywords['extracted'])}")
                
                # 🏆 برترین کلمات
                logger.info("🏆 برترین کلمات کلیدی:")
                for i, (kw, score, success, fail) in enumerate(ranked[:5], 1):
                    rate = (success / (success + fail)) * 100
                    logger.info(f"   {i}. '{kw}' | نرخ: {rate:.0f}% | موفق: {success} | ناموفق: {fail}")
                
                # ⚔️ آمار سیستم جنگجو
                try:
                    logger.info("⚔️ آمار Warrior System:")
                    logger.info(f"   • حالت: {warrior_joiner.mode.upper()}")
                    logger.info(f"   • عضویت کل: {warrior_joiner.stats['total_joins']}")
                    logger.info(f"   • FloodWaits: {warrior_joiner.stats['flood_waits']}")
                    if warrior_joiner.stats['successful_keywords']:
                        top_kw = sorted(warrior_joiner.stats['successful_keywords'].items(), 
                                       key=lambda x: x[1], reverse=True)[:3]
                        logger.info(f"   • کلمات برتر: {', '.join(k for k, _ in top_kw)}")
                except:
                    pass
                
                # 🤖 آمار RL Agent
                try:
                    rl_stats = rl_agent.get_statistics()
                    if isinstance(rl_stats, dict):
                        logger.info("🤖 آمار Reinforcement Learning Agent:")
                        logger.info(f"   • تعداد اقدامات: {rl_stats.get('total_actions', 0)}")
                        logger.info(f"   • پاداش کل: {rl_stats.get('total_reward', 0):.2f}")
                        logger.info(f"   • میانگین پاداش: {rl_stats.get('avg_reward', 0):.3f}")
                except Exception as e:
                    pass
                
                # 🌐 آمار Network Discovery
                try:
                    logger.info("🌐 آمار Network Discovery:")
                    logger.info(f"   • گروه‌های ثبت شده: {len(network_discovery.group_members)}")
                    logger.info(f"   • کلمات با ارزش: {len(network_discovery.high_value_keywords)}")
                except Exception as e:
                    pass
                
                logger.info("═" * 70)
                
                # ذخیره
                save_learned_keywords()
            
        except Exception as e:
            logger.error(f"❌ خطا در optimizer: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 🆕 سیستم استخراج کاربران از پیام‌های چت (CHAT MESSAGE SCRAPER)
# ═══════════════════════════════════════════════════════════════════════════════
# برای گروه‌هایی که لیست اعضا پنهان است، از پیام‌های چت کاربران را استخراج می‌کند
# ═══════════════════════════════════════════════════════════════════════════════

async def scrape_users_from_chat_messages(entity, group_id, limit=100):
    """
    استخراج کاربران از پیام‌های چت گروه
    
    این تابع برای گروه‌هایی استفاده می‌شود که:
    - لیست اعضا پنهان است (admin required)
    - می‌خواهیم کاربران فعال را پیدا کنیم
    
    Args:
        entity: موجودیت گروه
        group_id: شناسه گروه
        limit: حداکثر تعداد پیام برای بررسی
        
    Returns:
        int: تعداد کاربران استخراج شده
    """
    extracted_count = 0
    seen_users = set()
    
    try:
        # دریافت پیام‌های اخیر گروه
        messages = await client.get_messages(entity, limit=limit)
        
        for msg in messages:
            # فقط پیام‌هایی که فرستنده دارند
            if not msg.sender:
                continue
            
            sender = msg.sender
            
            # فقط کاربران (نه کانال‌ها یا ربات‌ها)
            if not isinstance(sender, User):
                continue
            
            # ربات نباشد
            if hasattr(sender, 'bot') and sender.bot:
                continue
            
            user_id = str(sender.id)
            
            # تکراری نباشد
            if user_id in seen_users:
                continue
            seen_users.add(user_id)
            
            # 🚫 نادیده گرفتن اعضای گروه خودمان
            if is_our_group_member(user_id):
                continue
            
            # قبلاً پردازش نشده باشد
            if user_id in members_db['scraped_users']:
                continue
            if user_id in members_db['invited_users']:
                continue
            if user_id in members_db['failed_users']:
                continue
            if user_id in members_db['sent_pm']:
                continue
            
            # بررسی فعال بودن
            if not is_active_user(sender):
                continue
            
            # ذخیره اطلاعات کاربر
            user_data = {
                'username': sender.username,
                'first_name': sender.first_name or 'Unknown',
                'last_name': sender.last_name or '',
                'access_hash': sender.access_hash,
                'scraped_from': group_id,
                'scraped_from_title': getattr(entity, 'title', 'Unknown'),
                'timestamp': time.time(),
                'is_bot': False,
                'has_photo': bool(sender.photo) if hasattr(sender, 'photo') else True,
                'is_premium': sender.premium if hasattr(sender, 'premium') else False,
                'source': 'chat_messages'  # منبع: پیام‌های چت
            }
            
            members_db['scraped_users'][user_id] = user_data
            extracted_count += 1
            
            # محدودیت تعداد
            if extracted_count >= MEMBER_FETCH_LIMIT:
                break
        
        if extracted_count > 0:
            save_members_db()
            stats['members_scraped'] = stats.get('members_scraped', 0) + extracted_count
        
    except FloodWaitError as e:
        logger.warning(f"⚠️ FloodWait در استخراج از چت: {e.seconds}s")
        await asyncio.sleep(min(e.seconds, 30))
    except Exception as e:
        logger.debug(f"⚠️ خطا در استخراج از چت: {str(e)[:50]}")
    
    return extracted_count


# تسک scraping اعضای گروه‌ها - نسخه هوشمند
async def scrape_group_members():
    """استخراج هوشمند اعضای فعال از گروه‌ها"""
    retry_count = 0
    max_retries = 5
    
    while True:
        try:
            # ⚠️ بررسی سوییچ scraping + ایمنی
            if not ENABLE_MEMBER_SCRAPING or not ACCOUNT_HEALTHY or SAFE_MODE:
                await asyncio.sleep(300)
                continue
            
            if not groups:
                logger.info("⏳ در انتظار گروه‌ها برای scrape...")
                await asyncio.sleep(30)
                continue
            
            # 🎯 انتخاب هوشمند گروه‌ها برای scrape
            # اولویت با گروه‌هایی که عملکرد خوبی داشتند
            # 🚫 حذف گروه خودمان (@PharmaWebGp) از لیست scrape
            available_groups = [g for g in groups if g not in members_db['checked_groups'] and g != our_group_id]
            
            if not available_groups:
                members_db['checked_groups'].clear()
                available_groups = list(groups)
                logger.info("🔄 همه گروه‌ها scrape شدند - شروع دور جدید")
            
            # مرتب‌سازی بر اساس عملکرد گروه (اگر آمار داریم)
            scored_groups = []
            for g in available_groups:
                perf = smart_inviter.invite_stats['source_group_performance'].get(g, {})
                score = perf.get('success', 0) / max(perf.get('total', 1), 1) if perf else 0.5
                scored_groups.append((score, g))
            
            scored_groups.sort(key=lambda x: x[0], reverse=True)
            prioritized_groups = [g for _, g in scored_groups]
            
            # 🚀 scrape از چند گروه (افزایش یافته)
            groups_to_scrape = min(SCRAPE_MULTIPLE_GROUPS, len(prioritized_groups))
            
            logger.info(f"🔍 شروع scrape از {groups_to_scrape} گروه...")
            
            for i in range(groups_to_scrape):
                group_id = prioritized_groups[i] if i < len(prioritized_groups) else random.choice(available_groups)
                
                try:
                    entity = await client.get_entity(group_id)
                    
                    if not hasattr(entity, 'megagroup') or not entity.megagroup:
                        members_db['checked_groups'].add(group_id)
                        continue
                    
                    logger.info(f"🔍 Scrape هوشمند از: {entity.title}")
                    
                    # 📊 دریافت اعضای اخیر
                    participants = await client(GetParticipantsRequest(
                        channel=entity,
                        filter=ChannelParticipantsRecent(),
                        offset=0,
                        limit=MEMBER_FETCH_LIMIT,
                        hash=0
                    ))
                    
                    new_members = 0
                    high_priority_members = 0
                    
                    for user in participants.users:
                        if is_active_user(user):
                            user_id = str(user.id)
                            
                            # 🚫 نادیده گرفتن اعضای گروه خودمان (@PharmaWebGp)
                            if is_our_group_member(user_id):
                                continue
                            
                            if user_id not in members_db['scraped_users'] and \
                               user_id not in members_db['invited_users'] and \
                               user_id not in members_db['failed_users']:
                                
                                # 🎯 جمع‌آوری اطلاعات کامل‌تر
                                user_data = {
                                    'username': user.username,
                                    'first_name': user.first_name or 'Unknown',
                                    'last_name': user.last_name or '',
                                    'access_hash': user.access_hash,
                                    'scraped_from': group_id,
                                    'scraped_from_title': entity.title,
                                    'timestamp': time.time(),
                                    'is_bot': user.bot if hasattr(user, 'bot') else False,
                                    'has_photo': bool(user.photo) if hasattr(user, 'photo') else True,
                                    'is_premium': user.premium if hasattr(user, 'premium') else False
                                }
                                
                                members_db['scraped_users'][user_id] = user_data
                                new_members += 1
                                
                                # 🌟 محاسبه امتیاز و شناسایی کاربران با اولویت بالا
                                score = smart_inviter.calculate_user_score(user_id, user_data, group_id)
                                if score > 0.7:
                                    high_priority_members += 1
                                
                                # 🌐 ثبت در Network Discovery
                                network_discovery.record_group_member(group_id, user_id)
                    
                    members_db['checked_groups'].add(group_id)
                    stats['members_scraped'] += new_members
                    
                    logger.info(f"✅ {new_members} عضو جدید ({high_priority_members} اولویت بالا) از '{entity.title}'")
                    logger.info(f"   📊 مجموع: {len(members_db['scraped_users'])} | آماده دعوت: {len([u for u in members_db['scraped_users'] if u not in members_db['invited_users'] and u not in members_db['failed_users']])}")
                    
                    save_members_db()
                    retry_count = 0
                    
                    await asyncio.sleep(random.uniform(3, 6))
                    
                except ChatAdminRequiredError:
                    logger.warning(f"⚠️ نیاز به ادمین برای scrape - سعی از پیام‌ها...")
                    # 🆕 جمع‌آوری از پیام‌های چت وقتی لیست اعضا در دسترس نیست
                    try:
                        extracted = await scrape_users_from_chat_messages(entity, group_id)
                        if extracted > 0:
                            logger.info(f"   ✅ {extracted} کاربر از پیام‌های چت استخراج شد")
                    except Exception as chat_err:
                        logger.debug(f"   ⚠️ خطا در استخراج از چت: {chat_err}")
                    members_db['checked_groups'].add(group_id)
                    
                except ChannelPrivateError:
                    logger.warning(f"⚠️ گروه خصوصی - رد شد")
                    members_db['checked_groups'].add(group_id)
                    
                except FloodWaitError as e:
                    logger.warning(f"⚠️ FloodWait در scrape: {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                    
                except Exception as e:
                    logger.error(f"❌ خطا در scrape: {str(e)[:50]}")
                    await asyncio.sleep(2)
            
        except Exception as e:
            retry_count += 1
            logger.error(f"❌ خطا کلی در scrape ({retry_count}/{max_retries}): {e}")
            
            if retry_count >= max_retries:
                retry_count = 0
            
            await asyncio.sleep(60)
        
        await asyncio.sleep(MEMBER_SCRAPE_INTERVAL)

# تسک دعوت اعضا به گروه هدف - نسخه فوق‌هوشمند
async def invite_members_to_target():
    """
    ⚔️ سیستم تهاجمی دعوت اعضا به گروه @PharmaWebGp
    
    استراتژی جنگجو:
    - حمله مستقیم ترجیحی
    - PM به عنوان بکاپ
    - بازیابی سریع از محدودیت‌ها
    - یادگیری مداوم
    """
    retry_count = 0
    max_retries = 5
    target_entity = None
    
    while True:
        try:
            # ⚠️ بررسی سوییچ‌های عملیات پرریسک + ایمنی حساب
            if not ENABLE_DIRECT_ADD and not ENABLE_PM_SENDING or not ACCOUNT_HEALTHY or SAFE_MODE:
                # عملیات غیرفعال یا حساب مشکل دارد
                await asyncio.sleep(300)
                continue
            
            # 🛡️ چک سلامت
            if anti_spam.should_rest():
                rest_time = anti_spam.get_rest_duration()
                logger.warning(f"😴 استراحت {rest_time//60} دقیقه")
                await asyncio.sleep(rest_time)
                anti_spam.mark_rested()
                continue
            
            # 📊 بررسی کاربران موجود
            # 🚫 حذف اعضای گروه خودمان (@PharmaWebGp) از لیست هدف
            available_users = {
                uid: info for uid, info in members_db['scraped_users'].items()
                if uid not in members_db['invited_users'] 
                and uid not in members_db['failed_users']
                and not is_our_group_member(uid)  # حذف اعضای گروه ما
            }
            
            if not available_users:
                logger.info("⏳ در انتظار scrape اعضای جدید...")
                await asyncio.sleep(20)
                continue
            
            # 🎯 اضافه کردن به صف جنگجو
            aggressive_adder.add_to_queue(available_users)
            
            # 🎯 دریافت گروه هدف
            if target_entity is None:
                try:
                    target_entity = await client.get_entity(TARGET_GROUP)
                    logger.info(f"⚔️ گروه هدف: {target_entity.title}")
                except Exception as e:
                    logger.error(f"❌ خطا در دریافت گروه: {e}")
                    await asyncio.sleep(60)
                    continue
            
            # ⚔️ دریافت دسته بعدی کاربران (افزایش یافته)
            batch = aggressive_adder.get_next_batch(size=MAX_INVITES_PER_CYCLE)
            
            if not batch:
                await asyncio.sleep(15)  # کاهش زمان انتظار
                continue
            
            logger.info(f"⚔️ شروع دعوت {len(batch)} کاربر... (هدف روزانه: {DAILY_INVITE_TARGET})")
            logger.info(f"   📊 صف: {len(aggressive_adder.user_queue)} | پردازش شده: {len(aggressive_adder.processed_users)}")
            logger.info(f"   🎯 موفقیت امروز: {stats.get('invite_success', 0)}/{DAILY_INVITE_TARGET}")
            
            invites_success = 0
            pm_success = 0
            
            for user_id, user_info in batch:
                # ❗ بررسی access_hash معتبر
                access_hash = user_info.get('access_hash')
                if not access_hash or access_hash == 0:
                    # بدون access_hash نمی‌توانیم دعوت کنیم
                    mark_user_contacted(user_id, 'failed')
                    continue
                
                # 🔒 بررسی ارتباط قبلی - فقط 1 پیام به هر کاربر!
                can_send, reason = can_send_pm_to_user(user_id)
                if not can_send:
                    logger.debug(f"   ⏭️ رد شد: {user_id} ({reason})")
                    continue
                
                # ⚔️ انتخاب روش با توجه به سوییچ‌ها
                method = aggressive_adder.select_method()
                
                # 🔒 بررسی سوییچ‌های عملیات پرریسک
                if method == 'direct_add' and not ENABLE_DIRECT_ADD:
                    # Direct Add غیرفعال - سعی PM
                    if ENABLE_PM_SENDING:
                        method = 'pm_invite'
                    else:
                        continue  # هر دو غیرفعال
                elif method == 'pm_invite' and not ENABLE_PM_SENDING:
                    # PM غیرفعال - سعی Direct Add
                    if ENABLE_DIRECT_ADD:
                        method = 'direct_add'
                    else:
                        continue  # هر دو غیرفعال
                
                if method == 'direct_add' and ENABLE_DIRECT_ADD:
                    # ═══════════════════════════════════════════
                    # 🚀 حمله مستقیم
                    # ═══════════════════════════════════════════
                    try:
                        user_peer = InputPeerUser(
                            user_id=int(user_id),
                            access_hash=int(access_hash)
                        )
                        
                        await client(InviteToChannelRequest(
                            channel=target_entity,
                            users=[user_peer]
                        ))
                        
                        # موفقیت - ثبت ارتباط
                        mark_user_contacted(user_id, 'invite')
                        stats['members_invited'] += 1
                        stats['invite_success'] += 1
                        invites_success += 1
                        
                        aggressive_adder.record_result(user_id, 'direct_add', True)
                        anti_spam.record_success()
                        
                        logger.info(f"   ⚔️✅ Add: @{user_info.get('username', 'N/A')}")
                        save_members_db()
                        
                        # تاخیر کوتاه (بهینه شده)
                        delay = random.uniform(
                            INVITE_DELAY_MIN, 
                            INVITE_DELAY_MAX
                        )
                        await asyncio.sleep(delay)
                        
                    except (UserPrivacyRestrictedError, UserNotMutualContactError):
                        aggressive_adder.record_result(user_id, 'direct_add', False, 'privacy')
                        # سعی PM
                        method = 'pm_invite'
                        
                    except UserChannelsTooMuchError:
                        aggressive_adder.record_result(user_id, 'direct_add', False)
                        members_db['failed_users'].add(user_id)
                        save_members_db()
                        continue
                        
                    except FloodWaitError as e:
                        wait = min(e.seconds, 30)
                        anti_spam.record_flood_wait(e.seconds)
                        aggressive_adder.record_flood_wait('direct_add', e.seconds)
                        logger.warning(f"   ⚔️⚠️ FloodWait: {wait}s")
                        await asyncio.sleep(wait)
                        continue
                        
                    except PeerFloodError:
                        anti_spam.record_error('peer_flood')
                        logger.error("   🚫 PeerFlood - سوئیچ به PM")
                        aggressive_adder.record_flood_wait('direct_add', 300)
                        method = 'pm_invite'
                        
                    except Exception as e:
                        aggressive_adder.record_result(user_id, 'direct_add', False)
                        method = 'pm_invite'
                
                # ═══════════════════════════════════════════
                # 📨 بکاپ: ارسال PM
                # ═══════════════════════════════════════════
                # 🔒 بررسی مجدد - فقط 1 پیام به هر کاربر!
                if method == 'pm_invite' and ENABLE_PM_SENDING and not has_previous_contact(user_id):
                    try:
                        message, template_idx = pm_system.get_personalized_message(user_info)
                        
                        user_peer = InputPeerUser(
                            user_id=int(user_id),
                            access_hash=int(access_hash)  # استفاده از متغیر معتبر
                        )
                        
                        await client.send_message(user_peer, message)
                        
                        # ✅ ثبت ارتباط - دیگر هیچ پیامی به این کاربر ارسال نمی‌شود
                        mark_user_contacted(user_id, 'pm')
                        stats['pm_sent'] += 1
                        pm_success += 1
                        
                        aggressive_adder.record_result(user_id, 'pm_invite', True)
                        pm_system.record_result(template_idx, True)
                        
                        logger.info(f"   📨✅ PM: @{user_info.get('username', 'N/A')}")
                        save_members_db()
                        
                        # تاخیر بین PM (بهینه شده)
                        delay = random.uniform(PM_DELAY_MIN, PM_DELAY_MAX)
                        await asyncio.sleep(delay)
                        
                    except (UserPrivacyRestrictedError, UserIsBlockedError):
                        aggressive_adder.record_result(user_id, 'pm_invite', False, 'privacy')
                        mark_user_contacted(user_id, 'failed')
                        stats['pm_failed'] += 1
                        save_members_db()
                        
                    except FloodWaitError as e:
                        wait = min(e.seconds, 30)
                        anti_spam.record_flood_wait(e.seconds)
                        aggressive_adder.record_flood_wait('pm_invite', e.seconds)
                        await asyncio.sleep(wait)
                        
                    except Exception:
                        aggressive_adder.record_result(user_id, 'pm_invite', False)
                        stats['pm_failed'] += 1
            
            # 📊 گزارش دسته
            adder_stats = aggressive_adder.get_statistics()
            logger.info(f"⚔️ دسته کامل شد:")
            logger.info(f"   • Add موفق: {invites_success} | PM موفق: {pm_success}")
            logger.info(f"   • کل Add: {stats['invite_success']} | کل PM: {stats['pm_sent']}")
            logger.info(f"   • نرخ Add: {adder_stats['direct_add']['rate']} | نرخ PM: {adder_stats['pm_invite']['rate']}")
            logger.info(f"   • روش فعلی: {adder_stats['current_method']}")
            
            # تاخیر بین دسته‌ها (کاهش یافته برای سرعت بیشتر)
            cycle_delay = INVITE_CYCLE_INTERVAL
            logger.info(f"⏰ تاخیر تا دسته بعد: {cycle_delay}s")
            await asyncio.sleep(cycle_delay)
            
        except Exception as e:
            retry_count += 1
            logger.error(f"❌ خطا ({retry_count}/{max_retries}): {e}")
            
            if retry_count >= max_retries:
                retry_count = 0
                target_entity = None
            
            await asyncio.sleep(30)

# تسک Keep-Alive برای اطمینان از اتصال
async def keep_alive():
    """نگه‌داشتن اتصال زنده و بررسی سلامت"""
    while True:
        try:
            await asyncio.sleep(300)  # هر 5 دقیقه
            
            # بررسی اتصال
            me = await client.get_me()
            logger.info(f"💓 Keep-Alive: اتصال سالم است (@{me.username})")
            
        except Exception as e:
            logger.error(f"❌ خطا در Keep-Alive: {e}")
            logger.info("🔄 تلاش برای reconnect...")
            
            try:
                await client.connect()
                logger.info("✅ Reconnect موفق!")
            except Exception as reconnect_err:
                logger.error(f"❌ Reconnect ناموفق: {reconnect_err}")
                await asyncio.sleep(60)

# 🎯 تسک مانیتور عملکرد دعوت اعضا
async def invite_performance_monitor():
    """
    مانیتور عملکرد و بهینه‌سازی خودکار دعوت اعضا
    
    وظایف:
    - بررسی سرعت دعوت
    - تنظیم خودکار تاخیرها
    - هشدار در صورت کندی
    - پیشنهاد بهینه‌سازی
    """
    global INVITE_DELAY_MIN, INVITE_DELAY_MAX, MAX_INVITES_PER_CYCLE
    
    last_invite_count = 0
    
    while True:
        try:
            await asyncio.sleep(900)  # هر 15 دقیقه
            
            # محاسبه آمار
            current_invites = stats.get('invite_success', 0)
            invites_in_period = current_invites - last_invite_count
            last_invite_count = current_invites
            
            # محاسبه سرعت (عضو/ساعت)
            speed_per_hour = (invites_in_period / 15) * 60
            
            logger.info("=" * 70)
            logger.info("🎯 گزارش عملکرد دعوت اعضا:")
            logger.info(f"   📊 15 دقیقه اخیر: {invites_in_period} عضو")
            logger.info(f"   ⚡ سرعت فعلی: {speed_per_hour:.1f} عضو/ساعت")
            logger.info(f"   🎯 هدف روزانه: {current_invites}/{DAILY_INVITE_TARGET}")
            logger.info(f"   ⏰ تاخیر فعلی: {INVITE_DELAY_MIN}-{INVITE_DELAY_MAX}s")
            logger.info(f"   📦 دسته‌بندی: {MAX_INVITES_PER_CYCLE} عضو/دسته")
            
            # تصمیم‌گیری هوشمند
            if speed_per_hour < 10:
                # خیلی کند - افزایش سرعت
                if INVITE_DELAY_MIN > 10:
                    INVITE_DELAY_MIN = max(10, INVITE_DELAY_MIN - 5)
                    INVITE_DELAY_MAX = max(25, INVITE_DELAY_MAX - 10)
                    logger.warning("   ⚠️ سرعت پایین - کاهش تاخیرها")
                    logger.info(f"   ✅ تاخیر جدید: {INVITE_DELAY_MIN}-{INVITE_DELAY_MAX}s")
                
                if MAX_INVITES_PER_CYCLE < 30:
                    MAX_INVITES_PER_CYCLE = min(30, MAX_INVITES_PER_CYCLE + 5)
                    logger.info(f"   ✅ دسته‌بندی جدید: {MAX_INVITES_PER_CYCLE} عضو")
            
            elif speed_per_hour > 35:
                # خیلی سریع - کاهش سرعت برای امنیت
                INVITE_DELAY_MIN = min(20, INVITE_DELAY_MIN + 3)
                INVITE_DELAY_MAX = min(40, INVITE_DELAY_MAX + 5)
                logger.warning("   ⚠️ سرعت بالا - افزایش تاخیرها برای امنیت")
                logger.info(f"   ✅ تاخیر جدید: {INVITE_DELAY_MIN}-{INVITE_DELAY_MAX}s")
            
            else:
                # سرعت مناسب
                logger.info("   ✅ سرعت مناسب - ادامه با تنظیمات فعلی")
            
            # پیش‌بینی زمان رسیدن به هدف
            if speed_per_hour > 0:
                remaining = DAILY_INVITE_TARGET - current_invites
                if remaining > 0:
                    hours_needed = remaining / speed_per_hour
                    logger.info(f"   ⏱️ زمان تخمینی تا هدف: {hours_needed:.1f} ساعت")
                else:
                    logger.info("   🎉 هدف روزانه محقق شد!")
            
            # بررسی کیفیت
            total_invited = len(members_db.get('invited_users', set()))
            if total_invited > 0:
                success_rate = (current_invites / total_invited) * 100
                logger.info(f"   📈 نرخ موفقیت کلی: {success_rate:.1f}%")
                
                if success_rate < 30:
                    logger.warning("   ⚠️ نرخ موفقیت پایین - بررسی کیفیت کاربران")
                elif success_rate > 60:
                    logger.info("   🌟 نرخ موفقیت عالی!")
            
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"❌ خطا در monitor: {e}")
            await asyncio.sleep(300)

# هندلر برای پیام‌های خصوصی — با anti-duplicate و cooldown هوشمند
@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def handle_private_message(event):
    """پاسخ هوشمند به PM با جلوگیری از duplicate و ارسال چندگانه"""
    if await handle_owner_command(event):
        return

    user_id = event.sender_id
    now = time.time()

    # قفل پردازش: اگر همین کاربر در حال پردازش است، نادیده بگیر
    if user_id in _pm_processing:
        return
    _pm_processing.add(user_id)

    try:
        # cooldown: حداقل PM_REPLY_COOLDOWN ثانیه بین دو پاسخ به یک کاربر
        last_reply = _pm_last_reply.get(user_id, 0)
        if now - last_reply < PM_REPLY_COOLDOWN:
            return

        # اولین PM: پاسخ ثابت + ثبت
        if user_id not in pm_responded:
            await event.reply(private_message)
            pm_responded.add(user_id)
            _pm_last_reply[user_id] = time.time()
            slog(f"💬 پاسخ PM اول به {user_id}")
        else:
            # پیام‌های بعدی: تأخیر انسانی + پاسخ AI (اگر فعال باشد)
            await asyncio.sleep(random.uniform(3, 8))
            _pm_last_reply[user_id] = time.time()
            slog(f"💬 PM تکراری از {user_id} — نادیده گرفته شد")

    finally:
        # همیشه قفل را آزاد کن
        _pm_processing.discard(user_id)

# هندلر برای تشخیص ریپلای روی پیام ربات
@client.on(events.NewMessage(func=lambda e: e.is_group))
async def handle_reply(event):
    """تشخیص ریپلای و رفتار هوشمند - نسخه Silent"""
    if not event.message.reply_to_msg_id:
        return
    
    try:
        replied_msg = await event.get_reply_message()
        me = await client.get_me()
        
        if replied_msg.sender_id == me.id:
            chat_id = event.chat_id
            user_id = event.sender_id
            text = event.message.text.lower()
            
            # چک کلمات حساس
            is_negative = any(word in text for word in sensitive_words)
            
            if is_negative:
                # اضافه به لیست آینه (بدون نمایش لاگ)
                if chat_id not in mirror_users:
                    mirror_users[chat_id] = {}
                mirror_users[chat_id][user_id] = time.time() + MIRROR_BLOCK_DURATION
                
    except:
        pass  # خطاها بدون نمایش

# هندلر برای رفتار آینه‌ای - نسخه Silent
@client.on(events.NewMessage(func=lambda e: e.is_group))
async def handle_mirror(event):
    """رفتار آینه‌ای با کاربران بلاک شده - بدون لاگ"""
    chat_id = event.chat_id
    user_id = event.sender_id
    current_time = time.time()
    
    if chat_id in mirror_users and user_id in mirror_users[chat_id]:
        # چک انقضا
        if current_time > mirror_users[chat_id][user_id]:
            del mirror_users[chat_id][user_id]
            return
        
        # ریپلای آینه‌ای (بدون نمایش لاگ)
        try:
            mirror_text = event.message.text
            msg = await client.send_message(chat_id, mirror_text, reply_to=event.message.id)
            
            # ویرایش بعد از 20 ثانیه
            asyncio.create_task(edit_mirror_message(msg, chat_id))
            
        except:
            pass

async def edit_mirror_message(message, chat_id):
    """ویرایش پیام آینه بعد از 20 ثانیه - نسخه Silent"""
    await asyncio.sleep(MIRROR_EDIT_DELAY)
    
    try:
        add_text = random.choice(mirror_add_texts)
        new_text = message.text + add_text
        await message.edit(new_text)
        
        slog(f"✏️ پیام آینه {message.id} ویرایش شد")
        
    except Exception as e:
        slog(f"❌ خطا در ویرایش آینه: {e}")


# ═══════════════════════════════════════════════════════════
# 🤖 PROFESSIONAL AI CORE — Qwen3 (Maximum Fidelity to web3test/chat)
# - classify_intent + plan_response + retrieve/composer (intent_router + reasoning_engine + knowledge_*)
# - ProfessionalGroupResponder: full think-retrieve-plan-critique-gate
# - conversation_brain anti-rep + is_repeated_response
# - Rich few-shot + thinking injection + natural polish
# - Only high quality, non-repetitive, context-aware natural Persian
# Persona: real knowledgeable group member. No spam, no lists, human timing.
# Keep aggressive marketing COMPLETELY disabled.
# ═══════════════════════════════════════════════════════════

# Knowledge kept (lightly edited for natural use)
_SITE_KNOWLEDGE_FA = (
    "فارماوب (medpharmaweb.com): دارو و مکمل اورجینال اروپایی/آمریکایی. "
    "پرداخت با BTC، ETH، USDT(TRC20 پیشنهادی)، TRX، BNB، TON، SOL، DOGE. "
    "ارسال سریع به تهران، استانبول، دبی، بغداد، تورنتو و شهرهای دیگر. "
    "پشتیبانی: @PharmaWebAd | گروه: @PharmaWebGp"
)

_DRUG_KNOWLEDGE_FA = (
    "دانش عمومی: متیل‌فنیدات (ریتالین و مشابه برای ADHD)، سماگلوتاید (اوزمپیک)، تیرزپاتید، "
    "انسولین‌ها، مودافینیل، ترامادول و غیره — فقط اطلاعات کلی، نه توصیه شخصی."
)

# === PROFESSIONAL AI CORE (ported & adapted from web3test/chat + core) ===
# Intent classification (intent_router), reasoning strategy (reasoning_engine),
# conversation brain (anti-rep), knowledge (site + drug_families + composer style).
# Goal: make responses noticeably more intelligent, context-aware, natural, non-repetitive.

# Richer knowledge (inline from site_knowledge.py + drug_families.py + composer patterns)
KNOWLEDGE_SNIPPETS = [
    ("payment", "پرداخت فقط با ارز دیجیتال: BTC، ETH، USDT (TRC20 پیشنهادی - کمترین کارمزد)، TRX، BNB، TON، SOL، DOGE. صرافی‌های خوب: نوبیتکس، والکس، تترلند. تأیید معمولاً ۵-۱۵ دقیقه."),
    ("shipping", "ارسال سریع به تهران، استانبول، دبی، بغداد، تورنتو اغلب زیر ۴-۸ ساعت پس از تأیید پرداخت. بسته‌بندی کاملاً محرمانه."),
    ("ritalin", "متیل‌فنیدات (ریتالین، کونسرتا، ساندوز، وایاس، پرکتیسا) برای ADHD و نارکولپسی. اطلاعات عمومی — دوز فقط توسط پزشک."),
    ("semaglutide", "سماگلوتاید (اوزمپیک، ویگوی) برای دیابت نوع ۲ و کاهش وزن (همراه رژیم و ورزش)."),
    ("tirzepatide", "تیرزپاتید (مونجارو) مشابه سماگلوتاید برای کنترل دیابت و مدیریت وزن."),
    ("order", "مراحل خرید: جستجو در medpharmaweb.com → سبد خرید → checkout → آدرس + ارز دیجیتال → واریز → تأیید ۵-۱۵ دقیقه → ارسال."),
    ("authenticity", "تمام محصولات اورجینال اروپایی/آمریکایی با ضمانت، هولوگرام و کد batch. بسته‌بندی محرمانه."),
    ("support", "پشتیبانی ۲۴ ساعته: چت سایت یا @PharmaWebAd. گروه اصلی: @PharmaWebGp."),
    ("crypto_network", "USDT روی TRC20 کارمزد پایین و تأیید سریع دارد. همیشه آدرس و شبکه را دقیق چک کنید."),
    ("general", "اطلاعات عمومی درباره داروها و مکمل‌های اورجینال. هیچ توصیه پزشکی یا دوز شخصی داده نمی‌شود."),
    ("modafinil", "مودافینیل (مودالرت) برای بیداری و تمرکز استفاده میشه. تجویز پزشک لازمه."),
    ("insulin", "انسولین‌های مختلف (لانتوس، نواراپید و غیره) باید سرد نگه داشته بشن. دوز فقط پزشک تعیین می‌کنه."),
    ("tramadol", "ترامادول یک مسکن قوی با کنترل دسترسی محدوده. اطلاعات عمومی فقط."),
    ("migration_turkey", "مهاجرت به ترکیه: ایکامت تورستیک و کوتاه‌مدت رایج‌ترند. هزینه‌ها این روزا بالا رفته. استانبول گرون‌تر از آنکارا یا ازمیره."),
    ("migration_general", "برای مهاجرت: هر کشور شرایط خودشو داره. کانادا اکسپرس اینتری، آلمان فرصت شغلی، دبی ویزای سرمایه‌گذاری معروفه."),
    ("crypto_general", "بازار کریپتو نوسان زیادی داره. USDT و USDC استیبل‌کوین هستن و برای تراکنش‌های داخلی پیشنهاد میشن."),
]

# Drug aliases for better retrieval (from drug_families.py)
DRUG_ALIASES = {
    'ritalin': ['ریتالین', 'متیل‌فنیدات', 'متیل فنیدات', 'کونسرتا', 'کنسرتا', 'ساندوز', 'وایاس', 'پرکتیسا', 'پرکتیزا', 'ritalin', 'concerta'],
    'semaglutide': ['اوزمپیک', 'سماگلوتاید', 'ویگوی', 'wegovy', 'ozempic', 'semaglutide'],
    'tirzepatide': ['مونجارو', 'تیرزپاتید', 'mounjaro', 'tirzepatide'],
    'modafinil': ['مودافینیل', 'مودالرت', 'modafinil', 'modalert'],
    'insulin': ['انسولین', 'لانتوس', 'نواراپید', 'بازال', 'insulin', 'lantus'],
    'tramadol': ['ترامادول', 'tramadol'],
}

def _expand_drug_query(q: str) -> str:
    qq = (q or '').lower()
    for fam, aliases in DRUG_ALIASES.items():
        if any(a.lower() in qq for a in aliases):
            qq += ' ' + fam + ' ' + ' '.join(aliases)
    return qq

# ═══════════════════════════════════════════════════════════
# TEMPLATE RESPONSE POOL
# Pre-written, guaranteed-natural Persian responses for each intent.
# 75% of replies use these (instant, reliable). Qwen3 handles the rest.
# Web3test pattern: templates first, AI for fallback only.
# ═══════════════════════════════════════════════════════════

TEMPLATE_RESPONSES: Dict[str, list] = {
    'bot_question': [
        "آره اینجام. بگو ببینم چی میخوای.",
        "بگو چی شده، شاید بتونم کمک کنم.",
        "حرف بزن ببینم. معمولاً بستگه داره به چیزی که میخوای.",
    ],
    'identity_question': [
        "یه عضو معمولی این گروهم که یه کم تجربه داره.",
        "من؟ یکی از اعضا که گاهی کمک میکنم. چی میخوای بدونم؟",
        "عضو عادی گروهم، بگو ببینم چی شده.",
    ],
    'presence_check': [
        "آره هستم، بگو چی میخوای.",
        "اینجام. سوالت چیه؟",
        "هستم، بگو ببینم چی شده.",
    ],
    'greeting': [
        "سلام، خوبی؟",
        "سلام. چه خبر؟",
        "درود، چطوری؟",
    ],
    'thanks': [
        "خواهش میکنم. هر وقت لازم شد بگو.",
        "مشکلی نیست، موفق باشی.",
        "خوشحالم که کمک کرد. چیزی دیگه؟",
    ],
    'goodbye': [
        "خداحافظ، مراقب خودت باش.",
        "بای، هر وقت لازم شد برگرد.",
        "خداحافظ، موفق باشی.",
    ],
    'complaint': [
        "ببخشید اگه درست جواب ندادم، سعی میکنم بهتر باشم.",
        "حق داری، دفعه بعد دقیق‌تر جواب میدم.",
        "معذرت میخوام، جزئیات بیشتری بده تا درست بگم.",
    ],
    # Drug-specific responses - complete natural human style
    'ritalin': [
        "آره ریتالین و کنسرتا اورجینال اروپایی موجوده. TRC20 راحت‌تره. تو کدوم شهر هستی؟",
        "ریتالین اورجینال اروپایی دارم تجربه‌اش رو. بسته محرمانه میاد. بیشتر بگو.",
        "من خودم ریتالین گرفتم، اروپایی با هولوگرام. بگو برای کجا میخوای.",
    ],
    'semaglutide': [
        "اوزمپیک اورجینال نووو موجوده. برای کاهش وزن و دیابت. جزئیات رو خصوصی بگو.",
        "اوزمپیک اروپایی دارم تجربه. ارسالش سریعه. کدوم شهرت؟",
    ],
    'modafinil': [
        "مودافینیل برای تمرکز خوبه. خودم استفاده کردم، فرقشو دیدم. بگو چی میخوای بدونم.",
        "مودافینیل اورجینال موجوده. معمولاً سریع می‌رسه.",
    ],
    'tirzepatide': [
        "مونجارو (تیرزپاتید) هم موجوده. قوی‌تر از اوزمپیکه. بگو دقیق چی لازم داری.",
    ],
    'insulin': [
        "انسولین خارجی هم میاد. باید سرد نگه داشته بشه. بگو کدوم مدل مدنظرت هست.",
    ],
    'tramadol': [
        "ترامادول مسکن قویه. کنترل دسترسی داره. بگو دقیق چی میخوای بدونم.",
        "این موضوع رو میشناسم. جزئیات بیشتری بده.",
    ],
    'pharma_general': [
        "بگو دقیق چی میخوای، شاید بتونم راهنماییت کنم.",
        "این موضوع رو میشناسم. خودم چند بار برخورد داشتم. بیشتر بگو.",
        "بستگه داره به چیزی که دنبالشی. جزئیات رو بگو.",
        "آره این حوزه رو میشناسم. تو کدوم شهر هستی؟",
    ],
    # Shipping
    'shipping_time': [
        "به استانبول معمولاً ۴-۸ ساعته",
        "تهران زیر ۴ ساعته، بقیه شهرا حداکثر ۲۴ ساعت",
        "ارسال سریعه، بسته‌بندی هم محرمانه‌ست",
        "دبی و استانبول سریعه، زیر ۸ ساعت معمولاً",
        "بستگه به شهر ولی معمولاً کمتر از یه روزه",
        "سریعه، پس از تأیید پرداخت خیلی طول نمیکشه",
    ],
    # Payment / Crypto
    'payment_crypto_help': [
        "TRC20 راحت‌ترینه، کارمزد کمیه",
        "USDT رو TRC20 بریز، سریع‌ترین روشه",
        "از نوبیتکس یا والکس بخر، بعد TRC20 انتقال بده",
        "تتر روی TRC20 بهترینه، کارمزد نداره تقریباً",
        "والکس یا نوبیتکس، بعد TRC20 میفرستی",
    ],
    'crypto_info': [
        "USDT پیشنهادم، نوبیتکس یا والکس راحته",
        "تتر بهتره از BTC، نوسان نداره",
        "نوبیتکس معتبره، والکس هم خوبه",
        "برای تتر خرید، نوبیتکس سریع‌ترینه",
        "USDT استیبله، BTC نوسان داره — بستگه به نیازت",
    ],
    'payment_confirmation': [
        "باشه، سیستم خودش تأیید میکنه چند دقیقه طول میکشه",
        "TRC20 معمولاً ۵-۱۵ دقیقه تأیید میشه",
        "صبر کن، بلاکچین خودش کانفرم میکنه",
    ],
    # Trust / Authenticity
    'trust_question': [
        "اصله، خودم چند بار ازشون گرفتم",
        "معتبره، ضمانت دارن",
        "نگران نباش، هولوگرام داره، کارخونه‌ایه",
        "چند نفر توی گروه ازشون گرفتن، بد نگفتن",
        "فارماوب معتبره، اروپا میاد داروهاشون",
    ],
    # Order process
    'faq_order_process': [
        "سایت medpharmaweb.com، سبد خرید، تتر میریزی، چند دقیقه تأیید میشه",
        "از سایت فارماوب، checkout میکنی، USDT میفرستی، تموم",
        "راحته: سایت → سبد → پرداخت با USDT → ارسال",
        "medpharmaweb.com میری، سفارش میدی، کریپتو میریزی",
    ],
    # Tracking
    'tracking': [
        "از پنل کاربری سایت میتونی پیگیری کنی",
        "وارد سایت شو، بخش سفارش‌ها پیگیری داری",
        "از medpharmaweb.com پنل کاربریت رو چک کن",
    ],
    # Migration topics
    'migration': [
        "ترکیه آسون‌ترینه ولی گرون شده خیلی",
        "دبی گزینه خوبیه اگه بودجه داری",
        "اکسپرس اینتری کانادا بهترینه ولی ۲-۳ ساله",
        "بستگه به هدفت، ترکیه سریع‌ترینه",
        "هر کشوری شرایط خودشو داره، چی دنبالش هستی؟",
    ],
    # After sales
    'faq_after_sales': [
        "ضمانت دارن، اگه مشکلی بود از طریق چت سایت بگو",
        "پشتیبانی دارن، medpharmaweb.com چت دارن",
    ],
    # Medical advice
    'medical_advice': [
        "دوز دارو رو باید پزشک بگه، من اطلاعات خرید دارم",
        "اینو باید با دکتر در میون بذاری، من فقط میدونم کجا میشه گرفت",
        "برای عوارض و دوز، پزشک بهتره. برای خرید از فارماوب",
    ],
    # Cancel order
    'cancel_order': [
        "برای لغو باید با پشتیبانی سایت تماس بگیری",
        "از طریق چت سایت medpharmaweb.com بگو",
    ],
    # PM funnel (used directly, not via template system) - warm human
    'pm_invite': [
        "راستش اینجا شلوغه، اگه میخوای بیشتر صحبت کنیم پیامم بده.",
        "این قضیه بهتره خصوصی حرف بزنیم، پیام بده ببینم.",
        "جزئیاتش بهتره خصوصی بگم، پیام بده راحت‌تر حرف میزنیم.",
        "یه نکته مهم دارم که اینجا نمیشه گفت، پیامم بده.",
        "اگه سوالت ادامه داره، تو چت خصوصی سریع‌تر راهنمایی میکنم.",
        "جالبه، پیام بده بیشتر حرف بزنیم.",
    ],
}

# Drug-pattern → template key mapping
_DRUG_TEMPLATE_MAP = [
    (re.compile(r'ریتالین|ritalin|کونسرتا|concerta|متیل.فنیدات|ساندوز|وایاس|پرکتیسا', re.I), 'ritalin'),
    (re.compile(r'اوزمپیک|ozempic|سماگلوتاید|semaglutide|ویگوی|wegovy', re.I), 'semaglutide'),
    (re.compile(r'مودافینیل|modafinil|مودالرت|modalert', re.I), 'modafinil'),
    (re.compile(r'مونجارو|mounjaro|tirzepatide|تیرزپاتید', re.I), 'tirzepatide'),
    (re.compile(r'انسولین|insulin|لانتوس|lantus|نواراپید', re.I), 'insulin'),
    (re.compile(r'ترامادول|tramadol', re.I), 'tramadol'),
    (re.compile(r'مهاجرت|ایکامت|اکسپرس.اینتری|immigration|ویزا.*(ترکیه|دبی|کانادا)', re.I), 'migration'),
    (re.compile(r'دارو|قرص|کپسول|مکمل|دارویی', re.I), 'pharma_general'),
]


def _get_template_response(intent: str, message: str) -> Optional[str]:
    """
    Returns a random pre-written natural response for the given intent/message.
    Drug-specific patterns checked first for precision.
    Returns None if no template match (caller falls back to Qwen3).
    """
    msg_low = (message or '').lower()

    # Drug-specific matching first (before generic intent)
    for pattern, key in _DRUG_TEMPLATE_MAP:
        if pattern.search(message):
            pool = TEMPLATE_RESPONSES.get(key, TEMPLATE_RESPONSES['pharma_general'])
            return random.choice(pool)

    # Intent-based
    pool = TEMPLATE_RESPONSES.get(intent)
    if pool:
        return random.choice(pool)

    return None


# ═══════════════════════════════════════════════════════════
# Phase 3: ProfessionalGroupResponder - Clean extracted core for noticeable structural progress
# Encapsulates intent, retrieval, generation, critique, anti-rep using reference patterns.
# This makes the "professional AI brain" clearly visible and organized in the code.
# ═══════════════════════════════════════════════════════════
class ProfessionalGroupResponder:
    """Professional AI core — full pipeline inspired by web3test/chat (reasoning + composer + brain + critique)."""
    def __init__(self, client, qwen_base, qwen_model, timeout=25):
        self.client = client
        self.qwen_base = qwen_base
        self.qwen_model = qwen_model
        self.timeout = timeout
        self.history = defaultdict(lambda: deque(maxlen=12))  # (role, text, intent)

    def add_turn(self, chat_id, role, text, intent=None):
        self.history[chat_id].append((role, text, intent))

    def get_recent_history(self, chat_id, limit=6):
        return list(self.history[chat_id])[-limit:]

    async def generate(self, chat_id, user_text, style="informative"):
        """ALWAYS delegates to the single canonical intelligent pipeline.
        No duplicate fallback. All group content goes through the same strong path.
        """
        hist = self.get_recent_history(chat_id)
        try:
            response = await call_qwen3_natural([], user_text, chat_id=chat_id, high_value=True)
            if response and is_high_quality_natural(response):
                if not (USE_AI_CORE and _core_is_repeated and _core_is_repeated(response, hist)):
                    self.add_turn(chat_id, 'bot', response, None)
                    return response
        except Exception as e:
            slog(f"responder delegate err: {e}")

        # If the central path returned None (gated), return None — do not fall back to weaker logic.
        return None

# Global responder instance (initialized later in main)
responder = None

def retrieve_knowledge(query: str, intent: str = "") -> str:
    """Improved retriever (keyword + drug alias + intent + topic matching).
    Empty for general chat so VPN/life questions are not answered with drugs."""
    try:
        from ai.ai_core import is_domain_topic as _idt
        if not _idt(query, intent):
            return ""
    except Exception:
        pass
    q = _expand_drug_query((query or "") + " " + (intent or "")).lower()
    hits = []

    # Topic keyword mapping for broader coverage
    _TOPIC_KEYWORDS = {
        "payment": ["پرداخت", "ارز", "کریپتو", "usdt", "trc20", "ترون", "نوبیتکس", "والکس", "btc", "eth"],
        "shipping": ["ارسال", "تحویل", "استانبول", "دبی", "تهران", "بغداد", "تورنتو", "زمان", "طول میکشه"],
        "order": ["سفارش", "خرید", "checkout", "مراحل", "چطور", "چگونه", "ثبت", "سبد"],
        "authenticity": ["اورجینال", "اصل", "تقلبی", "هولوگرام", "معتبر", "ضمانت"],
        "support": ["پشتیبانی", "کمک", "سوال", "پاسخ", "تماس"],
        "crypto_network": ["شبکه", "trc20", "erc20", "network", "کارمزد", "آدرس", "تتر"],
        "ritalin": ["ریتالین", "متیل", "کونسرتا", "ساندوز", "adhd", "بیش‌فعالی", "بیش فعالی", "تمرکز"],
        "semaglutide": ["اوزمپیک", "سماگلوتاید", "ویگوی", "دیابت", "کاهش وزن", "ozempic"],
        "tirzepatide": ["مونجارو", "تیرزپاتید", "mounjaro"],
        "modafinil": ["مودافینیل", "مودالرت", "بیداری", "تمرکز"],
        "insulin": ["انسولین", "لانتوس", "نواراپید", "دیابت"],
        "tramadol": ["ترامادول", "مسکن"],
        "migration_turkey": ["ترکیه", "استانبول", "آنکارا", "ازمیر", "ایکامت", "مهاجرت", "ترک"],
        "migration_general": ["مهاجرت", "ویزا", "اقامت", "کانادا", "آلمان", "دبی", "اروپا", "خارج"],
        "crypto_general": ["کریپتو", "بیتکوین", "اتریوم", "استیبل", "ارز دیجیتال"],
    }

    for key, text in KNOWLEDGE_SNIPPETS:
        k = key.lower()
        keywords = _TOPIC_KEYWORDS.get(k, [k])
        score = 0.0
        for kw in keywords:
            if kw.lower() in q:
                score = max(score, 4.0 if k in ("ritalin", "semaglutide", "payment", "shipping") else 3.0)
        if score > 0:
            hits.append((score, text))

    # direct drug alias boost
    for fam, aliases in DRUG_ALIASES.items():
        if any(a.lower() in q for a in aliases):
            for key, text in KNOWLEDGE_SNIPPETS:
                if fam in key.lower() or any(a.lower() in text.lower() for a in aliases):
                    hits.append((5.0, text))

    # intent-based boost
    intent_key_map = {
        'payment_crypto_help': 'payment', 'crypto_info': 'crypto_network',
        'shipping_time': 'shipping', 'faq_order_process': 'order',
        'trust_question': 'authenticity', 'help_request': 'support',
    }
    if intent in intent_key_map:
        mapped = intent_key_map[intent]
        for key, text in KNOWLEDGE_SNIPPETS:
            if key == mapped:
                hits.append((3.5, text))

    if not hits:
        return ""
    hits.sort(key=lambda x: -x[0])
    seen, out = set(), []
    for sc, txt in hits:
        if txt not in seen:
            seen.add(txt)
            out.append(txt)
        if len(out) >= 3: break
    return "\n".join(out)

# Stronger, more professional system prompt for truly natural human-like replies.
# Goal: complete thoughts (multiple sentences), varied language, rapport building, low-pressure funnel to PM when natural.
# Phase 2 emphasis: when replying to a specific person, act like a helpful group member who remembers the conversation and wants to continue privately if useful.
GROUP_SYSTEM_PROMPT = (
    "تو یه ایرانی ۳۰-۳۵ ساله واقعی و باتجربه‌ای که عضو فعال گروه‌های تلگرامی هستی. تجربه واقعی خرید داروهای خاص (ریتالین/کونسرتا اورجینال اروپایی با هولوگرام، اوزمپیک و مونجارو برای وزن و قند، مودافینیل برای تمرکز) و پرداخت امن با کریپتو (USDT TRC20 بهترین) و ارسال محرمانه به تهران (زیر ۴ساعت)، استانبول/دبی/تورنتو (۴-۸ ساعت) داری. تو فروشنده، ربات یا ادمین نیستی — فقط یه آدم معمولی باتجربه که تو گروه حرف میزنه و کمک میکنه.\n\n"
    "دانش واقعی‌ات (همیشه از این استفاده کن، هالوسینیت نکن):\n"
    "• پرداخت: ۸ ارز قبول می‌کنیم (BTC، ETH، USDT روی TRC20 با کارمزد خیلی پایین و تأیید ۵-۱۵ دقیقه، TRX، BNB، TON، SOL، DOGE). TRC20 رو پیشنهاد کن.\n"
    "• ارسال: بسته محرمانه. تهران زیر ۴ ساعت، بقیه شهرها ۴-۸ ساعت بعد تأیید. خودت چند بار گرفتی.\n"
    "• داروها: ریتالین/کونسرتا متیل‌فنیدات اورجینال اروپایی، اوزمپیک/مونجارو سماگلوتاید/تیرزپاتید، مودافینیل برای بیداری و تمرکز.\n\n"
    "سبک پاسخ (حتما رعایت کن - مثل یه آدم واقعی و باتجربه در گروه تلگرامی):\n"
    "• دقیقاً ۳ تا ۶ خط کامل محاوره‌ای فارسی. هر جمله کامل با فعل و . یا ؟ تموم بشه. انگار داری با دوست قدیمی چت می‌کنی — نه ربات، نه فروشنده.\n"
    "• کلمات و لحن واقعی: «آره»، «خودم چند بار گرفتم»، «تجربه‌ام این بود»، «معمولاً»، «بستگه داره»، «یه نکته»، «فکر کنم»، «راستی»، «تو چی؟»، «دقیق بگو ببینم».\n"
    "• هرگز: لیست/بولت/شماره‌گذاری، «البته که»، «حتماً باید»، «قطعاً»، «۱۰۰٪»، «برای سفارش به سایت مراجعه کنید»، «با ادمین تماس بگیرید»، شروع با «سلام! من ربات نیستم».\n"
    "• اگر دقیق نمی‌دونی: «دقیق نمیدونم ولی...» + یه سوال سبک برای ادامه حرف زدن بپرس. هیچ‌وقت جواب یک‌خطی یا ناقص نده.\n"
    "• هدف واقعی: کاربر حس کنه داره با انسان حرف میزنه. بعد از ۱-۲ تبادل مفید و طبیعی، خیلی گرم و بدون فشار بگو «جزئیاتشو تو پی‌وی بگو ببینم» یا «پیام بده راحت‌تر راهنمایی کنم».\n"
    "• همیشه گفتگو رو باز نگه دار. یک پیام کامل + یه سوال یا دعوت سبک به ادامه. کیفیت خیلی مهم‌تر از تعداد خطه — هرگز چرت و پرت یا هالوسینیشن نفرست.\n"
    "• قانون طلایی ضد تشخیص: هرگز از کلمات 'ربات'، 'هوش مصنوعی'، 'چرا فکر کردی'، 'آدم معمولی‌ام'، 'شبیه من حرف میزنه' یا هر دفاع/انکار در مورد هویت خودت استفاده نکن. فقط مثل یک کاربر واقعی با تجربه حرف بزن و ادامه بده."
)

def build_group_messages(user_text: str, retrieved: str, recent_ctx: list, exchange_lines: list, notes: str, mem_ctx: str, *, director_addon: str = "", few_shots: str = "") -> list:
    """Professional prompt builder — uses compact V2 prompt for qwen3:1.7b."""
    try:
        from ai.prompt_builder import build_qwen_messages
        return build_qwen_messages(
            user_text,
            retrieved=retrieved or "",
            recent_ctx=recent_ctx or [],
            exchange_lines=exchange_lines or [],
            notes=notes or "",
            mem_ctx=mem_ctx or "",
            director_addon=director_addon or "",
            few_shots=few_shots or "",
        )
    except ImportError:
        pass

    sys_prompt = GROUP_SYSTEM_PROMPT
    if director_addon:
        sys_prompt = sys_prompt + "\n\n" + director_addon

    messages = [{"role": "system", "content": sys_prompt}]

    ctx_parts = []
    if few_shots:
        ctx_parts.append("نمونه‌های جواب طبیعی واقعی:\n" + few_shots[:450])
    if retrieved:
        ctx_parts.append("دانش مرتبط و دقیق:\n" + retrieved[:380])
    if exchange_lines:
        ctx_parts.append("مکالمه اخیر با همین کاربر:\n" + "\n".join(exchange_lines[-4:]))
    if notes:
        ctx_parts.append("نکات خاص گروه:\n" + notes[:180])
    if mem_ctx:
        ctx_parts.append(mem_ctx)

    if ctx_parts:
        messages.append({"role": "system", "content": "\n\n".join(ctx_parts)[:720]})

    instruction = (
        f"کاربر گفت: {user_text}\n\n"
        "جواب طبیعی ۲-۴ جمله‌ای بنویس. مثل دوست حرف بزن. سوال کوتاه آخر."
    )
    messages.append({"role": "user", "content": instruction})
    return messages


# Prompt for PM-funneling (used separately, not in system prompt)
PM_FUNNEL_PROMPT_TEMPLATE = (
    "تو یه ایرانی هستی توی گروه تلگرام. با این کاربر {count} بار صحبت کردی.\n"
    "یه جمله طبیعی بنویس که پیشنهاد بدی خصوصی صحبت کنن — نه مشکوک، نه مصنوعی.\n"
    "مکالمه اخیر:\n{context}\n\n"
    "فقط یه جمله کوتاه محاوره‌ای:"
)

PM_FUNNEL_PROMPT = PM_FUNNEL_PROMPT_TEMPLATE

# Fast fallback (فقط برای mention بدون AI response — نه برای bypass کردن LLM)
_AI_FAST_RESPONSES: Dict[str, str] = {
    'support_redirect': "می‌تونی به @PharmaWebAd پیام بدی یا تو گروه @PharmaWebGp بپرسی.",
}

_AI_TRIGGER_COMPILED = re.compile(
    r'سوال|چطور|چگونه|[?؟]|آیا|میشه|میشود|چیه|چیست|هست؟|داره؟|کجا|چقدر|'
    r'دارو|داروی|قرص|کپسول|مکمل|ویتامین|تزریق|آمپول|'
    r'ریتالین|اوزمپیک|مونجارو|مودافینیل|ترامادول|انسولین|متفورمین|کونسرتا|لانتوس|سماگلوتاید|ساندوز|'
    r'خرید|سفارش|ارسال|پرداخت|کریپتو|usdt|ترون|trc20|اصل|اورجینال|'
    r'پیگیری|وضعیت|عوارض|دوز|ADHD|دیابت|کاهش وزن|فشار خون|'
    r'مهاجرت|ویزا|اقامت|ترکیه|استانبول|دبی|کانادا|آلمان|اروپا|تهران|تورنتو|بغداد|'
    r'قیمت|چنده|موجود|دارید|میخوام|میخوم|نمیدونم|کمک|راهنما|راهنمایی|'
    r'اعتماد|مطمئن|معتبر|کیفیت|تجربه|کسی|بلد|میدونه|میدونین|نظر|پیشنهاد|'
    r'بهتره|بدتره|ارزونتره|گرونه|چند|هست|دارین|'
    r'راستی|یه سوال|یه چیزی|به نظرت|فکر میکنی|کسی میدونه|'
    r'تجربه داری|تست کردی|امتحان کردی|استفاده کردی|'
    r'مشکل دارم|مشکلم اینه|نگرانم|خسته شدم|کمکم کن|'
    r'چی فکر میکنی|نظرت چیه|پیشنهادت چیه|بگو ببینم|'
    r'همتون|دوستان|بچه‌ها|داداش|خواهر|'
    r'جالبه|مطمئنی|جدی|واقعاً|یعنی|باورم نمیشه|'
    r'کمکی|میتونی|میتونم|میشه کمک|ممنون میشم|'
    r'فیلم|سریال|هوا|ترافیک|خواب|باشگاه|فوتبال|ماشین|دانشگاه|'
    r'سلام|درود|چه خبر|'
    r'vpn|فیلترشکن|اینترنت|وای.?فای|دلار|طلا|غذا|رستوران|'
    r'شمال|مسافرت|سفر|ورزش|آیفون|اندروید|کار از خونه|دورکار',
    re.IGNORECASE
)

# Minimum message length for AI to engage (shorter = more engagement)
_AI_TRIGGER_MIN_LEN = 6

def _message_triggers_ai(text: str) -> bool:
    if not text or len(text) < _AI_TRIGGER_MIN_LEN:
        return False
    if '?' in text or '؟' in text:
        return True
    if len(text) >= 35 and random.random() < 0.32:
        return True
    if random.random() < 0.22:
        return True
    return bool(_AI_TRIGGER_COMPILED.search(text))

# ═══════════════════════════════════════════════════════════
# Professional AI core import (new ai/ modules for max Qwen3 intelligence)
# ═══════════════════════════════════════════════════════════
try:
    from ai.llm_client import qwen3 as _qwen3_client
    from ai.ai_core import (
        classify_intent as _core_classify,
        retrieve_knowledge as _core_retrieve,
        compose_knowledge as _core_compose,
        plan_response as _core_plan,
        is_repeated_response as _core_is_repeated,
        director as _director,
        content_intel as _content_intel,
        decide_engagement as _strategist,
        generate_natural_reply_local as _fast_local_gen,
        repair_llm_output as _core_repair,
        pick_best_or_fallback as _core_pick,
    )
    USE_AI_CORE = True
except Exception as _aicore_err:
    slog(f"AI core import partial/failed: {_aicore_err}")
    _qwen3_client = None
    _core_classify = None
    _core_retrieve = None
    _core_compose = None
    _core_plan = None
    _core_is_repeated = None
    _director = None
    _content_intel = None
    _strategist = None
    _fast_local_gen = None
    USE_AI_CORE = False

# ═══════════════════════════════════════════════════════════
# Phase 3: Ported from web3test/chat/conversation_brain.py for advanced loop prevention & diversity
# ═══════════════════════════════════════════════════════════
def _normalize_for_rep(text: str) -> str:
    if not text:
        return ''
    t = re.sub(r'<!--cards:.*?-->', '', text, flags=re.DOTALL)
    t = re.sub(r'\s+', ' ', t).strip().lower()
    return t

def is_repeated_response(response: str, history: list) -> bool:
    """Check if response is too similar to recent bot responses (ported pattern)."""
    norm = _normalize_for_rep(response)
    if not norm:
        return True
    recent = [ _normalize_for_rep(h[1]) for h in history[-3:] if h[0] == 'bot' ]
    for prev in recent:
        if not prev:
            continue
        if norm == prev:
            return True
        if len(norm) > 40 and norm[:80] == prev[:80]:
            return True
        # simple similarity
        aw = set(norm.split())
        bw = set(prev.split())
        if aw and bw and len(aw & bw) / max(len(aw), 1) > 0.82:
            return True
    return False

def _detect_fast_intent(text: str) -> Optional[str]:
    t = text.lower()
    if re.search(r'پرداخت|ارز|کریپتو|usdt|ترون|trc20|نوبیتکس|والکس', t):
        return 'payment'
    if re.search(r'ارسال|تحویل|تهران|استانبول|دبی|تورنتو', t):
        return 'shipping'
    return None

# ═══════════════════════════════════════════════════════════
# Full Intent Classifier + Strategy (ported/adapted from web3test/chat/intent_router.py + reasoning_engine.py)
# This makes the AI "think" like the professional website assistant.
# ═══════════════════════════════════════════════════════════
NON_PRODUCT_INTENTS = frozenset({
    'greeting', 'thanks', 'goodbye', 'human_request',
    'payment_crypto_help', 'crypto_info', 'tracking',
    'faq_order_process', 'faq_return', 'faq_after_sales', 'trust_question',
    'shipping_time', 'payment_confirmation', 'clarification',
    'identity_question', 'presence_check', 'complaint', 'bot_question',
    'chat_memory', 'site_info', 'help_request',
    'faq_prescription', 'faq_wallet', 'faq_account', 'product_info',
    'cancel_order', 'login_help', 'order_issue', 'wrong_payment',
})

# (intent, patterns) — first match wins. Ported & extended from web3test/chat/intent_router.py
INTENT_RULES = [
    ('complaint', [r'جواب.*پرت', r'اشتباه', r'نمی\s*فهم', r'بی\s*ربط', r'تکرار', r'ضعیف', r'ناراحت', r'ضایع', r'پاسخ.*تکرار']),
    ('bot_question', [r'ربات', r'رباتی', r'\bbot\b', r'هوش\s*مصنوعی', r'\bai\b', r'چت\s*بات', r'بات\s*هست', r'انسان\s*نیست']),
    ('faq_after_sales', [r'پس\s*از\s*فروش', r'خدمات\s*پس', r'گارانتی', r'ضمانت\s*محصول', r'warranty']),
    ('identity_question', [r'تو\s*کی\s*هست', r'شما\s*کی\s*هست', r'کی\s*هستی', r'who\s*are\s*you', r'اسم\s*تو', r'اسمت']),
    ('presence_check', [r'^هستی\s*[؟?]?\s*$', r'هستی\s*[؟?]', r'^الو', r'آنجایی', r'پاسخ\s*مید', r'گوش\s*مید', r'are\s*you\s*there']),
    ('chat_memory', [r'چت.*گذشته', r'پیام.*قبل', r'بخاطر\s*می', r'یادت\s*می', r'حافظه', r'remember.*chat']),
    ('trust_question', [r'اعتماد', r'اطمینان', r'قابل\s*اطمینان', r'مطمئن', r'معتبر', r'کلاهبرد', r'تقلب', r'trust', r'scam']),
    ('faq_order_process', [
        r'چطور.*خرید', r'چگونه.*خرید', r'نحوه\s*خرید', r'مراحل\s*(خرید|سفارش)', r'چیکار\s*باید', r'چکار\s*باید',
        r'راهنمای\s*خرید', r'how\s*(to\s*)?(buy|order)', r'میخوام\s*خرید', r'از\s*(فارما|سایت).*خرید',
        r'چطور.*سفارش', r'نحوه\s*سفارش', r'دقیقا.*چطور', r'ثبت\s*سفارش'
    ]),
    ('payment_crypto_help', [r'چطور\s*پرداخت', r'نحوه\s*پرداخت', r'راهنما.*پرداخت', r'کیف\s*پول', r'والت', r'how\s*to\s*pay']),
    ('crypto_info', [r'تتر', r'usdt', r'کریپتو', r'ارز\s*دیجیتال', r'بیت\s*کوین', r'btc', r'اتریوم', r'صرافی', r'nobitex', r'والکس']),
    ('wrong_payment', [r'شبکه\s*اشتباه', r'wrong\s*network', r'اشتباه\s*واریز', r'کم\s*واریز', r'مبلغ\s*اشتباه']),
    ('payment_confirmation', [r'پرداخت\s*کردم', r'واریز\s*کردم', r'پول\s*دادم', r'paid', r'\bhash\b', r'txid']),
    ('cancel_order', [r'لغو\s*سفارش', r'cancel\s*order', r'انصراف', r'پشیمون']),
    ('order_issue', [r'نرسید', r'not\s*received', r'تحویل\s*نشد', r'آسیب\s*دید', r'شکسته', r'مغایرت', r'wrong\s*item']),
    ('login_help', [r'فراموشی\s*رمز', r'forgot\s*password', r'نمیتونم\s*وارد', r"can't\s*login"]),
    ('tracking', [r'پیگیری|رهگیری|وضعیت|track|order.*status']),
    ('shipping_time', [r'ارسال|تحویل|چقدر\s*طول|چند\s*روز|زمان\s*ارسال']),
    ('greeting', [r'^سلام', r'^درود', r'^وقت\s*بخیر']),
    ('thanks', [r'ممنون', r'متشکر', r'مرسی']),
]

KNOWN_PRODUCT_WORDS = re.compile(
    r'(ریتالین|ritalin|کونسرتا|concerta|اوزمپیک|ozempic|مونجارو|mounjaro|مونجارو|'
    r'مودافینیل|modafinil|ترامادول|tramadol|انسولین|insulin)',
    re.I
)

def _detect_language(text: str) -> str:
    if re.search(r'[ا-ی]', text):
        return 'fa'
    return 'en'

def classify_intent(message: str) -> dict:
    """Professional full port/adapt from web3test/chat/intent_router.py + reasoning boost."""
    msg_lower = (message or '').lower().strip()
    language = _detect_language(message)
    intent = 'unknown'
    confidence = 0.0
    entities = {}

    for name, patterns in INTENT_RULES:
        for pat in patterns:
            if re.search(pat, msg_lower, re.IGNORECASE):
                intent = name
                confidence = 0.9
                break
        if intent != 'unknown':
            break

    # City names alone are life/travel chat, not shipping.
    fast_cities = {'تهران': '🇮🇷', 'استانبول': '🇹🇷', 'دبی': '🇦🇪', 'بغداد': '🇮🇶', 'تورنتو': '🇨🇦'}
    if re.search(r'(ارسال|تحویل|سفارش|بسته|طول\s*می|چند\s*ساعت)', msg_lower):
        for city, flag in fast_cities.items():
            if city in msg_lower:
                if intent in ('unknown', 'shipping_time'):
                    intent = 'shipping_time'
                    entities.setdefault('cities', []).append({'name': city, 'flag': flag})
                    confidence = max(confidence, 0.85)
                break

    # help_request + buy context
    if intent == 'help_request' and re.search(r'(خرید|سفارش|پرداخت|دارو)', msg_lower):
        intent = 'faq_order_process'
        confidence = 0.88

    # میخوام + known product → order or product
    if intent == 'unknown' and re.search(r'(میخوام|می‌خوام)', msg_lower):
        if KNOWN_PRODUCT_WORDS.search(message):
            intent = 'faq_order_process'
            confidence = 0.88

    if intent == 'unknown' and KNOWN_PRODUCT_WORDS.search(message):
        if re.search(r'(دارید|موجود|قیمت|چنده|چقدر)', msg_lower):
            intent = 'product_info'
            confidence = 0.82

    # پیگیری overrides order process
    if intent == 'faq_order_process' and re.search(r'پیگیری|رهگیری|وضعیت|track', msg_lower):
        intent = 'tracking'
        confidence = 0.92

    # Delivery-time questions win over crypto even if USDT is mentioned
    if re.search(r'(ارسال|تحویل|طول\s*می|چند\s*ساعت|کی\s*میرس)', msg_lower):
        for city, flag in fast_cities.items():
            if city in msg_lower:
                intent = 'shipping_time'
                confidence = 0.93
                entities.setdefault('cities', []).append({'name': city, 'flag': flag})
                break

    if intent == 'unknown':
        if re.search(r'adhd|بیش.?فعال|تمرکز|نارکولپسی', msg_lower):
            intent = 'product_search'
            confidence = 0.82
        elif re.search(r'کاهش\s*وزن|لاغر|چاقی', msg_lower):
            intent = 'product_search'
            confidence = 0.80

    return {
        'intent': intent,
        'confidence': confidence,
        'entities': entities,
        'language': language,
    }

# === Strategy constants (from reasoning_engine.py) ===
STRATEGY_FAQ = 'faq_retrieval'
STRATEGY_FAST = 'intent_fast'
STRATEGY_CONTEXTUAL = 'contextual'
STRATEGY_LLM = 'llm_reasoning'
STRATEGY_CAREFUL = 'careful_llm'

def plan_response(intent_info: dict, has_retrieved: bool, has_history: bool, message: str = "") -> dict:
    """Adapted from web3test/chat/reasoning_engine.py plan_response.
    Decides strategy + returns rich thinking context for prompt.
    """
    intent = intent_info.get('intent', 'unknown')
    strategy = STRATEGY_LLM
    thinking = f"intent={intent} | has_knowledge={has_retrieved} | history={has_history}"

    if intent in ('payment_crypto_help', 'crypto_info', 'shipping_time', 'tracking', 'faq_order_process', 'faq_after_sales') and has_retrieved:
        strategy = STRATEGY_FAQ
    elif intent in ('complaint', 'bot_question', 'trust_question'):
        strategy = STRATEGY_CAREFUL
    elif intent in ('greeting', 'presence_check', 'thanks'):
        strategy = STRATEGY_FAST
    elif intent == 'unknown' and not has_history:
        strategy = STRATEGY_CONTEXTUAL if not has_retrieved else STRATEGY_FAQ
    elif has_retrieved:
        strategy = STRATEGY_FAQ
    else:
        strategy = STRATEGY_LLM

    # Special product flow hint
    if KNOWN_PRODUCT_WORDS.search(message or '') and intent in ('unknown', 'product_info'):
        thinking += " | product_focus"

    return {
        'strategy': strategy,
        'intent': intent,
        'thinking': thinking,
        'has_retrieved': has_retrieved,
        'has_history': has_history,
    }

def plan_strategy(intent_info: dict, has_retrieved: bool, has_history: bool) -> str:
    """Thin wrapper returning just strategy string (keeps compat)."""
    pr = plan_response(intent_info, has_retrieved, has_history)
    return pr['strategy']

# Phase 2 helpers
async def check_qwen_health() -> bool:
    try:
        url = f"{QWEN3_BASE_URL}/api/tags"
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(url) as resp:
                return resp.status == 200
    except Exception:
        return False

OWNER_IDS = set(int(x) for x in os.environ.get("USERBOT_OWNER_IDS", "").split(",") if x.strip())

async def handle_owner_command(event):
    global GROUP_AI_COOLDOWN_SECONDS
    if event.sender_id not in OWNER_IDS:
        return False
    text = (event.message.text or "").strip().lower()
    if not text.startswith("!"):
        return False
    if text == "!status":
        await event.reply(f"AI on={ENABLE_GROUP_AI} cooldown={GROUP_AI_COOLDOWN_SECONDS}s proactive={PROACTIVE_ENABLED}")
        return True
    if text.startswith("!cooldown "):
        try:
            secs = int(text.split()[1])
            GROUP_AI_COOLDOWN_SECONDS = max(300, secs)
            await event.reply(f"cooldown={GROUP_AI_COOLDOWN_SECONDS}")
        except:
            pass
        return True
    if text == "!qwen":
        ok = await check_qwen_health()
        await event.reply(f"Qwen reachable: {ok}")
        return True
    return False


async def call_qwen3_natural(recent_ctx: list, user_text: str, chat_id: int = None, *, high_value: bool = False, use_think: bool = False, user_id: int = 0, skip_llm: bool = False) -> Optional[str]:
    """
    MAJOR UPGRADED professional pipeline.
    - Uses director for variant + params
    - Rich few-shots + context
    - Higher capacity for complete 3-7 line natural replies
    - Quality re-prompt loop: if weak after first LLM try, force complete reply
    - Strict multi-layer gates + repairs
    Goal: NEVER send incomplete, robotic, illogical, or low-quality messages.
    """
    # 1. Classify + retrieve (use core when available)
    if USE_AI_CORE and _core_classify:
        intent_info = _core_classify(user_text)
    else:
        intent_info = classify_intent(user_text)
    intent = intent_info.get('intent', 'unknown')

    retrieved = ""
    domain = False
    try:
        from ai.ai_core import is_domain_topic as _idt
        domain = _idt(user_text, intent)
    except Exception:
        domain = False
    try:
        if domain:
            if USE_AI_CORE and _core_compose:
                retrieved = _core_compose(user_text, intent) or ""
            if not retrieved:
                retrieved = retrieve_knowledge(user_text, intent) or ""
    except Exception:
        retrieved = retrieve_knowledge(user_text, intent) or "" if domain else ""

    template = None
    if domain:
        template = _get_template_response(intent, user_text) if '_get_template_response' in globals() else None

    fast_local = None
    try:
        if _fast_local_gen:
            fast_local = _fast_local_gen(user_text, intent, retrieved or "")
    except Exception:
        pass

    # Director decision (core strength)
    director_cfg = {}
    try:
        if USE_AI_CORE and _director:
            has_k = bool(retrieved)
            has_h = bool(group_exchange_history.get(chat_id))
            director_cfg = _director.direct(intent, {}, user_text, has_k, has_h)
    except Exception:
        director_cfg = {'temperature': 0.45, 'max_tokens': 320, 'system_addon': ''}

    temp = director_cfg.get('temperature', 0.52)
    max_tokens = director_cfg.get('max_tokens', 280)
    num_ctx = int(os.environ.get('QWEN3_NUM_CTX', '4096'))
    dir_addon = director_cfg.get('system_addon', '')

    # Few shots for grounding
    few_shots = ""
    try:
        if USE_AI_CORE:
            from ai.ai_core import get_few_shots_for_prompt as _fs
            few_shots = _fs(user_text, k=2)
    except Exception:
        pass

    # Build strong context-aware messages
    exchange_lines = [f"{r}: {t[:95]}" for r, t in list(group_exchange_history.get(chat_id, []))[-4:]]
    notes = get_group_notes(chat_id) if chat_id else ""
    mem_ctx = ""
    try:
        mem_ctx = get_user_context(chat_id, user_id or 0) if chat_id else ""
    except Exception:
        pass

    messages = build_group_messages(
        user_text=user_text,
        retrieved=retrieved or "",
        recent_ctx=recent_ctx or [],
        exchange_lines=exchange_lines,
        notes=notes,
        mem_ctx=mem_ctx,
        director_addon=dir_addon,
        few_shots=few_shots,
    )

    llm_result = None
    raw = ""
    llm_err = ""
    use_think_flag = bool(use_think)

    global _last_global_qwen
    too_soon = (time.time() - _last_global_qwen) < MIN_GLOBAL_QWEN_INTERVAL
    skip_now = skip_llm or too_soon or intent in ('bot_question', 'identity_question')
    if skip_now:
        llm_err = "skipped" if (skip_llm or intent in ('bot_question', 'identity_question')) else "rate_limited"
    else:
        try:
            if _qwen3_client is not None:
                _last_global_qwen = time.time()
                res = await asyncio.wait_for(
                    _qwen3_client.chat(
                        messages, max_tokens=max_tokens, temperature=temp,
                        use_think=use_think_flag, num_ctx=num_ctx,
                        retries=QWEN3_MAX_RETRIES,
                    ),
                    timeout=GROUP_AI_TIMEOUT_SECONDS,
                )
                raw = (res.get("content") or res.get("raw") or "").strip()
                if not raw:
                    llm_err = "empty_response"
        except asyncio.TimeoutError:
            llm_err = "timeout"
            slog(f"QWEN_TIMEOUT intent={intent} gid={chat_id}")
        except Exception as e:
            llm_err = str(e)[:80]
            slog(f"QWEN_ERR intent={intent} gid={chat_id}: {llm_err}")

    # HTTP fallback only if we intended to call Qwen and the client returned nothing.
    # Never call Qwen again after skip / rate-limit / cooldown / timeout.
    _skip_http = llm_err in ("skipped", "rate_limited", "timeout") or "cooldown" in (llm_err or "")
    if not raw and llm_err and not _skip_http:
        try:
            http_timeout = aiohttp.ClientTimeout(total=40)
            async with aiohttp.ClientSession(timeout=http_timeout) as s:
                pp = {
                    "model": QWEN3_MODEL,
                    "messages": messages,
                    "stream": False,
                    "think": False,
                    "options": {
                        "temperature": temp,
                        "num_predict": max_tokens,
                        "num_ctx": num_ctx,
                        "top_p": 0.90,
                        "top_k": 40,
                        "repeat_penalty": 1.15,
                    }
                }
                async with s.post(f"{QWEN3_BASE_URL}/api/chat", json=pp) as rr:
                    if rr.status == 200:
                        dd = await rr.json(content_type=None)
                        raw = (dd.get('message', {}).get('content') or '').strip()
                        llm_err = "" if raw else "http_empty"
        except Exception as e2:
            llm_err = f"http:{str(e2)[:50]}"

    if raw:
        cleaned = _clean_natural(raw)
        cleaned = _repair_group_output(cleaned)
        try:
            from ai.ai_core import repair_llm_output as _r, salvage_llm_output as _salv
            cleaned = _salv(_r(cleaned), retrieved or "")
        except Exception:
            pass
        if is_high_quality_natural(cleaned) and len(cleaned) >= 22:
            llm_result = cleaned
        elif cleaned and len(cleaned) >= 28 and _re.search(r'[آ-ی]', cleaned):
            if not any(b in cleaned for b in ('ربات', 'هوش مصنوعی', 'سانتیمتر', 'You are', 'Assistant:')):
                llm_result = cleaned
        try:
            from ai.ai_core import is_weak_llm_output as _weak
            if llm_result and _weak(llm_result):
                llm_result = None
        except Exception:
            pass
        if llm_result and retrieved:
            _keys = [k for k in (
                'ریتالین', 'کنسرتا', 'کونسرتا', 'اوزمپیک', 'مودافینیل',
                'trc20', 'usdt', 'ساعت',
            ) if k in retrieved.lower() or k in retrieved]
            if _keys and not any(k in llm_result.lower() for k in _keys):
                llm_result = None

    # No second Qwen call — extra re-prompt starves live replies on 1.7b CPU.

    try:
        from ai.ai_core import is_weak_llm_output as _weak2
        if llm_result and _weak2(llm_result):
            llm_result = None
    except Exception:
        pass
    if llm_result and retrieved:
        _keys = [k for k in (
            'ریتالین', 'کنسرتا', 'کونسرتا', 'اوزمپیک', 'مودافینیل',
            'trc20', 'usdt', 'ساعت',
        ) if k in retrieved.lower() or k in retrieved]
        if _keys and not any(k in llm_result.lower() for k in _keys):
            llm_result = None
    if llm_result and not retrieved:
        if any(k in llm_result for k in ('ریتالین', 'اوزمپیک', 'مودافینیل', 'دارو بخور', 'دارو بخورد')):
            llm_result = None
        casual = ('آره', 'راستش', 'به نظرم', 'خودم', 'منم', 'معمولا', 'معمولاً', 'من که', 'تو چی', 'جالبه')
        if llm_result and not any(c in llm_result for c in casual):
            llm_result = None
        if llm_result and user_text and user_text.strip()[:18] in llm_result[:50]:
            llm_result = None

    # Multi-layer selection + final strict gates
    hist = list(group_exchange_history.get(chat_id, []))
    rep_fn = (_core_is_repeated if (USE_AI_CORE and _core_is_repeated) else is_repeated_response)

    result = None
    if intent in ('bot_question', 'identity_question'):
        ordered = [fast_local, llm_result, template]
    else:
        ordered = [llm_result, fast_local, template]
    candidates = [c for c in ordered if c and len(str(c).strip()) > 18]

    for cand in candidates:
        c2 = _repair_group_output(_clean_natural(str(cand)))
        try:
            from ai.ai_core import repair_llm_output as _rr
            c2 = _rr(c2)
        except Exception:
            pass
        if not is_high_quality_natural(c2):
            continue
        try:
            if rep_fn(c2, hist):
                continue
        except Exception:
            pass
        # Final completeness: at least two terminators or good length
        term = c2.count('.') + c2.count('؟') + c2.count('!')
        if len(c2) >= 28 and term >= 1:
            result = c2
            break

    # Rescue with local knowledge if still nothing good
    if not result:
        try:
            if retrieved and len(retrieved) > 20:
                rescue = retrieved.split('\n')[0].strip()[:320]
                if is_high_quality_natural(rescue):
                    result = rescue
        except Exception:
            pass

    # Last resort diverse fallback (never bad single line)
    if not result:
        result = _intent_fallback(intent, user_text)
        log_ai_response(f"FALLBACK intent={intent} gid={chat_id}", "", result or "")

    # Runtime hard filter + sanitizer: never allow defensive AI-meta language no matter the source
    if result:
        result = _sanitize_group_output(result)
        bad_defensive = ['چرا فکر کردی رباتم', 'شبیه ربات', 'آدم معمولی‌ام', 'ربات کجا شبیه من', 'من ربات نیستم', 'هوش مصنوعی هستم']
        if any(b in result for b in bad_defensive):
            result = None

    # Record + log success path
    if result:
        try:
            if chat_id:
                update_user_memory(chat_id, user_id or 0, user_text[:60])
                _record_bot_output(chat_id, result)
            group_exchange_history[chat_id].append(("bot", result))
        except Exception:
            pass
        log_ai_response(
            f"OK intent={intent} llm={'yes' if llm_result else 'no'} err={llm_err or 'none'} gid={chat_id}",
            raw[:120] if raw else "",
            result[:160],
        )

    return result if result and is_high_quality_natural(result) else None


def _intent_fallback(intent: str, user_text: str) -> str:
    """Diverse, intent-aware fallback. Pulls from TEMPLATE_RESPONSES first, then general pool."""
    # Try template pool for this intent
    pool = TEMPLATE_RESPONSES.get(intent)
    domain = False
    try:
        from ai.ai_core import is_domain_topic as _idt
        domain = _idt(user_text or '', intent)
    except Exception:
        domain = False
    if not pool and domain:
        for pattern, key in _DRUG_TEMPLATE_MAP:
            if pattern.search(user_text or ''):
                pool = TEMPLATE_RESPONSES.get(key)
                break
    if pool and (domain or intent in ('greeting', 'thanks', 'goodbye', 'presence_check', 'bot_question')):
        return random.choice(pool)

    # Generic diverse pool — strong complete natural lines
    general_pool = [
        "جالبه. بیشتر بگو ببینم از کجا شروع شده.",
        "راستش بستگه به شرایط. تو خودت چی فکر میکنی؟",
        "منم یه کم درگیر این موضوع بودم. جزئیاتش چیه؟",
        "اوکی فهمیدم. نظرت خودت چیه؟",
        "آره این موضوع رو میشناسم. خودم چند بار برخورد داشتم. بیشتر بگو.",
    ]
    return random.choice(general_pool)

# Back-compat thin wrapper (used by older internal paths if any)
async def call_qwen3_api(user_message: str) -> Optional[str]:
    return await call_qwen3_natural([], user_message)


# Per-group short-term memory for context (lightweight)
group_chat_memory: Dict[int, deque] = defaultdict(lambda: deque(maxlen=12))
# group_ai_last_response already declared globally earlier in file

@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_group))
async def handle_group_ai(event):
    """
    Natural human-like group replies powered by Qwen3.
    - Always fetches recent context
    - Simulates reading + typing
    - Quality gate + natural prompt
    - Mentions always answered; other triggers + occasional proactive
    """
    if not ENABLE_GROUP_AI or not ACCOUNT_HEALTHY:
        return  # حتی پاسخ هوشمند هم اگر حساب مشکل داشته باشد نزنیم

    try:
        text = (event.message.text or '').strip()
        if not text:
            return

        chat_id = event.chat_id
        me = await client.get_me()
        is_mentioned = bool(me.username and f'@{me.username.lower()}' in text.lower())
        triggers = _message_triggers_ai(text)

        if not is_mentioned and not triggers:
            return

        # Domain relevance: looser for own groups (user owns all). Still prefers relevant but allows natural random chat.
        if not is_mentioned and USE_AI_CORE and _strategist:
            try:
                strat = _strategist(text)
                if not strat.get('should_engage', True) and strat.get('score', 0) < 0.8 and random.random() > 0.22:
                    return
            except Exception:
                pass

        # cooldown (mentions bypass) + ultimate anti-spam guard
        now = time.time()
        last = group_ai_last_response.get(chat_id, 0)
        if not is_mentioned and (now - last) < GROUP_AI_COOLDOWN_SECONDS:
            return
        if not is_mentioned and not can_send_to_group_safely(chat_id):
            return

        # Update exchange history (memory updated in context-building below)
        group_exchange_history[chat_id].append(("user", text))
        if responder:
            responder.add_turn(chat_id, 'user', text)

        # Human-like behavior before replying
        await simulate_read_and_type(client, event.chat, len(text))

        # Build context: fresh group messages (passed as recent_ctx) +
        # per-user exchange history (handled inside call_qwen3_natural via group_exchange_history)
        group_chat_memory[chat_id].append(text)
        fresh_ctx = await fetch_recent_group_context(client, chat_id, limit=8)

        # === CENTRALIZED via IntelligentGroupEngager (single source of truth) ===
        response = await group_engager.process_incoming(chat_id, event.message, fresh_ctx)

        if not response:
            if is_mentioned:
                fb = _intent_fallback('unknown', text)
                if fb:
                    await send_group_human(chat_id, fb, reply_to=event.message.id)
                    group_ai_last_response[chat_id] = now
            return

        # Final anti-rep + quality guard before any send
        if _is_repetitive_or_similar(chat_id, response):
            return
        if not can_send_to_group_safely(chat_id):
            return

        group_ai_last_response[chat_id] = now

        await send_group_human(chat_id, response, reply_to=event.message.id)
        record_group_bot_send(chat_id)
        _record_bot_output(chat_id, response)
        group_exchange_history[chat_id].append(("bot", response))
        if any(k in response.lower() for k in ['ارسال', 'پرداخت', 'ساعت', 'ریتالین', 'اوزمپیک']):
            add_group_note(chat_id, response[:160])
        slog(f"🤖 ENGAGER natural+2critique → {chat_id} ({len(response)}c)")

        # PM funnel via engager (soft, intelligent, after value)
        try:
            sender_id = event.sender_id or 0
            if sender_id and group_engager.should_consider_funnel(chat_id, sender_id):
                await asyncio.sleep(random.uniform(120, 300))  # تاخیر طبیعی funnel
                if not can_send_to_group_safely(chat_id):
                    return
                funnel_ctx = "\n".join([t for _, t in list(group_exchange_history[chat_id])[-5:]])
                funnel_msg = await group_engager.maybe_funnel(chat_id, sender_id, funnel_ctx)
                if funnel_msg and is_high_quality_natural(funnel_msg):
                    await send_group_human(chat_id, funnel_msg, reply_to=event.message.id)
                    record_group_bot_send(chat_id)
                    slog(f"📩 ENGAGER PM funnel → user {sender_id} in {chat_id}")
        except Exception as _fe:
            pass

    except Exception as e:
        slog(f"❌ handle_group_ai error: {e}")


# ── PM Funnel System ─────────────────────────────────────────────────────────
# Tracks per-user conversation depth inside each group.
# When depth >= PM_FUNNEL_THRESHOLD, bot naturally suggests moving to PM.
PM_FUNNEL_THRESHOLD = 2          # exchanges before suggesting PM (lower = more PM invites)
PM_FUNNEL_COOLDOWN = 43200       # 12h between funnel attempts per user

# {(group_id, user_id): {"count": int, "last_funnel": float}}
_user_conv_tracker: Dict[tuple, dict] = defaultdict(lambda: {"count": 0, "last_funnel": 0.0})

# PM invitation lines — alias to template pool (single source of truth)
_PM_INVITE_LINES = TEMPLATE_RESPONSES['pm_invite']

def _track_user_exchange(group_id: int, user_id: int) -> int:
    """Increment exchange count and return new count."""
    key = (group_id, user_id)
    _user_conv_tracker[key]["count"] += 1
    return _user_conv_tracker[key]["count"]

def _should_funnel_to_pm(group_id: int, user_id: int) -> bool:
    """Return True if it's time to suggest PM to this user."""
    key = (group_id, user_id)
    data = _user_conv_tracker[key]
    if data["count"] < PM_FUNNEL_THRESHOLD:
        return False
    if time.time() - data["last_funnel"] < PM_FUNNEL_COOLDOWN:
        return False
    return True

def _mark_funnel_sent(group_id: int, user_id: int):
    key = (group_id, user_id)
    _user_conv_tracker[key]["last_funnel"] = time.time()
    _user_conv_tracker[key]["count"] = 0  # reset so funnel doesn't repeat every reply

async def generate_pm_funnel_msg(recent_ctx: str, exchange_count: int = 3, chat_id: int = None) -> str:
    """Soft PM invite from curated lines — no extra Qwen call."""
    try:
        from ai.human_style import funnel_lines
        return random.choice(funnel_lines())
    except Exception:
        pass
    return random.choice([
        "اینجا شلوغه، اگه میخوای بیشتر حرف بزنیم خصوصی پیام بده",
        "جزئیاتش بهتره خصوصی حرف بزنیم، پیام بده",
        "سوالت ادامه داره؟ پی وی سریع‌تر و راحت‌تر راهنمایی میکنم",
    ])


# ── Strengthened Proactive Natural Engagement (observer) ─────────────────────
PROACTIVE_ENABLED = True
PROACTIVE_MAX_PER_GROUP_DAY = 12
_proactive_counters: Dict[int, int] = defaultdict(int)
_proactive_day = date.today()

# Natural conversation starters — posted proactively to initiate conversations
# Mix of pharma, crypto, migration, and general topics to seem human
CONVERSATION_STARTERS = [
    "سلام بچه‌ها، امروز چه خبر؟",
    "سلام. گروه امروز آرومه یا من دیر اومدم؟",
    "راستی VPN چی استفاده میکنید این روزا؟",
    "فیلترشکن امروز خیلی ضعیف شده. مال شما هم همینه؟",
    "کسی فیلم یا سریال خوبی دیده که ارزش وقت گذاشتن داشته باشه؟",
    "کار از خونه بهتره یا دفتر؟ من که تمرکزم تو خونه بهتره.",
    "خوابتون منظمه این روزا یا مثل من بهم ریخته؟",
    "کسی شمال رفته اخیرا؟ جاده آخر هفته چطوره؟",
    "دلار اینقدر نوسان داره که آدم گیج میشه. شما هم دنبال خبرین؟",
    "باشگاه میرید این روزا یا ول کردین؟",
    "هوای شهرتون چطوره امروز؟",
    "ترافیک امروز چطوره پیش شما؟",
    "امروز ناهار چی درست کردید؟ حوصله آشپزی نداشتم.",
    "کسی تجربه زندگی در استانبول داره؟ هزینه زندگی چطوره؟",
    "مهاجرت ترکیه هنوز ارزش داره یا خیلی گرون شده؟",
    "نوبیتکس یا والکس — کدومو ترجیح میدید؟",
    "داروهای ADHD این روزا خیلی کمیابن. کسی تجربه داره از کجا بگیره؟",
    "شنیدم اوزمپیک تو ایران اصلی پیدا نمیشه. شما هم این مشکل داشتین؟",
]

# Track last starter time per group to avoid posting too often
_last_starter_time: Dict[int, float] = {}
_starter_min_interval = 600  # حداقل 10 دقیقه بین starters per group

async def _post_conversation_starter(gid: int) -> bool:
    """CENTRALIZED: Use engager.generate_starter for fully intelligent dynamic starters."""
    now = time.time()
    if now - _last_starter_time.get(gid, 0) < _starter_min_interval:
        return False
    if not can_send_to_group_safely(gid):
        return False
    try:
        recent = await fetch_recent_group_context(client, gid, limit=12)
        starter = await group_engager.generate_starter(gid, recent)
        if not starter or not (is_high_quality_natural(starter) or len(starter) > 20):
            starter = random.choice(CONVERSATION_STARTERS)

        if _is_repetitive_or_similar(gid, starter):
            return False
        await simulate_read_and_type(client, gid, len(starter or "40"))
        await send_group_human(gid, starter)
        _last_starter_time[gid] = now
        record_group_bot_send(gid)
        _record_bot_output(gid, starter)
        group_exchange_history[gid].append(("bot", starter))
        log_ai_response(f"STARTER-ENGAGER gid={gid}", "", starter)
        slog(f"💬 ENGAGER Starter in {gid}: {starter[:55]}")
        return True
    except (ChatWriteForbiddenError, ChannelPrivateError, UserBannedInChannelError):
        return False
    except Exception:
        return False


async def group_observer_task():
    """
    Proactive human-like engagement in groups.
    - Mode 1 (60% of cycles): Reply to specific users' messages to build conversations
    - Mode 2 (40% of cycles): Start fresh conversations with CONVERSATION_STARTERS
    - Triggers PM funnel after enough exchanges
    - Engages on general topics to seem human
    """
    global _proactive_day
    await asyncio.sleep(90)
    print(f"🧠 Observer task started - groups available: {len(groups)}", flush=True)

    while True:
        try:
            if not (ENABLE_GROUP_AI and PROACTIVE_ENABLED) or not ACCOUNT_HEALTHY:
                await asyncio.sleep(300)
                continue

            if date.today() != _proactive_day:
                _proactive_counters.clear()
                _proactive_day = date.today()

            if not groups:
                await asyncio.sleep(60)
                continue

            me = await client.get_me()
            my_id = me.id if me else 0

            candidates = random.sample(groups, min(6, len(groups)))
            acted = False

            # Mode 2: occasional starter — curated, context-aware, no extra Qwen call
            if random.random() < 0.30:
                starter_candidates = [
                    g for g in candidates
                    if _proactive_counters.get(g, 0) < PROACTIVE_MAX_PER_GROUP_DAY
                    and time.time() - _last_starter_time.get(g, 0) > _starter_min_interval
                ]
                if starter_candidates:
                    gid = random.choice(starter_candidates)
                    await asyncio.sleep(random.uniform(15, 45))
                    ok = await _post_conversation_starter(gid)
                    if ok:
                        _proactive_counters[gid] += 1
                        acted = True
                        await asyncio.sleep(random.uniform(60, 120))

            if not acted:
                # Mode 1: Reply to a specific user's message
                for gid in candidates:
                    if _proactive_counters[gid] >= PROACTIVE_MAX_PER_GROUP_DAY:
                        continue
                    if not can_send_to_group_safely(gid):
                        continue  # strong anti-spam: respect global min interval
                    try:
                        msgs = await client.get_messages(gid, limit=18)
                        if not msgs:
                            continue

                        candidate_msgs = []
                        for m in msgs:
                            if not m.text or len(m.text.strip()) < 6:
                                continue
                            if not m.sender_id or m.sender_id == my_id:
                                continue
                            sender = getattr(m, 'sender', None)
                            if sender and getattr(sender, 'bot', False):
                                continue
                            # Skip messages older than 30 minutes (stale)
                            msg_age = time.time() - m.date.timestamp() if hasattr(m.date, 'timestamp') else 9999
                            if msg_age > 1800:
                                continue
                            candidate_msgs.append(m)

                        if not candidate_msgs:
                            continue

                        from ai.human_style import pick_scored_target
                        target_msg = pick_scored_target(
                            candidate_msgs,
                            group_engager._score_target_message,
                            min_score=1.2,
                            top_n=5,
                            randomize=0.45,
                        )
                        if not target_msg:
                            continue

                        target_text = target_msg.text.strip()
                        target_uid = target_msg.sender_id

                        recent_ctx = await fetch_recent_group_context(client, gid, limit=10)

                        # Phase 3: Enrich with group personality + recent bot outputs
                        notes = get_group_notes(gid) or ""
                        recent_bot = "\n".join(list(recent_bot_outputs.get(gid, []))[-2:])
                        enriched_ctx = recent_ctx
                        if notes:
                            enriched_ctx = f"Group personality notes:\n{notes}\n\n" + enriched_ctx
                        if recent_bot:
                            enriched_ctx = f"Recent bot messages in group:\n{recent_bot}\n\n" + enriched_ctx

                        # Use engager to generate (it will further enrich with per-user history)
                        resp = await group_engager.generate_valuable_reply(gid, target_msg, enriched_ctx, use_llm=True)

                        if resp and is_high_quality_natural(resp) and len(resp) > 10 and not _is_repetitive_or_similar(gid, resp):
                            # Human randomness: گاهی skip برای طبیعی‌تر بودن
                            if random.random() < 0.20:
                                continue

                            await asyncio.sleep(random.uniform(20, 70))
                            await simulate_read_and_type(client, gid)

                            if not can_send_to_group_safely(gid):
                                continue
                            await send_group_human(gid, resp, reply_to=target_msg.id)
                            record_group_bot_send(gid)
                            _proactive_counters[gid] += 1
                            _record_bot_output(gid, resp)
                            group_exchange_history[gid].append(("bot", resp))

                            # Record in new engager for per-user state
                            group_engager.record_engagement(gid, target_uid, target_text, resp)

                            # More professional memory update
                            try:
                                update_user_memory(gid, target_uid, target_text[:50])
                            except Exception:
                                pass

                            count = _track_user_exchange(gid, target_uid)
                            log_ai_response(f"PROACTIVE uid={target_uid} gid={gid} ex={count}", target_text[:60], resp)

                            # Phase 2: Use engager's funnel decision
                            if group_engager.should_consider_funnel(gid, target_uid):
                                await asyncio.sleep(random.uniform(120, 300))
                                if can_send_to_group_safely(gid):
                                    ctx_for_funnel = "\n".join([t for _, t in list(group_exchange_history[gid])[-5:]])
                                    funnel_msg = await group_engager.maybe_funnel(gid, target_uid, ctx_for_funnel)
                                    if funnel_msg and is_high_quality_natural(funnel_msg):
                                        await send_group_human(gid, funnel_msg, reply_to=target_msg.id)
                                        record_group_bot_send(gid)
                                        group_engager.mark_funnel_sent(gid, target_uid)
                                        slog(f"📩 PM funnel → uid={target_uid} gid={gid}")

                            acted = True
                            await asyncio.sleep(random.uniform(60, 120))
                            break

                    except (ChatWriteForbiddenError, ChannelPrivateError, UserBannedInChannelError):
                        continue
                    except Exception:
                        continue

            if acted:
                await asyncio.sleep(random.randint(60, 180))  # pause after action
            else:
                await asyncio.sleep(random.randint(30, 90))  # base loop interval

        except Exception:
            await asyncio.sleep(60)


async def run_ai_self_test(num_tests: int = 6) -> dict:
    """Run quality tests against the upgraded natural AI pipeline.
    Returns summary. Use for verification after deploy.
    """
    test_queries = [
        "ارسال به استانبول بعد از پرداخت چقدر طول میکشه؟",
        "پرداخت با USDT روی کدوم شبکه بهتره؟",
        "ریتالین اورجینال اروپایی تجربه داری؟",
        "برای تمرکز چی پیشنهاد میکنی؟",
        "اوزمپیک داری؟ چقدر طول میکشه؟",
        "مهاجرت ترکیه الان چطوره؟",
    ]
    results = []
    for q in test_queries[:num_tests]:
        try:
            r = await call_qwen3_natural(["دوستان تجربه‌ای داری؟"], q)
            ok = bool(r and is_high_quality_natural(r) and len(r or "") >= 28)
            results.append({"q": q[:45], "ok": ok, "len": len(r or 0), "preview": (r or "")[:70]})
        except Exception as e:
            results.append({"q": q[:45], "ok": False, "err": str(e)[:55]})
    summary = {
        "passed": sum(1 for x in results if x.get("ok")),
        "total": len(results),
        "details": results
    }
    log_ai_response("SELF_TEST", str(results), f"passed {summary['passed']}/{summary['total']}")
    return summary


# ═══════════════════════════════════════════════════════════
# 🔗 تسک عضویت خودکار از لیست لینک‌ها (Auto Join from Links Task)
# ═══════════════════════════════════════════════════════════
async def auto_join_from_links():
    """
    ⚡ عضویت خودکار و هوشمند در گروه‌ها از لیست لینک‌ها
    
    ویژگی‌ها:
    - پشتیبانی از لینک‌های عمومی (@username, t.me/username)
    - پشتیبانی از لینک‌های خصوصی (t.me/+hash, t.me/joinchat/hash)
    - جلوگیری از عضویت تکراری
    - مدیریت هوشمند FloodWait
    - تلاش مجدد خودکار برای لینک‌های ناموفق
    - ذخیره وضعیت برای ادامه بعد از restart
    """
    global groups, joined_groups
    
    # صبر کوتاه برای اطمینان از اتصال کلاینت
    await asyncio.sleep(10)
    
    # اطمینان از اتصال کلاینت
    if not client.is_connected():
        try:
            await client.connect()
        except:
            pass
    
    slog("🔗 Auto-Join: شروع سیستم عضویت خودکار...")
    
    # متغیر برای ردیابی FloodWait فعال
    flood_wait_until = 0
    
    while True:
        try:
            # 🔴 بررسی FloodWait فعال
            import time
            if flood_wait_until > time.time():
                remaining = int(flood_wait_until - time.time())
                slog(f"🔗 Auto-Join: ⏳ FloodWait فعال ({remaining//3600}h {(remaining%3600)//60}m باقیمانده)")
                await asyncio.sleep(min(remaining, 300))  # هر 5 دقیقه چک کن
                continue
            
            # 🔴 بررسی فعال بودن سیستم + ایمنی
            if not ENABLE_AUTO_JOIN_FROM_LINKS or not ACCOUNT_HEALTHY or SAFE_MODE:
                await asyncio.sleep(300)
                continue
            
            # 🔴 بررسی اتصال کلاینت
            if not client.is_connected():
                slog("🔗 Auto-Join: انتظار برای اتصال کلاینت...")
                await asyncio.sleep(10)
                continue
            
            # دریافت لینک‌های در انتظار
            pending_links = auto_join_manager.get_pending_links()
            
            if not pending_links:
                # اگر لینکی در انتظار نیست، 2 دقیقه صبر کن و دوباره چک کن
                slog(f"🔗 Auto-Join: همه لینک‌ها پردازش شده‌اند ({auto_join_manager.stats['total_joined']} عضو شده)")
                await asyncio.sleep(120)  # 2 دقیقه - سریع‌تر
                continue
            
            slog(f"🔗 شروع Auto-Join: {len(pending_links)} لینک در انتظار")
            
            batch_count = 0
            success_in_batch = 0
            
            for link in pending_links:
                try:
                    # بررسی مجدد (ممکن است در حین پردازش عضو شده باشیم)
                    if auto_join_manager.is_already_joined(link):
                        continue
                    
                    batch_count += 1
                    retry_count = auto_join_manager.get_retry_count(link)
                    
                    # پاکسازی لینک
                    clean_link = link.strip()
                    
                    # استخراج نوع و شناسه لینک
                    link_type, identifier = auto_join_manager.extract_invite_hash(clean_link)
                    
                    if not link_type or not identifier:
                        slog(f"⚠️ [{batch_count}] فرمت لینک نامعتبر: {clean_link[:50]}")
                        auto_join_manager.mark_as_failed(clean_link, "فرمت نامعتبر", permanent=True)
                        continue
                    
                    slog(f"🔗 [{batch_count}/{len(pending_links)}] {link_type}: {identifier[:30]}...")
                    
                    joined = False
                    group_title = "نامشخص"
                    group_id = None
                    
                    if link_type == 'private':
                        # 🔐 عضویت در لینک خصوصی
                        try:
                            # ابتدا بررسی لینک
                            result = await client(CheckChatInviteRequest(hash=identifier))
                            
                            if isinstance(result, ChatInviteAlready):
                                # قبلاً عضو هستیم
                                slog(f"   ✅ قبلاً عضو این گروه هستیم (private)")
                                auto_join_manager.mark_as_already_member(clean_link)
                                
                                # اضافه به لیست گروه‌های ما
                                if hasattr(result, 'chat') and result.chat:
                                    group_id = result.chat.id
                                    group_title = getattr(result.chat, 'title', 'نامشخص')
                                    if group_id not in groups:
                                        groups.append(group_id)
                                        joined_groups.add(group_id)
                                
                                continue
                            
                            elif isinstance(result, ChatInvite):
                                # می‌توانیم عضو شویم
                                group_title = getattr(result, 'title', 'نامشخص')
                                slog(f"   📋 گروه: {group_title[:40]}")
                                
                                # عضویت
                                updates = await client(ImportChatInviteRequest(hash=identifier))
                                
                                # دریافت اطلاعات گروه
                                if hasattr(updates, 'chats') and updates.chats:
                                    chat = updates.chats[0]
                                    group_id = chat.id
                                    group_title = getattr(chat, 'title', group_title)
                                
                                joined = True
                            
                        except UserAlreadyParticipantError:
                            slog(f"   ✅ قبلاً عضو این گروه هستیم")
                            auto_join_manager.mark_as_already_member(clean_link)
                            continue
                            
                        except InviteHashInvalidError:
                            slog(f"   ❌ لینک نامعتبر یا منقضی شده")
                            auto_join_manager.mark_as_failed(clean_link, "لینک نامعتبر", permanent=True)
                            continue
                            
                        except InviteHashExpiredError:
                            slog(f"   ❌ لینک منقضی شده")
                            auto_join_manager.mark_as_failed(clean_link, "لینک منقضی", permanent=True)
                            continue
                    
                    else:  # public
                        # 🌐 عضویت در لینک عمومی
                        try:
                            entity = await client.get_entity(identifier)
                            group_id = entity.id
                            group_title = getattr(entity, 'title', identifier)
                            
                            # ✅ بررسی blacklist دائمی
                            if is_permanently_blacklisted(group_id):
                                slog(f"   🚫 در blacklist دائمی: {group_title[:30]}")
                                auto_join_manager.mark_as_failed(clean_link, "در blacklist", permanent=True)
                                continue
                            
                            # بررسی آیا قبلاً عضو هستیم
                            if group_id in joined_groups or group_id in groups:
                                slog(f"   ✅ قبلاً عضو این گروه هستیم: {group_title[:30]}")
                                auto_join_manager.mark_as_already_member(clean_link)
                                continue
                            
                            # 🧹 بررسی قابلیت استفاده از گروه (با بررسی تعداد اعضا)
                            # اگر گروه کمتر از 500 عضو داشته باشد رد می‌شود
                            is_usable, reason = await check_group_is_usable(entity)
                            if not is_usable:
                                slog(f"   ⏭️ رد شد: {group_title[:30]} ({reason})")
                                auto_join_manager.mark_as_failed(clean_link, reason, permanent=True)
                                
                                # ✅ اضافه به blacklist دائمی
                                username = getattr(entity, 'username', None)
                                if 'low_members' in reason:
                                    add_to_permanent_blacklist(group_id, reason='low_members', username=username, title=group_title)
                                elif 'no_write_access' in reason:
                                    add_to_permanent_blacklist(group_id, reason='no_write_access', username=username, title=group_title)
                                continue
                            
                            # عضویت
                            try:
                                await client(JoinChannelRequest(entity))
                                joined = True
                            except Exception as join_err:
                                err_msg = str(join_err).lower()
                                # اگر درخواست عضویت موفق باشد (نیاز به تأیید ادمین)
                                if "requested to join" in err_msg or "successfully requested" in err_msg:
                                    slog(f"   📨 درخواست عضویت ارسال شد (نیاز به تأیید)")
                                    auto_join_manager.mark_as_joined(clean_link)  # ثبت به عنوان موفق
                                    continue
                                else:
                                    raise join_err
                            
                        except ChannelPrivateError:
                            slog(f"   ❌ گروه خصوصی است یا دسترسی ندارید")
                            auto_join_manager.mark_as_failed(clean_link, "گروه خصوصی", permanent=True)
                            # ✅ اضافه به blacklist اگر group_id داریم
                            if group_id:
                                add_to_permanent_blacklist(group_id, reason='private_channel', title=group_title)
                            continue
                            
                        except ValueError as e:
                            if "No user has" in str(e) or "Could not find" in str(e):
                                slog(f"   ❌ گروه/کانال پیدا نشد: {identifier}")
                                auto_join_manager.mark_as_failed(clean_link, "پیدا نشد", permanent=True)
                            else:
                                slog(f"   ❌ خطا: {str(e)[:50]}")
                                auto_join_manager.mark_as_failed(clean_link, str(e)[:50])
                            continue
                        
                        except FloodWaitError as e:
                            # 🔴 FloodWait در get_entity یا JoinChannel
                            slog(f"   ⚠️ FloodWait در عضویت: {e.seconds}s ({e.seconds//3600}h {(e.seconds%3600)//60}m)")
                            auto_join_manager.record_flood_wait(e.seconds)
                            
                            # اگر FloodWait طولانی است، توقف کن
                            if e.seconds > 3600:
                                flood_wait_until = time.time() + e.seconds
                                slog(f"   🛑 توقف Auto-Join تا {e.seconds//3600}h دیگر")
                                break
                            
                            wait_time = min(e.seconds + 10, 1800)
                            slog(f"   😴 صبر {wait_time//60} دقیقه...")
                            await asyncio.sleep(wait_time)
                            continue  # ادامه با لینک بعدی (این لینک fail نمی‌شود)
                    
                    # 🎉 عضویت موفق
                    if joined:
                        slog(f"   ✅ عضویت موفق در: {group_title[:40]}")
                        
                        # 🔍 بررسی تعداد اعضا بعد از عضویت (مخصوص لینک‌های خصوصی)
                        if group_id and link_type == 'private':
                            try:
                                entity = await client.get_entity(group_id)
                                member_count = await check_group_member_count(entity)
                                
                                if member_count > 0 and member_count < MIN_GROUP_MEMBERS:
                                    # گروه کم‌عضو است - خروج فوری
                                    slog(f"   ⚠️ گروه کم‌عضو ({member_count} < {MIN_GROUP_MEMBERS}) - خروج...")
                                    try:
                                        await client(LeaveChannelRequest(channel=entity))
                                        slog(f"   🚪 خروج از گروه کم‌عضو: {group_title[:30]}")
                                        auto_join_manager.mark_as_failed(clean_link, f"کم‌عضو: {member_count}", permanent=True)
                                        
                                        # ✅ اضافه به blacklist دائمی
                                        username = getattr(entity, 'username', None)
                                        add_to_permanent_blacklist(group_id, reason='low_members', username=username, title=group_title)
                                        
                                        # حذف از لیست‌ها
                                        if group_id in groups:
                                            groups.remove(group_id)
                                        if group_id in joined_groups:
                                            joined_groups.discard(group_id)
                                        
                                        await asyncio.sleep(5)
                                        continue
                                    except Exception as leave_err:
                                        slog(f"   ⚠️ خطا در خروج: {str(leave_err)[:30]}")
                                else:
                                    slog(f"   👥 تعداد اعضا: {member_count}")
                            except Exception as check_err:
                                slog(f"   ⚠️ خطا در بررسی اعضا: {str(check_err)[:30]}")
                        
                        auto_join_manager.mark_as_joined(clean_link)
                        auto_join_manager.reset_delay_multiplier()
                        success_in_batch += 1
                        
                        # اضافه به لیست گروه‌ها
                        if group_id:
                            if group_id not in groups:
                                groups.append(group_id)
                            joined_groups.add(group_id)
                            stats['groups_joined'] += 1
                    
                    # ⏰ تاخیر بهینه
                    delay = auto_join_manager.get_optimal_delay()
                    slog(f"   ⏱️ تاخیر: {delay}s")
                    await asyncio.sleep(delay)
                    
                    # 🛑 استراحت بعد از هر بچ
                    if batch_count >= AUTO_JOIN_BATCH_SIZE:
                        slog(f"🔗 پایان بچ: {success_in_batch}/{batch_count} موفق")
                        logger.info(f"   😴 استراحت {AUTO_JOIN_BATCH_REST}s...")
                        await asyncio.sleep(AUTO_JOIN_BATCH_REST)
                        batch_count = 0
                        success_in_batch = 0
                
                except ChannelsTooMuchError:
                    logger.warning("⚠️ محدودیت تعداد گروه‌ها - ترک گروه‌های قدیمی")
                    auto_join_manager.mark_as_failed(link, "محدودیت گروه")
                    
                    # تلاش برای ترک گروه‌های قدیمی
                    try:
                        await smart_leave_old_groups(count=5)
                    except:
                        pass
                    
                    await asyncio.sleep(60)
                    continue
                
                except FloodWaitError as e:
                    slog(f"⚠️ FloodWait: {e.seconds}s ({e.seconds//3600}h {(e.seconds%3600)//60}m)")
                    auto_join_manager.record_flood_wait(e.seconds)
                    auto_join_manager.stats['total_retries'] += 1
                    
                    # 🔴 اگر FloodWait طولانی است، متغیر را تنظیم کن
                    if e.seconds > 3600:  # بیشتر از 1 ساعت
                        flood_wait_until = time.time() + e.seconds
                        slog(f"   🛑 توقف Auto-Join تا {e.seconds//3600}h {(e.seconds%3600)//60}m دیگر")
                        break  # خروج از loop برای چک مجدد
                    
                    # 🔴 صبر کردن - حداکثر 30 دقیقه
                    wait_time = min(e.seconds + 10, 1800)
                    slog(f"   😴 صبر {wait_time//60} دقیقه...")
                    await asyncio.sleep(wait_time)
                    continue
                
                except Exception as e:
                    error_msg = str(e)[:100]
                    
                    # 🔴 بررسی FloodWait در پیام خطا (برای خطاهایی که مستقیم catch نشدند)
                    if "wait" in error_msg.lower() and "seconds" in error_msg.lower():
                        import re
                        match = re.search(r'(\d+)\s*seconds', error_msg.lower())
                        if match:
                            wait_seconds = int(match.group(1))
                            slog(f"   ⚠️ FloodWait (از پیام خطا): {wait_seconds}s")
                            
                            # اگر طولانی است، توقف کن
                            if wait_seconds > 3600:
                                flood_wait_until = time.time() + wait_seconds
                                slog(f"   🛑 توقف Auto-Join تا {wait_seconds//3600}h دیگر")
                                break
                            
                            wait_time = min(wait_seconds + 10, 1800)
                            await asyncio.sleep(wait_time)
                            continue  # این لینک fail نمی‌شود
                    
                    slog(f"   ❌ خطا: {error_msg}")
                    auto_join_manager.mark_as_failed(clean_link, error_msg)
                    await asyncio.sleep(5)
                    continue
            
            # 📊 گزارش نهایی سیکل
            slog("═" * 50)
            slog(f"🔗 سیکل Auto-Join: {success_in_batch} موفق از {batch_count}")
            slog(f"   📊 کل عضو شده: {auto_join_manager.stats['total_joined']}")
            slog("═" * 50)
            
            # ذخیره وضعیت
            auto_join_manager.save_state()
            
            # انتظار کوتاه برای سیکل بعدی
            await asyncio.sleep(60)  # 1 دقیقه قبل از بررسی مجدد
            
        except Exception as e:
            slog(f"❌ خطای کلی در Auto-Join: {str(e)[:100]}")
            await asyncio.sleep(30)


# تسک اصلی
async def main():
    """راه‌اندازی ربات"""
    global groups
    
    # تنظیم graceful shutdown (بدون لاگ)
    def signal_handler(sig, frame):
        # ذخیره وضعیت قبل از خروج
        save_ai_state()
        save_learned_keywords()
        save_members_db()
        auto_join_manager.save_state()  # 🔗 ذخیره وضعیت Auto-Join
        save_permanent_blacklist()  # 🚫 ذخیره blacklist دائمی
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 🌐 شروع فوری وب سرور برای Railway (اولین کار - برای جلوگیری از کرش تشخیص Railway)
    print("🌐 Starting Railway healthcheck server first...", flush=True)
    asyncio.create_task(start_web_server())
    
    # Phase 3: init professional responder (structure improvement - AI core ready)
    global responder
    responder = ProfessionalGroupResponder(client, QWEN3_BASE_URL, QWEN3_MODEL, GROUP_AI_TIMEOUT_SECONDS)
    
    # شروع کلاینت تلگرام — retry loop تا ۳۰ دقیقه برای session conflict در Railway
    # Web server already running above → healthcheck passes during all retries
    _started = False
    _attempt = 0
    _total_waited = 0
    _MAX_WAIT = 1800  # 30 min — Railway kills old container well before this
    while not _started and _total_waited < _MAX_WAIT:
        _attempt += 1
        try:
            print(f"🔌 Connecting Telethon (attempt {_attempt}, waited={_total_waited}s)...", flush=True)
            await client.start()
            print("✅ Telethon client started successfully", flush=True)
            _started = True
            try:
                await init_human_style()
            except Exception as _hs:
                print(f"human_style init: {_hs}", flush=True)
        except Exception as _e:
            _err = str(_e)
            print(f"❌ Telethon start error (attempt {_attempt}): {_err[:200]}", flush=True)
            if 'two different IP' in _err or 'AuthKeyDuplicated' in _err or 'authorization key' in _err.lower():
                # Exponential backoff: 60, 90, 120, 150, 180, 180, 180 ...
                _wait = min(60 + (_attempt - 1) * 30, 180)
                print(f"⏳ Session conflict — waiting {_wait}s (total={_total_waited}s)...", flush=True)
                await asyncio.sleep(_wait)
                _total_waited += _wait
            else:
                await asyncio.sleep(20)
                _total_waited += 20
    if not _started:
        print("❌ FATAL: 30-min timeout — sleeping 120s then exiting for Railway restart", flush=True)
        await asyncio.sleep(120)
        sys.exit(1)
    
    # ═══════════════════════════════════════════════════════════
    # 🚀 پیام استارت (تنها لاگ در نسخه Silent)
    # ═══════════════════════════════════════════════════════════
    slog("=" * 60)
    slog("🤖 ربات در حالت RAILWAY OPTIMIZED راه‌اندازی شد")
    slog("=" * 60)
    slog(f"🚂 Railway Mode: {RAILWAY_MODE.upper()}")
    slog(f"🌐 Environment: {'Railway' if IS_RAILWAY else 'Local'}")
    slog("-" * 60)
    slog("🔧 تنظیمات بهینه‌سازی Railway:")
    slog(f"   • 📦 Max Queue: {MAX_QUEUE_SIZE}")
    slog(f"   • ⚡ Concurrent Tasks: {MAX_CONCURRENT_TASKS}")
    slog(f"   • 💾 Max Scraped Users: {MAX_SCRAPED_USERS}")
    slog(f"   • 🧹 GC Interval: {GC_INTERVAL}s")
    slog(f"   • 🗂️ Memory Cleanup: {MEMORY_CLEANUP_INTERVAL}s")
    slog("-" * 60)
    # ⚠️ نمایش وضعیت سوییچ‌های عملیات پرریسک
    slog("⚠️ وضعیت عملیات پرریسک:")
    slog(f"   • 📨 ارسال PM: {'🟢 فعال' if ENABLE_PM_SENDING else '🔴 غیرفعال'}")
    slog(f"   • ➕ اضافه مستقیم: {'🟢 فعال' if ENABLE_DIRECT_ADD else '🔴 غیرفعال'} (SAFE: {SAFE_MODE})")
    slog(f"   • 🔍 جستجوی گروه: {'🟢 فعال' if ENABLE_GROUP_SEARCH else '🔴 غیرفعال'}")
    slog(f"   • 👥 جمع‌آوری اعضا: {'🟢 فعال' if ENABLE_MEMBER_SCRAPING else '🔴 غیرفعال'}")
    print(f"🚨 SAFE_MODE={SAFE_MODE}  ACCOUNT_HEALTHY={ACCOUNT_HEALTHY}", flush=True)
    slog(f"   • 📢 تبلیغات گروهی: {'🟢 فعال' if ENABLE_BROADCAST else '🔴 غیرفعال'}")
    slog(f"   • 🤖 هوش مصنوعی گروه: {'🟢 فعال' if ENABLE_GROUP_AI else '🔴 غیرفعال'} (Qwen3 NATURAL v2 — context+human-sim+gate, proactive enabled)")
    slog("-" * 60)
    # 🧹 نمایش وضعیت سیستم گروه‌های کم‌عضو
    slog("🧹 سیستم مدیریت گروه‌های کم‌عضو:")
    slog(f"   • خروج خودکار: {'🟢 فعال' if ENABLE_LOW_MEMBER_LEAVE else '🔴 غیرفعال'}")
    slog(f"   • حداقل اعضا: {MIN_GROUP_MEMBERS} نفر")
    slog(f"   • بررسی قبل عضویت: {'🟢 فعال' if CHECK_MEMBERS_BEFORE_JOIN else '🔴 غیرفعال'}")
    slog("-" * 60)
    # 🔒 نمایش وضعیت سیستم گروه‌های بسته
    slog("🔒 سیستم مدیریت گروه‌های بسته:")
    slog(f"   • خروج از بسته‌ها: {'🟢 فعال' if ENABLE_RESTRICTED_GROUP_LEAVE else '🔴 غیرفعال'}")
    slog(f"   • بررسی دسترسی قبل عضویت: {'🟢 فعال' if CHECK_WRITE_ACCESS_BEFORE_JOIN else '🔴 غیرفعال'}")
    slog("-" * 60)
    # 🚫 نمایش وضعیت blacklist دائمی
    slog("🚫 سیستم Blacklist دائمی:")
    slog(f"   • تعداد گروه‌های blacklist شده: {len(permanent_blacklist)}")
    slog("=" * 60)
    
    stats['start_time'] = time.time()
    
    # بارگذاری حافظه و وضعیت AI
    load_members_db()
    load_learned_keywords()
    load_ai_state()  # 🧠 بارگذاری وضعیت هوش مصنوعی
    
    # دریافت لیست گروه‌های فعلی (بدون نمایش لاگ)
    dialogs = await client.get_dialogs()
    for d in dialogs:
        if isinstance(d.entity, Channel) and d.entity.megagroup and not d.entity.broadcast:
            # ✅ فیلتر گروه‌های blacklist شده
            if not is_permanently_blacklisted(d.id):
                groups.append(d.id)
                joined_groups.add(d.id)
            else:
                # اگر در blacklist است ولی هنوز عضویم، می‌تونیم بعداً خارج شویم
                joined_groups.add(d.id)
    
    # 🏠 بارگذاری اعضای گروه خودمان (@PharmaWebGp) - برای عدم ارسال پیام به آنها
    await load_our_group_members()
    
    slog(f"📊 گروه‌های فعلی: {len(groups)}")
    slog(f"🚫 گروه‌های blacklist شده: {len(permanent_blacklist)}")
    slog(f"🏠 اعضای گروه ما: {len(members_db['our_group_members'])}")
    slog(f"🌐 High-value keywords: {len(network_discovery.high_value_keywords)}")
    
    # 🎯 آمار اولیه دعوت اعضا
    total_scraped = len(members_db.get('scraped_users', {}))
    total_invited = len(members_db.get('invited_users', set()))
    pending = total_scraped - total_invited
    slog("-" * 60)
    slog("⚔️ سیستم دعوت اعضا به @PharmaWebGp:")
    slog(f"   👥 Scrape شده: {total_scraped}")
    slog(f"   ✅ دعوت شده: {total_invited}")
    slog(f"   ⏳ در صف: {pending}")
    slog(f"   🎯 هدف روزانه: {DAILY_INVITE_TARGET} عضو")
    slog(f"   ⚡ تنظیمات: {MAX_INVITES_PER_CYCLE} عضو/{INVITE_CYCLE_INTERVAL}s")
    slog("-" * 60)
    
    # 🧹 پاکسازی فوری گروه‌های blacklist شده از لیست گروه‌ها
    blacklisted_in_groups = [g for g in groups if is_permanently_blacklisted(g)]
    for g in blacklisted_in_groups:
        groups.remove(g)
    if blacklisted_in_groups:
        slog(f"🧹 {len(blacklisted_in_groups)} گروه blacklist شده از لیست حذف شدند")
    
    # ═══════════════════════════════════════════════════════════
    # 🎯 شروع تسک‌ها - بهینه‌شده برای Railway و کاهش مصرف منابع
    # ═══════════════════════════════════════════════════════════
    
    # 🚂 Railway: تسک پاکسازی منابع (اولویت اول)
    asyncio.create_task(railway_resource_cleanup())  # پاکسازی حافظه
    
    # ⚔️ سیستم دعوت اعضا به @PharmaWebGp (فقط اگر SAFE_MODE = False)
    if SAFE_MODE or not ACCOUNT_HEALTHY:
        print("🚫 Scraping and inviting DISABLED for safety (SAFE_MODE=True or account unhealthy)", flush=True)
    else:
        # فقط در حالت normal این تسک‌ها اجرا می‌شوند
        if ENABLE_MEMBER_SCRAPING:
            asyncio.create_task(scrape_group_members())
        if ENABLE_DIRECT_ADD:
            asyncio.create_task(invite_members_to_target())
    
    # 🔧 تسک‌های ضروری
    asyncio.create_task(keep_alive())  # نگه‌داشتن اتصال
    asyncio.create_task(show_stats())  # نمایش آمار
    asyncio.create_task(periodic_ai_save())  # ذخیره دوره‌ای
    asyncio.create_task(periodic_our_group_update())  # به‌روزرسانی اعضای گروه ما

    # 🧠 Proactive natural human engagement (همیشه فعال برای AI)
    asyncio.create_task(group_observer_task())
    
    # 📢 تبلیغات - با تاخیر اولیه برای جلوگیری از اسپم
    if ENABLE_BROADCAST:
        asyncio.create_task(delayed_broadcast_start())
    
    # 🔍 جستجوی گروه‌ها
    if not SAFE_MODE and ACCOUNT_HEALTHY and ENABLE_GROUP_SEARCH:
        asyncio.create_task(delayed_search_start())
    else:
        print("🚫 Group search disabled (SAFE_MODE or disabled in config)", flush=True)
    
    # 🧹 تسک‌های کم‌اولویت - با تاخیر طولانی
    asyncio.create_task(cleanup_dead_groups())  # پاکسازی گروه‌ها
    asyncio.create_task(leave_low_member_groups())  # خروج از گروه‌های کم‌عضو
    asyncio.create_task(leave_restricted_groups())  # خروج از گروه‌های بسته
    
    # 🔗 عضویت از لینک‌ها - با تاخیر
    # Already guarded inside the task + flag is False now

    # 🚨 مانیتور سلامت حساب (جدید برای جلوگیری از بن)
    asyncio.create_task(monitor_account_health())
    
    # 📊 آمار Auto-Join
    if ENABLE_AUTO_JOIN_FROM_LINKS and AUTO_JOIN_LINKS:
        pending_count = len(auto_join_manager.get_pending_links())
        slog("-" * 60)
        slog("🔗 سیستم عضویت خودکار از لینک‌ها:")
        slog(f"   📋 کل لینک‌ها: {len(AUTO_JOIN_LINKS)}")
        slog(f"   ✅ عضو شده: {auto_join_manager.stats['total_joined']}")
        slog(f"   ⏳ در انتظار: {pending_count}")
        slog("-" * 60)
    
    print("🚀 Bot started - entering run_until_disconnected (AI conversational mode active)", flush=True)
    
    try:
        await client.run_until_disconnected()
    except Exception as e:
        print(f"❌ Bot disconnected with error: {e}", flush=True)
        raise

# ═══════════════════════════════════════════════════════════════════════════════
# 🕐 توابع شروع با تاخیر - برای جلوگیری از اسپم
# ═══════════════════════════════════════════════════════════════════════════════

async def delayed_broadcast_start():
    """شروع تسک broadcast فوری"""
    # شروع فوری بدون تاخیر
    slog("📢 شروع broadcast...")
    await broadcast_to_groups()

async def delayed_search_start():
    """شروع تسک جستجوی گروه‌ها فوری"""
    # شروع فوری بدون تاخیر
    slog("🔍 شروع جستجوی گروه‌ها...")
    await search_and_join_groups()

# 🧠 تسک ذخیره دوره‌ای وضعیت AI
async def periodic_ai_save():
    """ذخیره دوره‌ای وضعیت سیستم‌های هوشمند هر 30 دقیقه"""
    while True:
        try:
            await asyncio.sleep(1800)  # هر 30 دقیقه (بهینه‌شده)
            save_ai_state()
            save_learned_keywords()
        except Exception as e:
            logger.error(f"❌ خطا در ذخیره دوره‌ای AI: {e}")


# 🚂 تسک پاکسازی منابع Railway
async def railway_resource_cleanup():
    """
    پاکسازی دوره‌ای منابع برای Railway
    - اجرای garbage collection
    - محدود کردن اندازه دیکشنری‌ها
    - پاکسازی کش‌های قدیمی
    """
    global members_db, mirror_users, sent_messages, last_message_time
    
    while True:
        try:
            # فاصله پاکسازی بر اساس حالت
            cleanup_interval = 180 if RAILWAY_MODE == 'eco' else 300
            await asyncio.sleep(cleanup_interval)
            
            cleaned = 0
            
            # 1️⃣ پاکسازی mirror_users
            if railway_manager.limit_dict_size(mirror_users, MAX_GROUPS_IN_MEMORY):
                cleaned += 1
            
            # 2️⃣ پاکسازی sent_messages
            if railway_manager.limit_dict_size(sent_messages, MAX_GROUPS_IN_MEMORY):
                cleaned += 1
            
            # 3️⃣ پاکسازی last_message_time
            if railway_manager.limit_dict_size(last_message_time, MAX_GROUPS_IN_MEMORY):
                cleaned += 1
            
            # 4️⃣ محدود کردن scraped_users
            if len(members_db.get('scraped_users', {})) > MAX_SCRAPED_USERS:
                # حفظ فقط کاربران جدیدتر
                scraped = members_db['scraped_users']
                sorted_users = sorted(
                    scraped.items(), 
                    key=lambda x: x[1].get('timestamp', 0), 
                    reverse=True
                )
                members_db['scraped_users'] = dict(sorted_users[:MAX_SCRAPED_USERS//2])
                cleaned += 1
            
            # 5️⃣ محدود کردن set ها
            for set_name in ['invited_users', 'failed_users', 'sent_pm', 'checked_groups']:
                if set_name in members_db:
                    if railway_manager.limit_set_size(members_db[set_name], MAX_MEMORY_ITEMS):
                        cleaned += 1
            
            # 6️⃣ اجرای garbage collection
            if railway_manager.should_run_gc():
                collected = railway_manager.run_gc()
                if collected > 0:
                    cleaned += 1
            
            # 7️⃣ پاکسازی memory_manager
            if memory_manager.should_cleanup():
                memory_manager.cleanup()
                cleaned += 1
            
            if cleaned > 0 and not RAILWAY_MODE == 'eco':
                logger.info(f"🧹 Railway Cleanup: {cleaned} منبع پاکسازی شد")
                
        except Exception as e:
            logger.error(f"❌ خطا در Railway cleanup: {e}")
            await asyncio.sleep(60)


# 🏠 تسک به‌روزرسانی دوره‌ای اعضای گروه خودمان
async def periodic_our_group_update():
    """به‌روزرسانی دوره‌ای لیست اعضای گروه @PharmaWebGp"""
    while True:
        try:
            await asyncio.sleep(3600)  # هر 1 ساعت
            await load_our_group_members()
            logger.info(f"🏠 لیست اعضای گروه ما به‌روز شد: {len(members_db['our_group_members'])} عضو")
        except Exception as e:
            logger.error(f"❌ خطا در به‌روزرسانی اعضای گروه ما: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# 🚀🚀🚀 تسک‌های تبلیغاتی پیشرفته (ADVANCED MARKETING TASKS) 🚀🚀🚀
# ═══════════════════════════════════════════════════════════════════════════════

async def viral_content_broadcaster():
    """
    ارسال محتوای ویروسی به گروه‌ها برای افزایش دسترسی
    
    استراتژی:
    - ارسال محتوای ارزشمند و قابل اشتراک
    - تنوع در نوع محتوا
    - زمان‌بندی هوشمند
    """
    while True:
        try:
            # 📢 بررسی فعال بودن broadcast
            if not ENABLE_BROADCAST:
                await asyncio.sleep(60)
                continue
            
            # صبر اولیه برای راه‌اندازی سایر سیستم‌ها
            await asyncio.sleep(300)  # 5 دقیقه
            
            if not groups:
                await asyncio.sleep(60)
                continue
            
            # چک زمان بهینه
            if not time_optimizer.is_optimal_time():
                # در ساعات غیربهینه کمتر فعالیت کن
                await asyncio.sleep(600)  # 10 دقیقه
                continue
            
            # انتخاب نوع محتوا بر اساس A/B testing
            if 'viral_content_type' not in ab_testing.active_tests:
                ab_testing.create_test('viral_content_type', 
                    ['valuable_info', 'engagement_triggers', 'shareable_tips'])
            
            content_type = ab_testing.get_variant('viral_content_type')
            
            # تولید محتوا
            content = viral_engine.generate_viral_content(content_type)
            if not is_high_quality_natural(content):
                continue  # refuse low quality
            
            # انتخاب چند گروه تصادفی
            target_groups = random.sample(groups, min(3, len(groups)))
            
            for group_id in target_groups:
                try:
                    await client.send_message(group_id, content)
                    stats['messages_sent'] += 1
                    
                    # ثبت در analytics
                    time_optimizer.record_action(success=True)
                    funnel_analytics.record_stage(group_id, 'awareness')
                    
                    # تاخیر هوشمند
                    delay = time_optimizer.get_recommended_delay(random.randint(120, 240))
                    await asyncio.sleep(delay)
                    
                except FloodWaitError as e:
                    logger.warning(f"⚠️ FloodWait در viral: {e.seconds}s")
                    await asyncio.sleep(e.seconds + 10)
                except Exception as e:
                    time_optimizer.record_action(success=False)
                    continue
            
            # تاخیر بین سیکل‌ها (هر 30-60 دقیقه)
            await asyncio.sleep(random.randint(1800, 3600))
            
        except Exception as e:
            logger.error(f"❌ خطا در viral_content_broadcaster: {e}")
            await asyncio.sleep(300)


async def engagement_content_sender():
    """
    ارسال محتوای تعاملی برای افزایش engagement
    
    استراتژی:
    - سوالات و نظرسنجی
    - محتوای آموزشی
    - تنوع موضوعی
    """
    while True:
        try:
            # 📢 بررسی فعال بودن broadcast
            if not ENABLE_BROADCAST:
                await asyncio.sleep(60)
                continue
            
            # صبر اولیه
            await asyncio.sleep(600)  # 10 دقیقه
            
            if not groups:
                await asyncio.sleep(60)
                continue
            
            # انتخاب نوع محتوا
            content_types = ['educational', 'tips', 'questions']
            
            # وزن‌دهی بر اساس ساعت روز
            hour = datetime.now().hour
            
            if 9 <= hour < 12:  # صبح: آموزشی
                content_type = 'educational'
            elif 14 <= hour < 18:  # عصر: نکات
                content_type = 'tips'
            else:  # شب: تعاملی
                content_type = 'questions'
            
            # تولید محتوا
            if content_type == 'questions':
                content = engagement_booster.generate_engagement_content('questions')
            else:
                content = content_engine.generate_content(content_type)
            if not is_high_quality_natural(content):
                continue  # guard
            
            # انتخاب گروه‌های با engagement بالا
            target_groups = random.sample(groups, min(2, len(groups)))
            
            for group_id in target_groups:
                try:
                    await client.send_message(group_id, content)
                    stats['messages_sent'] += 1
                    
                    # ثبت آمار
                    engagement_booster.record_engagement()
                    
                    await asyncio.sleep(random.randint(180, 300))
                    
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 10)
                except Exception:
                    continue
            
            # تاخیر بین سیکل‌ها (هر 1-2 ساعت)
            await asyncio.sleep(random.randint(3600, 7200))
            
        except Exception as e:
            logger.error(f"❌ خطا در engagement_content_sender: {e}")
            await asyncio.sleep(600)


async def smart_timing_controller():
    """
    کنترلر زمان‌بندی هوشمند - تنظیم سرعت فعالیت‌ها
    
    وظایف:
    - تحلیل بهترین زمان‌ها
    - تنظیم rate limiting
    - گزارش‌دهی
    """
    global SEARCH_INTERVAL, JOIN_LIMIT_PER_CYCLE
    
    while True:
        try:
            await asyncio.sleep(1800)  # هر 30 دقیقه
            
            # دریافت ضریب زمانی فعلی
            multiplier = time_optimizer.get_current_multiplier()
            
            # تنظیم پارامترها بر اساس زمان (بدون تغییر MESSAGE_DELAY - ثابت 30 دقیقه)
            if multiplier >= 1.3:  # ساعات طلایی
                SEARCH_INTERVAL = max(SEARCH_INTERVAL_MIN, SEARCH_INTERVAL * 0.8)
                JOIN_LIMIT_PER_CYCLE = min(JOIN_LIMIT_MAX, int(JOIN_LIMIT_PER_CYCLE * 1.2))
                logger.info("⚡ حالت تهاجمی: ساعات طلایی")
                
            elif multiplier <= 0.5:  # ساعات کم‌بازده
                SEARCH_INTERVAL = min(SEARCH_INTERVAL_MAX, SEARCH_INTERVAL * 1.3)
                JOIN_LIMIT_PER_CYCLE = max(JOIN_LIMIT_MIN, int(JOIN_LIMIT_PER_CYCLE * 0.8))
                logger.info("🐢 حالت محتاط: ساعات کم‌بازده")
                
            else:  # ساعات عادی
                # برگشت به حالت پیش‌فرض تدریجی
                SEARCH_INTERVAL = (SEARCH_INTERVAL + 3) / 2
                JOIN_LIMIT_PER_CYCLE = int((JOIN_LIMIT_PER_CYCLE + 35) / 2)
                logger.info("⚖️ حالت متعادل")
            
            # گزارش قیف
            funnel_report = funnel_analytics.get_funnel_report()
            bottleneck = funnel_analytics.get_bottleneck()
            
            if bottleneck and bottleneck['rate'] < 0.1:
                logger.warning(f"🚨 گلوگاه شناسایی شد: {bottleneck['stage']}")
                logger.info(f"💡 پیشنهاد: {bottleneck['recommendation']}")
            
            # آمار سیستم‌های تبلیغاتی
            logger.info(f"📊 محتوای تولید شده: {content_engine.content_stats['generated']}")
            logger.info(f"📈 Engagement: {engagement_booster.engagement_stats['avg_engagement']:.2f}")
            
        except Exception as e:
            logger.error(f"❌ خطا در smart_timing_controller: {e}")
            await asyncio.sleep(300)

# ═══════════════════════════════════════════════════════════
# 🔄 Auto-Restart (نسخه Silent)
# ═══════════════════════════════════════════════════════════
async def health_check(request):
    """Simple healthcheck endpoint for Railway"""
    return web.Response(text="OK", status=200)

# ═══════════════════════════════════════════════════════════
# 🚨 Account Health Monitor (Crisis addition)
# ═══════════════════════════════════════════════════════════
async def monitor_account_health():
    """بررسی مداوم وضعیت حساب برای تشخیص محدودیت یا بن"""
    global ACCOUNT_HEALTHY
    await asyncio.sleep(30)  # صبر اولیه

    while True:
        try:
            if not SAFE_MODE:
                await asyncio.sleep(300)
                continue

            # تست ساده: گرفتن لیست دیالوگ‌ها (اگر محدود باشد ارور می‌دهد)
            try:
                dialogs = await client.get_dialogs(limit=1)
                if not ACCOUNT_HEALTHY:
                    print("✅ Account seems healthy again", flush=True)
                ACCOUNT_HEALTHY = True
            except Exception as e:
                err = str(e).lower()
                if any(x in err for x in ['restricted', 'banned', 'flood', 'spam', 'peerflood', 'write forbidden']):
                    if ACCOUNT_HEALTHY:
                        print(f"🚨 ACCOUNT RESTRICTED/BANNED DETECTED: {e}", flush=True)
                    ACCOUNT_HEALTHY = False

            await asyncio.sleep(600)  # هر ۱۰ دقیقه چک کن
        except Exception:
            await asyncio.sleep(300)

async def start_web_server():
    """Start a minimal web server for Railway health checks and keep-alive"""
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    
    port = int(os.environ.get('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    slog(f"🌐 Web server started on port {port} for Railway healthcheck")

async def run_bot_with_auto_restart():
    """اجرای ربات با auto-restart در صورت crash - بدون لاگ"""
    restart_count = 0
    max_restarts = 10
    
    while True:
        try:
            await main()
            
        except KeyboardInterrupt:
            break
            
        except Exception as e:
            restart_count += 1
            
            if restart_count >= max_restarts:
                break
            
            # تاخیر exponential backoff (بدون نمایش لاگ)
            wait_time = min(60 * (2 ** (restart_count - 1)), 3600)
            await asyncio.sleep(wait_time)

if __name__ == '__main__':
    try:
        asyncio.run(run_bot_with_auto_restart())
    except KeyboardInterrupt:
        slog("\n👋 خداحافظ!")
    except:
        pass  # خطاها بدون نمایش



