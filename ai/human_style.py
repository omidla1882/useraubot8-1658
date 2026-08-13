"""
Natural group presence: topic-aware unicode emoji + context starters.
Premium custom-emoji document IDs are attached at send time in bot.py.
"""

import random
import re
from typing import List, Optional, Tuple

# One emoji max. Humans rarely stack three.
_TOPIC_EMOJI = [
    (re.compile(r'vpn|فیلترشکن|وایرفگارد|کلش|اینترنت', re.I), ['😅', '🫠', '👍']),
    (re.compile(r'فیلم|سریال|تماشا', re.I), ['🎬', '😅']),
    (re.compile(r'غذا|خورم|درست کرد', re.I), ['😋', '😅']),
    (re.compile(r'باشگاه|ورزش|فوتبال', re.I), ['💪', '😅']),
    (re.compile(r'هوا|بارون|ترافیک', re.I), ['😅', '☁️']),
    (re.compile(r'خواب|بیدار', re.I), ['😴', '😅']),
    (re.compile(r'سفر|شمال|استانبول|ترکیه', re.I), ['✈️', '😅']),
    (re.compile(r'دلار|قیمت|نوسان', re.I), ['😅', '💸']),
    (re.compile(r'سلام|درود|چه خبر', re.I), ['✌️', '👋', '🙂']),
    (re.compile(r'پی.?وی|خصوصی', re.I), ['✌️', '🙂']),
    (re.compile(r'ریتالین|اوزمپیک|adhd|مودافینیل', re.I), ['🤔', '👍']),
]

_GENERIC_EMOJI = ['😅', '🤔', '🙂', '😂', '✌️', '👍']

_HAS_EMOJI = re.compile(
    r'[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U0001F1E0-\U0001F1FF]'
)


def decorate_human_text(text: str, chance: float = 0.55) -> Tuple[str, str]:
    """Maybe append one fitting emoji. Returns (text, emoji_or_empty)."""
    t = (text or "").strip()
    if not t or _HAS_EMOJI.search(t):
        return t, ""
    if random.random() > chance:
        return t, ""
    pool = _GENERIC_EMOJI
    for pat, emos in _TOPIC_EMOJI:
        if pat.search(t):
            pool = emos
            break
    emo = random.choice(pool)
    # Prefer end of message — like a person, not a header icon
    if t.endswith(('?', '؟')):
        return t + " " + emo, emo
    return t + " " + emo, emo


STARTER_BANK = {
    'greeting': [
        "سلام بچه‌ها، امروز چه خبر؟",
        "سلام. گروه امروز آرومه یا من دیر اومدم؟",
        "درود، همه خوبینا؟",
    ],
    'vpn': [
        "راستی VPN چی استفاده میکنید این روزا؟",
        "فیلترشکن امروز خیلی ضعیف شده. مال شما هم همینه؟",
    ],
    'film': [
        "کسی فیلم یا سریال خوبی دیده که ارزش وقت گذاشتن داشته باشه؟",
        "یه سریال شروع کردم ولی نصفه رها کردم. پیشنهاد دارید؟",
    ],
    'life': [
        "کار از خونه بهتره یا دفتر؟ من که تمرکزم تو خونه بهتره.",
        "خوابتون منظمه این روزا یا مثل من بهم ریخته؟",
        "باشگاه میرید این روزا یا ول کردین؟",
    ],
    'city': [
        "هوای شهرتون چطوره امروز؟",
        "ترافیک امروز چطوره پیش شما؟",
        "کسی شمال رفته اخیرا؟ جاده آخر هفته چطوره؟",
    ],
    'travel': [
        "کسی تجربه زندگی در استانبول داره؟ هزینه زندگی چطوره؟",
        "مهاجرت ترکیه هنوز ارزش داره یا خیلی گرون شده؟",
    ],
    'money': [
        "دلار اینقدر نوسان داره که آدم گیج میشه. شما هم دنبال خبرین؟",
        "نوبیتکس یا والکس — کدومو ترجیح میدید؟",
    ],
    'food': [
        "امروز ناهار چی درست کردید؟ حوصله آشپزی نداشتم.",
        "یه غذای ساده سریع چی پیشنهاد میکنید؟",
    ],
    'domain': [
        "داروهای ADHD این روزا خیلی کمیابن. کسی تجربه داره از کجا بگیره؟",
        "شنیدم اوزمپیک تو ایران اصلی پیدا نمیشه. شما هم این مشکل داشتین؟",
    ],
}


def pick_context_starter(recent_ctx: str = "") -> str:
    """Prefer a starter that fits recent group talk; otherwise mix all topics."""
    ctx = (recent_ctx or "").lower()
    keyed: List[str] = []
    if any(w in ctx for w in ['فیلم', 'سریال']):
        keyed.extend(STARTER_BANK['film'])
    if any(w in ctx for w in ['vpn', 'فیلتر', 'اینترنت']):
        keyed.extend(STARTER_BANK['vpn'])
    if any(w in ctx for w in ['استانبول', 'ترکیه', 'مهاجرت']):
        keyed.extend(STARTER_BANK['travel'])
    if any(w in ctx for w in ['دلار', 'تتر', 'نوبیتکس']):
        keyed.extend(STARTER_BANK['money'])
    if any(w in ctx for w in ['غذا', 'ناهار']):
        keyed.extend(STARTER_BANK['food'])
    if any(w in ctx for w in ['سلام', 'چه خبر']):
        keyed.extend(STARTER_BANK['greeting'])
    if keyed and random.random() < 0.7:
        return random.choice(keyed)
    # Weighted mix: mostly everyday, rarely domain
    bags = (
        STARTER_BANK['greeting'] + STARTER_BANK['life'] + STARTER_BANK['city']
        + STARTER_BANK['film'] + STARTER_BANK['vpn'] + STARTER_BANK['food']
        + STARTER_BANK['travel'] + STARTER_BANK['money']
    )
    if random.random() < 0.12:
        bags = bags + STARTER_BANK['domain']
    return random.choice(bags)


def funnel_lines() -> List[str]:
    return [
        "اگه خواستی جزئیاتش رو پی‌وی بگو راحت‌تر حرف میزنیم.",
        "اینجا شلوغه، پی‌وی پیام بده ادامه بدیم.",
        "جزئیاتش بهتره خصوصی حرف بزنیم، پیام بده.",
        "اگه ادامه داره پی‌وی بده، اینجا شلوغه.",
    ]
