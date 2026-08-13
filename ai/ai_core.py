"""
Professional AI core for userbotai — ports + enhancements from web3test/chat
for maximum intelligence on Qwen3.

Includes:
- Full intent classification (complete rules)
- Conversation brain (anti-rep, boost)
- Reasoning / plan
- Knowledge retrieval (expanded for group use)
- Helper for rich prompt building
"""

import re
import random
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────────────
# Full INTENT_RULES + classify (complete from web3test/chat/intent_router.py)
# ──────────────────────────────────────────────────────────────────────────────

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

PRODUCT_INTENTS = frozenset({'product_search', 'price_only', 'stock_check', 'supplement_search'})

SHIPPING_CITIES = {
    'fast': {
        'تهران': '🇮🇷', 'tehran': '🇮🇷',
        'استانبول': '🇹🇷', 'istanbul': '🇹🇷',
        'دبی': '🇦🇪', 'dubai': '🇦🇪',
        'بغداد': '🇮🇶', 'baghdad': '🇮🇶',
        'تورنتو': '🇨🇦', 'toronto': '🇨🇦',
    },
}

INTENT_RULES: List[Tuple[str, List[str]]] = [
    ('complaint', [
        r'جواب.*پرت', r'اشتباه', r'نمی\s*فهم', r'منظور.*نمی', r'درست\s*جواب',
        r'چرا.*خداحافظ', r'بی\s*ربط', r'ناراحت', r'ضعیف', r'تکرار', r'پاسخ.*تکرار',
        r'ضایع', r'پیاده\s*سازی', r'repetitive', r'useless',
    ]),
    ('bot_question', [
        r'ربات', r'رباتی', r'\bbot\b', r'هوش\s*مصنوعی', r'\bai\b', r'چت\s*بات',
        r'بات\s*هست', r'انسان\s*نیست',
    ]),
    ('faq_after_sales', [
        r'پس\s*از\s*فروش', r'خدمات\s*پس', r'گارانتی', r'ضمانت\s*محصول',
        r'after\s*sales', r'warranty', r'support\s*after',
    ]),
    ('identity_question', [
        r'تو\s*کی\s*هست', r'شما\s*کی\s*هست', r'کی\s*هستی', r'who\s*are\s*you',
        r'اسم\s*تو', r'اسمت', r'معرفی\s*کن',
    ]),
    ('presence_check', [
        r'^هستی\s*[؟?]?\s*$', r'هستی\s*[؟?]', r'^الو\s*[؟?]?\s*$', r'^alo\s*[؟?]?\s*$',
        r'آنجایی', r'پاسخ\s*مید', r'جواب\s*مید', r'گوش\s*مید',
        r'are\s*you\s*there', r'online\s*[؟?]', r'^\?+\s*$', r'^؟+\s*$',
    ]),
    ('chat_memory', [
        r'چت.*گذشته', r'پیام.*قبل', r'بخاطر\s*می', r'یادت\s*می', r'حافظه',
        r'remember.*chat', r'previous\s*message',
    ]),
    ('trust_question', [
        r'اعتماد', r'اطمینان', r'قابل\s*اطمینان', r'مطمئن', r'معتبر', r'کلاهبرد', r'تقلب',
        r'trust', r'legit', r'reliable', r'scam',
        r'فارما\s*وب.*(معتبر|اطمینان|اعتماد)',
    ]),
    ('faq_order_process', [
        r'چطور.*خرید', r'چگونه.*خرید', r'نحوه\s*خرید', r'مراحل\s*(خرید|سفارش)',
        r'چیکار\s*باید\s*بکن', r'چکار\s*باید\s*بکن', r'سفارش\s*بدم', r'خرید\s*کنم',
        r'راهنمای\s*خرید', r'how\s*(to\s*)?(buy|order)', r'order\s*process',
        r'how\s*do\s*i\s*(buy|order|purchase)', r'purchase\s*steps',
        r'میخوام\s*خرید', r'می‌خوام\s*خرید', r'به\s*ترتیب.*خرید',
        r'از\s*(فارما|سایت|فروشگاه).*خرید', r'خرید\s*از\s*سایت',
        r'چطور.*سفارش', r'چگونه.*سفارش', r'سفارش\s*ثبت', r'ثبت\s*سفارش',
        r'دقیقا.*چطور', r'دقیقاً.*چطور', r'نحوه\s*سفارش', r'راهنمای\s*سفارش',
        r'place\s*an?\s*order', r'checkout\s*process', r'register\s*order',
    ]),
    ('payment_crypto_help', [
        r'چطور\s*پرداخت', r'نحوه\s*پرداخت', r'راهنما.*پرداخت', r'کیف\s*پول', r'والت',
        r'how\s*to\s*pay', r'payment\s*guide',
    ]),
    ('crypto_info', [
        r'تتر', r'usdt', r'کریپتو', r'ارز\s*دیجیتال', r'بیت\s*کوین', r'btc', r'اتریوم',
        r'صرافی', r'nobitex', r'والکس',
    ]),
    ('wrong_payment', [
        r'شبکه\s*اشتباه', r'wrong\s*network', r'اشتباه\s*واریز', r'کم\s*واریز',
        r'مبلغ\s*اشتباه', r'wrong\s*amount', r'less\s*than',
    ]),
    ('payment_confirmation', [
        r'پرداخت\s*کردم', r'واریز\s*کردم', r'پول\s*دادم', r'paid', r'transferred',
        r'\bhash\b', r'txid', r'شناسه\s*تراکنش',
    ]),
    ('cancel_order', [
        r'لغو\s*سفارش', r'cancel\s*order', r'انصراف', r'پشیمون', r'نمیخوام\s*سفارش',
    ]),
    ('order_issue', [
        r'نرسید', r'not\s*received', r'تحویل\s*نشد', r'آسیب\s*دید', r'شکسته',
        r'مغایرت', r'wrong\s*item', r'اشتباه\s*فرستاد', r'گم\s*شد',
    ]),
    ('login_help', [
        r'فراموشی\s*رمز', r'forgot\s*password', r'نمیتونم\s*وارد', r"can't\s*login",
        r'رمز\s*عبور', r'ورود\s*نمی',
    ]),
    ('tracking', [
        r'پیگیری', r'رهگیری', r'وضعیت\s*سفارش', r'کد\s*رهگیری', r'سفارش.*پیگیری',
        r'پیگیری.*سفارش', r'track', r'order\s*status', r'where\s*is\s*my\s*order',
    ]),
    ('shipping_time', [
        r'ارسال.*(به|در)', r'دریافت.*(در|به)', r'امکان.*(دریافت|ارسال)',
        r'کی\s*می.?رس', r'زمان.*ارسال', r'چند\s*روز', r'چقدر.*طول', r'تحویل',
        r'delivery', r'shipping',
    ]),
    ('faq_return', [
        r'مرجوع', r'بازگشت', r'refund', r'return', r'سیاست.*بازگشت',
    ]),
    ('faq_prescription', [
        r'نسخه', r'بدون\s*نسخه', r'غیرنسخه', r'prescription', r'\brx\b', r'دکتر',
    ]),
    ('faq_wallet', [
        r'کیف\s*پول', r'wallet', r'شارژ\s*کیف', r'charge\s*wallet', r'اعتبار',
    ]),
    ('faq_account', [
        r'ثبت\s*نام', r'register', r'حساب\s*کاربری', r'account', r'پروفایل', r'profile',
        r'login', r'ورود', r'لاگین',
    ]),
    ('site_info', [
        r'فارسیت', r'زبان\s*فارسی', r'چند\s*محصول', r'درباره\s*سایت', r'about\s*site',
    ]),
    ('clarification', [
        r'متوجه\s*ن', r'نمی\s*فهم', r'کاملتر', r'بیشتر\s*توضیح', r'واضحتر', r'explain\s*more',
    ]),
    ('help_request', [
        r'کمکم\s*کن', r'کمک\s*کن', r'راهنمایی', r'help\s*me',
    ]),
    ('human_request', [
        r'اپراتور', r'انسان', r'پشتیبان\s*واقعی', r'مدیر', r'ادمین', r'انسان\s*نداره',
        r'human\s*agent', r'real\s*person', r'talk\s*to\s*human',
    ]),
    ('greeting', [
        r'^سلام', r'^درود', r'^وقت\s*بخیر', r'^وقت\s*$', r'^صبح\s*بخیر', r'^hello', r'^hi\b',
    ]),
    ('thanks', [
        r'ممنون', r'متشکر', r'مرسی', r'thank',
    ]),
    ('goodbye', [
        r'خداحافظ', r'خدانگهدار', r'\bbye\b', r'مع\s*السلامه', r'فعلا\s*$',
    ]),
    ('supplement_search', [
        r'مکمل\s*تقویتی', r'مکمل.*دار', r'مکمل\s*چی', r'supplement',
    ]),
    ('stock_check', [
        r'موجودی', r'ناموجود', r'out\s*of\s*stock', r'\bstock\b',
        r'(دارید|داری|دارین|موجود)\s*[؟?]',
        r'(دارو|مکمل|قرص).*(موجود|دارید|داری)',
        r'(موجود|دارید|داری).*(دارو|مکمل|قرص)',
    ]),
    ('product_info', [
        r'تاریخ\s*انقضا', r'انقضا', r'\bexpir', r'کشور\s*ساز', r'country\s*of',
        r'مقایسه', r'\bcompare\b', r'فرق\s+', r'\bdosage\b', r'دوز\s*مصنوع',
        r'جزئیات\s*محصول', r'product\s*page', r'مشخصات',
    ]),
    ('product_search', [
        r'(ریتالین|ritalin|percista|پرکتیسا|فیتو|phyto|ozempic|اوزمپیک|'
        r'انسولین|insulin|مونجارو|monjaro|mounjaro|ترامادول|tramadol|'
        r'کنسرتا|concerta|زاناکس|xanax|modafinil|مودافینیل)',
        r'(دارید|داری|دارین|موجود).*(دارو|مکمل|قرص|کپسول)',
        r'(دارو|مکمل|قرص|کپسول).*(دارید|داری|دارین|موجود)',
        r'قیمت\s+', r'چند\s*میشه', r'چقدر\s*هست',
    ]),
]

KNOWN_PRODUCT_WORDS = re.compile(
    r'(ریتالین|ritalin|percista|پرکتیسا|فیتو|phyto|ozempic|اوزمپیک|انسولین|insulin|'
    r'مونجارو|monjaro|mounjaro|ترامادول|tramadol|کنسرتا|concerta|زاناکس|xanax|'
    r'modafinil|مودافینیل|سلنیوم|selenium|هرسپتین|herceptin|پارنات|parnate|زالپلون|zaleplon)',
    re.I,
)

# Only inject pharma/crypto knowledge when the message is actually about those topics.
# City names alone (استانبول زندگی) must NOT pull shipping snippets.
_DOMAIN_TOPIC_RE = re.compile(
    r'ریتالین|ritalin|اوزمپیک|ozempic|مونجارو|mounjaro|مودافینیل|modafinil|'
    r'انسولین|insulin|ترامادول|tramadol|کونسرتا|کنسرتا|concerta|سماگلوتاید|'
    r'\busdt\b|trc20|تتر|ترون|\bbtc\b|اتریوم|نوبیتکس|والکس|کریپتو|ارز\s*دیجیتال|'
    r'adhd|بیش.?فعال|نارکولپسی|'
    r'(ارسال|تحویل|بسته).{0,20}(ساعت|شهر|استانبول|تهران|دبی|تورنتو)|'
    r'(پرداخت|واریز).{0,16}(تتر|usdt|کریپتو|ترون)|'
    r'سفارش\s*(بدم|بدم|کنم|ثبت)|گمرک',
    re.I,
)

_DOMAIN_INTENTS = frozenset({
    'product_search', 'shipping_time', 'crypto_info', 'payment_crypto_help',
    'stock_check', 'faq_order_process', 'trust_question', 'product_info',
    'tracking', 'payment_confirmation', 'faq_prescription', 'supplement_search',
    'price_only',
})


def is_domain_topic(text: str, intent: str = "") -> bool:
    """True only when the message is actually about drugs/crypto/shipping."""
    if intent and intent in _DOMAIN_INTENTS:
        # shipping_time from a city name alone is a false positive — require domain words
        if intent == 'shipping_time' and not _DOMAIN_TOPIC_RE.search(text or ''):
            if not re.search(r'(ارسال|تحویل|سفارش|بسته|طول\s*می|چند\s*ساعت)', text or '', re.I):
                return False
        return True
    return bool(_DOMAIN_TOPIC_RE.search(text or ''))


def _detect_language(text: str) -> str:
    if re.search(r'[\u0600-\u06FF]', text or ''):
        return 'fa'
    return 'en'


def classify_intent(message: str) -> Dict:
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
    # Only treat as shipping when delivery words are also present.

    if intent == 'help_request' and re.search(r'(خرید|سفارش|پرداخت|دارو)', msg_lower):
        intent = 'faq_order_process'
        confidence = 0.88

    if intent == 'unknown' and re.search(r'(میخوام|می‌خوام)', msg_lower):
        if KNOWN_PRODUCT_WORDS.search(message or ''):
            intent = 'product_search'
            confidence = 0.88

    if intent == 'unknown' and KNOWN_PRODUCT_WORDS.search(message or ''):
        if re.search(r'(دارید|موجود|قیمت|چنده|چقدر)', msg_lower):
            intent = 'product_search'
            confidence = 0.82

    if intent == 'faq_order_process' and re.search(r'پیگیری|رهگیری|وضعیت|track', msg_lower):
        intent = 'tracking'
        confidence = 0.92

    # Delivery-time questions win over crypto even if USDT is mentioned
    if re.search(r'(ارسال|تحویل|طول\s*می|چند\s*ساعت|کی\s*میرس)', msg_lower):
        for city in SHIPPING_CITIES['fast']:
            if city in msg_lower:
                intent = 'shipping_time'
                confidence = 0.93
                entities.setdefault('cities', []).append({'name': city, 'flag': SHIPPING_CITIES['fast'][city]})
                break

    # ADHD / focus / weight without a brand name — require medical context for "تمرکز"
    if intent == 'unknown':
        if re.search(r'adhd|بیش.?فعال|نارکولپسی', msg_lower):
            intent = 'product_search'
            confidence = 0.82
        elif re.search(r'تمرکز', msg_lower) and re.search(r'(دارو|قرص|ریتالین|مودافینیل|adhd)', msg_lower):
            intent = 'product_search'
            confidence = 0.80
        elif re.search(r'کاهش\s*وزن|لاغر|چاقی', msg_lower) and re.search(r'(دارو|اوزمپیک|مونجارو|قرص)', msg_lower):
            intent = 'product_search'
            confidence = 0.80

    return {
        'intent': intent,
        'confidence': confidence,
        'entities': entities,
        'language': language,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Conversation brain (anti-repetition + boost)
# ──────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    if not text:
        return ''
    t = re.sub(r'<!--cards:.*?-->', '', text, flags=re.DOTALL)
    t = re.sub(r'\s+', ' ', t).strip().lower()
    return t


def is_repeated_response(response: str, history: Optional[List[Tuple]]) -> bool:
    """history is list of (role, text, ...) tuples"""
    norm = _normalize(response)
    if not norm:
        return True
    recent = []
    for item in reversed(history or []):
        if item[0] == 'bot':
            recent.append(_normalize(item[1]))
        if len(recent) >= 3:
            break
    for prev_norm in recent:
        if not prev_norm:
            continue
        if norm == prev_norm or (len(norm) > 40 and norm[:80] == prev_norm[:80]):
            return True
        # simple overlap
        aw = set(norm.split())
        bw = set(prev_norm.split())
        if aw and len(aw & bw) / max(len(aw), 1) > 0.82:
            return True
    return False


def boost_intent_from_context(message: str, intent: str, history: Optional[List]) -> str:
    msg = (message or '').strip().lower()
    if not msg:
        return intent
    if len(msg) <= 12 and intent in ('unknown', 'greeting', 'presence_check'):
        last = None
        for h in reversed(history or []):
            if h[0] == 'user':
                last = h[1]
                break
        if last:
            prev = classify_intent(last).get('intent', 'unknown')
            if prev not in ('unknown', 'greeting', 'thanks', 'goodbye'):
                return prev
    # add more keyword boost if needed
    return intent


# ──────────────────────────────────────────────────────────────────────────────
# Simple reasoning / plan (adapted)
# ──────────────────────────────────────────────────────────────────────────────

STRATEGY_FAST = 'fast'
STRATEGY_LLM = 'llm_reasoning'
STRATEGY_CAREFUL = 'careful'
STRATEGY_FAQ = 'knowledge'

_DRUG_KWS = ['ریتالین', 'اوزمپیک', 'مونجارو', 'مودافینیل', 'ترامادول', 'انسولین']

def plan_response(intent_info: dict, has_retrieved: bool, has_history: bool, message: str = "") -> dict:
    intent = intent_info.get('intent', 'unknown')
    strategy = STRATEGY_LLM
    thinking = f"intent={intent} | retrieved={has_retrieved} | history={has_history}"

    if intent in ('payment_crypto_help', 'crypto_info', 'shipping_time', 'tracking', 'faq_order_process') and has_retrieved:
        strategy = STRATEGY_FAQ
    elif intent in ('complaint', 'bot_question', 'trust_question'):
        strategy = STRATEGY_CAREFUL
    elif intent in ('greeting', 'presence_check', 'thanks'):
        strategy = STRATEGY_FAST
    elif any(k in (message or '').lower() for k in _DRUG_KWS):
        thinking += " | drug_focus"
    else:
        strategy = STRATEGY_LLM if not has_retrieved else STRATEGY_FAQ

    return {'strategy': strategy, 'intent': intent, 'thinking': thinking}


# ──────────────────────────────────────────────────────────────────────────────
# Knowledge (light but useful for groups)
# ──────────────────────────────────────────────────────────────────────────────

KNOWLEDGE_SNIPPETS = [
    ("payment", "پرداخت فقط با ارز دیجیتال: BTC، ETH، USDT(TRC20 بهترین)، TRX، BNB، TON، SOL، DOGE. نوبیتکس و والکس خوبه. TRC20 کارمزد پایینی داره."),
    ("shipping", "ارسال سریع به تهران، استانبول، دبی، بغداد، تورنتو — معمولاً زیر ۴-۸ ساعت بعد تأیید. بسته محرمانه. خودم گرفتم سریع رسید."),
    ("shipping_time", "استانبول ۴-۸ ساعته، تهران زیر ۴ ساعت، دبی و تورنتو هم معمولاً همین‌قدر. بسته محرمانه میاد."),
    ("ritalin", "ریتالین، کونسرتا، ساندوز — همه متیل‌فنیدات هستن برای ADHD و بیش‌فعالی. اورجینال اروپایی موجوده. هولوگرام داره."),
    ("adhd", "برای ADHD معمولاً ریتالین یا کونسرتا اول مطرح میشه. دوز رو باید پزشک بده. اورجینال اروپایی با ایرانی فرق کیفیت داره."),
    ("semaglutide", "اوزمپیک / سماگلوتاید برای دیابت نوع ۲ و کاهش وزن. ویگوی هم موجوده. نووو نوردیسک اورجینال."),
    ("tirzepatide", "مونجارو (تیرزپاتید) مشابه اوزمپیک ولی قوی‌تر. برای کنترل قند و کاهش وزن. اورجینال موجوده."),
    ("modafinil", "مودافینیل برای تمرکز و بیداری. مودالرت هم همون ماده‌ست. خودم استفاده کردم خوبه."),
    ("crypto", "USDT روی TRC20 کارمزد خیلی پایینه و سریع تأیید میشه. همیشه شبکه رو دقیق چک کن. BNB هم خوبه."),
    ("crypto_network", "TRC20 بهترین شبکه برای USDT هست — کارمزد پایین، تأیید سریع. ERC20 گرونه."),
    ("site", "medpharmaweb.com — دارو و مکمل اورجینال اروپایی/آمریکایی با ضمانت و هولوگرام."),
    ("order", "سفارش از سایت medpharmaweb.com: سبد خرید → checkout → USDT میفرستی → چند دقیقه تأیید → ارسال."),
    ("migration_turkey", "ترکیه ایکامت تورستیک و کوتاه‌مدت رایجه. استانبول گرون‌تر از آنکارا و ازمیره. این روزا هزینه‌ها بالا رفته."),
    ("migration_general", "مهاجرت: کانادا اکسپرس اینتری، آلمان فرصت شغلی، دبی ویزای سرمایه‌گذاری. هر کشور شرایط خودشو داره."),
    ("crypto_general", "USDT و USDC استیبل‌کوین هستن، نوسان ندارن. بیتکوین نوسان داره. برای تراکنش پیشنهادم USDT روی TRC20."),
    ("tramadol", "ترامادول مسکن اپیوئیدی قوی‌ست. کنترل دسترسی داره. اطلاعات عمومی فقط."),
    ("insulin", "انسولین‌های مختلف (لانتوس، نواراپید) باید سرد باشن. دوز فقط توسط پزشک تعیین میشه."),
]

DRUG_ALIASES = {
    'methylphenidate': ['ریتالین', 'متیل‌فنیدات', 'کونسرتا', 'ساندوز', 'پرکتیسا', 'وایاس', 'آدرال', 'adhd', 'بیش فعالی', 'بیش‌فعالی'],
    'semaglutide': ['اوزمپیک', 'سماگلوتاید', 'ویگوی', 'wegovy'],
    'tirzepatide': ['مونجارو', 'تیرزپاتید', 'mounjaro'],
    'modafinil': ['مودافینیل', 'مودالرت', 'پروویجیل'],
    'tramadol': ['ترامادول'],
    'insulin': ['انسولین', 'لانتوس', 'نوورپید'],
}

# Full professional port of DRUG_FAMILIES + match/get from web3test/chat/drug_families.py + drug_knowledge
# + composer logic for real grounded answers + natural relevant inserts
DRUG_FAMILIES: Dict[str, Dict] = {
    'methylphenidate': {
        'groups': ['ritalin', 'sandoz', 'methyl_groups', 'concerta', 'vyas', 'aderal', 'mydayis'],
        'active_ingredient_fa': 'متیل‌فنیدات',
        'active_ingredient_en': 'Methylphenidate',
        'indication_fa': 'درمان اختلال کمبود توجه و بیش‌فعالی (ADHD) و نارکولپسی',
        'indication_en': 'Treatment of ADHD and narcolepsy',
        'aliases_fa': ['ریتالین', 'ریتالین la', 'پرکتیسا', 'پرکتیزا', 'کونسرتا', 'کنسرتا', 'متیل فنیدات', 'متیل‌فنیدات', 'ساندوز', 'وایاس', 'آدرال', 'مایدیس', 'مشکا', 'مدی کی', 'بیش فعالی', 'بیش‌فعالی', 'adhd'],
        'aliases_en': ['ritalin', 'ritalin la', 'percista', 'percitza', 'concerta', 'methylphenidate', 'sandoz', 'vyas', 'adderall', 'mydayis', 'medikinet'],
        'iran_brands_fa': ['پرکتیسا', 'مشکا', 'مدی کی'],
    },
    'semaglutide': {
        'groups': ['monjaro_groups', 'ozempic', 'wegovy'],
        'active_ingredient_fa': 'سماگلوتاید',
        'active_ingredient_en': 'Semaglutide',
        'indication_fa': 'درمان دیابت نوع ۲ و کمک به کاهش وزن',
        'indication_en': 'Type 2 diabetes treatment and weight management',
        'aliases_fa': ['اوزمپیک', 'ویگوی', 'سماگلوتاید', 'مونجارو'],
        'aliases_en': ['ozempic', 'wegovy', 'semaglutide', 'mounjaro'],
    },
    'tirzepatide': {
        'groups': ['monjaro_groups'],
        'active_ingredient_fa': 'تیرزپاتید',
        'active_ingredient_en': 'Tirzepatide',
        'indication_fa': 'درمان دیابت و مدیریت وزن',
        'indication_en': 'Diabetes and weight management',
        'aliases_fa': ['مونجارو', 'تیرزپاتید'],
        'aliases_en': ['mounjaro', 'tirzepatide', 'zepbound'],
    },
    'modafinil': {
        'groups': ['modafinil', 'sandoz'],
        'active_ingredient_fa': 'مودافینیل',
        'active_ingredient_en': 'Modafinil',
        'indication_fa': 'درمان خواب‌آلودگی بیش‌ازحد (نارکولپسی) و تقویت هوشیاری',
        'indication_en': 'Narcolepsy and wakefulness promotion',
        'aliases_fa': ['مودافینیل', 'پروویجیل', 'مودالرت'],
        'aliases_en': ['modafinil', 'provigil', 'modalert'],
    },
    'tramadol': {
        'groups': ['tramadol_groups'],
        'active_ingredient_fa': 'ترامادول',
        'active_ingredient_en': 'Tramadol',
        'indication_fa': 'مسکن اپیوئیدی برای درد متوسط تا شدید',
        'indication_en': 'Opioid analgesic for moderate to severe pain',
        'aliases_fa': ['ترامادول', 'اولترام'],
        'aliases_en': ['tramadol', 'ultram'],
    },
    'insulin': {
        'groups': ['insuline_groups'],
        'active_ingredient_fa': 'انسولین',
        'active_ingredient_en': 'Insulin',
        'indication_fa': 'درمان دیابت — کنترل قند خون',
        'indication_en': 'Diabetes — blood glucose control',
        'aliases_fa': ['انسولین', 'لانتوس', 'نوورپید', 'توجئو', 'هومالوگ'],
        'aliases_en': ['insulin', 'lantus', 'novorapid', 'toujeo', 'humalog', 'apidra'],
    },
    'phyto': {
        'groups': ['phyto_groups'],
        'active_ingredient_fa': 'فیتو (مکمل گیاهی)',
        'active_ingredient_en': 'Phyto (herbal supplement)',
        'indication_fa': 'مکمل تقویتی و گیاهی',
        'indication_en': 'Herbal strengthening supplement',
        'aliases_fa': ['فیتو', 'مکمل تقویتی'],
        'aliases_en': ['phyto', 'supplement'],
    },
}

def match_drug_family(text: str) -> Optional[str]:
    if not text:
        return None
    t = text.lower().strip()
    for key, fam in DRUG_FAMILIES.items():
        for alias in fam.get('aliases_fa', []) + fam.get('aliases_en', []):
            if alias.lower() in t:
                return key
    return None

def get_family_info(family_key: str, language: str = 'fa') -> Optional[Dict]:
    fam = DRUG_FAMILIES.get(family_key)
    if not fam:
        return None
    return {
        'family': family_key,
        'active_ingredient': fam.get(f'active_ingredient_{language}') or fam.get('active_ingredient_en'),
        'indication': fam.get(f'indication_{language}') or fam.get('indication_en'),
        'groups': fam.get('groups', []),
    }

def get_drug_context_snippet(text: str, language: str = 'fa') -> str:
    """Factual drug context for LLM prompt — indication + active ingredient. No personal claims."""
    try:
        fam_key = match_drug_family(text)
        if fam_key:
            info = get_family_info(fam_key, language)
            if info:
                ai = info.get('active_ingredient', fam_key)
                ind = info.get('indication', '')
                if language == 'fa':
                    return f"{ai} برای {ind}."
                return f"{ai} for {ind}."
    except Exception:
        pass
    t = (text or '').lower()
    for fam, data in DRUG_FAMILIES.items():
        aliases = data.get('aliases_fa', []) + data.get('aliases_en', [])
        if any(a.lower() in t for a in aliases):
            ind = data.get('indication_fa') or data.get('indication_en', '')
            ai = data.get('active_ingredient_fa') or data.get('active_ingredient_en', fam)
            return f"{ai}: {ind}."
    return ''

_RETRIEVE_STOP = frozenset({
    'برای', 'چیه', 'چیست', 'پیشنهاد', 'میکنید', 'میکنی', 'هست', 'داره', 'کنید',
    'بده', 'بگو', 'کسی', 'این', 'اون', 'که', 'از', 'با', 'تو', 'رو', 'چی',
    'product', 'search', 'unknown', 'help', 'request',
})

def retrieve_knowledge(query: str, intent: str = "") -> str:
    """Stronger retrieval (scoring + topic + drug + intent map). Ignores stopwords.
    Returns empty for general chat so Qwen is not forced into a drug/crypto script."""
    if not is_domain_topic(query, intent):
        return ""
    q = ((query or "") + " " + (intent or "")).lower()
    hits = []
    for key, txt in KNOWLEDGE_SNIPPETS:
        sc = 0.0
        ql = q.lower()
        if key in ql:
            sc += 4.0
        for tok in key.split('_'):
            if tok and len(tok) > 3 and tok in ql:
                sc += 2.0
        for w in ql.split():
            if len(w) > 3 and w not in _RETRIEVE_STOP and w in txt.lower():
                sc += 0.8
        if sc > 0:
            hits.append((sc, txt))
    # drug family boost (full)
    fam_key = match_drug_family(query)
    if fam_key:
        info = get_family_info(fam_key)
        if info:
            hits.append((4.2, f"{info.get('active_ingredient','')}: {info.get('indication','')}"))
    for fam, als in DRUG_ALIASES.items():
        if any(a.lower() in q for a in als):
            for key, txt in KNOWLEDGE_SNIPPETS:
                if fam in key.lower() or any(a.lower() in txt.lower() for a in als):
                    hits.append((5.0, txt))
    # intent map boost
    intent_key_map = {
        'payment_crypto_help': 'payment', 'crypto_info': 'crypto_network',
        'shipping_time': 'shipping', 'faq_order_process': 'order',
        'trust_question': 'authenticity',
    }
    if intent in intent_key_map:
        for key, txt in KNOWLEDGE_SNIPPETS:
            if key == intent_key_map[intent]:
                hits.append((4.0, txt))
    if not hits:
        return ""
    hits.sort(key=lambda x: -x[0])
    out, seen = [], set()
    for sc, t in hits:
        if t not in seen:
            seen.add(t)
            out.append(t)
        if len(out) >= 3:
            break
    return "\n".join(out)

def compose_knowledge_response(
    message: str, language: str = 'fa', intent: Optional[str] = None
) -> Optional[str]:
    """Professional composer port (web3test knowledge_composer): drug first if match, score-based, combine for real answers."""
    drug_ctx = get_drug_context_snippet(message, language)
    if drug_ctx and (not intent or intent in ('product_search', 'price_only', 'faq_order_process', 'shipping_time', 'crypto_info')):
        return drug_ctx
    k = retrieve_knowledge(message, intent or "")
    if not k:
        return drug_ctx or None
    if drug_ctx:
        return f"{drug_ctx}\n\n{k}"[:650]
    return k[:650]

def compose_knowledge_for_prompt(query: str, intent: str = "", base: str = "") -> str:
    """Improved composer (web3test-inspired) for real grounded answers + natural relevant inserts."""
    parts = []
    if base:
        parts.append(base)
    composed = compose_knowledge_response(query, 'fa', intent)
    if composed:
        parts.append(composed)
    else:
        k = retrieve_knowledge(query, intent)
        if k:
            parts.append("دانش مرتبط:\n" + k)
    drug_ctx = get_drug_context_snippet(query)
    if drug_ctx and drug_ctx not in (parts[-1] if parts else ""):
        parts.append(drug_ctx)
    return "\n".join([p for p in parts if p])[:750]


# ═══════════════════════════════════════════════════════════════════════════════
# MORE COMPLETE: ModelDirector + ContentIntelligence for "very intelligent" interactions
# "properly directed to and processed by the model"
# "sometimes insert relevant content"
# "give real answers"
# Attract audience with natural professional value
# ═══════════════════════════════════════════════════════════════════════════════

class ModelDirector:
    """Routes every request intelligently to best prompt variant, think mode, options.
    Ensures requests are properly directed and processed by Qwen3 for max intelligence.
    """
    def __init__(self):
        self.variants = {
            'attract_value': "یه نکته کوتاه از تجربه خودت بگو. اگه موضوع دارو یا کریپتو نبود همون موضوع گروه رو ادامه بده. فروش نکن.",
            'real_answer': "اگه دانش مرتبط داده شده حتماً ازش استفاده کن. محاوره‌ای و مستقیم، مثل چت تلگرام. با فکر کنم شروع نکن.",
            'funnel': "بعد از چند پیام مفید، خیلی نرم بگو جزئیاتش رو پی‌وی راحت‌تر میشه گفت. فشار نده.",
            'general_engage': "مثل یه عضو باهوش گروه حرف بزن. موضوع هر چیه نظر واقعی بده. فروش نکن. ۲-۴ جمله.",
        }

    def decide_variant(self, intent: str, user_text: str, high_value: bool = False) -> dict:
        """Bot-compatible routing wrapper — called by bot.py as _director.decide_variant(intent, text, hv)."""
        has_knowledge = bool(retrieve_knowledge(user_text, intent))
        plan = {'thinking': f"intent={intent}", 'strategy': 'llm_reasoning' if high_value else 'fast'}
        return self.direct(intent, plan, user_text, has_knowledge, high_value)

    def direct(self, intent: str, plan: dict, user_text: str, has_knowledge: bool, has_history: bool) -> dict:
        """Return directed config optimized for qwen3:1.7b on CPU."""
        use_think = False  # think mode too slow on 1.7b CPU — disabled by default
        factual = intent in ('faq_order_process', 'shipping_time', 'tracking', 'payment_crypto_help', 'crypto_info')
        temp = 0.38 if (factual and has_knowledge) else 0.52

        variant = 'general_engage'
        if factual and has_knowledge:
            variant = 'real_answer'
        elif intent in ('greeting', 'thanks', 'presence_check', 'goodbye', 'unknown'):
            variant = 'general_engage'
        elif has_knowledge and random.random() < 0.20:
            variant = 'attract_value'
        if has_history and has_knowledge and random.random() < 0.28:
            variant = 'funnel'

        addon = self.variants.get(variant, self.variants['general_engage'])
        max_t = 300 if has_knowledge or has_history else 260

        completeness = " ۲-۴ جمله کامل محاوره‌ای. مثل آدم واقعی."
        addon = addon + completeness

        return {
            'system_addon': addon,
            'use_think': use_think,
            'temperature': temp,
            'max_tokens': max_t,
            'variant': variant,
            'notes': f"Directed for {variant} | grounded={has_knowledge}"
        }


class ContentIntelligence:
    """For 'sometimes insert relevant content' naturally to attract audience.
    Produces grounded value snippets (real answers) from knowledge.
    Adapted from web3test knowledge_composer patterns.
    """
    def __init__(self, snippets: list, aliases: dict):
        self.snippets = snippets
        self.aliases = aliases

    def should_insert(self, intent: str, text: str, history_len: int) -> float:
        """Probabilistic: higher for questions, after some history, on value topics."""
        score = 0.15
        if '?' in text or '؟' in text:
            score += 0.25
        if any(k in text.lower() for k in ['چطور', 'ارسال', 'پرداخت', 'تجربه', 'کجا', 'قیمت']):
            score += 0.2
        if history_len > 1:
            score += 0.15
        if intent in ('shipping_time', 'crypto_info', 'payment_crypto_help', 'product_search'):
            score += 0.15
        return min(score, 0.65)  # "sometimes" not always

    def get_relevant_snippet(self, query: str, intent: str) -> str:
        """Return 1 short real helpful snippet (grounded, peer voice) if relevant."""
        q = (query or "").lower() + " " + (intent or "").lower()
        candidates = []
        for key, txt in self.snippets:
            if any(tok in q for tok in key.split('_') + [key]):
                candidates.append(txt)
        for fam, als in self.aliases.items():
            if any(a.lower() in q for a in als):
                for key, txt in self.snippets:
                    if fam in key.lower():
                        candidates.append(txt)
        drug = get_drug_context_snippet(query)
        if drug:
            candidates.append(drug)
        if not candidates:
            return ""
        choice = random.choice(candidates)[:160]
        # make it sound like real shared experience
        return choice.replace("فارماوب داره", "من از فارماوب گرفتم").replace("ارسال سریع", "ارسالشون سریع بود برام").replace("فارماوب", "اونجا")

# Global instances for use in bot
director = ModelDirector()
content_intel = ContentIntelligence(KNOWLEDGE_SNIPPETS, DRUG_ALIASES)

def compose_knowledge(query: str, intent: str = "") -> str:
    return compose_knowledge_for_prompt(query, intent)

# Re-export for bot.py wiring + new professional composer
def get_drug_context(query: str) -> str:
    return get_drug_context_snippet(query)

def compose_knowledge(query: str, intent: str = "") -> str:
    return compose_knowledge_for_prompt(query, intent) or retrieve_knowledge(query, intent)

# expose new
__all__ = ['classify_intent', 'retrieve_knowledge', 'compose_knowledge_for_prompt', 'compose_knowledge_response', 'plan_response', 'is_repeated_response', 'get_drug_context_snippet', 'match_drug_family', 'get_family_info', 'decide_engagement', 'ModelDirector', 'ContentIntelligence', 'is_weak_llm_output', 'get_few_shots_for_prompt', 'repair_llm_output', 'salvage_llm_output', 'pick_best_or_fallback', 'is_domain_topic']


# ── Lightweight Conversation Strategist (Phase 2) ─────────────────────────────
# Weak output guard (inspired by web3test model_guard)
WEAK_LLM_PATTERNS = [
    r'متأسفم.*نمی\s*توانم', r'i\s*cannot\s*help', r'as\s*an\s*ai', r'language\s*model',
    r'نمی\s*دانم', r"i\s*don't\s*know", r'فقط\s*ترون', r'only\s*tron',
    r'مدفارماوب', r'\bimed\b', r'\bsara\b', r'فقط بگو', r'لیست', r'^\s*۱\.',
    r'قطعا|حتما|۱۰۰٪|بدون شک|دقیقا همین',
    r'^(بله|نه|آره|خیر)\s*$', r'^\s*[\.؟!]{1,3}\s*$',
    r'برای سفارش|به سایت|لطفاً به', r'پیام بده به', r'ادمین',
    r'چیزی است که', r'به منظور', r'تخمیر', r'هواپلتر', r'حماست',
    r'به دلیل وجود خودمونی', r'^فکر کنم[،,\.]?\s*تو رباتی',
]

def is_weak_llm_output(text: str, language: str = 'fa') -> bool:
    if not text or len(text.strip()) < 18:
        return True
    t = text.lower()
    for pat in WEAK_LLM_PATTERNS:
        if re.search(pat, t, re.I):
            return True
    lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
    if len(lines) >= 2 and len(set(lines)) == 1:
        return True
    # Too short or single sentence without depth for group chat
    if len(text) < 45 and text.count('.') + text.count('؟') + text.count('!') < 1:
        return True
    if re.search(r'قطعا|حتما|۱۰۰٪|بدون شک|دقیقا همین', t):
        return True
    if t.startswith('فکر کنم') and any(x in t for x in ('چیزی است', 'به منظور', 'به دلیل وجود')):
        return True
    if (text.count('؟') + text.count('?')) >= 3:
        return True
    if any(x in t for x in ('نبادن', 'به من بودن', 'چه جا؟', 'شما نبودید', 'دارو بخورد', 'چت تلگرام', 'بسیار مهم است')):
        return True
    return False


def repair_llm_output(text: str, language: str = 'fa') -> str:
    """Strong repair for small-model hallucinations (port + extension from web3test model_guard)."""
    if not text:
        return text
    # Brand fixes (from reference)
    text = text.replace('مدفارماوب', 'فارماوب')
    text = re.sub(r'medpharmaweb|imed|sara', 'فارماوب', text, flags=re.I)
    # "only tron" fix — we accept 8 cryptos
    if re.search(r'فقط\s*ترون|only\s*tron', text, re.I):
        if language == 'fa':
            text = re.sub(r'فقط\s*ترون[^.\n]*', '۸ ارز دیجیتال قبول می‌کنیم (BTC، ETH، USDT روی TRC20، TRX، BNB، TON، SOL، DOGE)', text, flags=re.I)
        else:
            text = re.sub(r'only\s*tron[^.\n]*', 'We accept 8 cryptos (BTC, ETH, USDT on TRC20, etc.)', text, flags=re.I)
    # Additional common small model fixes
    text = re.sub(r'تراکته', '', text, flags=re.I)
    text = re.sub(r'فروشگاه آنلاین \(Online Store\)', 'فروشگاه آنلاین', text, flags=re.I)
    # Time expressed as physical units (seen live: "۲ تا ۳ سانتیمتر طول میکشد")
    text = re.sub(
        r'\d+\s*تا\s*\d+\s*(سانتیمتر|سانتی‌متر|متر|کیلومتر|گرم)',
        'چند ساعت',
        text,
        flags=re.I,
    )
    text = re.sub(r'(سانتیمتر|سانتی‌متر|کیلومتر)', '', text, flags=re.I)
    # Random clock hallucinations ("وقت ۲ صبح")
    text = re.sub(r'وقت\s*\d+\s*(صبح|شب|ظهر)', '', text)
    text = re.sub(r'ساعت\s*\d+(\s*تا\s*\d+)?\s*(صبح|شب|ظهر|روز)', '', text)
    text = re.sub(r'کلکشن', '', text, flags=re.I)
    text = re.sub(r'تخمیر|هواپلتر|حماست', '', text)
    text = re.sub(r'^فکر کنم[،,\.]?\s*', '', text)
    text = re.sub(
        r'^(به این موضوع اشاره می‌کنم|در پاسخ باید گفت|لازم به ذکر است)[:：]?\s*',
        '',
        text,
    )
    text = re.sub(
        r'[^.؟!]*('
        r'چت تلگرام|فقط نظر بده|با فکر کنم شروع نکن|از اطلاعات بالا|'
        r'جواب طبیعی|تعریف لغت نده|فروش نکن|میتونم به جواب سوال'
        r')[^.؟!]*[.؟!]?\s*',
        '',
        text,
    )
    text = re.sub(r'\.\s*است\.', '.', text)
    # Don't spam the same personal claim
    if text.count('خودم گرفتم') > 1:
        first = text.find('خودم گرفتم')
        text = text[: first + len('خودم گرفتم')] + text[first + len('خودم گرفتم'):].replace('خودم گرفتم', '')
    # Strip leftover double spaces
    text = re.sub(r'  +', ' ', text)
    # Remove repetitive identical lines
    lines = [ln.strip() for ln in text.split('\n') if ln.strip()]
    seen = set()
    clean = []
    for ln in lines:
        norm = ln[:70].lower()
        if norm not in seen:
            seen.add(norm)
            clean.append(ln)
    text = '\n'.join(clean)
    if len(text) > 650:
        text = text[:650].rsplit(' ', 1)[0] + '…'
    return text.strip()


def salvage_llm_output(text: str, retrieved: str = "") -> str:
    """Keep the useful sentences from a mixed small-model reply; drop instruction echo."""
    text = repair_llm_output(text or "")
    if not text:
        return ""
    parts = re.split(r'(?<=[.؟!])\s+', text)
    keep = []
    for part in parts:
        p = part.strip()
        if len(p) < 12:
            continue
        if re.search(
            r'چت تلگرام|فقط نظر بده|فکر کنم شروع|از اطلاعات بالا|جواب سوال من',
            p,
        ):
            continue
        if p.count('؟') + p.count('?') >= 2:
            continue
        keep.append(p)
    if not keep:
        return ""
    out = ' '.join(keep).strip()
    if retrieved:
        keys = [k for k in (
            'ریتالین', 'کنسرتا', 'کونسرتا', 'اوزمپیک', 'مودافینیل', 'trc20', 'usdt', 'ساعت',
        ) if k in retrieved.lower() or k in retrieved]
        if keys and not any(k in out.lower() for k in keys):
            keyed = [s for s in keep if any(k in s.lower() for k in keys)]
            if keyed:
                out = ' '.join(keyed).strip()
    return repair_llm_output(out)


def pick_best_or_fallback(llm_text: str, local_text: str, intent: str = "") -> str:
    """Guard + prefer grounded local/composed when LLM weak (web3test pick_faq_over_llm pattern)."""
    if llm_text and not is_weak_llm_output(llm_text):
        return repair_llm_output(llm_text)
    if local_text and not is_weak_llm_output(local_text):
        return repair_llm_output(local_text)
    # last resort — short safe line
    return "جزئیات بیشتری بده تا دقیق‌تر راهنمایی کنم."

# Few-shot bank — general group chat first, domain examples only when relevant
FEW_SHOT_BANK = [
    ("VPN چی استفاده میکنید؟", "من وایرفگارد رو با کلش قاطی میکنم. وایرفگارد پایدارتره، کلش برای روزمره راحت‌تره. تو چی داری؟"),
    ("هوای تهران امروز چطوره؟", "مثل همیشه شلوغ و یه کم گرفته. اگه میتونی عصر برو بیرون بهتره. تو کدوم منطقه‌ای؟"),
    ("فیلم خوبی دیدی اخیرا؟", "یه سریال شروع کردم ولی نصفه رها کردم. تو چی دیدی که ارزش وقت گذاشتن داشته باشه؟"),
    ("کسی استانبول زندگی کرده؟", "چند بار رفتم. محله خیلی فرق میکنه؛ بشیکتاش گرون‌تره، کادیکوی آروم‌تر. هدفت کاره یا زندگی؟"),
    ("دلار امروز چطوره به نظرتون؟", "نوسان زیاده، کسی دقیق نمیدونه. من کوتاه‌مدت حدس نمیزنم. تو دنبال خریدی یا فقط خبر؟"),
    ("خوابم بهم ریخته، کسی راهی داره؟", "کافئین عصر رو قطع کن، نور گوشی قبل خواب کم کن. خودم با همین دو تا بهتر شدم. شیفت کاری داری؟"),
    ("باشگاه میرید این روزا؟", "هفته‌ای سه بار میرم ولی نظمم ضعیفه. تو قدرته یا هوازی؟"),
    ("اینترنت امروز افتضاحه", "آره منم کند شده. فیلترشکن عوض کردم یه کم بهتر شد. مال تو کدوم اپراتوره؟"),
    ("کسی شمال رفته اخیرا؟", "جاده شلوغه آخر هفته. وسط هفته بهتره. کجا می‌خوای بری؟"),
    ("ترافیک امروز چطوره؟", "مثل همیشه قفل. اگه اجباری نیست یه ساعت دیرتر بزن. تو کدوم مسیر هستی؟"),
    ("آیفون بهتره یا سامسونگ؟", "بستگه به کارتت. من اندرویدم چون آزادی بیشتری داره، ولی دوربین آیفون ثابته. تو چیکار میکنی باهاش؟"),
    ("شب فوتبال میبینی؟", "گهگاهی. اگه بازی حساس باشه میشینم. امشب چیزی هست؟"),
    ("غذا چی درست کردید؟", "یه چیزی ساده. حوصله آشپزی طولانی نداشتم. تو چی خوردی؟"),
    ("فیلترشکن چی خوبه الان؟", "وایرفگارد پایدارتره برام. کلش هم برای روزمره بد نیست. مال تو چیه؟"),
    ("تو رباتی؟", "نه داداش، اینجام. بگو چی شده."),
    ("سلام بچه‌ها", "سلام. چه خبر، امروز گروه آرومه."),
    ("نظرتون چیه کار از خونه بهتره؟", "برای تمرکز خوبه ولی آدم تنبل میشه. من ترکیبی بهتر نتیجه گرفتم. تو چیکار میکنی؟"),
    ("ارسال به استانبول چقدر طول میکشه؟", "معمولاً ۴-۸ ساعت بعد تأیید پرداخت. بسته محرمانه میاد. من خودم چند بار گرفتم، سریع بود."),
    ("چطور با تتر پرداخت کنم؟", "TRC20 رو انتخاب کن، کارمزدش پایینه. آدرس رو دقیق کپی کن. بعد از واریز ۵-۱۵ دقیقه تأیید میشه."),
    ("سلام، ریتالین موجوده؟", "آره ریتالین و کنسرتا موجوده، اورجینال اروپایی. TRC20 راحت‌تره برا پرداخت. تو کدوم شهر هستی؟"),
    ("اوزمپیک دارید؟", "آره موجوده. اورجینال نووو نوردیسک. برای کاهش وزن و دیابت نوع ۲. قیمتشو بخوای پیام بده."),
    ("کریپتو چطور؟ کدوم بهتره؟", "USDT روی TRC20 بهترینه — کارمزد پایین، سریع تأیید میشه. BNB هم خوبه. اتریوم گرونه."),
    ("مودافینیل چیه؟", "داروی بیداری و تمرکزه. خودم استفاده کردم، فرق محسوسی داشت. پیدا کردنش یکم سخته ولی میشه."),
    ("بسته گمرک نمیشه؟", "بسته محرمانه ارسال میشه. من تو استانبول و دبی گرفتم، مشکلی نداشتم اصلاً."),
    ("مهاجرت ترکیه الان چطوره؟", "هنوز گزینه‌ست ولی هزینه‌ها رفته بالا. استانبول گرونه. آنکارا یا ازمیر ارزون‌تره. چه هدفی داری؟"),
    ("نوبیتکس یا والکس؟", "هر دو خوبن. نوبیتکس حجم بیشتری داره. والکس رابط کاربریش راحت‌تره. برای چه مقداری میخوای؟"),
    ("ریتالین ایرانی با اروپایی فرق داره؟", "بله، داره. اروپایی کیفیت ثابت‌تری داره، هولوگرام و کد batch داره قابل چک. ایرانیش نوسان کیفیت داره."),
    ("ADHD دارم چی بگیرم؟", "ریتالین یا کونسرتا معمولاً اول تجویز میشه. دوز رو باید دکتر بده. برای تهیه هم میشه راهنماییت کرد."),
    ("ارسال به دبی چقدر طول میکشه؟", "دبی هم سریعه معمولاً، ۶-۱۲ ساعت بعد تأیید. بسته کاملاً محرمانه میاد."),
    ("چطور میتونم TRC20 بخرم؟", "از نوبیتکس یا والکس USDT بخر، بعد روی آدرس TRC20 که بهت داده میشه انتقال بده. ساده‌ست."),
    ("برای تمرکز چی خوبه؟", "مودافینیل یا ریتالین بسته به شرایط. من مودافینیل رو برای کار طولانی تست کردم خوبه."),
    ("پرداخت با کریپتو امن هست؟", "بله اگر شبکه درست انتخاب کنی. TRC20 کم ریسک‌تره و سریع. خودم چند بار زدم مشکلی نبود."),
]

def get_few_shots_for_prompt(query: str, k: int = 3) -> str:
    q = (query or "").lower()
    domain = is_domain_topic(query)
    scored = []
    for ex_q, ex_a in FEW_SHOT_BANK:
        if not domain and is_domain_topic(ex_q):
            continue
        sc = sum(1 for w in q.split() if w and len(w) > 1 and w in ex_q.lower())
        if sc:
            scored.append((sc, f"کاربر: «{ex_q}»\nپاسخ طبیعی: «{ex_a}»"))
    scored.sort(reverse=True)
    if scored:
        return "\n".join([s[1] for s in scored[:k]])
    if not domain:
        generals = [(eq, ea) for eq, ea in FEW_SHOT_BANK if not is_domain_topic(eq)]
        if generals:
            ex_q, ex_a = random.choice(generals)
            return f"کاربر: «{ex_q}»\nپاسخ طبیعی: «{ex_a}»"
    return ""

_LAST_LOCAL_REPLIES = []


def generate_natural_reply_local(user_text: str, intent: str = "", retrieved: str = "", style: str = "general_engage") -> str:
    """High-quality local reply when Qwen is slow/unavailable. Never raw knowledge paste."""

    # Enrich from retrieved knowledge into natural sentences
    def _naturalize_knowledge(raw: str, intent: str) -> Optional[str]:
        if not raw or len(raw) < 15:
            return None
        lines = [ln.strip() for ln in raw.split('\n') if ln.strip()]
        first = lines[0][:200]
        # Skip encyclopedia "Ingredient: indication" lines — prefer conversational snippets
        if ':' in first[:48] and len(lines) > 1:
            first = lines[1][:200]
        # Convert factual snippets to peer voice — vary endings so it doesn't loop
        peer_prefixes = ["راستش ", "معمولاً ", "به نظرم ", ""]
        endings = {
            'shipping_time': [" کدوم شهر مدنظرت هست؟", " بسته‌بندی محرمانه‌ست.", ""],
            'crypto_info': [" شبکه رو دقیق چک کن.", " TRC20 معمولاً کم‌دردسرتره.", ""],
            'payment_crypto_help': [" آدرس رو دقیق کپی کن.", " بعد واریز چند دقیقه طول میکشه.", ""],
            'product_search': [" بگو دقیق کدومش.", " اورجینالش فرق داره با ایرانیش.", ""],
            'stock_check': [" بگو برای کجاست.", " موجودیش بستگه به مدل.", ""],
        }
        prefix = random.choice(peer_prefixes)
        ending = random.choice(endings.get(intent, ["", " بیشتر بگو."]))
        return (prefix + first + ending).strip()

    # Try knowledge-grounded natural reply first
    if retrieved:
        natural = _naturalize_knowledge(retrieved, intent)
        if natural and len(natural) >= 30:
            _LAST_LOCAL_REPLIES.append(natural)
            return natural
    # 1. Instant pools for social intents
    _SOCIAL = {
        'greeting': [
            "سلام، خوبی؟ چه خبر ازت؟",
            "سلام. چه خبر، امروز چطوره؟",
            "درود، چطوری؟ بگو ببینم.",
        ],
        'thanks': [
            "خواهش. هر چیزی بود بگو.",
            "خواهش میکنم.",
            "قابلی نداره.",
        ],
        'goodbye': [
            "فعلاً، مواظب خودت باش.",
            "خداحافظ.",
            "باشه بعداً حرف میزنیم.",
        ],
        'presence_check': [
            "آره هستم. بگو.",
            "اینجام.",
        ],
        'bot_question': [
            "نه داداش اینجام. بگو چی شده، گوش میدم.",
            "حرف بزن، اینجام. چی پیش اومده؟",
            "آره اینجام. بگو ببینم چی شده.",
        ],
    }
    if intent in _SOCIAL:
        return random.choice(_SOCIAL[intent])

    # 2. Direct FEW_SHOT_BANK match — these are already natural replies
    q = (user_text or "").lower()
    matches = []
    for ex_q, ex_a in FEW_SHOT_BANK:
        score = sum(1 for w in q.split() if len(w) > 2 and w in ex_q.lower())
        if score >= 1:
            matches.append((score, ex_a))
    if matches:
        matches.sort(reverse=True)
        top_score, top_answer = matches[0]
        close = [a for s, a in matches if s >= max(1, top_score - 1)]
        unused = [a for a in close if a not in _LAST_LOCAL_REPLIES[-6:]]
        pick = random.choice(unused or close)
        if top_score >= 2 and (is_domain_topic(user_text) or not is_domain_topic(pick)):
            _LAST_LOCAL_REPLIES.append(pick)
            return pick
        if top_score >= 1 and intent not in ('unknown', 'clarification', 'help_request', 'complaint') and is_domain_topic(user_text):
            _LAST_LOCAL_REPLIES.append(pick)
            return pick

    # 3. Intent-specific fallback pools (grounded, varied, natural)
    _INTENT_POOL: Dict[str, List[str]] = {
        'product_search': [
            "ریتالین، اوزمپیک، مودافینیل — همه موجودن. کدوم میخوای؟",
            "موجوده. بگو کدومشو میخوای و برای کجا.",
            "آره داریم. بیشتر توضیح بده.",
            "بگو کدوم محصولو میخوای، کمکت میکنم.",
        ],
        'stock_check': [
            "آره موجوده، اورجینال. پیام بده.",
            "موجوده. بگو برای کجاست.",
            "داریم. بپرس.",
        ],
        'shipping_time': [
            "معمولاً ۴-۸ ساعت بعد تأیید پرداخت. بسته محرمانه میاد.",
            "سریعه. تهران زیر ۴ ساعت، استانبول ۶-۸ ساعت.",
            "بسته محرمانه — معمولاً همون روز میرسه.",
        ],
        'crypto_info': [
            "USDT روی TRC20 بهترینه — کم‌کارمزد و سریع تأیید میشه.",
            "از نوبیتکس یا والکس USDT بخر روی TRC20 بفرست.",
            "TRC20 برای USDT بهترینه. هر دو صرافی خوبن.",
        ],
        'payment_crypto_help': [
            "TRC20 رو انتخاب کن — ارزونه. آدرس رو دقیق کپی کن.",
            "USDT روی TRC20. بعد واریز ۵-۱۵ دقیقه تأیید میشه.",
            "از نوبیتکس USDT بخر، شبکه TRC20 رو انتخاب کن.",
        ],
        'faq_order_process': [
            "محصول رو انتخاب کن، USDT پرداخت کن — همین.",
            "سفارش از سایت ساده‌ست — انتخاب، USDT، تحویل.",
        ],
        'trust_question': [
            "هولوگرام داره، batch number قابل چکه. اورجینال اروپایی.",
            "اصل اروپایی با هولوگرام. اگه خواستی جزئیات بیشتر بگم.",
        ],
        'faq_prescription': [
            "برای بعضی داروها نسخه لازمه ولی راه‌حل داره. بگو دقیق چی میخوای.",
            "بستگه داره. بگو چی میخوای راهنماییت کنم.",
        ],
        'unknown': [
            "جالبه. بیشتر بگو ببینم از کجا شروع شده.",
            "راستش بستگه به شرایط. تو خودت چی فکر میکنی؟",
            "منم یه کم درگیر این موضوع بودم. جزئیاتش چیه؟",
            "اوکی فهمیدم. نظرت خودت چیه؟",
            "آره این موضوع آشناست. یه کم بیشتر بگو.",
        ],
    }
    pool = _INTENT_POOL.get(intent, _INTENT_POOL['unknown'])
    choice = random.choice(pool)
    recent = _LAST_LOCAL_REPLIES[-6:]
    if choice in recent:
        unused = [p for p in pool if p not in recent]
        if unused:
            choice = random.choice(unused)
    _LAST_LOCAL_REPLIES.append(choice)
    if len(_LAST_LOCAL_REPLIES) > 24:
        del _LAST_LOCAL_REPLIES[:12]
    return choice

def decide_engagement(user_text: str, recent_ctx: str = "", group_notes: str = "") -> dict:
    """Engage on any real group conversation, not only drugs/crypto."""
    txt = (user_text or "").lower().strip()
    score = 0.0
    style = "general_engage"

    if len(txt) < 8:
        return {'should_engage': False, 'score': 0.0, 'style': style, 'system_addon': ''}

    tiny = {'ok', 'اوکی', 'باشه', 'آره', 'نه', '👍', '😂', '😅', 'خب'}
    if txt in tiny:
        return {'should_engage': False, 'score': 0.0, 'style': style, 'system_addon': ''}

    if any(q in txt for q in ['؟', '?', 'چطور', 'چگونه', 'چقدر', 'کجا', 'کی', 'چرا', 'نظرت']):
        score += 3.0
    if any(k in txt for k in ['نمیدونم', 'مشکل', 'تجربه', 'پیشنهاد', 'فکر میکنی', 'کسی', 'به نظرت']):
        score += 2.0
    if is_domain_topic(txt):
        score += 2.2
        style = "real_answer"
    if any(k in txt for k in ['فیلم', 'سریال', 'هوا', 'کار', 'خواب', 'vpn', 'دلار', 'فوتبال',
                               'سفر', 'ماشین', 'دانشگاه', 'غذا', 'اینترنت', 'باشگاه', 'شمال']):
        score += 1.8
    if len(txt) >= 24:
        score += 1.2
    if len(txt) >= 50:
        score += 0.8
    if 'مهاجرت' in txt or 'ترکیه' in txt or 'دبی' in txt:
        score += 1.0

    should = score >= 1.5 or len(txt) >= 22
    addon = ""
    if should:
        fs = get_few_shots_for_prompt(user_text, k=1)
        addon = f"Style like a smart group member ({style}). {fs}"
    return {'should_engage': should, 'score': score, 'style': style, 'system_addon': addon}
