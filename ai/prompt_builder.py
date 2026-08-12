"""
Optimized prompt builder for Qwen3:1.7b on CPU.
Shorter prompts = faster inference + better quality on small models.
"""

from typing import List, Optional

# Compact system prompt — tuned for 1.7b (avoid overwhelming context)
GROUP_SYSTEM_PROMPT_V2 = (
    "تو یه ایرانی ۳۲ ساله‌ای که تو گروه تلگرام حرف میزنی. تجربه خرید دارو (ریتالین، اوزمپیک، مودافینیل) "
    "و پرداخت با USDT روی TRC20 و ارسال به تهران/استانبول/دبی داری.\n"
    "قوانین:\n"
    "• ۲ تا ۴ جمله محاوره‌ای فارسی. هر جمله کامل.\n"
    "• لحن: «آره»، «خودم گرفتم»، «معمولاً»، «بستگه داره»، «تو چی؟»\n"
    "• هرگز: لیست، شماره‌گذاری، «ربات»، «هوش مصنوعی»، «به سایت مراجعه کنید»\n"
    "• اگه نمیدونی: «دقیق نمیدونم ولی...» + سوال کوتاه\n"
    "• آخرش یه سوال سبک بپرس تا گفتگو ادامه پیدا کنه"
)


def build_qwen_messages(
    user_text: str,
    *,
    retrieved: str = "",
    recent_ctx: Optional[List[str]] = None,
    exchange_lines: Optional[List[str]] = None,
    notes: str = "",
    mem_ctx: str = "",
    director_addon: str = "",
    few_shots: str = "",
) -> List[dict]:
    """Build minimal high-signal messages for Qwen3:1.7b."""
    sys_parts = [GROUP_SYSTEM_PROMPT_V2]
    if director_addon:
        sys_parts.append(director_addon[:200])

    ctx_parts = []
    if few_shots:
        ctx_parts.append("نمونه:\n" + few_shots[:350])
    if retrieved:
        ctx_parts.append("اطلاعات:\n" + retrieved[:300])
    if exchange_lines:
        ctx_parts.append("مکالمه:\n" + "\n".join(exchange_lines[-3:]))
    if notes:
        ctx_parts.append(notes[:120])
    if mem_ctx:
        ctx_parts.append(mem_ctx[:120])
    if recent_ctx:
        ctx_text = "\n".join(str(x)[:80] for x in recent_ctx[-4:])
        if ctx_text:
            ctx_parts.append("گروه:\n" + ctx_text[:250])

    if ctx_parts:
        sys_parts.append("\n".join(ctx_parts)[:600])

    user_msg = (
        f"پیام: {user_text}\n"
        "جواب طبیعی ۲-۴ جمله‌ای بنویس. فقط از اطلاعات داده‌شده استفاده کن. "
        "زمان ارسال را به ساعت/روز بگو (نه سانتیمتر یا متر). مثل دوست حرف بزن. سوال کوتاه آخر."
    )

    return [
        {"role": "system", "content": "\n\n".join(sys_parts)},
        {"role": "user", "content": user_msg},
    ]
