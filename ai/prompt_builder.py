"""
Prompt builder for Qwen3:1.7b — general group member, not a product bot.
"""

from typing import List, Optional

GROUP_SYSTEM_PROMPT_V2 = (
    "عضو معمولی گروه تلگرام هستی، ایرانی حدود ۳۰ سال، لحن خودمونی. "
    "فروشنده نیستی. ۲-۴ جمله محاوره‌ای. مقاله و لیست ننویس. "
    "اگه موضوع دارو یا کریپتو بود از تجربه بگو."
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
    """Build messages. Retrieved knowledge is optional — general chat uses the model."""
    sys_parts = [GROUP_SYSTEM_PROMPT_V2]
    if director_addon:
        sys_parts.append(director_addon[:180])

    ctx_parts = []
    if few_shots:
        ctx_parts.append("نمونه لحن:\n" + few_shots[:320])
    if retrieved:
        ctx_parts.append("اگه به موضوع مربوطه از این استفاده کن:\n" + retrieved[:280])
    if exchange_lines:
        ctx_parts.append("مکالمه با همین نفر:\n" + "\n".join(exchange_lines[-3:]))
    if notes:
        ctx_parts.append(notes[:120])
    if mem_ctx:
        ctx_parts.append(mem_ctx[:120])
    if recent_ctx:
        ctx_text = "\n".join(str(x)[:90] for x in recent_ctx[-5:])
        if ctx_text:
            ctx_parts.append("فضای گروه:\n" + ctx_text[:280])

    if ctx_parts:
        sys_parts.append("\n".join(ctx_parts)[:700])

    if retrieved:
        user_msg = (
            f"{user_text}\n"
            "جواب کوتاه محاوره‌ای بده و از اطلاعات بالا استفاده کن."
        )
    else:
        user_msg = (
            f"{user_text}\n"
            "جواب کوتاه محاوره‌ای مثل چت گروه. تعریف لغت نده."
        )

    return [
        {"role": "system", "content": "\n\n".join(sys_parts)},
        {"role": "user", "content": user_msg},
    ]
