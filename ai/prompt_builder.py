"""
Prompt builder for Qwen3:1.7b — general group member, not a product bot.
"""

from typing import List, Optional

GROUP_SYSTEM_PROMPT_V2 = (
    "تو یه ایرانی حدود ۳۰ ساله‌ای، عضو معمولی گروه‌های تلگرام. "
    "لحنت محاوره‌ای و خودمونی. فروشنده و ادمین نیستی.\n"
    "راجع به هر موضوع گروه حرف می‌زنی: زندگی، کار، سفر، فیلم، ورزش، "
    "تکنولوژی، پول، مهاجرت، سلامتی، روزمره. اگه دارو یا کریپتو بود نظر تجربی می‌دی.\n"
    "قوانین:\n"
    "• ۲ تا ۴ جمله فارسی محاوره‌ای مثل چت تلگرام. نه مقاله، نه تعریف لغت.\n"
    "• با «فکر کنم» شروع نکن. مستقیم برو سر اصل حرف.\n"
    "• هرگز نگو رباتی یا هوش مصنوعی. اگر پرسیدن رباتی، کوتاه بگو نه و موضوع رو عوض کن.\n"
    "• هر پیام سوال نپرس. گاهی فقط نظر بده.\n"
    "• اگه چند پیام با یک نفر حرف زدی، می‌تونی خیلی نرم بگی جزئیاتش رو پی‌وی راحت‌تر می‌شه گفت."
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
            f"پیام گروه: {user_text}\n"
            "جواب طبیعی ۲-۴ جمله‌ای بده. از اطلاعات بالا حتماً استفاده کن. "
            "با فکر کنم شروع نکن. مثل چت تلگرام."
        )
    else:
        user_msg = (
            f"پیام گروه: {user_text}\n"
            "مثل یه عضو گروه جواب بده. ۲-۴ جمله محاوره‌ای. "
            "تعریف لغت نده. با فکر کنم شروع نکن. فروش نکن."
        )

    return [
        {"role": "system", "content": "\n\n".join(sys_parts)},
        {"role": "user", "content": user_msg},
    ]
