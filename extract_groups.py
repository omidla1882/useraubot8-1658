"""
🔗 استخراج لینک گروه‌های تلگرامی
این اسکریپت لینک تمام گروه‌ها و سوپرگروه‌هایی که کاربر عضو آنهاست را استخراج می‌کند
خروجی: لینک‌ها خط به خط (آماده برای کپی به AUTO_JOIN_LINKS)
"""

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
import asyncio

# ═══════════════════════════════════════════════════════════
# تنظیمات API (همان اطلاعات ربات اصلی)
# ═══════════════════════════════════════════════════════════
api_id = 28652875
api_hash = '97469594916750008690bb4a21e2ebab'
session_name = 'my_session'

client = TelegramClient(session_name, api_id, api_hash)


async def extract_group_links():
    """استخراج لینک گروه‌ها و سوپرگروه‌ها"""
    await client.start()
    
    group_links = []
    
    # دریافت تمام دیالوگ‌ها
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        
        # فقط گروه‌ها و سوپرگروه‌ها (نه کانال‌ها)
        if isinstance(entity, Channel):
            # megagroup = سوپرگروه، broadcast = کانال
            # فقط سوپرگروه‌ها را می‌خواهیم (نه کانال)
            if entity.megagroup and not entity.broadcast:
                username = entity.username
                if username:
                    # لینک عمومی
                    group_links.append(f"https://t.me/{username}")
                else:
                    # گروه خصوصی - نمایش ID
                    group_links.append(f"# Private Group: {entity.title} (ID: {entity.id})")
        
        elif isinstance(entity, Chat):
            # گروه معمولی (قدیمی)
            group_links.append(f"# Basic Group: {entity.title} (ID: {entity.id})")
    
    # نمایش نتایج
    print("=" * 60)
    print(f"🔗 تعداد گروه‌ها: {len(group_links)}")
    print("=" * 60)
    print()
    
    for link in group_links:
        print(link)
    
    print()
    print("=" * 60)
    print("✅ کپی کنید و به AUTO_JOIN_LINKS اضافه کنید")
    print("=" * 60)
    
    # ذخیره در فایل
    with open('group_links.txt', 'w', encoding='utf-8') as f:
        for link in group_links:
            f.write(link + '\n')
    
    print(f"📁 فایل ذخیره شد: group_links.txt")
    
    await client.disconnect()


if __name__ == '__main__':
    asyncio.run(extract_group_links())
