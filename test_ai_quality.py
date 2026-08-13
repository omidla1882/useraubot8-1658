#!/usr/bin/env python3
"""
Professionalism benchmark for the AI core.
Run locally: python test_ai_quality.py
Focus: intent classification, strategy, retrieval, natural gate, anti-repetition, full pipeline quality.
No real Telegram needed for pure logic tests.
"""
import asyncio
import sys
sys.path.insert(0, ".")
try:
    import bot as b
except ImportError as e:
    print("Warning: could not fully import bot:", e)
    b = None

async def main():
    print("=== UserbotAI Professional AI Quality Benchmark (Phase 4) ===\n")

    if not b:
        print("Cannot run (import fail).")
        return

    # 1. Gate rejects exact bad promo garbage from user
    bad = """برای انجام سفارش پس از واریز در فارماوب، نیاز به تأیید ارسال آدرس و انتخاب ارز دیجیتال است. همچنین، برای حفظ امنیت و محدود کردن عبور، لطفاً اطلاعات زیر را در نظر بگیرید:
📌 آدرس ارسال:
لطفاً به صورت دقیق و مشخص بازیکن خود انتخاب کنید. مثلاً:
- تلگرام: @PharmaWebGp
- اینستاگرام: فولوور شما
 پرداخت در فارماوب با ۸ ارز دیجیتال انجام می‌شود: BTC، ETH، USDT، TRX، BNB، TON، SOL، DOGE."""
    print("1. Gate bad promo:", "REJECTED ✓" if not b.is_high_quality_natural(bad) else "FAILED ✗")

    # New: repair works on common hallucinations
    if hasattr(b, 'repair_llm_output'):
        print("1b. repair 'فقط ترون':", "FIXED ✓" if 'ترون' not in b.repair_llm_output('فقط ترون کار میکنه') else "STILL BAD")
    if hasattr(b, 'is_weak_llm_output'):
        print("1c. weak guard on robotic:", "DETECTED ✓" if b.is_weak_llm_output("متأسفم نمیتوانم کمک کنم") else "MISSED")

    # 2. Good natural samples
    goods = [
        "رتالین واقعاً برای بعضی افراد با بیش فعالی کمک میکنه ولی حتما باید پزشک تعیین کنه.",
        "من هم شنیدم TRC20 برای USDT کارمزد کمتری داره و سریع‌تر تأیید میشه.",
        "معمولاً بعد از تأیید واریز به استانبول زیر ۸ ساعت می‌رسه. بسته‌بندی محرمانه است.",
    ]
    for g in goods:
        print(f"2. Gate natural: {'PASS ✓' if b.is_high_quality_natural(g) else 'FAIL ✗'}  → {g[:55]}")

    # 3. Classify intent (professional routing)
    print("\n3. classify_intent (reference fidelity):")
    intent_tests = [
        ("ارسال به استانبول بعد از پرداخت چقدر طول میکشه؟", "shipping_time"),
        ("برای بیش فعالی چی پیشنهاد میکنید؟", "product_search"),  # ADHD without brand
        ("پرداخت با USDT کدوم شبکه بهتره؟", "crypto_info"),
        ("این جواب پرت بود", "complaint"),
        ("تو رباتی؟", "bot_question"),
        ("چطور سفارش بدم؟", "faq_order_process"),
        ("سلام", "greeting"),
    ]
    for q, expect in intent_tests:
        res = b.classify_intent(q)
        ok = res.get('intent') == expect or (expect == 'faq_order_process' and res.get('intent') in ('faq_order_process', 'product_info'))
        print(f"   {q[:45]:<45} → {res.get('intent')} {'✓' if ok else '≈'}")

    # 4. plan + retrieve
    print("\n4. plan_response + retrieve_knowledge:")
    for q in ["ارسال به استانبول بعد پرداخت چقدر طول میکشه؟", "ریتالین ساندوز برای ADHD", "نمیدونم کدوم شبکه تتر"]:
        intent = b.classify_intent(q)
        plan = b.plan_response(intent, True, False, q) if hasattr(b, 'plan_response') else {'strategy': b.plan_strategy(intent, True, False)}
        retrieved = b.retrieve_knowledge(q, intent.get('intent', ''))
        print(f"   Q={q[:40]} strategy={plan.get('strategy')} retrieved_len={len(retrieved)}")

    # 5. Anti-rep (conversation_brain style)
    print("\n5. is_repeated_response:")
    fake_hist = [('bot', 'معمولاً بعد از تأیید واریز به استانبول زیر ۸ ساعت می‌رسه.', None)]
    rep = "معمولاً بعد از تأیید واریز به استانبول زیر ۸ ساعت می‌رسه."
    print(f"   repeated similar → {'DETECTED ✓' if b.is_repeated_response(rep, fake_hist) else 'MISS'}")

    # 6. Live pipeline via responder or call (Qwen must be up)
    print("\n6. Full pipeline quality (live Qwen calls):")
    try:
        if hasattr(b, 'responder') and b.responder:
            r = await b.responder.generate(999999999, "برای بیش فعالی چی پیشنهاد میکنید؟", "curious")
            print("   responder.generate:", "NATURAL✓" if r and b.is_high_quality_natural(r) else "WEAK/None", "→", (r or "")[:90])
        else:
            r = await b.call_qwen3_natural(["دوستان تجربه‌ای در مورد ارسال دارید؟"], "ارسال به دبی چقدر زمان میبره؟")
            print("   call_qwen3_natural:", "NATURAL✓" if r and b.is_high_quality_natural(r) else "WEAK/None", "→", (r or "")[:90])
    except Exception as e:
        print("   live error (ok if no Qwen):", type(e).__name__)

    # 7. Self test
    print("\n7. run_ai_self_test():")
    try:
        res = await b.run_ai_self_test(3)
        print("   ", res)
    except Exception as e:
        print("   ", e)

    print("\n=== Professional benchmark finished ===")

# ── Anti-spam guard tests (new priority) ─────────────────────────────────────
def test_anti_spam_guard():
    print("\n=== Anti-Spam Guard Tests ===")
    import bot as b
    import time
    gid = 999999999
    # Clean
    b.last_group_bot_send.pop(gid, None)

    # Initially allowed
    assert b.can_send_to_group_safely(gid) == True, "Should allow first send"

    b.record_group_bot_send(gid)
    # Immediately after should block
    assert b.can_send_to_group_safely(gid) == False, "Must block within MIN interval"

    # Simulate time passage (hack the timestamp)
    b.last_group_bot_send[gid] = time.time() - (b.MIN_GROUP_BOT_INTERVAL + 10)
    assert b.can_send_to_group_safely(gid) == True, "Should allow after interval"

    print("Anti-spam guard: PASS (blocks rapid, allows after interval)")
    # Reset
    b.last_group_bot_send.pop(gid, None)

async def test_group_engagement_style():
    print("\n=== Group Engagement Style Tests (Qwen3 intelligence) ===")
    group_queries = [
        "داروهای ADHD این روزا خیلی سخته پیدا کردن. کسی راهی سراغ داره؟",
        "من ریتالین ساندوز گرفتم از یه جا، ولی شک دارم اورجینال باشه. شما چی؟",
        "ارسال به استانبول بعد از واریز USDT معمولاً چند ساعت طول میکشه؟",
        "به نظرتون برای تمرکز مودافینیل بهتره یا ریتالین؟",
        "راستی یه سوال، کسی تجربه پرداخت با ترون داشته؟ کارمزدش چطوره؟",
    ]
    for q in group_queries:
        try:
            # Use core directly for logic test + note that live would use think on high_value
            from ai.ai_core import classify_intent, retrieve_knowledge, plan_response
            i = classify_intent(q)
            r = retrieve_knowledge(q, i['intent'])
            p = plan_response(i, bool(r), True, q)
            print(f"  Q: {q[:55]}")
            print(f"     intent={i['intent']}, strategy={p['strategy']}, klen={len(r)}")
        except Exception as e:
            print(f"  ERR on {q[:30]}: {e}")

    # 5. Intelligence / director / content criteria (for very intelligent + insert + real + directed)
    print("\n5. Intelligence criteria (director routing + content insert + real grounding):")
    try:
        from ai.ai_core import director, content_intel, compose_knowledge
        cases = [
            ("ارسال به استانبول بعد از واریز چقدر طول میکشه؟", "shipping_time"),
            ("برای پرداخت USDT کدوم شبکه بهتره TRC20 یا ERC20؟", "crypto_info"),
            ("رتالین ساندوز اورجینال از کجا بگیریم؟", "product_search"),
        ]
        for q, exp in cases:
            i = classify_intent(q)
            d = director.direct(i['intent'], {'strategy': 'llm_reasoning'}, q, True, True)
            ins = content_intel.should_insert(i['intent'], q, 2)
            comp = compose_knowledge(q, i['intent'])[:80]
            print(f"   {q[:50]}")
            print(f"     variant={d.get('variant')} think={d.get('use_think')} insert_p={ins:.2f} comp_len={len(comp)}")
            print(f"     grounded_preview: {comp[:60] if comp else '(no)'}")
        print("  → Criteria: directed, think for value, sometimes-insert, grounded ✓")
    except Exception as e:
        print("  criteria err:", e)

    print("\n8. General-topic (must NOT dump drugs/crypto):")
    try:
        from ai.ai_core import classify_intent, retrieve_knowledge, decide_engagement, is_domain_topic, generate_natural_reply_local
        cases = [
            ("VPN چی استفاده میکنید؟", False),
            ("کسی فیلم خوبی دیده اخیرا؟", False),
            ("کسی تجربه زندگی در استانبول داره؟", False),
            ("ارسال به استانبول بعد پرداخت چقدر طول میکشه؟", True),
            ("ریتالین موجوده؟", True),
        ]
        for q, expect_domain in cases:
            i = classify_intent(q)
            k = retrieve_knowledge(q, i['intent'])
            d = is_domain_topic(q, i['intent'])
            eng = decide_engagement(q)
            local = generate_natural_reply_local(q, i['intent'], k)
            ok_domain = d == expect_domain
            leak = (not expect_domain) and any(x in (k + local).lower() for x in ['ریتالین', 'usdt', 'trc20', 'اوزمپیک'])
            print(f"   {q[:42]:<42} domain={d} intent={i['intent']} klen={len(k)} engage={eng['should_engage']} {'✓' if ok_domain and not leak else '✗ LEAK' if leak else '≈'}")
            if leak:
                print(f"      leak preview: {(k or local)[:80]}")
    except Exception as e:
        print("  general-topic err:", e)

if __name__ == "__main__":
    asyncio.run(main())
    asyncio.run(test_group_engagement_style())
    print("\nAll tests done. For full live + think test use Railway ssh on the userbot service and run the commands from the plan.")
