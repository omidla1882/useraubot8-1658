#!/usr/bin/env python3
"""
Helper to inspect/tune the Qwen3 service for maximum performance (per plan).

Usage (from your machine with railway CLI):
  python inspect_qwen.py --qwen
  python inspect_qwen.py --webui

Or run the printed ssh commands manually.

Commands are taken directly from the user-provided Railway service list.
"""

import argparse
import subprocess
import sys

QWEN_SSH = 'railway ssh --project=5468a68d-4b4b-4867-838b-68ee92ef25cc --environment=f4f95a90-6fcf-4c67-8a56-9611fff95d51 --service=8085e69d-e4cc-4895-a064-61b20f7e572f'
USERBOT_SSH = 'railway ssh --project=5468a68d-4b4b-4867-838b-68ee92ef25cc --environment=f4f95a90-6fcf-4c67-8a56-9611fff95d51 --service=3b6dfc79-32dc-44ee-b5b0-a10bace31707'

def run_ssh(cmd: str, extra: str = ""):
    full = f"{cmd} {extra}".strip()
    print(f"\n>>> Running: {full}\n")
    try:
        subprocess.run(full, shell=True, check=False)
    except Exception as e:
        print("Error:", e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--qwen', action='store_true', help='Inspect the raw qwen3 service')
    parser.add_argument('--webui', action='store_true', help='Inspect Open-WebUI (recommended for model settings & system prompt)')
    parser.add_argument('--test', action='store_true', help='Run a quick local ai core + client test')
    args = parser.parse_args()

    if args.qwen:
        print("Inspecting Qwen3 service (model list, status)...")
        run_ssh(QWEN_SSH, '"curl -s http://localhost:11434/api/tags || echo no direct curl; ps aux | grep ollama || true"')

    if args.webui:
        print("Open-WebUI not configured for this project. Use --qwen instead.")

    if args.test:
        print("Local test of ai/ modules (no live Qwen required for logic):")
        import asyncio
        from ai.ai_core import classify_intent, retrieve_knowledge, plan_response
        from ai.llm_client import qwen3
        q = "ارسال به استانبول با USDT چقدر طول میکشه؟"
        print("Query:", q)
        print("classify:", classify_intent(q))
        print("retrieve len:", len(retrieve_knowledge(q)))
        print("plan:", plan_response(classify_intent(q), True, False, q))
        print("Client available check would call the real endpoint when bot runs.")

    if not any([args.qwen, args.webui, args.test]):
        print("Recommended commands (copy-paste):")
        print("  Userbot:", USERBOT_SSH)
        print("  Qwen3:  ", QWEN_SSH)
        print("\nInside userbot shell:")
        print('  python -c "import asyncio, bot as b; print(asyncio.run(b.check_qwen_health())); print(asyncio.run(b.run_ai_self_test(3)))"')
        print("  tail -n 30 remember/ai_logs/responses-$(date +%Y-%m-%d).log")
        print("\nInside Qwen shell:")
        print("  curl -s http://localhost:11434/api/tags")

if __name__ == "__main__":
    main()
