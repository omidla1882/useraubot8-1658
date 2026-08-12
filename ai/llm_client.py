"""
Async LLM client for userbotai — optimized for qwen3:1.7b on Railway CPU.
Supports think=true for complex queries only (slow on CPU).
Retry + shared session for reliability.
"""

import asyncio
import logging
import os
import re
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

_log = logging.getLogger("qwen3")

# Protect small CPU model from overload
_inference_sem = asyncio.Semaphore(int(os.getenv('CHAT_AI_MAX_CONCURRENT', '1')))

# Shared session (reuse TCP connections to Qwen service)
_shared_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


def _parse_think_blocks(text: str) -> Tuple[str, str]:
    """Return (thinking_text, final_text). Strips <think>...</think>."""
    if not text:
        return "", ""
    # Capture thinking
    think_match = re.search(r'<think>([\s\S]*?)</think>', text, re.IGNORECASE)
    thinking = think_match.group(1).strip() if think_match else ""
    # Remove all think blocks
    final = re.sub(r'<think>[\s\S]*?</think>', '', text, flags=re.IGNORECASE)
    final = re.sub(r'</?think[^>]*>', '', final, flags=re.IGNORECASE).strip()
    return thinking, final


async def _get_session() -> aiohttp.ClientSession:
    """Reuse HTTP session for Qwen API calls."""
    global _shared_session
    async with _session_lock:
        if _shared_session is None or _shared_session.closed:
            total = float(os.getenv('QWEN3_TIMEOUT', '95'))
            _shared_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=total, connect=12, sock_read=total),
            )
        return _shared_session


class Qwen3Client:
    """Async client tuned for qwen3:1.7b on Railway CPU."""

    def __init__(self):
        self.base_url = os.getenv(
            'QWEN3_BASE_URL',
            os.getenv('OLLAMA_BASE_URL', 'http://qwen3.railway.internal:11434'),
        ).rstrip('/')
        self.model = os.getenv('QWEN3_MODEL', os.getenv('OLLAMA_MODEL', 'qwen3:1.7b'))
        self.timeout = float(os.getenv('QWEN3_TIMEOUT', '95'))
        self.default_max_tokens = int(os.getenv('QWEN3_MAX_TOKENS', '280'))
        self.default_temperature = float(os.getenv('QWEN3_TEMPERATURE', '0.44'))
        self.default_num_ctx = int(os.getenv('QWEN3_NUM_CTX', '3072'))
        self._last_health: Tuple[bool, float] = (False, 0.0)
        self._cooldown_until = 0.0

    async def is_available(self) -> bool:
        """Cached health check (30s TTL)."""
        now = time.time()
        if now - self._last_health[1] < 30:
            return self._last_health[0]
        ok = False
        try:
            sess = await _get_session()
            async with sess.get(f"{self.base_url}/api/tags") as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    models = [m.get('name', '') for m in data.get('models', [])]
                    ok = any(self.model.split(':')[0] in m for m in models) or bool(models)
        except Exception as e:
            _log.warning("Qwen health check failed: %s", e)
        self._last_health = (ok, now)
        return ok

    async def chat(
        self,
        messages: List[Dict[str, str]],
        *,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        use_think: bool = False,
        num_ctx: Optional[int] = None,
        retries: int = 1,
    ) -> Dict:
        """
        Send chat to Qwen3. One retry max — extra retries overload CPU 1.7b.
        After timeout/503, cool down so group fallbacks can answer.
        """
        if time.time() < self._cooldown_until:
            raise RuntimeError("Qwen cooldown after recent timeout")
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "think": bool(use_think),
            "options": {
                "temperature": temperature if temperature is not None else self.default_temperature,
                "num_predict": max_tokens or self.default_max_tokens,
                "num_ctx": num_ctx or self.default_num_ctx,
                "top_p": 0.90,
                "top_k": 40,
                "repeat_penalty": 1.15,
                "repeat_last_n": 96,
                "num_thread": int(os.getenv('QWEN3_NUM_THREAD', '4')),
            },
        }

        last_err = None
        start = time.time()

        for attempt in range(retries + 1):
            try:
                async with _inference_sem:
                    sess = await _get_session()
                    async with sess.post(f"{self.base_url}/api/chat", json=payload) as r:
                        if r.status != 200:
                            body = await r.text()
                            raise RuntimeError(f"Qwen3 HTTP {r.status}: {body[:120]}")
                        data = await r.json(content_type=None)

                elapsed = time.time() - start
                msg = data.get("message", {}) or {}
                raw = (msg.get("content") or "").strip()
                thinking, final = _parse_think_blocks(raw)

                return {
                    "content": final,
                    "thinking": thinking,
                    "raw": raw,
                    "model": self.model,
                    "time": round(elapsed, 2),
                    "tokens": data.get("eval_count", 0) + data.get("prompt_eval_count", 0),
                    "ok": bool(final),
                }
            except (asyncio.TimeoutError, aiohttp.ClientError, RuntimeError) as e:
                last_err = e
                _log.warning("Qwen attempt %d/%d failed: %s", attempt + 1, retries + 1, e)
                if 'timeout' in str(e).lower() or '503' in str(e):
                    self._cooldown_until = time.time() + 20
                    break
                if attempt < retries:
                    await asyncio.sleep(1.2 * (attempt + 1))

        raise RuntimeError(f"Qwen3 failed after {retries + 1} attempts: {last_err}")


# Global instance used by the bot
qwen3 = Qwen3Client()
