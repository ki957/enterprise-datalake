"""
Shared LLM factory for all agents.
streaming=False is critical — Groq streaming mode causes intermittent
"Failed to call a function" errors when models generate multi-tool schemas.

Singleton pattern: ChatGroq is expensive to construct (validates the API key
on init). A module-level instance is shared across all 7 agents.
"""

import os
import threading

from langchain_groq import ChatGroq

_llm: ChatGroq | None = None
_llm_lock = threading.Lock()
_llm_key: str = ""   # tracks which key the singleton was built with


def get_llm() -> ChatGroq:
    global _llm, _llm_key
    current_key = os.getenv("GROQ_API_KEY", "")
    if _llm is None or current_key != _llm_key:
        with _llm_lock:
            if _llm is None or current_key != _llm_key:
                _llm = ChatGroq(
                    model=os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct"),
                    api_key=current_key,
                    temperature=0,
                    streaming=False,       # ← fixes "Failed to call a function" errors
                    max_retries=2,
                )
                _llm_key = current_key
    return _llm


def reset_llm() -> None:
    """Force the singleton to be rebuilt on next call — use after rotating GROQ_API_KEY."""
    global _llm, _llm_key
    with _llm_lock:
        _llm = None
        _llm_key = ""
