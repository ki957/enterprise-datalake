# ADR-002: Groq + Llama 4 Scout as the LLM Backend

**Status:** Accepted  
**Date:** 2025-04-15  
**Deciders:** Data Platform Team

---

## Context

The AI Agent service (`services/ai-agent/`) needs a Large Language Model (LLM) to power:
- A LangGraph supervisor that routes natural-language queries to 7 specialist agents
- Tool-use / function-calling to query ClickHouse, MinIO, Airflow, and Airbyte
- Conversational memory stored in PostgreSQL
- RAG retrieval against a ChromaDB knowledge base

Constraints:
1. **Zero marginal cost** — the free tier must handle daily development usage
2. **Low latency** — interactive chat; P95 response < 5 seconds
3. **Function calling** — agents use structured tool schemas; the LLM must reliably emit valid JSON tool calls
4. **Local-first** — no data sent to OpenAI/Anthropic by default; acceptable if provider is free-tier API

Candidates evaluated:
1. **Groq (Llama 4 Scout)** — free API, ~500 tokens/sec inference, function-calling support
2. **OpenAI GPT-4o** — best function-calling, but costly (~$5/M tokens)
3. **Anthropic Claude (Haiku)** — good function-calling, ~$0.25/M tokens, not free
4. **Ollama (local Llama 3.1 8B)** — truly local, but requires 8GB VRAM; excluded on hardware grounds
5. **Together AI (Llama 3.3 70B)** — free tier available, slower cold starts

---

## Decision

Use **Groq API** with model `meta-llama/llama-4-scout-17b-16e-instruct` as the LLM backend for all agents.

---

## Rationale

| Criterion | Groq / Llama 4 Scout | GPT-4o | Claude Haiku | Ollama local |
|-----------|---------------------|--------|--------------|--------------|
| Cost | Free tier | ~$5/M tokens | ~$0.25/M | $0 (compute) |
| Inference speed | ~500 tok/s | ~40 tok/s | ~80 tok/s | ~20 tok/s (CPU) |
| Function-calling | Yes (≤4 tools) | Excellent | Excellent | Unreliable |
| Context window | 131K tokens | 128K | 200K | 8K (Llama 3.1 8B) |
| Hardware required | None | None | None | 8GB VRAM |
| API availability | Public free | Paid | Paid | Self-hosted |

### Why Groq wins
1. **Free tier for development** — daily token allowance covers the full agent usage pattern for a single-developer data lake.
2. **Speed** — ~500 tokens/sec on H100 inference hardware means near-instant tool-call round trips.
3. **Llama 4 Scout** — the 17B MoE model with 16 experts handles multi-turn tool-use reliably within the ≤4-tool-per-agent constraint.

### Key constraints discovered during implementation
- `llama3-70b-8192` is **decommissioned** on Groq (returns 404).
- `llama-3.3-70b-versatile` **breaks with 5+ tools** — emits malformed JSON. Each agent is capped at 4 tools.
- `streaming=False` is **required** in `agents/base.py` — streaming mode triggers intermittent "Failed to call a function" errors specific to the Groq endpoint.
- Retry logic (3 attempts, truncating the prompt on retry 2) is required for Groq reliability.

---

## Consequences

- **Positive:** Zero cost during development; fast interactive responses.
- **Positive:** Llama 4 Scout runs function-calling reliably within the 4-tool limit.
- **Negative:** Groq free tier has daily rate limits (~14,400 requests/day as of 2025) — not suitable for production multi-tenant use.
- **Negative:** Model is not self-hosted; data leaves the environment on every call.
- **Negative:** 4-tool-per-agent cap requires deliberate agent decomposition — the Airbyte agent is further capped at 3 tools due to observed failures.
- **Migration path:** If a production-grade deployment is needed, swap `GROQ_API_KEY` for an OpenAI or Anthropic key and update the `ChatGroq` initialization in `agents/base.py` — the LangGraph graph structure is provider-agnostic.

---

## Alternatives Rejected

- **GPT-4o**: Excluded for development phase due to cost; viable for production.
- **Claude Haiku**: Good function-calling but paid; lower priority than Groq free tier.
- **Ollama local**: Requires dedicated GPU hardware not available in the current local stack.
- **Together AI**: Higher cold-start latency; Groq is faster and equally free.
