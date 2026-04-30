---
type: community
cohesion: 0.25
members: 9
---

# Chat API & Frontend Data

**Cohesion:** 0.25 - loosely connected
**Members:** 9 nodes

## Members
- [[apichat SSE endpoint (FastAPI at 8502)]] - code - services/ai-agent-v2/frontend/src/hooks/useChat.js
- [[AGENTS constant (frontend agent registry)]] - code - services/ai-agent-v2/frontend/src/data/agents.js
- [[QUICK_ACTIONS constant (persona-based quick prompts)]] - code - services/ai-agent-v2/frontend/src/data/quickActions.js
- [[SERVICES constant (infrastructure service links)]] - code - services/ai-agent-v2/frontend/src/data/services.js
- [[Vite build config (dev server + proxy)]] - code - services/ai-agent-v2/frontend/vite.config.js
- [[Zustand store (global frontend state)]] - code - services/ai-agent-v2/frontend/src/store/index.js
- [[useChat hook (SSE message streaming)]] - code - services/ai-agent-v2/frontend/src/hooks/useChat.js
- [[useKeyboard hook (Cmd+K command palette shortcut)]] - code - services/ai-agent-v2/frontend/src/hooks/useKeyboard.js
- [[useToast  useToastAutoDismiss hooks]] - code - services/ai-agent-v2/frontend/src/hooks/useToast.js

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Chat_API_&_Frontend_Data
SORT file.name ASC
```
