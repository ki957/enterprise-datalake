---
type: community
cohesion: 0.50
members: 5
---

# TLS Architecture ADR

**Cohesion:** 0.50 - moderately connected
**Members:** 5 nodes

## Members
- [[ADR-003 TLS Strategy for Production Deployment]] - document - docs/adr/ADR-003-tls-strategy.md
- [[Rationale Vault PKI selected for mTLS because Vault is already deployed]] - document - docs/adr/ADR-003-tls-strategy.md
- [[TLS Production Architecture]] - document - docs/adr/ADR-003-tls-strategy.md
- [[Traefik Reverse Proxy TLS Ingress]] - document - docs/adr/ADR-003-tls-strategy.md
- [[Vault PKI Secrets Engine as Internal CA]] - document - docs/adr/ADR-003-tls-strategy.md

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/TLS_Architecture_ADR
SORT file.name ASC
```
