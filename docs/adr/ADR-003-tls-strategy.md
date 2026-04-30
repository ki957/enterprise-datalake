# ADR-003: TLS Strategy for Production Deployment

**Status:** Accepted  
**Date:** 2026-04-16  
**Deciders:** Platform Engineering

---

## Context

The current PoC stack runs all services over HTTP. Every service-to-service call
(Airflow → Vault, Superset → Keycloak, Grafana → ClickHouse) and every browser
session transmits credentials in plaintext. This is acceptable in an isolated dev
environment but is a production blocker for any data that is PII-adjacent, financial,
or subject to compliance review (SOC 2, GDPR, HIPAA).

The gap is acknowledged in the PoC. This ADR records the target production TLS
architecture so reviewers can see the intent, not just the current state.

---

## Decision

TLS will be enforced at three layers in production:

### Layer 1 — Ingress (browser → platform)

A reverse proxy (Traefik or nginx) sits in front of all user-facing services.
The proxy terminates TLS using certificates from Let's Encrypt (ACME) or an
internal CA, and forwards plain HTTP to containers on the private backend network.

Affected services: Airflow, Grafana, Superset, Keycloak, MinIO Console, dbt Docs, Spark UI.

```
Browser ──HTTPS──► Traefik (443) ──HTTP──► container:internal-port
```

All OAuth2 redirect URIs in Keycloak clients will be updated from `http://` to `https://`.  
Keycloak `sslRequired` will be set to `"external"` (or `"all"` if internal traffic is
also TLS-terminated).

### Layer 2 — Service-to-service (internal mTLS)

Internal calls between pipeline services will use mutual TLS in production.
Options:

| Approach | Complexity | Fits this stack |
|---|---|---|
| Service mesh (Istio / Linkerd) | High | Yes, if moving to k8s |
| cert-manager + per-service certs | Medium | Yes, for Docker Compose |
| Vault PKI secrets engine | Low-Medium | Yes — Vault is already deployed |

**Selected approach:** Vault PKI secrets engine.  
Each service requests a short-lived TLS certificate from Vault at startup. Vault
acts as the internal CA. Certificate TTL = 24 h; automatic rotation via sidecar or
init container.

This covers: ClickHouse HTTP API, PostgreSQL connections (`sslmode=require`),
MinIO API, Vault API itself, Airflow-to-Vault (AppRole over HTTPS).

### Layer 3 — Encryption at rest

Volumes containing PII or credentials will use OS-level or cloud-level encryption:

- **MinIO:** Enable server-side encryption (SSE-S3 or SSE-KMS) using Vault Transit
  secrets engine as the KMS provider.
- **PostgreSQL / ClickHouse:** Encrypted EBS/block volumes at the infrastructure layer
  (cloud provider feature, no application change required).
- **Vault:** Seal configuration using AWS KMS or Azure Key Vault (replaces dev-mode
  in-memory seal). Auto-unseal on restart without storing the unseal key on disk.

---

## Consequences

**What this unblocks:**
- Keycloak `sslRequired: external` — OAuth2 flows become secure.
- Vault audit log contains cleartext paths but not secret values — acceptable with
  TLS in transit.
- SOC 2 Type II control CC6.1 (logical access) and CC6.7 (transmission protection)
  become satisfiable.

**What is deferred in the PoC:**
- Layer 1 TLS is not implemented — `KC_HTTP_ENABLED: "true"` remains in
  `docker-compose.security.yml` for local dev convenience.
- mTLS between containers is not implemented — Docker internal network provides
  isolation but not encryption.
- Vault PKI engine is not configured — Vault runs in dev mode.

**Effort estimate for Layer 1 (highest ROI):**
- Add a Traefik service to `docker-compose.base.yml` with Let's Encrypt ACME config.
- Update all OAuth2 redirect URIs in `setup_keycloak.py`.
- Update Keycloak `sslRequired` to `"external"`.
- Estimated: 1–2 days.

---

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| Self-signed certs on every container | Cert distribution and rotation overhead; browsers reject self-signed for OAuth flows |
| Cloudflare Tunnel | Adds external dependency; not suitable for air-gapped/on-prem deployments |
| No TLS in production | Non-starter for any regulated data or enterprise customer |
