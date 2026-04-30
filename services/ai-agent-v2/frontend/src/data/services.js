// When accessed from a phone on the same WiFi, window.location.hostname is the
// PC's LAN IP (e.g. 192.168.1.10), not "localhost". Rewrite service links so
// clicking them from mobile opens the right host instead of localhost.
export function resolveUrl(url) {
  if (typeof window === 'undefined') return url
  const { hostname } = window.location
  if (hostname === 'localhost' || hostname === '127.0.0.1') return url
  return url.replace('localhost', hostname)
}

// Ordered by operational importance — Tier 1 is critical daily ops
export const SERVICES = [
  // ── Tier 1 — Core Pipeline ────────────────────────────────────────────────
  {
    id: 'airflow',
    label: 'Airflow',
    port: 8080,
    url: 'http://localhost:8080',
    icon: '◈',
    tier: 1,
    description: 'DAG runs & orchestration',
  },
  {
    id: 'clickhouse',
    label: 'ClickHouse',
    port: 8123,
    url: 'http://localhost:8123/play',
    icon: '◉',
    tier: 1,
    description: 'Analytics query engine',
  },
  {
    id: 'grafana',
    label: 'Grafana',
    port: 3000,
    url: 'http://localhost:3000',
    icon: '◎',
    tier: 1,
    description: 'Pipeline monitoring',
  },

  // ── Tier 2 — Data & Ingestion ─────────────────────────────────────────────
  {
    id: 'superset',
    label: 'Superset',
    port: 8088,
    url: 'http://localhost:8088',
    icon: '▦',
    tier: 2,
    description: 'Business dashboards',
  },
  {
    id: 'airbyte',
    label: 'Airbyte',
    port: 8000,
    url: 'http://localhost:8000',
    icon: '⟳',
    tier: 2,
    description: 'Connector health',
  },
  {
    id: 'minio',
    label: 'MinIO',
    port: 9001,
    url: 'http://localhost:9001',
    icon: '⬡',
    tier: 2,
    description: 'Raw file landing zone',
  },

  // ── Tier 3 — Dev / Infra ──────────────────────────────────────────────────
  {
    id: 'dbt',
    label: 'dbt Docs',
    port: 8082,
    url: 'http://localhost:8082',
    icon: '⊞',
    tier: 3,
    description: 'Model lineage & schema',
  },
  {
    id: 'keycloak',
    label: 'Keycloak',
    port: 8180,
    url: 'http://localhost:8180',
    icon: '⊙',
    tier: 3,
    description: 'Auth & SSO admin',
  },
  {
    id: 'vault',
    label: 'Vault',
    port: 8200,
    url: 'http://localhost:8200',
    icon: '⊗',
    tier: 3,
    description: 'Secrets management',
  },
  {
    id: 'prometheus',
    label: 'Prometheus',
    port: 9090,
    url: 'http://localhost:9090',
    icon: '◌',
    tier: 3,
    description: 'Raw metrics',
  },
  {
    id: 'spark',
    label: 'Spark',
    port: 8081,
    url: 'http://localhost:8081',
    icon: '⚡',
    tier: 3,
    description: 'Job UI',
  },
]

export const TIER_LABELS = {
  1: 'Core Pipeline',
  2: 'Data & Ingestion',
  3: 'Dev / Infra',
}
