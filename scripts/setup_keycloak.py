"""
Keycloak Setup Script
======================
Creates the 'datalake' realm with a Grafana OIDC client and a test viewer
user. Run this AFTER `make security` (Keycloak must be up at localhost:8180).

    python scripts/setup_keycloak.py

What gets created:
  Realm:         datalake
  OIDC Client:   grafana   (Authorization Code flow, redirects to localhost:3000)
  Test user:     dataviewer / Viewer@2024

After running this script, restart Grafana to activate SSO:
  docker compose -f infrastructure/docker/docker-compose.observability.yml \\
    restart grafana

Then visit http://localhost:3000 and click "Sign in with Keycloak".
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

# ── Configuration ─────────────────────────────────────────────────────────────

KEYCLOAK_BASE  = os.getenv("KEYCLOAK_BASE_URL", "http://localhost:8180")
ADMIN_USER     = os.getenv("KEYCLOAK_ADMIN", "admin")
ADMIN_PASS     = os.getenv("KEYCLOAK_ADMIN_PASSWORD", "changeme_keycloak")
REALM          = "datalake"
CLIENT_ID      = "grafana"
CLIENT_SECRET  = "grafana-datalake-secret"   # must match docker-compose.observability.yml
TEST_USER      = "dataviewer"
TEST_PASSWORD  = "Viewer@2024"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _http(method: str, path: str, data=None, token: str = None) -> dict:
    url = f"{KEYCLOAK_BASE}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            return json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        if exc.code == 409:
            return {"_already_exists": True}
        raise RuntimeError(f"HTTP {exc.code} {method} {path}: {detail}") from exc


def _admin_token() -> str:
    """Fetch a short-lived admin-cli token from the master realm."""
    url = f"{KEYCLOAK_BASE}/realms/master/protocol/openid-connect/token"
    payload = (
        "grant_type=password"
        f"&client_id=admin-cli"
        f"&username={ADMIN_USER}"
        f"&password={ADMIN_PASS}"
    ).encode()
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())["access_token"]


# ── Readiness check ───────────────────────────────────────────────────────────

def _wait_for_keycloak(timeout: int = 120) -> None:
    print(f"Waiting for Keycloak at {KEYCLOAK_BASE} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{KEYCLOAK_BASE}/realms/master", timeout=5)
            print("  Keycloak is ready.")
            return
        except Exception:
            time.sleep(4)
    print("ERROR: Keycloak did not become ready. Is `make security` running?", file=sys.stderr)
    sys.exit(1)


# ── Setup steps ───────────────────────────────────────────────────────────────

def create_realm(token: str) -> None:
    result = _http("POST", "/admin/realms", data={
        "realm": REALM,
        "enabled": True,
        "displayName": "Enterprise Data Lake",
        "loginTheme": "keycloak",
        "registrationAllowed": False,
        "loginWithEmailAllowed": True,
        "duplicateEmailsAllowed": False,
        "sslRequired": "none",
        # MFA: require TOTP configuration for all new users by default.
        # For realm-wide browser login enforcement, configure a Conditional OTP
        # policy via Keycloak Admin UI → Authentication → Browser flow.
        "otpPolicyType": "totp",
        "otpPolicyAlgorithm": "HmacSHA1",
        "otpPolicyDigits": 6,
        "otpPolicyPeriod": 30,
        "defaultRequiredActions": ["CONFIGURE_TOTP", "VERIFY_EMAIL"],
    }, token=token)
    if result.get("_already_exists"):
        print(f"  Realm '{REALM}' already exists — skipping.")
    else:
        print(f"  Realm '{REALM}' created (TOTP MFA required for new users).")


def create_grafana_client(token: str) -> None:
    """Create an OIDC client for Grafana (Authorization Code + fixed secret)."""
    result = _http("POST", f"/admin/realms/{REALM}/clients", data={
        "clientId": CLIENT_ID,
        "secret": CLIENT_SECRET,
        "name": "Grafana",
        "description": "Grafana dashboards — SSO login via Keycloak",
        "enabled": True,
        "publicClient": False,
        "standardFlowEnabled": True,   # Authorization Code flow
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": False,
        "serviceAccountsEnabled": False,
        "protocol": "openid-connect",
        "redirectUris": [
            "http://localhost:3000/*",
            "http://grafana:3000/*",
        ],
        "webOrigins": ["http://localhost:3000"],
        "attributes": {
            "post.logout.redirect.uris": "http://localhost:3000/*",
            "backchannel.logout.session.required": "true",
        },
    }, token=token)
    if result.get("_already_exists"):
        print(f"  Client '{CLIENT_ID}' already exists — skipping.")
    else:
        print(f"  OIDC client '{CLIENT_ID}' created (secret: {CLIENT_SECRET}).")


def create_superset_client(token: str) -> None:
    """Create an OIDC client for Superset."""
    result = _http("POST", f"/admin/realms/{REALM}/clients", data={
        "clientId": "superset",
        "secret": "superset-datalake-secret",
        "name": "Superset",
        "description": "Apache Superset — SSO login via Keycloak",
        "enabled": True,
        "publicClient": False,
        "standardFlowEnabled": True,
        "implicitFlowEnabled": False,
        "directAccessGrantsEnabled": True,   # Superset needs this for token exchange
        "serviceAccountsEnabled": False,
        "protocol": "openid-connect",
        "redirectUris": [
            "http://localhost:8088/*",
            "http://superset:8088/*",
        ],
        "webOrigins": ["http://localhost:8088"],
        "attributes": {
            "post.logout.redirect.uris": "http://localhost:8088/*",
            "backchannel.logout.session.required": "true",
        },
    }, token=token)
    if result.get("_already_exists"):
        print("  Client 'superset' already exists — skipping.")
    else:
        print("  OIDC client 'superset' created (secret: superset-datalake-secret).")


def create_test_user(token: str) -> None:
    """Create a viewer test user for manual SSO login verification."""
    result = _http("POST", f"/admin/realms/{REALM}/users", data={
        "username": TEST_USER,
        "email": f"{TEST_USER}@datalake.local",
        "firstName": "Data",
        "lastName": "Viewer",
        "enabled": True,
        "emailVerified": True,
        # CONFIGURE_TOTP prompts the user to enroll an authenticator app on first login.
        "requiredActions": ["CONFIGURE_TOTP"],
        "credentials": [{
            "type": "password",
            "value": TEST_PASSWORD,
            "temporary": False,
        }],
    }, token=token)
    if result.get("_already_exists"):
        print(f"  User '{TEST_USER}' already exists — skipping.")
    else:
        print(f"  Test user '{TEST_USER}' / {TEST_PASSWORD} created (MFA enrollment required on first login).")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    _wait_for_keycloak()

    print("\nFetching admin token ...")
    token = _admin_token()
    print("  Token obtained.")

    print(f"\n[1/3] Creating realm '{REALM}' ...")
    create_realm(token)
    token = _admin_token()  # refresh after realm creation

    print("\n[2/4] Creating Grafana OIDC client ...")
    create_grafana_client(token)

    print("\n[3/4] Creating Superset OIDC client ...")
    create_superset_client(token)

    print("\n[4/4] Creating test user ...")
    create_test_user(token)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  Keycloak setup complete                                     ║
╠══════════════════════════════════════════════════════════════╣
║  Realm:              {REALM:<40}║
║  Grafana client:     grafana / grafana-datalake-secret       ║
║  Superset client:    superset / superset-datalake-secret     ║
║  Test login:         {TEST_USER} / {TEST_PASSWORD:<33}║
║  MFA:                TOTP required on first login            ║
╠══════════════════════════════════════════════════════════════╣
║  NOTE: For realm-wide browser MFA enforcement, configure a  ║
║  Conditional OTP policy in Keycloak Admin UI:               ║
║    Authentication → Flows → Browser → Add condition         ║
╚══════════════════════════════════════════════════════════════╝

Next steps:
  1. Recreate Grafana:  make observability
  2. Recreate Superset: make visualization
  3. Open http://localhost:3000 → Sign in with Keycloak
  4. Open http://localhost:8088 → Sign in with Keycloak
  5. First login with dataviewer will prompt TOTP enrollment
""")


if __name__ == "__main__":
    main()
