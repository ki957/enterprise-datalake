# Superset runtime configuration
# Mounted into the container at /app/pythonpath/superset_config.py
#
# Keycloak SSO: run `make setup-keycloak` first to provision the
# datalake realm and superset OIDC client, then restart Superset:
#   docker compose -f infrastructure/docker/docker-compose.visualization.yml restart superset

from flask_appbuilder.security.manager import AUTH_OAUTH

# ── Auth ──────────────────────────────────────────────────────────────────────

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Gamma"   # default role for new SSO users
AUTH_ROLES_SYNC_AT_LOGIN = True

OAUTH_PROVIDERS = [
    {
        "name":      "keycloak",
        "token_key": "access_token",
        "icon":      "fa-key",
        "remote_app": {
            "client_id":     "superset",
            "client_secret": "superset-datalake-secret",
            "client_kwargs": {"scope": "openid email profile"},
            "access_token_method": "POST",
            # Browser-facing — must use host-accessible URL
            "authorize_url": "http://localhost:8180/realms/datalake/protocol/openid-connect/auth",
            # Server-side calls — use internal Docker hostname
            "access_token_url": "http://keycloak:8080/realms/datalake/protocol/openid-connect/token",
            "api_base_url":     "http://keycloak:8080/realms/datalake/protocol/openid-connect/",
            "jwks_uri":         "http://keycloak:8080/realms/datalake/protocol/openid-connect/certs",
            "userinfo_endpoint": "http://keycloak:8080/realms/datalake/protocol/openid-connect/userinfo",
        },
    }
]


# ── Security manager override ─────────────────────────────────────────────────
# Maps Keycloak user info fields to Superset user fields.

from superset.security import SupersetSecurityManager  # noqa: E402


class KeycloakSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        if provider == "keycloak":
            me = self.appbuilder.sm.oauth_remoteapp.userinfo()
            return {
                "username":   me.get("preferred_username", me.get("email", "")),
                "email":      me.get("email", ""),
                "first_name": me.get("given_name", ""),
                "last_name":  me.get("family_name", ""),
            }
        return super().oauth_user_info(provider, response)


CUSTOM_SECURITY_MANAGER = KeycloakSecurityManager

# ── Feature flags ─────────────────────────────────────────────────────────────

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS":   True,
}

# ── Cache (Redis) ─────────────────────────────────────────────────────────────

CACHE_CONFIG = {
    "CACHE_TYPE":              "RedisCache",
    "CACHE_DEFAULT_TIMEOUT":   300,
    "CACHE_KEY_PREFIX":        "superset_",
    "CACHE_REDIS_URL":         "redis://:${REDIS_PASSWORD}@redis:6379/0",
}
