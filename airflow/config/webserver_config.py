import os
from flask_appbuilder.const import AUTH_OAUTH
from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"  # Default role untuk user baru

# Ambil dari environment variable
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_AUTH_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_AUTH_CLIENT_SECRET")

if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    raise ValueError(
        "GOOGLE_AUTH_CLIENT_ID and GOOGLE_AUTH_CLIENT_SECRET "
        "must be set in environment variables"
    )

OAUTH_PROVIDERS = [
    {
        "name": "google",
        "token_key": "access_token",
        "icon": "fa-google",
        "remote_app": {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "api_base_url": "https://www.googleapis.com/oauth2/v2/",
            "client_kwargs": {"scope": "openid email profile"},
            "access_token_url": "https://oauth2.googleapis.com/token",
            "authorize_url": "https://accounts.google.com/o/oauth2/auth",
            "request_token_url": None,
        },
    }
]

ADMIN_EMAILS = {
    "yudafihan@student.uns.ac.id",
}

ALLOWED_DOMAINS = {
    "student.uns.ac.id",
}


class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    def oauth_user_info(self, provider, response):
        if provider != "google":
            return {}

        userinfo = response.get("userinfo", {})
        email = userinfo.get("email", "")

        if ALLOWED_DOMAINS:
            domain = email.split("@")[-1] if "@" in email else ""
            if domain not in ALLOWED_DOMAINS:
                return {}

        if email in ADMIN_EMAILS:
            userinfo["role_keys"] = ["Admin"]
        else:
            userinfo["role_keys"] = ["Viewer"]

        return userinfo


SECURITY_MANAGER_CLASS = CustomSecurityManager

AUTH_ROLES_SYNC_AT_LOGIN = True
