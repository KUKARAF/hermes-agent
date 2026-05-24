"""External secret source integrations."""
from agent.secret_sources.online_kv import apply_online_kv_secrets, fetch_kv_secrets, FetchResult

__all__ = ["apply_online_kv_secrets", "fetch_kv_secrets", "FetchResult"]