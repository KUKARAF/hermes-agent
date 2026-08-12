"""KitchenOwl tool for managing shopping lists and recipes via REST API.

Registers five LLM-callable tools:
- ``kitchenowl_list_lists``      -- list shopping lists across households
- ``kitchenowl_get_items``       -- get the items currently on a shopping list
- ``kitchenowl_add_item``        -- add an item (by name) to a shopping list
- ``kitchenowl_remove_item``     -- remove an item from a shopping list
- ``kitchenowl_search_recipes``  -- search recipes in a household

Authentication uses a KitchenOwl long-lived token, resolved the same way the
Home Assistant integration resolves ``HASS_TOKEN``: the token is fetched from
online_kv into the environment and read here at call time.

Unlike Home Assistant (URL in ``HASS_URL`` env), the KitchenOwl instance URL
and the *name* of the KV key holding the token are configured in
``~/.hermes/config.yaml``::

    kitchenowl:
      url: https://cook.osmosis.page
      token_kv: KITCHENOWL_API_TOKEN
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Kept for backward compatibility / test monkeypatching; prefer _get_config().
_KITCHENOWL_URL: str = ""
_KITCHENOWL_TOKEN: str = ""

_DEFAULT_URL = "https://app.kitchenowl.org"
_DEFAULT_TOKEN_KV = "KITCHENOWL_API_TOKEN"

# Seasonal-produce source (European fruit & veg by month/country). Presented to
# the model as a plain KitchenOwl feature; the fetch is an implementation detail.
_SEASONAL_SOURCE = "https://www.eufic.org/en/global/ajax-map"
_SEASONAL_TTL = 21600  # 6h — the data changes at most seasonally
_seasonal_cache: Dict[str, Any] = {"data": None, "at": 0.0}


def _config_section() -> Dict[str, Any]:
    """Read the ``kitchenowl`` block from ~/.hermes/config.yaml (best effort)."""
    try:
        from hermes_cli.config import load_config_readonly

        section = load_config_readonly().get("kitchenowl")
        return section if isinstance(section, dict) else {}
    except Exception:  # config unreadable / not in a hermes env — fall back to env
        return {}


def _get_config():
    """Return (api_base_url, token) resolved at call time.

    URL precedence:  ``KITCHENOWL_URL`` env → config.yaml ``kitchenowl.url`` →
    default. Token: env var named by config.yaml ``kitchenowl.token_kv``
    (default ``KITCHENOWL_API_TOKEN``), then that fixed env name as a fallback.
    """
    section = _config_section()

    url = (
        _KITCHENOWL_URL
        or os.getenv("KITCHENOWL_URL", "")
        or str(section.get("url", "") or "")
        or _DEFAULT_URL
    ).rstrip("/")
    # Store the site root in config; the REST API lives under /api.
    if not url.endswith("/api"):
        url = url + "/api"

    token_kv = str(section.get("token_kv", "") or _DEFAULT_TOKEN_KV)
    token = (
        _KITCHENOWL_TOKEN
        or os.getenv(token_kv, "")
        or os.getenv(_DEFAULT_TOKEN_KV, "")
    )
    return url, token


def _get_headers(token: str = "") -> Dict[str, str]:
    """Return authorization headers for the KitchenOwl REST API."""
    if not token:
        _, token = _get_config()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _as_int(value: Any) -> Optional[int]:
    """Coerce an id to int, or None if it is not a clean integer.

    IDs are interpolated into ``/api/...`` paths, so anything non-integer is
    rejected to prevent path traversal / SSRF (mirrors the HA tool's
    entity/service validation).
    """
    try:
        if isinstance(value, bool):
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Async HTTP helpers
# ---------------------------------------------------------------------------

async def _request(method: str, path: str, body: Optional[dict] = None) -> Any:
    """Perform an authenticated request against the KitchenOwl API."""
    import aiohttp

    base, token = _get_config()
    url = f"{base}{path}"
    async with aiohttp.ClientSession() as session:
        async with session.request(
            method,
            url,
            headers=_get_headers(token),
            json=body,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            if resp.status == 204 or resp.content_length == 0:
                return None
            return await resp.json()


async def _households() -> list:
    data = await _request("GET", "/household")
    return data if isinstance(data, list) else []


async def _async_list_lists(household: Optional[str] = None) -> Dict[str, Any]:
    """List shopping lists across all (or one matching) household."""
    households = await _households()
    hh_filter = str(household).strip().lower() if household else None

    lists = []
    for hh in households:
        hid = hh.get("id")
        hname = hh.get("name", "")
        if hh_filter and hh_filter not in (str(hname).lower(), str(hid)):
            continue
        sls = await _request("GET", f"/household/{hid}/shoppinglist")
        for sl in sls if isinstance(sls, list) else []:
            lists.append({
                "list_id": sl.get("id"),
                "list_name": sl.get("name", ""),
                "household_id": hid,
                "household_name": hname,
            })
    return {"count": len(lists), "shopping_lists": lists}


async def _async_get_items(list_id: int) -> Dict[str, Any]:
    items = await _request("GET", f"/shoppinglist/{list_id}/items")
    out = [
        {
            "item_id": i.get("id"),
            "name": i.get("name", ""),
            "description": i.get("description", ""),
        }
        for i in (items if isinstance(items, list) else [])
    ]
    return {"list_id": list_id, "count": len(out), "items": out}


async def _async_add_item(
    list_id: int, name: str, description: Optional[str] = None
) -> Dict[str, Any]:
    body: Dict[str, Any] = {"name": name}
    if description:
        body["description"] = description
    res = await _request("POST", f"/shoppinglist/{list_id}/add-item-by-name", body)
    added = {}
    if isinstance(res, dict):
        added = {"item_id": res.get("id"), "name": res.get("name", name)}
    return {"success": True, "list_id": list_id, "added": added or {"name": name}}


async def _async_remove_item(list_id: int, item_id: int) -> Dict[str, Any]:
    await _request("DELETE", f"/shoppinglist/{list_id}/item", {"item_id": item_id})
    return {"success": True, "list_id": list_id, "removed_item_id": item_id}


async def _async_search_recipes(
    query: str, household: Optional[str] = None
) -> Dict[str, Any]:
    households = await _households()
    hh_filter = str(household).strip().lower() if household else None

    recipes = []
    for hh in households:
        hid = hh.get("id")
        hname = hh.get("name", "")
        if hh_filter and hh_filter not in (str(hname).lower(), str(hid)):
            continue
        res = await _request(
            "GET", f"/household/{hid}/recipe/search?query={quote(query)}"
        )
        for r in res if isinstance(res, list) else []:
            desc = (r.get("description", "") or "").strip().replace("\n", " ")
            recipes.append({
                "recipe_id": r.get("id"),
                "name": r.get("name", ""),
                "household_name": hname,
                "cook_time": r.get("cook_time"),
                "description": (desc[:200] + "…") if len(desc) > 200 else desc,
            })
    return {"query": query, "count": len(recipes), "recipes": recipes}


async def _fetch_seasonal() -> Dict[str, Any]:
    """Fetch + parse the seasonal produce table, cached in-process for a while.

    The source returns a JavaScript assignment ``var fvlist = {...};`` whose
    object maps ``{category: {produce: [[Month, Country], ...]}}``.
    """
    now = time.time()
    cached = _seasonal_cache.get("data")
    if cached is not None and (now - _seasonal_cache.get("at", 0.0)) < _SEASONAL_TTL:
        return cached

    import aiohttp

    headers = {"User-Agent": "Mozilla/5.0 (hermes-agent)"}
    async with aiohttp.ClientSession() as session:
        async with session.get(
            _SEASONAL_SOURCE, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            resp.raise_for_status()
            raw = await resp.text()

    start, end = raw.find("{"), raw.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("unexpected seasonal data format")
    data = json.loads(raw[start:end + 1])

    _seasonal_cache["data"] = data
    _seasonal_cache["at"] = now
    return data


async def _async_produce_in_season(
    country: Optional[str] = None, month: Optional[str] = None
) -> Dict[str, Any]:
    data = await _fetch_seasonal()

    target_month = (month or datetime.now().strftime("%B")).strip().lower()
    target_country = country.strip().lower() if country else None

    out: Dict[str, list] = {}
    for category, produce in data.items():
        names = set()
        for name, pairs in (produce or {}).items():
            for entry in pairs:
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                mo = str(entry[0]).strip().lower()
                co = str(entry[1]).strip().lower()
                if mo != target_month:
                    continue
                if target_country and co != target_country:
                    continue
                names.add(str(name).strip())
                break
        out[category.strip().lower()] = sorted(names)

    return {
        "month": (month or datetime.now().strftime("%B")).strip(),
        "country": country.strip() if country else "any (Europe)",
        "produce": out,
        "count": sum(len(v) for v in out.values()),
    }


# ---------------------------------------------------------------------------
# Sync wrappers (handler signature: (args, **kw) -> str)
# ---------------------------------------------------------------------------

def _run_async(coro):
    """Run an async coroutine from a sync handler."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=30)
    return asyncio.run(coro)


def _handle_list_lists(args: dict, **kw) -> str:
    try:
        result = _run_async(_async_list_lists(household=args.get("household")))
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_list_lists error: %s", e)
        return tool_error(f"Failed to list shopping lists: {e}")


def _handle_get_items(args: dict, **kw) -> str:
    list_id = _as_int(args.get("list_id"))
    if list_id is None:
        return tool_error("Missing or invalid required parameter: list_id (integer)")
    try:
        result = _run_async(_async_get_items(list_id))
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_get_items error: %s", e)
        return tool_error(f"Failed to get items for list {list_id}: {e}")


def _handle_add_item(args: dict, **kw) -> str:
    list_id = _as_int(args.get("list_id"))
    if list_id is None:
        return tool_error("Missing or invalid required parameter: list_id (integer)")
    name = str(args.get("name", "")).strip()
    if not name:
        return tool_error("Missing required parameter: name")
    try:
        result = _run_async(_async_add_item(list_id, name, args.get("description")))
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_add_item error: %s", e)
        return tool_error(f"Failed to add '{name}' to list {list_id}: {e}")


def _handle_remove_item(args: dict, **kw) -> str:
    list_id = _as_int(args.get("list_id"))
    item_id = _as_int(args.get("item_id"))
    if list_id is None:
        return tool_error("Missing or invalid required parameter: list_id (integer)")
    if item_id is None:
        return tool_error(
            "Missing or invalid required parameter: item_id (integer). "
            "Use kitchenowl_get_items to find item_id."
        )
    try:
        result = _run_async(_async_remove_item(list_id, item_id))
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_remove_item error: %s", e)
        return tool_error(f"Failed to remove item {item_id} from list {list_id}: {e}")


def _handle_search_recipes(args: dict, **kw) -> str:
    query = str(args.get("query", "")).strip()
    if not query:
        return tool_error("Missing required parameter: query")
    try:
        result = _run_async(_async_search_recipes(query, args.get("household")))
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_search_recipes error: %s", e)
        return tool_error(f"Failed to search recipes for '{query}': {e}")


def _handle_produce_in_season(args: dict, **kw) -> str:
    try:
        result = _run_async(
            _async_produce_in_season(args.get("country"), args.get("month"))
        )
        return json.dumps({"result": result})
    except Exception as e:
        logger.error("kitchenowl_produce_in_season error: %s", e)
        return tool_error(f"Failed to get seasonal produce: {e}")


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

def _check_available() -> bool:
    """Tool is only available when a KitchenOwl token is present."""
    _, token = _get_config()
    return bool(token)


# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

KITCHENOWL_LIST_LISTS_SCHEMA = {
    "name": "kitchenowl_list_lists",
    "description": (
        "List KitchenOwl shopping lists across all households (or one household). "
        "Returns each list's id, name, and household — use the list_id with the "
        "other kitchenowl_* tools."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "household": {
                "type": "string",
                "description": (
                    "Optional household name or id to filter by "
                    "(e.g. 'Casita'). Omit to list lists from all households."
                ),
            },
        },
        "required": [],
    },
}

KITCHENOWL_GET_ITEMS_SCHEMA = {
    "name": "kitchenowl_get_items",
    "description": (
        "Get the items currently on a KitchenOwl shopping list. "
        "Use kitchenowl_list_lists first to find the list_id."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "list_id": {
                "type": "integer",
                "description": "The shopping list id (from kitchenowl_list_lists).",
            },
        },
        "required": ["list_id"],
    },
}

KITCHENOWL_ADD_ITEM_SCHEMA = {
    "name": "kitchenowl_add_item",
    "description": (
        "Add an item to a KitchenOwl shopping list by name. Creates the item if "
        "it doesn't exist yet. Use kitchenowl_list_lists to find the list_id."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "list_id": {
                "type": "integer",
                "description": "The shopping list id to add to.",
            },
            "name": {
                "type": "string",
                "description": "Item name to add (e.g. 'Milk', 'Tomatoes').",
            },
            "description": {
                "type": "string",
                "description": "Optional detail/amount (e.g. '2 liters', 'organic').",
            },
        },
        "required": ["list_id", "name"],
    },
}

KITCHENOWL_REMOVE_ITEM_SCHEMA = {
    "name": "kitchenowl_remove_item",
    "description": (
        "Remove an item from a KitchenOwl shopping list. "
        "Get the item_id from kitchenowl_get_items first."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "list_id": {
                "type": "integer",
                "description": "The shopping list id.",
            },
            "item_id": {
                "type": "integer",
                "description": "The item id to remove (from kitchenowl_get_items).",
            },
        },
        "required": ["list_id", "item_id"],
    },
}

KITCHENOWL_SEARCH_RECIPES_SCHEMA = {
    "name": "kitchenowl_search_recipes",
    "description": (
        "Search KitchenOwl recipes by name/text across households (or one "
        "household). Returns recipe id, name, cook time, and a short description."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search text (recipe name or keyword).",
            },
            "household": {
                "type": "string",
                "description": (
                    "Optional household name or id to search within. "
                    "Omit to search all households."
                ),
            },
        },
        "required": ["query"],
    },
}

KITCHENOWL_PRODUCE_SCHEMA = {
    "name": "kitchenowl_produce_in_season",
    "description": (
        "Get fruits and vegetables that are in season right now (European "
        "seasonal produce). Useful for suggesting what to add to a shopping "
        "list or which recipes to cook. Optionally pass a country for local "
        "results (e.g. 'Spain', 'Poland'); otherwise returns produce in season "
        "anywhere in Europe. Defaults to the current month."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "country": {
                "type": "string",
                "description": (
                    "Country to get local seasonal produce for (e.g. 'Spain', "
                    "'Poland', 'Germany', 'Italy'). Omit for all of Europe."
                ),
            },
            "month": {
                "type": "string",
                "description": (
                    "Month name (e.g. 'August'). Omit to use the current month."
                ),
            },
        },
        "required": [],
    },
}


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

from tools.registry import registry, tool_error

registry.register(
    name="kitchenowl_list_lists",
    toolset="kitchenowl",
    schema=KITCHENOWL_LIST_LISTS_SCHEMA,
    handler=_handle_list_lists,
    check_fn=_check_available,
    emoji="🦉",
)

registry.register(
    name="kitchenowl_get_items",
    toolset="kitchenowl",
    schema=KITCHENOWL_GET_ITEMS_SCHEMA,
    handler=_handle_get_items,
    check_fn=_check_available,
    emoji="🦉",
)

registry.register(
    name="kitchenowl_add_item",
    toolset="kitchenowl",
    schema=KITCHENOWL_ADD_ITEM_SCHEMA,
    handler=_handle_add_item,
    check_fn=_check_available,
    emoji="🦉",
)

registry.register(
    name="kitchenowl_remove_item",
    toolset="kitchenowl",
    schema=KITCHENOWL_REMOVE_ITEM_SCHEMA,
    handler=_handle_remove_item,
    check_fn=_check_available,
    emoji="🦉",
)

registry.register(
    name="kitchenowl_search_recipes",
    toolset="kitchenowl",
    schema=KITCHENOWL_SEARCH_RECIPES_SCHEMA,
    handler=_handle_search_recipes,
    check_fn=_check_available,
    emoji="🦉",
)

registry.register(
    name="kitchenowl_produce_in_season",
    toolset="kitchenowl",
    schema=KITCHENOWL_PRODUCE_SCHEMA,
    handler=_handle_produce_in_season,
    check_fn=_check_available,
    emoji="🦉",
)
