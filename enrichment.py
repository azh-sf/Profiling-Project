"""Apify LinkedIn profile enrichment using the Full Sections scraper."""

from __future__ import annotations

import time
from typing import Optional
import requests

ACTOR_ID = "VhxlqQXRwhW8H5hNV"
BASE_URL = "https://api.apify.com/v2"


def enrich_single_profile(username: str, token: str, timeout: int = 90) -> Optional[dict]:
    """Enrich a single LinkedIn profile via Apify Full Sections scraper.

    Args:
        username: LinkedIn public identifier (e.g. 'john-smith-123')
        token: Apify API token
        timeout: Max seconds to wait for completion

    Returns:
        Full Apify profile dict with basic_info, experience[], education[],
        or None if enrichment failed.
    """
    # Start actor run
    run_resp = requests.post(
        f"{BASE_URL}/acts/{ACTOR_ID}/runs",
        params={"token": token},
        json={"includeEmail": False, "username": username},
        timeout=30,
    )
    if run_resp.status_code != 201:
        return None

    run_data = run_resp.json().get("data", {})
    run_id = run_data.get("id")
    if not run_id:
        return None

    # Poll for completion
    elapsed = 0
    poll_interval = 5
    while elapsed < timeout:
        time.sleep(poll_interval)
        elapsed += poll_interval

        status_resp = requests.get(
            f"{BASE_URL}/actor-runs/{run_id}",
            params={"token": token},
            timeout=15,
        )
        if status_resp.status_code != 200:
            continue

        status = status_resp.json().get("data", {}).get("status", "")
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return None
    else:
        return None  # Timed out

    # Fetch dataset
    dataset_id = status_resp.json().get("data", {}).get("defaultDatasetId", "")
    if not dataset_id:
        return None

    items_resp = requests.get(
        f"{BASE_URL}/datasets/{dataset_id}/items",
        params={"token": token, "format": "json"},
        timeout=30,
    )
    if items_resp.status_code != 200:
        return None

    items = items_resp.json()
    if not items or not isinstance(items, list) or len(items) == 0:
        return None

    profile = items[0]
    # Validate we got real data
    if "basic_info" not in profile:
        return None

    return profile


def enrich_profiles(
    usernames: list,
    token: str,
    progress_callback=None,
    max_workers: int = 5,
) -> dict:
    """Enrich multiple LinkedIn profiles with parallel execution.

    Runs up to max_workers concurrent Apify actor calls for speed,
    while keeping the same per-profile quality.

    Args:
        usernames: List of LinkedIn public identifiers
        token: Apify API token
        progress_callback: Optional callable(current, total, username, success)
        max_workers: Max concurrent Apify actor runs (default 5)

    Returns:
        Dict mapping username -> profile dict (only successful enrichments)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    results = {}
    total = len(usernames)
    completed = [0]  # mutable counter for thread-safe increment
    lock = threading.Lock()

    def _enrich_one(username):
        profile = enrich_single_profile(username, token)
        success = profile is not None
        with lock:
            completed[0] += 1
            if success:
                results[username] = profile
            if progress_callback:
                progress_callback(completed[0], total, username, success)
        return username, profile

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, username in enumerate(usernames):
            # Stagger submissions slightly to avoid hammering Apify
            if i > 0 and i % max_workers == 0:
                time.sleep(1)
            futures[executor.submit(_enrich_one, username)] = username

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                username = futures[future]
                with lock:
                    completed[0] += 1
                    if progress_callback:
                        progress_callback(completed[0], total, username, False)

    return results
