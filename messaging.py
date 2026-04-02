"""Message generation using Claude Opus 4.6 via Anthropic API."""

import json
import anthropic
from prompts import SYSTEM_PROMPT, build_user_prompt

MODEL = "claude-opus-4-6"
EMPTY_MESSAGES = {
    "msg_connection_request": "",
    "msg_follow_up_accepted": "",
    "msg_reengage_previous": "",
    "msg_reengage_cold": "",
    "msg_email_detailed": "",
    "msg_email_forwardable": "",
}
MESSAGE_KEYS = list(EMPTY_MESSAGES.keys())


def generate_messages_for_profile(
    profile: dict,
    tier_data: dict,
    api_key: str,
) -> dict:
    """Generate 6 personalised messages for a single profile.

    Args:
        profile: Full Apify profile dict
        tier_data: Tier result dict from tiering.py
        api_key: Anthropic API key

    Returns:
        Dict with 6 message keys, or empty messages on failure.
    """
    # Skip profiles that shouldn't receive messages
    if tier_data.get('tier') == 'Out of Scope':
        return EMPTY_MESSAGES.copy()
    if tier_data.get('customer_exclusion_flag') == 'YES':
        return EMPTY_MESSAGES.copy()

    client = anthropic.Anthropic(api_key=api_key)
    user_prompt = build_user_prompt(profile, tier_data)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        raw_text = response.content[0].text.strip()

        # Strip markdown fences if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("\n", 1)[1] if "\n" in raw_text else raw_text[3:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
        raw_text = raw_text.strip()

        messages = json.loads(raw_text)

        # Validate all keys present
        result = {}
        for key in MESSAGE_KEYS:
            result[key] = messages.get(key, "")

        # Enforce connection request length
        cr = result["msg_connection_request"]
        if len(cr) > 300:
            result["msg_connection_request"] = cr[:297] + "..."

        return result

    except (json.JSONDecodeError, anthropic.APIError, IndexError, KeyError) as e:
        return {**EMPTY_MESSAGES, "msg_connection_request": f"[ERROR: {type(e).__name__}]"}


def generate_messages(
    enriched: dict,
    tiered: dict,
    api_key: str,
    progress_callback=None,
) -> dict:
    """Generate messages for all tiered profiles.

    Args:
        enriched: {username: apify_profile_dict}
        tiered: {username: tier_result_dict}
        api_key: Anthropic API key
        progress_callback: Optional callable(current, total, username)

    Returns:
        {username: message_dict}
    """
    results = {}
    # Only generate for profiles that passed tiering
    eligible = {
        u: t for u, t in tiered.items()
        if t.get('tier') != 'Out of Scope' and t.get('customer_exclusion_flag') != 'YES'
    }
    total = len(eligible)
    skipped = set(tiered.keys()) - set(eligible.keys())

    # Fill skipped profiles with empty messages
    for u in skipped:
        results[u] = EMPTY_MESSAGES.copy()

    # Generate for eligible profiles
    for i, (username, tier_data) in enumerate(eligible.items()):
        profile = enriched.get(username, {})
        results[username] = generate_messages_for_profile(profile, tier_data, api_key)

        if progress_callback:
            progress_callback(i + 1, total, username)

    return results
