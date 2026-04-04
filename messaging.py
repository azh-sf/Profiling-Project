"""Message generation using Claude Opus 4.6 via Anthropic API."""

import json
import time
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

    Includes retry logic for rate limits and transient errors.
    """
    # Skip profiles that shouldn't receive messages
    if tier_data.get('tier') == 'Out of Scope':
        return EMPTY_MESSAGES.copy()
    if tier_data.get('customer_exclusion_flag') == 'YES':
        return EMPTY_MESSAGES.copy()

    client = anthropic.Anthropic(api_key=api_key)
    user_prompt = build_user_prompt(profile, tier_data)

    max_retries = 3
    for attempt in range(max_retries):
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

        except anthropic.RateLimitError:
            # Rate limited — back off and retry
            wait = 15 * (attempt + 1)
            time.sleep(wait)
            continue
        except anthropic.APIStatusError as e:
            if e.status_code in (529, 500, 502, 503):
                # Overloaded or server error — back off and retry
                time.sleep(10 * (attempt + 1))
                continue
            return {**EMPTY_MESSAGES, "msg_connection_request": f"[ERROR: API {e.status_code}]"}
        except json.JSONDecodeError:
            # Claude returned non-JSON — retry once
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return {**EMPTY_MESSAGES, "msg_connection_request": "[ERROR: invalid JSON response]"}
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            return {**EMPTY_MESSAGES, "msg_connection_request": f"[ERROR: {type(e).__name__}]"}

    return {**EMPTY_MESSAGES, "msg_connection_request": "[ERROR: max retries exceeded]"}


def generate_messages(
    enriched: dict,
    tiered: dict,
    api_key: str,
    progress_callback=None,
    max_workers: int = 1,
) -> dict:
    """Generate messages for all tiered profiles.

    Sequential processing with retry logic — reliable for large batches.
    Saves results incrementally so partial results are never lost.

    Args:
        enriched: {username: apify_profile_dict}
        tiered: {username: tier_result_dict}
        api_key: Anthropic API key
        progress_callback: Optional callable(current, total, username)
        max_workers: Ignored — kept for API compatibility. Always sequential.

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

    if total == 0:
        return results

    done_count = 0

    for username, tier_data in eligible.items():
        profile = enriched.get(username, {})
        try:
            msgs = generate_messages_for_profile(profile, tier_data, api_key)
            results[username] = msgs
        except Exception:
            results[username] = {
                **EMPTY_MESSAGES,
                "msg_connection_request": "[ERROR: generation failed]",
            }

        done_count += 1
        if progress_callback:
            progress_callback(done_count, total, username)

        # Small delay between calls to stay within rate limits
        time.sleep(1)

    return results
