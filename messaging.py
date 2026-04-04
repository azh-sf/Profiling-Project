"""Message generation using Claude or Gemini API."""

import json
import time
from prompts import SYSTEM_PROMPT, build_user_prompt

EMPTY_MESSAGES = {
    "msg_connection_request": "",
    "msg_follow_up_accepted": "",
    "msg_reengage_previous": "",
    "msg_reengage_cold": "",
    "msg_email_detailed": "",
    "msg_email_forwardable": "",
}
MESSAGE_KEYS = list(EMPTY_MESSAGES.keys())

# Model configs
MODELS = {
    "Claude Opus 4.6": {"provider": "anthropic", "model_id": "claude-opus-4-6", "cost_per_profile": "$0.08-0.10"},
    "Claude Sonnet 4.6": {"provider": "anthropic", "model_id": "claude-sonnet-4-6-20250514", "cost_per_profile": "$0.05-0.06"},
    "Gemini 2.5 Pro": {"provider": "google", "model_id": "gemini-2.5-pro", "cost_per_profile": "$0.03-0.04"},
}


def _parse_response(raw_text):
    """Parse JSON response from any model, stripping markdown fences."""
    if raw_text.startswith("```"):
        raw_text = raw_text.split("\n", 1)[1] if "\n" in raw_text else raw_text[3:]
    if raw_text.endswith("```"):
        raw_text = raw_text[:-3]
    raw_text = raw_text.strip()

    messages = json.loads(raw_text)

    result = {}
    for key in MESSAGE_KEYS:
        result[key] = messages.get(key, "")

    cr = result["msg_connection_request"]
    if len(cr) > 300:
        result["msg_connection_request"] = cr[:297] + "..."

    return result


def _call_anthropic(user_prompt, api_key, model_id):
    """Call Claude API."""
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model_id,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return response.content[0].text.strip()


def _call_google(user_prompt, api_key, model_id):
    """Call Gemini API."""
    from google import genai
    client = genai.Client(api_key=api_key)
    # Gemini uses system_instruction instead of system message
    full_prompt = f"{SYSTEM_PROMPT}\n\n---\n\n{user_prompt}"
    response = client.models.generate_content(
        model=model_id,
        contents=full_prompt,
    )
    return response.text.strip()


def generate_messages_for_profile(
    profile,
    tier_data,
    api_key,
    model_name="Claude Opus 4.6",
    google_api_key=None,
):
    """Generate 6 personalised messages for a single profile.

    Supports Claude (Opus/Sonnet) and Gemini 2.5 Pro.
    """
    if tier_data.get('tier') == 'Out of Scope':
        return EMPTY_MESSAGES.copy()
    if tier_data.get('customer_exclusion_flag') == 'YES':
        return EMPTY_MESSAGES.copy()

    user_prompt = build_user_prompt(profile, tier_data)
    model_config = MODELS.get(model_name, MODELS["Claude Opus 4.6"])

    max_retries = 3
    for attempt in range(max_retries):
        try:
            if model_config["provider"] == "google":
                raw_text = _call_google(user_prompt, google_api_key, model_config["model_id"])
            else:
                raw_text = _call_anthropic(user_prompt, api_key, model_config["model_id"])

            return _parse_response(raw_text)

        except json.JSONDecodeError:
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return {**EMPTY_MESSAGES, "msg_connection_request": "[ERROR: invalid JSON response]"}
        except Exception as e:
            error_str = str(e).lower()
            if 'rate' in error_str or '429' in error_str or 'resource' in error_str:
                time.sleep(15 * (attempt + 1))
                continue
            if '529' in error_str or '500' in error_str or '503' in error_str or 'overloaded' in error_str:
                time.sleep(10 * (attempt + 1))
                continue
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            return {**EMPTY_MESSAGES, "msg_connection_request": f"[ERROR: {type(e).__name__}]"}

    return {**EMPTY_MESSAGES, "msg_connection_request": "[ERROR: max retries exceeded]"}


def generate_messages(
    enriched,
    tiered,
    api_key,
    progress_callback=None,
    max_workers=1,
    model_name="Claude Opus 4.6",
    google_api_key=None,
):
    """Generate messages for all tiered profiles. Sequential with retry logic."""
    results = {}
    eligible = {
        u: t for u, t in tiered.items()
        if t.get('tier') != 'Out of Scope' and t.get('customer_exclusion_flag') != 'YES'
    }
    total = len(eligible)
    skipped = set(tiered.keys()) - set(eligible.keys())

    for u in skipped:
        results[u] = EMPTY_MESSAGES.copy()

    if total == 0:
        return results

    done_count = 0

    for username, tier_data in eligible.items():
        profile = enriched.get(username, {})
        try:
            msgs = generate_messages_for_profile(
                profile, tier_data, api_key,
                model_name=model_name,
                google_api_key=google_api_key,
            )
            results[username] = msgs
        except Exception:
            results[username] = {
                **EMPTY_MESSAGES,
                "msg_connection_request": "[ERROR: generation failed]",
            }

        done_count += 1
        if progress_callback:
            progress_callback(done_count, total, username)

        time.sleep(1)

    return results
