"""Utility functions for LinkedIn URL cleaning, CSV parsing, and data helpers."""

import re
import csv
import io
import pandas as pd


def clean_linkedin_username(raw: str) -> str:
    """Extract LinkedIn public identifier from a URL or raw username.

    Handles:
      - Full URLs: https://www.linkedin.com/in/john-smith-123/
      - Partial URLs: linkedin.com/in/john-smith-123
      - Raw usernames: john-smith-123
    """
    raw = raw.strip()
    if not raw:
        return ""
    # Strip URL prefix (with or without protocol)
    raw = re.sub(r'^(https?://)?(www\.)?linkedin\.com/in/', '', raw)
    # Strip trailing slash and query params
    raw = re.sub(r'[/?#].*$', '', raw)
    return raw


def parse_input_usernames(text: str) -> list[str]:
    """Parse a block of text into a list of cleaned LinkedIn usernames.

    Accepts one username/URL per line. Skips blanks and comments (#).
    """
    usernames = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        username = clean_linkedin_username(line)
        if username:
            usernames.append(username)
    return usernames


def parse_csv_usernames(uploaded_file) -> list[str]:
    """Extract LinkedIn usernames from an uploaded CSV file.

    Looks for columns: linkedin_url, linkedin, url, public_identifier, username.
    Falls back to the first column if none match.
    """
    df = pd.read_csv(uploaded_file)
    col_candidates = ['linkedin_url', 'linkedin', 'url', 'public_identifier', 'username']
    col = None
    for c in col_candidates:
        matches = [dc for dc in df.columns if dc.strip().lower() == c]
        if matches:
            col = matches[0]
            break
    if col is None:
        col = df.columns[0]

    usernames = []
    for val in df[col].dropna():
        username = clean_linkedin_username(str(val))
        if username:
            usernames.append(username)
    return usernames


def profile_display_name(profile: dict) -> str:
    """Extract display name from an Apify profile dict."""
    bi = profile.get('basic_info', {})
    first = bi.get('first_name', '')
    last = bi.get('last_name', '')
    name = f"{first} {last}".strip()
    return name or bi.get('public_identifier', 'Unknown')


def profile_current_role(profile: dict) -> tuple[str, str]:
    """Return (current_title, current_company) from Apify profile."""
    for exp in profile.get('experience', []):
        if exp.get('is_current', False):
            return (exp.get('title', ''), exp.get('company', ''))
    return ('', '')


def profile_location(profile: dict) -> str:
    """Return location from Apify profile."""
    return profile.get('basic_info', {}).get('location', '')


def build_results_dataframe(
    usernames: list[str],
    enriched: dict,
    tiered: dict,
    messages: dict,
) -> pd.DataFrame:
    """Combine enrichment, tiering, and messaging into a single output DataFrame.

    Args:
        usernames: Original input usernames
        enriched: {username: apify_profile_dict}
        tiered: {username: tier_result_dict}
        messages: {username: message_dict}
    """
    rows = []
    for u in usernames:
        profile = enriched.get(u, {})
        tier_data = tiered.get(u, {})
        msg_data = messages.get(u, {})

        name = profile_display_name(profile)
        title, company = profile_current_role(profile)
        location = profile_location(profile)

        row = {
            'full_name': name,
            'linkedin_url': f'https://www.linkedin.com/in/{u}',
            'current_title': title,
            'current_company': company,
            'location': location,
            # Tiering columns
            'tier': tier_data.get('tier', ''),
            'tier_confidence': tier_data.get('tier_confidence', ''),
            'investor_or_customer': tier_data.get('investor_or_customer', ''),
            'priority_score': tier_data.get('priority_score', 0),
            'priority_bucket': tier_data.get('priority_bucket', ''),
            'investor_fit_summary': tier_data.get('investor_fit_summary', ''),
            'rationale_for_tier': tier_data.get('rationale_for_tier', ''),
            'key_career_signals': tier_data.get('key_career_signals', ''),
            'customer_exclusion_flag': tier_data.get('customer_exclusion_flag', ''),
            # Message columns
            'msg_connection_request': msg_data.get('msg_connection_request', ''),
            'msg_follow_up_accepted': msg_data.get('msg_follow_up_accepted', ''),
            'msg_reengage_previous': msg_data.get('msg_reengage_previous', ''),
            'msg_reengage_cold': msg_data.get('msg_reengage_cold', ''),
            'msg_email_detailed': msg_data.get('msg_email_detailed', ''),
            'msg_email_forwardable': msg_data.get('msg_email_forwardable', ''),
            # Review
            'notes_for_review': tier_data.get('notes_for_review', ''),
            'send_recommendation': _derive_send_rec(tier_data),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df.sort_values('priority_score', ascending=False).reset_index(drop=True)
    return df


def _derive_send_rec(tier_data: dict) -> str:
    """Derive send recommendation from tier and score."""
    tier = tier_data.get('tier', '')
    bucket = tier_data.get('priority_bucket', '')
    cust = tier_data.get('customer_exclusion_flag', '')

    if cust == 'YES':
        return 'Do Not Send'
    if tier == 'Out of Scope':
        return 'Do Not Send'
    if bucket == 'High':
        return 'Send Now'
    if bucket == 'Medium':
        return 'Edit Before Sending'
    return 'Do Not Send'


def dataframe_to_csv(df: pd.DataFrame) -> str:
    """Convert DataFrame to CSV string for download."""
    return df.to_csv(index=False)
