"""Google Sheets integration for persisting batch results."""

import json
import gspread
import pandas as pd
from datetime import datetime

SHEET_URL = "https://docs.google.com/spreadsheets/d/15v1T-LOS2MjFyebG4zVJv3nhhP_XMsoWuN5VY8PL2iY"


def _get_client(streamlit_secrets):
    """Create gspread client from Streamlit secrets."""
    try:
        creds_dict = json.loads(streamlit_secrets["GOOGLE_SERVICE_ACCOUNT"])
        return gspread.service_account_from_dict(creds_dict)
    except (KeyError, json.JSONDecodeError):
        return None


def save_batch_to_sheets(df, streamlit_secrets, stage="full"):
    """Save a DataFrame as a new tab in the Google Sheet.

    Args:
        df: Results DataFrame
        streamlit_secrets: Streamlit secrets object
        stage: 'tiering' for tiering-only, 'full' for tiering + messages
    """
    client = _get_client(streamlit_secrets)
    if client is None:
        return False, "Google Sheets not configured (missing GOOGLE_SERVICE_ACCOUNT secret)"

    try:
        sh = client.open_by_url(SHEET_URL)

        # Create tab name with timestamp
        now = datetime.now()
        tab_name = now.strftime('%d-%b %H:%M') + f" {stage} ({len(df)})"

        # Truncate to 100 chars (Sheets tab name limit)
        tab_name = tab_name[:100]

        # Create new worksheet
        ws = sh.add_worksheet(title=tab_name, rows=len(df) + 1, cols=len(df.columns))

        # Write headers + data
        # Convert all values to strings to avoid serialization issues
        headers = df.columns.tolist()
        rows = df.fillna('').astype(str).values.tolist()

        ws.update([headers] + rows)

        return True, tab_name

    except Exception as e:
        return False, f"Google Sheets error: {type(e).__name__}: {str(e)[:200]}"


def get_batch_history(streamlit_secrets):
    """Get list of previous batch tabs from the Google Sheet.

    Returns:
        List of dicts with 'name' and 'url' for each tab
    """
    client = _get_client(streamlit_secrets)
    if client is None:
        return []

    try:
        sh = client.open_by_url(SHEET_URL)
        tabs = []
        for ws in sh.worksheets():
            if ws.title == 'Sheet1':
                continue
            tabs.append({
                'name': ws.title,
                'url': f"{SHEET_URL}#gid={ws.id}",
            })
        return list(reversed(tabs))  # Most recent first
    except Exception:
        return []
