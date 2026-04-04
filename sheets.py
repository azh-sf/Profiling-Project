"""Google Sheets integration for persisting batch results."""

import json
import gspread
import pandas as pd
from datetime import datetime

SHEET_URL = "https://docs.google.com/spreadsheets/d/15v1T-LOS2MjFyebG4zVJv3nhhP_XMsoWuN5VY8PL2iY"


def _get_client(streamlit_secrets):
    """Create gspread client from Streamlit secrets."""
    try:
        # Try as TOML table first (Streamlit native format)
        creds_dict = dict(streamlit_secrets["GOOGLE_SERVICE_ACCOUNT"])
        return gspread.service_account_from_dict(creds_dict)
    except (KeyError, TypeError):
        pass
    try:
        # Fallback: try as JSON string
        creds_dict = json.loads(streamlit_secrets["GOOGLE_SERVICE_ACCOUNT"])
        return gspread.service_account_from_dict(creds_dict)
    except (KeyError, json.JSONDecodeError, TypeError):
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
        # Convert all values to strings, truncate long cells to avoid API limits
        headers = df.columns.tolist()
        rows = []
        for _, row in df.iterrows():
            row_data = []
            for val in row:
                s = str(val) if val is not None else ''
                # Google Sheets cell limit is 50K chars, but keep reasonable for API payload
                if len(s) > 5000:
                    s = s[:5000] + '...'
                row_data.append(s)
            rows.append(row_data)

        # Write in chunks to avoid 10MB request limit
        chunk_size = 50
        # Write headers first
        ws.update('A1', [headers])

        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            start_row = i + 2  # +2 because row 1 is headers, Sheets is 1-indexed
            ws.update(f'A{start_row}', chunk)

        return True, tab_name

    except Exception as e:
        return False, f"Google Sheets error: {type(e).__name__}: {str(e)[:200]}"


def get_batches_needing_messages(streamlit_secrets):
    """Find tiering batches in Sheets that have profiles missing messages.

    Returns:
        List of dicts: {'name', 'tab_id', 'total', 'missing_msgs', 'df'}
    """
    client = _get_client(streamlit_secrets)
    if client is None:
        return []

    try:
        sh = client.open_by_url(SHEET_URL)
        results = []
        for ws in sh.worksheets():
            if ws.title == 'Sheet1':
                continue
            # Only check tiering tabs (they have profiles without messages)
            try:
                data = ws.get_all_records()
                if not data:
                    continue
                df = pd.DataFrame(data)

                # Must have tiering columns
                if 'tier' not in df.columns or 'linkedin_url' not in df.columns:
                    continue

                # Check for eligible profiles missing messages
                # Sheets may return tier as int or string
                msg_col = 'msg_connection_request'
                df['tier'] = df['tier'].astype(str)
                eligible = df[
                    df['tier'].isin(['1', '2', '3'])
                    & (df.get('customer_exclusion_flag', pd.Series(dtype=str)).astype(str) != 'YES')
                ]

                if msg_col in df.columns:
                    missing = eligible[eligible[msg_col].fillna('').str.strip().eq('')]
                else:
                    missing = eligible

                if len(missing) > 0:
                    results.append({
                        'name': ws.title,
                        'tab_id': ws.id,
                        'total': len(df),
                        'eligible': len(eligible),
                        'missing_msgs': len(missing),
                        'df': df,
                    })
            except Exception:
                continue

        return results
    except Exception:
        return []


def update_sheet_tab(df, streamlit_secrets, tab_name):
    """Update an existing tab in the Google Sheet with new data."""
    client = _get_client(streamlit_secrets)
    if client is None:
        return False, "Not configured"

    try:
        sh = client.open_by_url(SHEET_URL)
        ws = sh.worksheet(tab_name)

        headers = df.columns.tolist()
        rows = []
        for _, row in df.iterrows():
            row_data = []
            for val in row:
                s = str(val) if val is not None else ''
                if len(s) > 5000:
                    s = s[:5000] + '...'
                row_data.append(s)
            rows.append(row_data)

        ws.clear()
        ws.update('A1', [headers])
        chunk_size = 50
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            ws.update(f'A{i + 2}', chunk)

        return True, tab_name
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:200]}"


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
