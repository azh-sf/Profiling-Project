"""Stellar Fusion Investor Outreach Pipeline — Streamlit App."""

import streamlit as st
import pandas as pd
from datetime import datetime

from utils import (
    parse_input_usernames,
    parse_csv_usernames,
    build_results_dataframe,
    dataframe_to_csv,
)
from enrichment import enrich_profiles
from tiering import tier_profiles
from messaging import generate_messages


# ─── Batch history helpers ───────────────────────────────────────────────────

def save_batch_to_history(df: pd.DataFrame):
    """Save a completed batch to session_state history."""
    if 'batch_history' not in st.session_state:
        st.session_state['batch_history'] = []

    now = datetime.now()
    profile_count = len(df)
    tier_counts = df['tier'].value_counts().to_dict()
    label = now.strftime('%d %b %Y %H:%M') + f" — {profile_count} profiles"

    st.session_state['batch_history'].append({
        'label': label,
        'timestamp': now.isoformat(),
        'profile_count': profile_count,
        'tier_1': tier_counts.get('1', 0),
        'tier_2': tier_counts.get('2', 0),
        'tier_3': tier_counts.get('3', 0),
        'oos': tier_counts.get('Out of Scope', 0),
        'csv': dataframe_to_csv(df),
        'df': df,
    })

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Stellar Fusion — Investor Outreach",
    page_icon="*",
    layout="wide",
)

# ─── Secrets ─────────────────────────────────────────────────────────────────

def get_secret(key: str) -> str:
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return ""

APIFY_TOKEN = get_secret("APIFY_TOKEN")
ANTHROPIC_API_KEY = get_secret("ANTHROPIC_API_KEY")

# ─── Header ──────────────────────────────────────────────────────────────────

st.title("Stellar Fusion — Investor Outreach Pipeline")
st.markdown(
    "Input LinkedIn profiles, get enriched career data, tiering, and "
    "personalised outreach messages in one step."
)

# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")

    apify_token = st.text_input(
        "Apify API Token",
        value=APIFY_TOKEN,
        type="password",
        help="Your Apify API token for LinkedIn enrichment",
    )
    anthropic_key = st.text_input(
        "Anthropic API Key",
        value=ANTHROPIC_API_KEY,
        type="password",
        help="Your Anthropic API key for message generation (Opus 4.6)",
    )

    st.divider()
    st.subheader("Pipeline Options")

    skip_enrichment = st.checkbox(
        "Skip enrichment (use pre-enriched data)",
        value=False,
        help="Check if you're uploading profiles that are already enriched",
    )

    generate_msgs = st.checkbox(
        "Generate messages",
        value=True,
        help="Uncheck to only enrich and tier (no Claude API cost)",
    )

    st.divider()
    st.caption("Enrichment: Apify Full Sections Scraper")
    st.caption("Tiering: Deterministic Python (no AI cost)")
    st.caption("Messaging: Claude Opus 4.6")

    # ── Previous Batches ─────────────────────────────────────────────
    if st.session_state.get('batch_history'):
        st.divider()
        st.subheader("Previous Batches")
        for i, batch in enumerate(reversed(st.session_state['batch_history'])):
            st.markdown(
                f"**{batch['label']}**  \n"
                f"T1: {batch['tier_1']} | T2: {batch['tier_2']} | "
                f"T3: {batch['tier_3']} | OOS: {batch['oos']}"
            )
            col_dl, col_view = st.columns(2)
            with col_dl:
                st.download_button(
                    "CSV",
                    data=batch['csv'],
                    file_name=f"outreach_batch_{batch['timestamp'][:10]}.csv",
                    mime="text/csv",
                    key=f"hist_dl_{i}",
                )
            with col_view:
                if st.button("View", key=f"hist_view_{i}"):
                    st.session_state['results_df'] = batch['df']
                    st.session_state['pipeline_complete'] = True
                    st.rerun()

# ─── Input ───────────────────────────────────────────────────────────────────

st.header("1. Input LinkedIn Profiles")

input_method = st.radio(
    "Input method:",
    ["Paste usernames", "Upload CSV"],
    horizontal=True,
)

usernames = []

if input_method == "Paste usernames":
    text_input = st.text_area(
        "LinkedIn usernames or URLs (one per line)",
        height=200,
        placeholder="john-smith-123\nhttps://www.linkedin.com/in/jane-doe-456/\npierre-safa-76b51829",
    )
    if text_input:
        usernames = parse_input_usernames(text_input)
        st.info(f"Parsed {len(usernames)} usernames")

else:
    uploaded = st.file_uploader("Upload CSV with LinkedIn URLs", type=["csv"])
    if uploaded:
        usernames = parse_csv_usernames(uploaded)
        st.info(f"Found {len(usernames)} usernames in CSV")

# ─── Run Pipeline ────────────────────────────────────────────────────────────

if usernames:
    st.header("2. Run Pipeline")

    errors = []
    if not apify_token and not skip_enrichment:
        errors.append("Apify API Token required for enrichment")
    if not anthropic_key and generate_msgs:
        errors.append("Anthropic API Key required for message generation")

    if errors:
        for e in errors:
            st.error(e)
    else:
        if st.button(f"Process {len(usernames)} profiles", type="primary"):
            # ── Stage 1: Enrichment ──────────────────────────────────
            if not skip_enrichment:
                st.subheader("Stage 1: Enriching profiles via Apify")
                enrich_progress = st.progress(0, text="Starting enrichment...")
                enrich_status = st.empty()

                def enrich_callback(current, total, username, success):
                    enrich_progress.progress(
                        current / total,
                        text=f"Enriching {current}/{total}: {username}",
                    )
                    status = "OK" if success else "FAILED"
                    enrich_status.text(f"  {username}: {status}")

                enriched = enrich_profiles(
                    usernames, apify_token, progress_callback=enrich_callback
                )
                enrich_progress.progress(1.0, text="Enrichment complete")

                success_count = len(enriched)
                fail_count = len(usernames) - success_count
                st.success(f"Enriched {success_count}/{len(usernames)} profiles")
                if fail_count > 0:
                    st.warning(f"{fail_count} profiles failed enrichment")
            else:
                st.info("Skipping enrichment (using pre-enriched data)")
                enriched = {}

            # ── Stage 2: Tiering ─────────────────────────────────────
            if enriched:
                st.subheader("Stage 2: Tiering profiles")
                with st.spinner("Running tiering engine..."):
                    tiered = tier_profiles(enriched)

                tier_counts = {}
                for t in tiered.values():
                    tier = t.get('tier', 'Unknown')
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1

                cols = st.columns(4)
                cols[0].metric("Tier 1", tier_counts.get('1', 0))
                cols[1].metric("Tier 2", tier_counts.get('2', 0))
                cols[2].metric("Tier 3", tier_counts.get('3', 0))
                cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))

                cust_count = sum(
                    1 for t in tiered.values()
                    if t.get('customer_exclusion_flag') == 'YES'
                )
                if cust_count > 0:
                    st.warning(f"{cust_count} customer exclusion(s) detected")

                # ── Stage 3: Messaging ───────────────────────────────
                if generate_msgs:
                    eligible_count = sum(
                        1 for t in tiered.values()
                        if t.get('tier') != 'Out of Scope'
                        and t.get('customer_exclusion_flag') != 'YES'
                    )
                    st.subheader(f"Stage 3: Generating messages for {eligible_count} eligible profiles")
                    msg_progress = st.progress(0, text="Starting message generation...")

                    def msg_callback(current, total, username):
                        msg_progress.progress(
                            current / total,
                            text=f"Generating messages {current}/{total}: {username}",
                        )

                    messages = generate_messages(
                        enriched, tiered, anthropic_key, progress_callback=msg_callback
                    )
                    msg_progress.progress(1.0, text="Message generation complete")
                    st.success(f"Generated messages for {eligible_count} profiles")
                else:
                    messages = {}

                # ── Store results in session state + history ─────────
                df = build_results_dataframe(
                    list(enriched.keys()), enriched, tiered, messages
                )
                st.session_state['results_df'] = df
                st.session_state['pipeline_complete'] = True
                save_batch_to_history(df)

            else:
                st.warning("No profiles were enriched successfully. Check your Apify token and LinkedIn usernames.")

elif not st.session_state.get('pipeline_complete'):
    st.info("Enter LinkedIn usernames above to get started.")

# ─── Results (persisted via session_state) ───────────────────────────────────

if st.session_state.get('pipeline_complete') and 'results_df' in st.session_state:
    df = st.session_state['results_df']

    st.header("3. Results")

    # Summary table
    summary_cols = [
        'full_name', 'current_title', 'current_company',
        'tier', 'priority_score', 'priority_bucket',
        'investor_or_customer', 'send_recommendation',
    ]
    st.dataframe(
        df[summary_cols],
        use_container_width=True,
        height=400,
    )

    # Tier stats
    tier_counts = df['tier'].value_counts().to_dict()
    cols = st.columns(4)
    cols[0].metric("Tier 1", tier_counts.get('1', 0))
    cols[1].metric("Tier 2", tier_counts.get('2', 0))
    cols[2].metric("Tier 3", tier_counts.get('3', 0))
    cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))

    # Expandable detail per profile
    st.subheader("Profile Details")
    for idx, row in df.iterrows():
        tier_label = f"Tier {row['tier']}" if row['tier'] in ('1', '2', '3') else row['tier']
        with st.expander(
            f"{row['full_name']} — {tier_label} | Score: {row['priority_score']} | {row['send_recommendation']}"
        ):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Tiering**")
                st.text(f"Tier: {row['tier']} ({row['tier_confidence']})")
                st.text(f"Score: {row['priority_score']} ({row['priority_bucket']})")
                st.text(f"Type: {row['investor_or_customer']}")
                st.text(f"Summary: {row['investor_fit_summary']}")
                st.text(f"Rationale: {row['rationale_for_tier']}")
                if row['notes_for_review']:
                    st.warning(f"Notes: {row['notes_for_review']}")

            with col2:
                st.markdown("**Career Signals**")
                st.text(row.get('key_career_signals', ''))

            if row['msg_connection_request']:
                st.markdown("---")
                st.markdown("**Messages**")
                tabs = st.tabs([
                    "Connection Request",
                    "Follow-Up",
                    "Reengage (Previous)",
                    "Reengage (Cold)",
                    "Email (Detailed)",
                    "Email (Forwardable)",
                ])
                msg_cols = [
                    'msg_connection_request',
                    'msg_follow_up_accepted',
                    'msg_reengage_previous',
                    'msg_reengage_cold',
                    'msg_email_detailed',
                    'msg_email_forwardable',
                ]
                for tab, col_name in zip(tabs, msg_cols):
                    with tab:
                        msg = row.get(col_name, '')
                        if msg:
                            st.text_area(
                                "Copy message:",
                                value=msg,
                                height=200,
                                key=f"msg_{idx}_{col_name}",
                            )
                            if col_name == 'msg_connection_request':
                                st.caption(f"Length: {len(msg)}/300 chars")

    # ── Downloads ────────────────────────────────────────────────────────
    st.header("4. Download")

    col1, col2, col3 = st.columns(3)

    with col1:
        csv_all = dataframe_to_csv(df)
        st.download_button(
            "Download Full CSV",
            data=csv_all,
            file_name="stellar_fusion_outreach_full.csv",
            mime="text/csv",
            type="primary",
        )

    with col2:
        df_actionable = df[df['send_recommendation'].isin(['Send Now', 'Edit Before Sending'])]
        if not df_actionable.empty:
            csv_action = dataframe_to_csv(df_actionable)
            st.download_button(
                f"Download Actionable Only ({len(df_actionable)})",
                data=csv_action,
                file_name="stellar_fusion_outreach_actionable.csv",
                mime="text/csv",
            )

    with col3:
        df_t1 = df[df['tier'] == '1']
        if not df_t1.empty:
            csv_t1 = dataframe_to_csv(df_t1)
            st.download_button(
                f"Download Tier 1 Only ({len(df_t1)})",
                data=csv_t1,
                file_name="stellar_fusion_outreach_tier1.csv",
                mime="text/csv",
            )

    # Clear results button
    if st.button("Clear results and start over"):
        del st.session_state['results_df']
        del st.session_state['pipeline_complete']
        st.rerun()

# ─── Footer ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Stellar Fusion Group Limited | Investor Outreach Pipeline | "
    "Enrichment: Apify | Tiering: Python | Messaging: Claude Opus 4.6"
)
