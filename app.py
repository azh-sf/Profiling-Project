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


# ─── Batch history ───────────────────────────────────────────────────────────

def save_batch_to_history(df):
    if 'batch_history' not in st.session_state:
        st.session_state['batch_history'] = []
    now = datetime.now()
    tier_counts = df['tier'].value_counts().to_dict()
    st.session_state['batch_history'].append({
        'label': now.strftime('%d %b %Y %H:%M') + f" — {len(df)} profiles",
        'timestamp': now.isoformat(),
        'profile_count': len(df),
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

def get_secret(key):
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return ""

APIFY_TOKEN = get_secret("APIFY_TOKEN")
ANTHROPIC_API_KEY = get_secret("ANTHROPIC_API_KEY")


# ─── Header ──────────────────────────────────────────────────────────────────

st.title("Stellar Fusion — Investor Outreach Pipeline")
st.markdown(
    "Upload a CSV or paste LinkedIn profiles. The pipeline enriches career data, "
    "tiers profiles, and generates personalised outreach messages."
)


# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Settings")

    apify_token = st.text_input(
        "Apify API Token", value=APIFY_TOKEN, type="password",
    )
    anthropic_key = st.text_input(
        "Anthropic API Key", value=ANTHROPIC_API_KEY, type="password",
    )

    st.divider()
    st.subheader("Pipeline Options")

    skip_enrichment = st.checkbox(
        "Skip enrichment (pre-enriched data)", value=False,
    )
    generate_msgs = st.checkbox(
        "Generate messages", value=True,
        help="Uncheck to only enrich and tier (no Claude cost)",
    )

    st.divider()
    st.caption("Enrichment: Apify (5 parallel workers)")
    st.caption("Tiering: Python regex (instant)")
    st.caption("Messaging: Claude Opus 4.6 (3 parallel workers)")

    # ── Previous Batches ─────────────────────────────────────────
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
                    "CSV", data=batch['csv'],
                    file_name=f"outreach_{batch['timestamp'][:10]}.csv",
                    mime="text/csv", key=f"hist_dl_{i}",
                )
            with col_view:
                if st.button("View", key=f"hist_view_{i}"):
                    st.session_state['results_df'] = batch['df']
                    st.session_state['pipeline_complete'] = True
                    st.rerun()


# ─── Input ───────────────────────────────────────────────────────────────────

st.header("1. Input LinkedIn Profiles")

input_method = st.radio(
    "Input method:", ["Paste usernames", "Upload CSV"], horizontal=True,
)

usernames = []

if input_method == "Paste usernames":
    text_input = st.text_area(
        "LinkedIn usernames or URLs (one per line)", height=200,
        placeholder="john-smith-123\nhttps://www.linkedin.com/in/jane-doe-456/",
    )
    if text_input:
        usernames = parse_input_usernames(text_input)
        st.info(f"Parsed **{len(usernames)}** usernames")

else:
    uploaded = st.file_uploader(
        "Upload CSV with LinkedIn URLs",
        type=["csv"],
        help="CSV must have a column with LinkedIn URLs or usernames (linkedin_url, url, etc.)",
    )
    if uploaded:
        usernames = parse_csv_usernames(uploaded)

        # Preview
        if usernames:
            st.success(f"Found **{len(usernames)}** LinkedIn profiles in CSV")

            # Estimate time
            enrich_time = len(usernames) * 15 / 5  # 15s per profile, 5 parallel
            msg_time = len(usernames) * 5 / 3  # 5s per profile, 3 parallel
            total_est = (enrich_time + msg_time) / 60
            st.info(f"Estimated processing time: **~{total_est:.0f} minutes** "
                    f"(enrichment ~{enrich_time/60:.0f}min + messaging ~{msg_time/60:.0f}min)")

            with st.expander(f"Preview first 10 of {len(usernames)} profiles"):
                preview_df = pd.DataFrame({
                    'Username': usernames[:10],
                    'LinkedIn URL': [f'linkedin.com/in/{u}' for u in usernames[:10]],
                })
                st.dataframe(preview_df, use_container_width=True)
                if len(usernames) > 10:
                    st.caption(f"... and {len(usernames) - 10} more")


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
        if st.button(
            f"Process {len(usernames)} profiles",
            type="primary",
            help=f"Enriches, tiers, and generates messages for all {len(usernames)} profiles",
        ):
            start_time = datetime.now()

            # ── Stage 1: Enrichment ──────────────────────────────────
            if not skip_enrichment:
                st.subheader("Stage 1: Enriching profiles (5 parallel workers)")
                enrich_progress = st.progress(0, text="Starting enrichment...")
                enrich_stats = st.empty()
                success_count = [0]
                fail_count = [0]

                def enrich_callback(current, total, username, success):
                    enrich_progress.progress(
                        current / total,
                        text=f"Enriching {current}/{total}...",
                    )
                    if success:
                        success_count[0] += 1
                    else:
                        fail_count[0] += 1
                    enrich_stats.text(
                        f"Success: {success_count[0]} | Failed: {fail_count[0]} | "
                        f"Remaining: {total - current}"
                    )

                enriched = enrich_profiles(
                    usernames, apify_token,
                    progress_callback=enrich_callback,
                    max_workers=5,
                )
                enrich_progress.progress(1.0, text="Enrichment complete")
                st.success(
                    f"Enriched **{len(enriched)}/{len(usernames)}** profiles "
                    f"({fail_count[0]} failed)"
                )
            else:
                st.info("Skipping enrichment")
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

                cols = st.columns(5)
                cols[0].metric("Tier 1", tier_counts.get('1', 0))
                cols[1].metric("Tier 2", tier_counts.get('2', 0))
                cols[2].metric("Tier 3", tier_counts.get('3', 0))
                cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))
                cust_count = sum(
                    1 for t in tiered.values()
                    if t.get('customer_exclusion_flag') == 'YES'
                )
                cols[4].metric("Customers", cust_count)

                # ── Stage 3: Messaging ───────────────────────────────
                if generate_msgs:
                    eligible_count = sum(
                        1 for t in tiered.values()
                        if t.get('tier') != 'Out of Scope'
                        and t.get('customer_exclusion_flag') != 'YES'
                    )
                    st.subheader(
                        f"Stage 3: Generating messages for {eligible_count} "
                        f"eligible profiles (3 parallel workers)"
                    )
                    msg_progress = st.progress(0, text="Starting message generation...")

                    def msg_callback(current, total, username):
                        msg_progress.progress(
                            current / total,
                            text=f"Generating messages {current}/{total}...",
                        )

                    messages = generate_messages(
                        enriched, tiered, anthropic_key,
                        progress_callback=msg_callback,
                        max_workers=3,
                    )
                    msg_progress.progress(1.0, text="Message generation complete")
                    st.success(f"Generated messages for **{eligible_count}** profiles")
                else:
                    messages = {}

                # ── Finish ───────────────────────────────────────────
                elapsed = (datetime.now() - start_time).total_seconds()
                st.info(f"Pipeline complete in **{elapsed/60:.1f} minutes** ({len(enriched)} profiles)")

                df = build_results_dataframe(
                    list(enriched.keys()), enriched, tiered, messages
                )
                st.session_state['results_df'] = df
                st.session_state['pipeline_complete'] = True
                save_batch_to_history(df)

            else:
                st.warning("No profiles enriched. Check your Apify token and usernames.")

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
    st.dataframe(df[summary_cols], use_container_width=True, height=400)

    # Tier stats
    tier_counts = df['tier'].value_counts().to_dict()
    cols = st.columns(4)
    cols[0].metric("Tier 1", tier_counts.get('1', 0))
    cols[1].metric("Tier 2", tier_counts.get('2', 0))
    cols[2].metric("Tier 3", tier_counts.get('3', 0))
    cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))

    # Expandable detail
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
                    "Connection Request", "Follow-Up",
                    "Reengage (Previous)", "Reengage (Cold)",
                    "Email (Detailed)", "Email (Forwardable)",
                ])
                msg_cols = [
                    'msg_connection_request', 'msg_follow_up_accepted',
                    'msg_reengage_previous', 'msg_reengage_cold',
                    'msg_email_detailed', 'msg_email_forwardable',
                ]
                for tab, col_name in zip(tabs, msg_cols):
                    with tab:
                        msg = row.get(col_name, '')
                        if msg:
                            st.text_area(
                                "Copy:", value=msg, height=200,
                                key=f"msg_{idx}_{col_name}",
                            )
                            if col_name == 'msg_connection_request':
                                st.caption(f"{len(msg)}/300 chars")

    # ── Downloads ────────────────────────────────────────────────────
    st.header("4. Download")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.download_button(
            "Download Full CSV",
            data=dataframe_to_csv(df),
            file_name="stellar_fusion_outreach_full.csv",
            mime="text/csv", type="primary",
        )

    with col2:
        df_action = df[df['send_recommendation'].isin(['Send Now', 'Edit Before Sending'])]
        if not df_action.empty:
            st.download_button(
                f"Actionable Only ({len(df_action)})",
                data=dataframe_to_csv(df_action),
                file_name="stellar_fusion_outreach_actionable.csv",
                mime="text/csv",
            )

    with col3:
        df_t1 = df[df['tier'] == '1']
        if not df_t1.empty:
            st.download_button(
                f"Tier 1 Only ({len(df_t1)})",
                data=dataframe_to_csv(df_t1),
                file_name="stellar_fusion_outreach_tier1.csv",
                mime="text/csv",
            )

    if st.button("Clear results and start over"):
        del st.session_state['results_df']
        del st.session_state['pipeline_complete']
        st.rerun()


# ─── Footer ──────────────────────────────────────────────────────────────────

st.divider()
st.caption(
    "Stellar Fusion Group Limited | Enrichment: Apify | "
    "Tiering: Python | Messaging: Claude Opus 4.6"
)
