"""Stellar Fusion Investor Outreach Pipeline — Streamlit App.

SHEETS RULE: Every write to Google Sheets creates a NEW tab. Never update existing tabs.
"""

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
from sheets import save_batch_to_sheets, get_batch_history


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


def _save_to_sheets(df, stage):
    """Save to Sheets — ALWAYS creates a new tab. Never updates."""
    ok, tab_name = save_batch_to_sheets(df, st.secrets, stage=stage)
    if ok:
        st.success(f"Saved to Google Sheets: **{tab_name}**")
    else:
        st.warning(f"Could not save to Sheets: {tab_name}")


# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Stellar Fusion — Investor Outreach",
    page_icon="*",
    layout="wide",
)

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

    apify_token = st.text_input("Apify API Token", value=APIFY_TOKEN, type="password")
    anthropic_key = st.text_input("Anthropic API Key", value=ANTHROPIC_API_KEY, type="password")
    google_key = st.text_input("Google AI API Key (for Gemini)", value=get_secret("GOOGLE_AI_KEY"), type="password")

    st.divider()
    st.subheader("Pipeline Options")

    from messaging import MODELS
    model_name = st.selectbox(
        "Messaging model",
        options=list(MODELS.keys()),
        index=0,
        help="Opus = highest quality, Gemini = cheapest, Sonnet = middle ground",
    )
    st.caption(f"Est. cost: {MODELS[model_name]['cost_per_profile']} per profile")

    skip_enrichment = st.checkbox("Skip enrichment (pre-enriched data)", value=False)
    generate_msgs = st.checkbox("Generate messages", value=True, help="Uncheck to only enrich and tier")

    st.divider()
    st.caption("Enrichment: Apify (5 parallel workers)")
    st.caption("Tiering: Python regex (instant)")
    st.caption(f"Messaging: {model_name}")

    # Session history
    if st.session_state.get('batch_history'):
        st.divider()
        st.subheader("This Session")
        for i, batch in enumerate(reversed(st.session_state['batch_history'])):
            st.markdown(
                f"**{batch['label']}**  \n"
                f"T1: {batch['tier_1']} | T2: {batch['tier_2']} | "
                f"T3: {batch['tier_3']} | OOS: {batch['oos']}"
            )
            st.download_button(
                "CSV", data=batch['csv'],
                file_name=f"outreach_{batch['timestamp'][:10]}.csv",
                mime="text/csv", key=f"hist_dl_{i}",
            )

    # Google Sheets history
    st.divider()
    st.subheader("All Batches (Google Sheets)")
    sheets_history = get_batch_history(st.secrets)
    if sheets_history:
        for tab in sheets_history[:10]:
            st.markdown(f"[{tab['name']}]({tab['url']})")
    else:
        st.caption("No batches saved yet")


# ─── Input ───────────────────────────────────────────────────────────────────

st.header("1. Input LinkedIn Profiles")

input_method = st.radio(
    "Input method:",
    ["Paste usernames", "Upload CSV", "Backfill messages from Sheets"],
    horizontal=True,
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

elif input_method == "Upload CSV":
    uploaded = st.file_uploader("Upload CSV with LinkedIn URLs", type=["csv"])
    if uploaded:
        usernames = parse_csv_usernames(uploaded)
        if usernames:
            st.success(f"Found **{len(usernames)}** LinkedIn profiles in CSV")

elif input_method == "Backfill messages from Sheets":
    from sheets import get_batches_needing_messages

    st.markdown("Scanning Google Sheets for batches with missing messages...")
    batches = get_batches_needing_messages(st.secrets)

    if not batches:
        st.success("All batches in Google Sheets have complete messages.")
    else:
        for batch in batches:
            st.markdown(
                f"**{batch['name']}** — "
                f"{batch['missing_msgs']} of {batch['eligible']} eligible profiles missing messages"
            )

        selected = st.selectbox(
            "Select batch to backfill:",
            options=range(len(batches)),
            format_func=lambda i: f"{batches[i]['name']} ({batches[i]['missing_msgs']} missing)",
        )

        if selected is not None:
            batch = batches[selected]
            prev_df = batch['df']
            prev_df['tier'] = prev_df['tier'].astype(str)

            from utils import clean_linkedin_username
            msg_col = 'msg_connection_request'
            eligible_mask = (
                prev_df['tier'].isin(['1', '2', '3'])
                & (prev_df.get('customer_exclusion_flag', pd.Series(dtype=str)).astype(str) != 'YES')
            )
            if msg_col in prev_df.columns:
                msg_vals = prev_df[msg_col].fillna('').astype(str).str.strip()
                missing_mask = eligible_mask & (msg_vals.eq('') | msg_vals.str.startswith('[ERROR'))
            else:
                missing_mask = eligible_mask

            usernames = [
                clean_linkedin_username(str(u))
                for u in prev_df.loc[missing_mask, 'linkedin_url'].dropna()
                if clean_linkedin_username(str(u))
            ]

            if usernames:
                st.info(f"**{len(usernames)} profiles** need enrichment + messaging.")
                st.session_state['_skip_tiering'] = True
                st.session_state['_prev_batch_df'] = prev_df


# ─── Run Pipeline ────────────────────────────────────────────────────────────

if usernames:
    st.header("2. Run Pipeline")

    errors = []
    if not apify_token and not skip_enrichment:
        errors.append("Apify API Token required for enrichment")
    if not anthropic_key and generate_msgs and 'Claude' in model_name:
        errors.append("Anthropic API Key required for Claude messaging")
    if not google_key and generate_msgs and 'Gemini' in model_name:
        errors.append("Google AI API Key required for Gemini messaging")

    if errors:
        for e in errors:
            st.error(e)
    else:
        if st.button(f"Process {len(usernames)} profiles", type="primary"):
            # Clear stale state
            for key in ['_enriched', '_tiered', '_messages', '_messaging_complete',
                        'results_df', 'pipeline_complete']:
                st.session_state.pop(key, None)

            start_time = datetime.now()

            # ── Stage 1: Enrichment ──────────────────────────────
            if not skip_enrichment:
                st.subheader("Stage 1: Enriching profiles (5 parallel workers)")
                enrich_progress = st.progress(0, text="Starting enrichment...")
                enrich_stats = st.empty()
                success_count = [0]
                fail_count = [0]

                def enrich_callback(current, total, username, success):
                    enrich_progress.progress(current / total, text=f"Enriching {current}/{total}...")
                    if success:
                        success_count[0] += 1
                    else:
                        fail_count[0] += 1
                    enrich_stats.text(f"Success: {success_count[0]} | Failed: {fail_count[0]} | Remaining: {total - current}")

                enriched = enrich_profiles(usernames, apify_token, progress_callback=enrich_callback, max_workers=5)
                enrich_progress.progress(1.0, text="Enrichment complete")
                st.success(f"Enriched **{len(enriched)}/{len(usernames)}** profiles ({fail_count[0]} failed)")
            else:
                enriched = {}

            # ── Stage 2: Tiering ─────────────────────────────────
            if enriched:
                skip_tiering = st.session_state.get('_skip_tiering', False)

                if skip_tiering and '_prev_batch_df' in st.session_state:
                    st.subheader("Stage 2: Using existing tiers from Google Sheets")
                    prev_df = st.session_state['_prev_batch_df']
                    prev_df['tier'] = prev_df['tier'].astype(str)
                    from utils import clean_linkedin_username
                    tiered = {}
                    for _, row in prev_df.iterrows():
                        username = clean_linkedin_username(str(row.get('linkedin_url', '')))
                        if username and username in enriched:
                            tiered[username] = {
                                'tier': str(row.get('tier', 'Out of Scope')),
                                'tier_confidence': str(row.get('tier_confidence', 'Medium')),
                                'investor_or_customer': str(row.get('investor_or_customer', 'Investor')),
                                'priority_score': row.get('priority_score', 0),
                                'priority_bucket': str(row.get('priority_bucket', 'Medium')),
                                'investor_fit_summary': str(row.get('investor_fit_summary', '')),
                                'rationale_for_tier': str(row.get('rationale_for_tier', '')),
                                'key_career_signals': str(row.get('key_career_signals', '')),
                                'customer_exclusion_flag': str(row.get('customer_exclusion_flag', 'NO')),
                                'notes_for_review': str(row.get('notes_for_review', '')),
                            }
                    st.success(f"Loaded tiers for **{len(tiered)}** profiles from Sheets")
                else:
                    st.subheader("Stage 2: Tiering profiles")
                    with st.spinner("Running tiering engine..."):
                        tiered = tier_profiles(enriched)

                # Show tier stats
                tier_counts = {}
                for t in tiered.values():
                    tier = t.get('tier', 'Unknown')
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1
                cols = st.columns(4)
                cols[0].metric("Tier 1", tier_counts.get('1', 0))
                cols[1].metric("Tier 2", tier_counts.get('2', 0))
                cols[2].metric("Tier 3", tier_counts.get('3', 0))
                cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))

                # Save state for messaging step
                st.session_state['_enriched'] = enriched
                st.session_state['_tiered'] = tiered
                st.session_state['_messaging_complete'] = False
                st.session_state['pipeline_complete'] = True

                # Save tiering to Sheets (NEW TAB) and as CSV download
                tiering_df = build_results_dataframe(list(enriched.keys()), enriched, tiered, {})
                st.session_state['results_df'] = tiering_df
                save_batch_to_history(tiering_df)

                tiering_csv = dataframe_to_csv(tiering_df)
                st.download_button(
                    f"Download Tiering CSV ({len(tiering_df)} profiles)",
                    data=tiering_csv,
                    file_name=f"stellar_tiering_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv", key="tiering_backup",
                )
                _save_to_sheets(tiering_df, "tiering")

                elapsed = (datetime.now() - start_time).total_seconds()
                st.info(f"Enrichment + tiering complete in **{elapsed/60:.1f} minutes**. Click **Generate Messages** below.")

            else:
                st.warning("No profiles enriched. Check your Apify token and usernames.")

elif not st.session_state.get('pipeline_complete'):
    st.info("Enter LinkedIn usernames above to get started.")


# ─── Generate Messages ───────────────────────────────────────────────────────

if (st.session_state.get('_enriched') and st.session_state.get('_tiered')
        and not st.session_state.get('_messaging_complete')):

    enriched = st.session_state['_enriched']
    tiered = st.session_state['_tiered']
    eligible_count = sum(
        1 for t in tiered.values()
        if t.get('tier') != 'Out of Scope' and t.get('customer_exclusion_flag') != 'YES'
    )

    existing_messages = st.session_state.get('_messages', {})
    already_done = sum(
        1 for m in existing_messages.values()
        if m.get('msg_connection_request', '').strip()
        and not m['msg_connection_request'].startswith('[ERROR')
    )
    remaining = eligible_count - already_done

    if remaining > 0:
        est_mins = int(remaining * 7 / 60) + 1
        st.divider()
        st.subheader("Generate Messages")
        if already_done > 0:
            st.success(f"**{already_done}/{eligible_count}** messages already generated.")
        st.info(
            f"**{remaining} messages remaining.** "
            f"Estimated: **~{est_mins} minutes**. "
            f"Saves to Sheets every 25 profiles."
        )

        if st.button(f"Generate messages ({remaining} remaining)", type="primary", key="gen_msgs"):
            CHUNK_SIZE = 25

            eligible = {
                u: t for u, t in tiered.items()
                if t.get('tier') != 'Out of Scope'
                and t.get('customer_exclusion_flag') != 'YES'
                and u not in existing_messages
            }
            # Re-do errored ones
            for u, m in existing_messages.items():
                if m.get('msg_connection_request', '').startswith('[ERROR'):
                    if u in tiered:
                        eligible[u] = tiered[u]

            eligible_list = list(eligible.items())
            total_to_do = len(eligible_list)
            done_so_far = 0

            msg_progress = st.progress(0, text=f"Generating messages 0/{total_to_do}...")
            msg_status = st.empty()

            for chunk_start in range(0, total_to_do, CHUNK_SIZE):
                chunk = dict(eligible_list[chunk_start:chunk_start + CHUNK_SIZE])
                chunk_num = chunk_start // CHUNK_SIZE + 1
                total_chunks = (total_to_do + CHUNK_SIZE - 1) // CHUNK_SIZE

                msg_status.text(f"Chunk {chunk_num}/{total_chunks} ({len(chunk)} profiles)...")

                try:
                    def msg_callback(current, total, username):
                        overall = done_so_far + current
                        msg_progress.progress(min(overall / total_to_do, 1.0), text=f"Generating messages {overall}/{total_to_do}...")

                    chunk_messages = generate_messages(
                        enriched, chunk, anthropic_key,
                        progress_callback=msg_callback, max_workers=1,
                        model_name=model_name, google_api_key=google_key,
                    )

                    existing_messages.update(chunk_messages)
                    st.session_state['_messages'] = existing_messages
                    done_so_far += len(chunk)

                    # Build current results
                    all_messages = {**existing_messages}
                    for u in tiered:
                        if u not in all_messages:
                            all_messages[u] = {k: '' for k in ['msg_connection_request', 'msg_follow_up_accepted', 'msg_reengage_previous', 'msg_reengage_cold', 'msg_email_detailed', 'msg_email_cold_followup']}

                    df = build_results_dataframe(list(enriched.keys()), enriched, tiered, all_messages)
                    st.session_state['results_df'] = df
                    st.session_state['pipeline_complete'] = True

                    generated_count = sum(
                        1 for m in existing_messages.values()
                        if m.get('msg_connection_request', '').strip()
                        and not m['msg_connection_request'].startswith('[ERROR')
                    )

                    # Save chunk progress to Sheets — ALWAYS NEW TAB
                    _save_to_sheets(df, f"msgs {generated_count}/{eligible_count}")

                    msg_status.text(f"Chunk {chunk_num}/{total_chunks} done. {generated_count}/{eligible_count} messages saved.")

                except Exception as e:
                    st.warning(f"Chunk {chunk_num} failed: {type(e).__name__}. Click the button again to continue.")
                    break

            # Final
            msg_progress.progress(1.0, text="Message generation complete")

            all_messages = st.session_state.get('_messages', {})
            for u in tiered:
                if u not in all_messages:
                    all_messages[u] = {k: '' for k in ['msg_connection_request', 'msg_follow_up_accepted', 'msg_reengage_previous', 'msg_reengage_cold', 'msg_email_detailed', 'msg_email_cold_followup']}

            df = build_results_dataframe(list(enriched.keys()), enriched, tiered, all_messages)
            st.session_state['results_df'] = df
            st.session_state['pipeline_complete'] = True

            generated_total = sum(
                1 for m in all_messages.values()
                if m.get('msg_connection_request', '').strip()
                and not m['msg_connection_request'].startswith('[ERROR')
            )

            if generated_total >= eligible_count:
                st.session_state['_messaging_complete'] = True

            save_batch_to_history(df)
            _save_to_sheets(df, "complete")

            st.success(f"**{generated_total}/{eligible_count}** messages generated.")
            if generated_total < eligible_count:
                st.info("Some messages remaining. Click the button again to continue.")

            st.rerun()


# ─── Results ─────────────────────────────────────────────────────────────────

if st.session_state.get('pipeline_complete') and 'results_df' in st.session_state:
    df = st.session_state['results_df']

    st.header("3. Results")

    summary_cols = ['full_name', 'current_title', 'current_company', 'tier', 'priority_score', 'priority_bucket', 'investor_or_customer', 'send_recommendation']
    st.dataframe(df[summary_cols], use_container_width=True, height=400)

    tier_counts = df['tier'].value_counts().to_dict()
    cols = st.columns(4)
    cols[0].metric("Tier 1", tier_counts.get('1', 0))
    cols[1].metric("Tier 2", tier_counts.get('2', 0))
    cols[2].metric("Tier 3", tier_counts.get('3', 0))
    cols[3].metric("Out of Scope", tier_counts.get('Out of Scope', 0))

    st.subheader("Profile Details")
    for idx, row in df.iterrows():
        tier_label = f"Tier {row['tier']}" if row['tier'] in ('1', '2', '3') else row['tier']
        with st.expander(f"{row['full_name']} — {tier_label} | Score: {row['priority_score']} | {row['send_recommendation']}"):
            col1, col2 = st.columns(2)
            with col1:
                st.text(f"Tier: {row['tier']} ({row['tier_confidence']})")
                st.text(f"Score: {row['priority_score']} ({row['priority_bucket']})")
                st.text(f"Type: {row['investor_or_customer']}")
                st.text(f"Summary: {row['investor_fit_summary']}")
                if row['notes_for_review']:
                    st.warning(f"Notes: {row['notes_for_review']}")
            with col2:
                st.text(row.get('key_career_signals', ''))

            msg_val = str(row.get('msg_connection_request', '')).strip()
            if msg_val and not msg_val.startswith('[ERROR'):
                st.markdown("---")
                tabs = st.tabs(["Connection Request", "Follow-Up", "Reengage (Prev)", "Reengage (Cold)", "Email", "Cold Email"])
                msg_cols = ['msg_connection_request', 'msg_follow_up_accepted', 'msg_reengage_previous', 'msg_reengage_cold', 'msg_email_detailed', 'msg_email_cold_followup']
                for tab, col_name in zip(tabs, msg_cols):
                    with tab:
                        msg = row.get(col_name, '')
                        if msg:
                            st.text_area("Copy:", value=msg, height=200, key=f"msg_{idx}_{col_name}")
                            if col_name == 'msg_connection_request':
                                st.caption(f"{len(str(msg))}/300 chars")

    st.header("4. Download")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button("Download Full CSV", data=dataframe_to_csv(df), file_name="stellar_fusion_outreach_full.csv", mime="text/csv", type="primary")
    with col2:
        df_action = df[df['send_recommendation'].isin(['Send Now', 'Edit Before Sending'])]
        if not df_action.empty:
            st.download_button(f"Actionable Only ({len(df_action)})", data=dataframe_to_csv(df_action), file_name="stellar_fusion_outreach_actionable.csv", mime="text/csv")
    with col3:
        df_t1 = df[df['tier'] == '1']
        if not df_t1.empty:
            st.download_button(f"Tier 1 Only ({len(df_t1)})", data=dataframe_to_csv(df_t1), file_name="stellar_fusion_outreach_tier1.csv", mime="text/csv")

    if st.button("Clear results and start over"):
        for key in list(st.session_state.keys()):
            if key != 'batch_history':
                del st.session_state[key]
        st.rerun()

st.divider()
st.caption("Stellar Fusion Group Limited | Enrichment: Apify | Tiering: Python | Messaging: Claude / Gemini")
