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
    google_key = st.text_input(
        "Google AI API Key (for Gemini)", value=get_secret("GOOGLE_AI_KEY"), type="password",
    )

    st.divider()
    st.subheader("Pipeline Options")

    from messaging import MODELS
    model_name = st.selectbox(
        "Messaging model",
        options=list(MODELS.keys()),
        index=0,
        help="Opus = highest quality, Gemini = cheapest, Sonnet = middle ground",
    )
    model_cost = MODELS[model_name]["cost_per_profile"]
    st.caption(f"Est. cost: {model_cost} per profile")

    skip_enrichment = st.checkbox(
        "Skip enrichment (pre-enriched data)", value=False,
    )
    generate_msgs = st.checkbox(
        "Generate messages", value=True,
        help="Uncheck to only enrich and tier (no API cost)",
    )

    st.divider()
    st.caption("Enrichment: Apify (5 parallel workers)")
    st.caption("Tiering: Python regex (instant)")
    st.caption(f"Messaging: {model_name}")

    # ── Previous Batches ─────────────────────────────────────────
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

    # Persistent Google Sheets history
    st.divider()
    st.subheader("All Batches (Google Sheets)")
    sheets_history = get_batch_history(st.secrets)
    if sheets_history:
        for tab in sheets_history[:10]:  # Show last 10
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

elif input_method == "Backfill messages from Sheets":
    from sheets import get_batches_needing_messages, update_sheet_tab

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

            # Extract usernames for profiles needing messages
            from utils import clean_linkedin_username
            msg_col = 'msg_connection_request'
            prev_df['tier'] = prev_df['tier'].astype(str)
            eligible_mask = (
                prev_df['tier'].isin(['1', '2', '3'])
                & (prev_df.get('customer_exclusion_flag', pd.Series(dtype=str)).astype(str) != 'YES')
            )
            if msg_col in prev_df.columns:
                missing_mask = eligible_mask & prev_df[msg_col].fillna('').str.strip().eq('')
            else:
                missing_mask = eligible_mask

            need_msg = prev_df.loc[missing_mask]
            usernames = [
                clean_linkedin_username(str(u))
                for u in need_msg['linkedin_url'].dropna()
                if clean_linkedin_username(str(u))
            ]

            if usernames:
                st.info(
                    f"**{len(usernames)} profiles** need enrichment (for career data) + messaging. "
                    f"Tiering will be skipped — using existing tiers from the Sheet."
                )
                st.session_state['_prev_batch_df'] = prev_df
                st.session_state['_prev_batch_tab'] = batch['name']
                st.session_state['_rerun_mode'] = True
                st.session_state['_skip_tiering'] = True


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
        # Warn if previous results exist and haven't been cleared
        if st.session_state.get('pipeline_complete'):
            st.warning(
                "You have results from a previous batch. Make sure you've "
                "**downloaded the CSV** before processing a new batch — "
                "previous results will be overwritten."
            )

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
                skip_tiering = st.session_state.get('_skip_tiering', False)

                if skip_tiering and '_prev_batch_df' in st.session_state:
                    # Backfill mode: use tiering from Sheets, skip re-tiering
                    st.subheader("Stage 2: Using existing tiers from Google Sheets")
                    prev_df = st.session_state['_prev_batch_df']
                    prev_df['tier'] = prev_df['tier'].astype(str)

                    # Build tiered dict from Sheet data, keyed by username
                    from utils import clean_linkedin_username
                    tiered = {}
                    for _, row in prev_df.iterrows():
                        url = str(row.get('linkedin_url', ''))
                        username = clean_linkedin_username(url)
                        if username and username in enriched:
                            tiered[username] = {
                                'tier': str(row.get('tier', 'Out of Scope')),
                                'tier_confidence': str(row.get('tier_confidence', 'Medium')),
                                'investor_or_customer': str(row.get('investor_or_customer', 'Investor')),
                                'domain_relevance': row.get('domain_relevance', 0),
                                'investor_plausibility': row.get('investor_plausibility', 0),
                                'strategic_fit': row.get('strategic_fit', 0),
                                'non_customer_suitability': row.get('non_customer_suitability', 0),
                                'personalisation_strength': row.get('personalisation_strength', 0),
                                'priority_score': row.get('priority_score', 0),
                                'priority_bucket': str(row.get('priority_bucket', 'Medium')),
                                'investor_fit_summary': str(row.get('investor_fit_summary', '')),
                                'rationale_for_tier': str(row.get('rationale_for_tier', '')),
                                'key_career_signals': str(row.get('key_career_signals', '')),
                                'customer_exclusion_flag': str(row.get('customer_exclusion_flag', 'NO')),
                                'notes_for_review': str(row.get('notes_for_review', '')),
                            }
                    st.success(f"Loaded tiers for **{len(tiered)}** profiles from Sheets (no re-tiering)")
                else:
                    # Normal mode: run tiering
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

                # ── Save state for messaging ────────────────────────────
                st.session_state['_enriched'] = enriched
                st.session_state['_tiered'] = tiered
                st.session_state['_messaging_complete'] = False
                st.session_state['pipeline_complete'] = True

                if not skip_tiering:
                    # Only save tiering to Sheets for new batches (not backfills)
                    tiering_df = build_results_dataframe(
                        list(enriched.keys()), enriched, tiered, {}
                    )
                    st.session_state['results_df'] = tiering_df
                    save_batch_to_history(tiering_df)

                    tiering_csv = dataframe_to_csv(tiering_df)
                    tier_fname = f"stellar_tiering_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.success("Enrichment + tiering saved. Download tiering-only CSV now as a backup:")
                    st.download_button(
                        f"Download Tiering CSV ({len(tiering_df)} profiles, no messages)",
                        data=tiering_csv,
                        file_name=tier_fname,
                        mime="text/csv",
                        key="tiering_backup",
                    )

                    ok, tab_name = save_batch_to_sheets(tiering_df, st.secrets, stage="tiering")
                    if ok:
                        st.session_state['_sheets_tab'] = tab_name
                        st.success(f"Tiering saved to Google Sheets: **{tab_name}**")
                    else:
                        st.warning(f"Could not save to Sheets: {tab_name}")

                # ── Ready for messaging ───────────────────────────────
                elapsed = (datetime.now() - start_time).total_seconds()
                st.info(
                    f"Enrichment complete in **{elapsed/60:.1f} minutes**. "
                    f"Click **Generate Messages** below when ready."
                )

            else:
                st.warning("No profiles enriched. Check your Apify token and usernames.")

elif not st.session_state.get('pipeline_complete'):
    st.info("Enter LinkedIn usernames above to get started.")

# ─── Resume Messaging Button ─────────────────────────────────────────────────
# Shows when enrichment + tiering completed but messaging failed or was skipped

if (st.session_state.get('_enriched') and st.session_state.get('_tiered')
        and not st.session_state.get('_messaging_complete')):
    enriched = st.session_state['_enriched']
    tiered = st.session_state['_tiered']
    eligible_count = sum(
        1 for t in tiered.values()
        if t.get('tier') != 'Out of Scope' and t.get('customer_exclusion_flag') != 'YES'
    )

    # Check if current results have empty messages
    current_df = st.session_state.get('results_df')
    has_messages = False
    if current_df is not None:
        has_messages = current_df['msg_connection_request'].str.strip().ne('').any()

    # Count how many already have messages (from previous partial runs)
    existing_messages = st.session_state.get('_messages', {})
    already_done = sum(
        1 for u, m in existing_messages.items()
        if m.get('msg_connection_request', '').strip()
        and not m['msg_connection_request'].startswith('[ERROR')
    )
    remaining = eligible_count - already_done

    if remaining > 0:
        est_mins = int(remaining * 7 / 60) + 1
        st.divider()
        st.subheader("Generate Messages")
        if already_done > 0:
            st.success(f"**{already_done}/{eligible_count}** messages already generated from previous run.")
        st.info(
            f"**{remaining} messages remaining.** "
            f"Estimated time: **~{est_mins} minutes**. "
            f"Progress is saved every 25 profiles — if it times out, click the button again to continue."
        )
        if st.button(
            f"Generate messages ({remaining} remaining)",
            type="primary",
            key="resume_messaging",
        ):
            CHUNK_SIZE = 25

            # Get eligible profiles that haven't been messaged yet
            eligible = {
                u: t for u, t in tiered.items()
                if t.get('tier') != 'Out of Scope'
                and t.get('customer_exclusion_flag') != 'YES'
                and u not in existing_messages
            }
            # Also include ones that errored
            for u, m in existing_messages.items():
                if m.get('msg_connection_request', '').startswith('[ERROR'):
                    if u in tiered:
                        eligible[u] = tiered[u]

            eligible_list = list(eligible.items())
            total_to_do = len(eligible_list)
            done_so_far = 0

            msg_progress = st.progress(0, text=f"Generating messages 0/{total_to_do}...")
            msg_status = st.empty()

            # Process in chunks — save after each chunk
            for chunk_start in range(0, total_to_do, CHUNK_SIZE):
                chunk = dict(eligible_list[chunk_start:chunk_start + CHUNK_SIZE])
                chunk_num = chunk_start // CHUNK_SIZE + 1
                total_chunks = (total_to_do + CHUNK_SIZE - 1) // CHUNK_SIZE

                msg_status.text(f"Chunk {chunk_num}/{total_chunks} ({len(chunk)} profiles)...")

                try:
                    def msg_callback(current, total, username):
                        overall = done_so_far + current
                        msg_progress.progress(
                            min(overall / total_to_do, 1.0),
                            text=f"Generating messages {overall}/{total_to_do}...",
                        )

                    chunk_messages = generate_messages(
                        enriched, chunk, anthropic_key,
                        progress_callback=msg_callback,
                        max_workers=1,
                        model_name=model_name,
                        google_api_key=google_key,
                    )

                    # Merge into existing messages
                    existing_messages.update(chunk_messages)
                    st.session_state['_messages'] = existing_messages
                    done_so_far += len(chunk)

                    # Save progress after each chunk
                    all_messages = {**existing_messages}
                    # Fill in empty messages for skipped profiles
                    for u in tiered:
                        if u not in all_messages:
                            all_messages[u] = {
                                'msg_connection_request': '',
                                'msg_follow_up_accepted': '',
                                'msg_reengage_previous': '',
                                'msg_reengage_cold': '',
                                'msg_email_detailed': '',
                                'msg_email_forwardable': '',
                            }

                    df = build_results_dataframe(
                        list(enriched.keys()), enriched, tiered, all_messages
                    )
                    st.session_state['results_df'] = df
                    st.session_state['pipeline_complete'] = True

                    # Save chunk progress to Sheets
                    generated_count = sum(
                        1 for m in existing_messages.values()
                        if m.get('msg_connection_request', '').strip()
                        and not m['msg_connection_request'].startswith('[ERROR')
                    )

                    # Update the ORIGINAL Sheets tab in-place
                    from sheets import update_sheet_tab
                    target_tab = st.session_state.get('_prev_batch_tab') or st.session_state.get('_sheets_tab')
                    ok = False
                    if target_tab:
                        ok, _ = update_sheet_tab(df, st.secrets, target_tab)

                    msg_status.text(
                        f"Chunk {chunk_num}/{total_chunks} done. "
                        f"{generated_count}/{eligible_count} messages. "
                        f"{'Updated in Sheets.' if ok else 'Sheets not updated.'}"
                    )

                except Exception as e:
                    st.warning(
                        f"Chunk {chunk_num} failed: {type(e).__name__}. "
                        f"Progress saved. Click the button again to continue from where you left off."
                    )
                    break

            # Final save
            msg_progress.progress(1.0, text="Message generation complete")

            all_messages = st.session_state.get('_messages', {})
            for u in tiered:
                if u not in all_messages:
                    all_messages[u] = {
                        'msg_connection_request': '',
                        'msg_follow_up_accepted': '',
                        'msg_reengage_previous': '',
                        'msg_reengage_cold': '',
                        'msg_email_detailed': '',
                        'msg_email_forwardable': '',
                    }

            df = build_results_dataframe(
                list(enriched.keys()), enriched, tiered, all_messages
            )
            st.session_state['results_df'] = df
            st.session_state['pipeline_complete'] = True

            generated_total = sum(
                1 for m in all_messages.values()
                if m.get('msg_connection_request', '').strip()
                and not m['msg_connection_request'].startswith('[ERROR')
            )

            if generated_total >= eligible_count:
                st.session_state['_messaging_complete'] = True

            # If re-run mode, merge new messages into previous batch
            if st.session_state.get('_rerun_mode') and '_prev_batch_df' in st.session_state:
                from sheets import update_sheet_tab
                prev_df = st.session_state['_prev_batch_df']
                msg_cols = [
                    'msg_connection_request', 'msg_follow_up_accepted',
                    'msg_reengage_previous', 'msg_reengage_cold',
                    'msg_email_detailed', 'msg_email_forwardable',
                ]
                # Merge on linkedin_url
                for _, new_row in df.iterrows():
                    url = new_row.get('linkedin_url', '')
                    if not url:
                        continue
                    mask = prev_df['linkedin_url'] == url
                    if mask.any():
                        for col in msg_cols:
                            new_val = new_row.get(col, '')
                            if new_val and str(new_val).strip():
                                prev_df.loc[mask, col] = new_val

                df = prev_df
                st.session_state['results_df'] = df
                st.info("Messages merged back into the original batch.")

                # Update the original Sheets tab
                tab_name = st.session_state.get('_prev_batch_tab')
                if tab_name:
                    ok, msg = update_sheet_tab(df, st.secrets, tab_name)
                    if ok:
                        st.success(f"Updated Google Sheet tab: **{tab_name}**")
                    else:
                        st.warning(f"Could not update Sheet: {msg}")

            save_batch_to_history(df)

            # Final update to the original Sheets tab
            from sheets import update_sheet_tab
            target_tab = st.session_state.get('_prev_batch_tab') or st.session_state.get('_sheets_tab')
            if target_tab:
                ok, _ = update_sheet_tab(df, st.secrets, target_tab)
                if ok:
                    st.success(f"Google Sheet updated: **{target_tab}**")

            st.success(f"**{generated_total}/{eligible_count}** messages generated.")
            if generated_total < eligible_count:
                st.info("Some messages remaining. Click the button again to continue.")

            st.rerun()


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
    "Tiering: Python | Messaging: Claude / Gemini"
)
