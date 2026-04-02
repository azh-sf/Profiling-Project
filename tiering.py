"""Deterministic profile tiering using regex-based career signal detection.

Ported from tier-profiles-v2.py. No API calls — pure Python.
"""

import re
from datetime import datetime

# ─── keyword patterns ────────────────────────────────────────────────────────

SELLSIDE_RESEARCH_TITLE = re.compile(
    r'\b(equity research|research analyst|research associate|head of research|'
    r'director of research|head of equity research|research director|'
    r'research coo|research operations|head of research ops|'
    r'equity analyst|sector analyst|covering analyst|sell.?side analyst)\b', re.I)

SELLSIDE_FIRMS = re.compile(
    r'\b(goldman sachs|morgan stanley|jp morgan|jpmorgan|barclays|deutsche bank|'
    r'berenberg|evercore|credit suisse|ubs|citigroup|citi\b|bank of america|'
    r'bofa|hsbc|jefferies|piper sandler|raymond james|rbc|wells fargo|truist|'
    r'numis|panmure|canaccord|liberum|peel hunt|shore capital|stifel|bernstein|'
    r'alliance bernstein|macquarie|lazard|nomura|mizuho|société générale|'
    r'societe generale|bnp paribas|natwest markets|investec|daiwa|'
    r'exane|exane bnp|kepler|kepler cheuvreux|mediobanca|santander|bbva|'
    r'cowen|td cowen|oppenheimer|william blair|baird|needham|'
    r'roth capital|hc wainwright|cantor fitzgerald|imperial capital|'
    r'ladenburg|maxim group|aegis capital|wolfe research|redburn|'
    r'autonomous|autonomous research|kbw|keefe bruyette|guggenheim|'
    r'independent equity research|sell.?side|broker.?dealer)\b', re.I)

BUYSIDE_TITLE = re.compile(
    r'\b(portfolio manager|fund manager|investment manager|'
    r'chief investment officer|cio\b|head of equities|'
    r'hedge fund|long.?short|long only|asset management|fund management|'
    r'investment analyst|equity pm|head of public equities|'
    r'equity investments)\b', re.I)

ECM_IB_TITLE = re.compile(
    r'\b(equity capital markets|ecm\b|investment banking|equity syndicate|'
    r'equity sales|equity trading|equity derivatives|'
    r'head of equity sales|md equity|managing director equity)\b', re.I)

ANGEL_SIGNALS_STRONG = re.compile(
    r'\b(angel investor|angel investing|angel syndicate|angel network|'
    r'family office|venture capital|venture partner|'
    r'general partner|managing partner|investment partner|'
    r'founding partner|private equity|growth equity)\b', re.I)

ANGEL_SIGNALS_WEAK = re.compile(
    r'\b(advisor|adviser|board member|non.?exec)\b', re.I)

FINANCIAL_CONTEXT = re.compile(
    r'\b(fintech|financial|capital markets|investment|fund|'
    r'equity|asset management|wealth|banking|securities|'
    r'hedge|portfolio|venture|startup|seed|series [a-d])\b', re.I)

FINTECH_SIGNALS = re.compile(
    r'\b(fintech|financial technology|capital markets technology|'
    r'financial data|financial infrastructure|wealthtech|insurtech|'
    r'regtech|paytech)\b', re.I)

OUT_OF_SCOPE_TITLE = re.compile(
    r'\b(hr |human resources|marketing manager|supply chain|logistics|'
    r'manufacturing|retail manager|hospitality|healthcare|nursing|medical|'
    r'teacher|professor|legal counsel|attorney|lawyer\b|accountant\b|'
    r'tax advisor|audit manager|auditor\b|project manager|product manager|'
    r'program manager|scrum master|devops|software engineer|data engineer|'
    r'operations manager|facilities)\b', re.I)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _is_current(e):  return e.get('is_current', False)
def _title_of(e):    return (e.get('title') or '').lower()
def _company_of(e):  return (e.get('company') or '').lower()
def _exp_text(e):    return _title_of(e) + ' ' + _company_of(e)


def _years_since_end(e):
    """Return years since role ended, or None."""
    end = e.get('end_date')
    if not end:
        return None
    try:
        if isinstance(end, dict):
            y = end.get('year')
            m = end.get('month', 6)
        elif isinstance(end, str):
            parts = end.split('-')
            y = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 6
        else:
            return None
        if y is None:
            return None
        now = datetime.now()
        return now.year - int(y) + (now.month - int(m)) / 12
    except (ValueError, TypeError):
        return None


def _has_angel_signal(full_text, exps):
    if ANGEL_SIGNALS_STRONG.search(full_text):
        return True
    if any(ANGEL_SIGNALS_STRONG.search(_exp_text(e)) for e in exps):
        return True
    for e in exps:
        et = _exp_text(e)
        if ANGEL_SIGNALS_WEAK.search(et) and FINANCIAL_CONTEXT.search(et):
            return True
    if ANGEL_SIGNALS_WEAK.search(full_text) and FINANCIAL_CONTEXT.search(full_text):
        return True
    return False


def _build_result(tier, tier_conf, inv_cust,
                  domain_rel, inv_plaus, strat_fit, non_cust, pers_str,
                  summary, rationale, signals, cust_flag, notes):
    score = domain_rel + inv_plaus + strat_fit + non_cust + pers_str
    if score >= 16:   bucket = 'High'
    elif score >= 10: bucket = 'Medium'
    else:             bucket = 'Low'
    return {
        'tier': tier,
        'tier_confidence': tier_conf,
        'investor_or_customer': inv_cust,
        'domain_relevance': domain_rel,
        'investor_plausibility': inv_plaus,
        'strategic_fit': strat_fit,
        'non_customer_suitability': non_cust,
        'personalisation_strength': pers_str,
        'priority_score': score,
        'priority_bucket': bucket,
        'investor_fit_summary': summary,
        'rationale_for_tier': rationale,
        'key_career_signals': signals,
        'customer_exclusion_flag': cust_flag,
        'notes_for_review': notes,
    }


def _build_summary(tier, angel, senior_research, recency):
    if tier == '1':
        base = 'Former sell-side research professional'
        if senior_research:
            base = 'Former senior sell-side research professional'
        if recency and recency > 12:
            base += ' (role ended 12+ years ago)'
    elif tier == '2':
        base = 'Buy-side / capital markets professional'
    else:
        base = 'Angel / fintech / family office profile'
    if angel:
        base += ' with investing signals'
    return base + '.'


def _build_rationale(tier, conf, recency, senior, named_firm):
    if tier == '1':
        parts = [f'Tier 1 ({conf}): former sell-side research']
        if senior:   parts.append('senior role')
        if named_firm: parts.append('recognised firm')
        if recency:  parts.append(f'ended ~{int(recency)}y ago')
        return ', '.join(parts)
    elif tier == '2':
        return f'Tier 2 ({conf}): buy-side / ECM / IB role detected'
    else:
        return f'Tier 3 ({conf}): angel / fintech / family office signals'


# ─── main tiering function ───────────────────────────────────────────────────

def tier_profile(profile: dict) -> dict:
    """Tier a single enriched Apify profile.

    Args:
        profile: Full Apify profile dict with basic_info, experience[], education[]

    Returns:
        Dict with 15 tiering columns (tier, scores, summary, etc.)
    """
    bi = profile.get('basic_info', {})
    exps = profile.get('experience', [])
    headline = (bi.get('headline') or '').lower()
    about = (bi.get('about') or '').lower()
    full_text = headline + ' ' + about

    current_exps = [e for e in exps if _is_current(e)]
    past_exps = [e for e in exps if not _is_current(e)]

    # Step 1: Domain relevance check
    domain_signals = (
        SELLSIDE_RESEARCH_TITLE.search(full_text) or
        BUYSIDE_TITLE.search(full_text) or
        ECM_IB_TITLE.search(full_text) or
        ANGEL_SIGNALS_STRONG.search(full_text) or
        any(SELLSIDE_RESEARCH_TITLE.search(_exp_text(e)) for e in exps) or
        any(BUYSIDE_TITLE.search(_exp_text(e)) for e in exps) or
        any(ECM_IB_TITLE.search(_exp_text(e)) for e in exps) or
        any(ANGEL_SIGNALS_STRONG.search(_exp_text(e)) for e in exps) or
        any(SELLSIDE_FIRMS.search(_company_of(e)) for e in exps) or
        _has_angel_signal(full_text, exps)
    )
    if not domain_signals:
        return _build_result('Out of Scope', 'Low', 'Ambiguous', 0, 0, 0, 5, 0,
                             'No meaningful domain relevance detected.',
                             'No equity research, buy-side, or capital markets signals.', '', 'NO', '')

    # Step 2: Customer exclusion (current sell-side research)
    current_is_research = any(
        SELLSIDE_RESEARCH_TITLE.search(_title_of(e)) for e in current_exps
    )
    if current_is_research:
        return _build_result('Out of Scope', 'High', 'Customer', 1, 0, 1, 0, 2,
                             'Currently in sell-side research — potential Stellar Fusion customer.',
                             'Active sell-side research role detected.', '', 'YES',
                             'CUSTOMER EXCLUSION: currently in sell-side research.')

    # Step 3: Tier assignment

    # Research role analysis with recency
    research_past_exps = [e for e in past_exps if SELLSIDE_RESEARCH_TITLE.search(_title_of(e))]
    past_has_research = len(research_past_exps) > 0

    past_has_senior_research = any(
        re.search(r'\b(head of research|director of research|senior|vp|md|managing director|'
                  r'vice president|principal|research coo|research director)\b', _title_of(e), re.I)
        and SELLSIDE_RESEARCH_TITLE.search(_title_of(e))
        for e in past_exps
    )

    research_recency_years = None
    for e in research_past_exps:
        yrs = _years_since_end(e)
        if yrs is not None:
            if research_recency_years is None or yrs < research_recency_years:
                research_recency_years = yrs

    research_at_named_firm = any(
        SELLSIDE_FIRMS.search(_company_of(e)) for e in research_past_exps
    )

    headline_research = bool(
        SELLSIDE_RESEARCH_TITLE.search(headline) and
        not re.search(r'\b(currently|present)\b', headline, re.I)
    )

    current_buyside = any(BUYSIDE_TITLE.search(_title_of(e)) for e in current_exps)
    current_ecm_ib = any(ECM_IB_TITLE.search(_title_of(e)) for e in current_exps)
    past_buyside = any(BUYSIDE_TITLE.search(_title_of(e)) for e in past_exps)
    past_ecm_ib = any(ECM_IB_TITLE.search(_title_of(e)) for e in past_exps)
    angel = _has_angel_signal(full_text, exps)
    fintech = bool(FINTECH_SIGNALS.search(full_text) or
                   any(FINTECH_SIGNALS.search(_exp_text(e)) for e in exps))

    # Tier 1: Former sell-side research
    if (past_has_research or headline_research) and not current_is_research:
        tier = '1'
        is_senior = past_has_senior_research
        is_named_firm = research_at_named_firm
        is_recent = research_recency_years is not None and research_recency_years <= 12
        is_stale = research_recency_years is not None and research_recency_years > 12

        if is_senior and is_named_firm and is_recent:
            tier_conf = 'High'
            domain_rel = 5
            strat_fit = 5
        elif (is_senior or is_named_firm) and not is_stale:
            tier_conf = 'High'
            domain_rel = 5
            strat_fit = 4
        elif is_stale and not is_senior:
            tier_conf = 'Low'
            domain_rel = 3
            strat_fit = 2
        else:
            tier_conf = 'Medium'
            domain_rel = 4
            strat_fit = 3

    # Tier 2: Buy-side or ECM/IB
    elif current_buyside or current_ecm_ib:
        tier = '2'
        tier_conf = 'High'
        domain_rel = 4
        strat_fit = 3
    elif past_buyside or past_ecm_ib:
        tier = '2'
        tier_conf = 'Medium'
        domain_rel = 3
        strat_fit = 3

    # Tier 3: Angel / fintech / family office
    elif angel or fintech:
        tier = '3'
        tier_conf = 'Medium'
        domain_rel = 2
        strat_fit = 2

    # Weak signals — not enough to tier
    else:
        return _build_result('Out of Scope', 'Low', 'Ambiguous', 1, 0, 1, 4, 1,
                             'Weak domain signals — insufficient for targeting.',
                             'Some financial background but no clear investor or equity research signal.',
                             '', 'NO', 'Edge case — verify manually.')

    # Step 4: Scoring
    inv_plaus = 0
    if angel:
        inv_plaus += 3
    if any(re.search(r'\b(angel|investor|investing|family office|venture|'
                     r'seed|series [a-d])\b', _exp_text(e), re.I) for e in exps):
        inv_plaus = min(5, inv_plaus + 1)
    if tier == '1' and past_has_senior_research:
        inv_plaus = min(5, inv_plaus + 1)
    if tier == '2' and current_buyside:
        inv_plaus = min(5, inv_plaus + 1)

    non_cust = 5 if not current_is_research else 0

    roles_with_detail = sum(1 for e in exps if _title_of(e) and _company_of(e))
    pers_str = min(3, roles_with_detail)

    priority_score = domain_rel + inv_plaus + strat_fit + non_cust + pers_str

    investor_or_customer = 'Investor'

    # Career signals
    signals = []
    for e in exps[:6]:
        t = (e.get('title') or '').strip()
        c = (e.get('company') or '').strip()
        curr = ' (current)' if e.get('is_current') else ''
        if t or c:
            signals.append(f"{t} @ {c}{curr}")

    summary = _build_summary(tier, angel, past_has_senior_research, research_recency_years)
    rationale = _build_rationale(tier, tier_conf, research_recency_years,
                                 past_has_senior_research, research_at_named_firm)

    notes = ''
    if current_buyside and not angel:
        notes = 'Active buy-side — may have compliance constraints on personal investing.'
    if tier_conf == 'Low':
        notes += ' Low confidence — verify manually.'
    if tier == '1' and research_recency_years and research_recency_years > 12:
        notes += f' Research role ended ~{int(research_recency_years)} years ago — recency risk.'

    return _build_result(tier, tier_conf, investor_or_customer,
                         domain_rel, inv_plaus, strat_fit, non_cust, pers_str,
                         summary, rationale, ' | '.join(signals[:5]), 'NO', notes.strip())


def tier_profiles(enriched: dict) -> dict:
    """Tier multiple enriched profiles.

    Args:
        enriched: {username: apify_profile_dict}

    Returns:
        {username: tier_result_dict}
    """
    return {username: tier_profile(profile) for username, profile in enriched.items()}
