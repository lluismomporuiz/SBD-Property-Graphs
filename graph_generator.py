"""
graph_generator.py
==================
Reads raw DBLP CSV exports (articles + inproceedings), filters to a focused
database/data-mining subgraph, builds a fully connected Property Graph schema
aligned with the SDM Lab A.1/A.3 model, and writes two sets of output CSVs:

  Neo4j/          — clean, human-readable CSVs for inspection / LOAD CSV.
  Neo4j_import/   — header-annotated CSVs ready for neo4j-admin database import.

Graph schema overview
---------------------
Nodes:
  Paper, Author, Keyword, Organization,
  Journal, Volume,
  Conference, Workshop, Edition

Relationships:
  (Author)  -[:WROTE         {role}]->                    (Paper)
  (Paper)   -[:CITES]->                                   (Paper)
  (Paper)   -[:HAS_KEYWORD]->                             (Keyword)
  (Author)  -[:AFFILIATED_TO]->                           (Organization)
  (Author)  -[:REVIEWED      {review_id, score,
                               decision, comments}]->     (Paper)
  (Paper)   -[:PUBLISHED_IN_VOLUME]->                     (Volume)
  (Volume)  -[:BELONGS_TO]->                              (Journal)
  (Paper)   -[:PRESENTED_IN]->                            (Edition)
  (Edition) -[:EDITION_OF]->                              (Conference)
  (Edition) -[:EDITION_OF]->                              (Workshop)

Usage
-----
  python graph_generator.py

Expected input layout:
  output_csv/
    dblp_article_header.csv
    dblp_article.csv
    dblp_inproceedings_header.csv
    dblp_inproceedings.csv
"""

import os
import re
import uuid
import random
import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

# Only include papers published from this year onwards.
# 2015 gives a good balance: hundreads of papers, fast import, enough data for all
# lab queries (B, C, D) to return meaningful results.
MIN_YEAR = 2015

# Regex used to filter relevant venues (journals + conferences in DB / DM field).
REGEX_PATTERN = (
    r'VLDB|SIGMOD|ICDE|KDD|TODS|TKDE|WSDM|WWW|Workshop|'
    r'Trans\. Database Syst\.|Trans\. Knowl\. Data Eng\.|'
    r'Inf\. Syst\.|Data Knowl\. Eng\.|Knowl\. Based Syst\.|'
    r'Trans\. Web|World Wide Web|Expert Syst\.'
)

# Maps the short acronym extracted from DBLP keys to full venue names.
VENUE_FULL_NAMES = {
    'VLDB':  'International Conference on Very Large Data Bases',
    'SIGMOD':'ACM SIGMOD International Conference on Management of Data',
    'ICDE':  'IEEE International Conference on Data Engineering',
    'KDD':   'ACM SIGKDD International Conference on Knowledge Discovery and Data Mining',
    'WSDM':  'ACM International Conference on Web Search and Data Mining',
    'WWW':   'The Web Conference',
    'PVLDB': 'Proceedings of the VLDB Endowment',
    'TODS':  'ACM Transactions on Database Systems',
    'TKDE':  'IEEE Transactions on Knowledge and Data Engineering',
    'IS':    'Information Systems',
    'DKE':   'Data & Knowledge Engineering',
}

# Synthetic data pools
FIELDS_LIST = [
    'Databases', 'Data Mining', 'Artificial Intelligence',
    'Machine Learning', 'Information Retrieval', 'Data Science',
]
CITIES = [
    'San Francisco', 'New York', 'London', 'Berlin', 'Tokyo',
    'Sydney', 'Paris', 'Barcelona', 'Toronto', 'Singapore',
]
PUBLISHERS = ['ACM', 'IEEE', 'Springer', 'Elsevier', 'VLDB Endowment']

# Keywords aligned with the SDM lab "database community" definition (Part C).
# The FIRST 7 are the EXACT community keywords required by query C.1.
# The remaining 5 are extras to give papers variety.
KEYWORD_LABELS = [
    'Data Management', 'Indexing', 'Data Modeling', 'Big Data',
    'Data Processing', 'Data Storage', 'Data Querying',      # <- DB community (C.1)
    'Machine Learning', 'Graph Databases', 'Optimization',
    'Deep Learning', 'Natural Language Processing',
]

# Seed organizations used for synthetic author affiliations (A.3).
ORG_NAMES = [
    "MIT", "Stanford University", "CMU", "UC Berkeley", "Oxford",
    "Cambridge", "ETH Zurich", "EPFL", "Tsinghua University", "NUS",
    "Google Research", "Microsoft Research", "IBM Research", "Meta AI",
    "Amazon Web Services", "UPC BarcelonaTech", "University of Toronto",
    "University of Washington", "Cornell University", "Max Planck Institute",
]

# Output directories
DIR_RAW    = 'Neo4j'         # human-readable CSVs (inspection / LOAD CSV)
DIR_IMPORT = 'Neo4j_import'  # neo4j-admin annotated CSVs (bulk import)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_doi(ee_str: str) -> str:
    """
    Extract a DOI from the 'ee' (electronic edition) field.
    DBLP stores multiple URLs separated by '|'; we prefer doi.org links.
    Falls back to a synthetic DOI when none is found.
    """
    if pd.isna(ee_str):
        return f"10.fake/{uuid.uuid4().hex[:8]}"
    for link in str(ee_str).split('|'):
        if 'doi.org' in link:
            return link.replace('https://doi.org/', '').strip()
    return f"10.fake/{uuid.uuid4().hex[:8]}"


def make_email(name: str) -> str:
    """Generate a plausible academic e-mail address from an author name."""
    clean = re.sub(r'[^a-zA-Z\s]', '', name).lower().replace(' ', '.')
    return f"{clean}@university.edu"


def review_decision(score: int) -> str:
    """Map a numeric review score (1-10) to a textual decision."""
    if score >= 7:
        return "Accept"
    elif score >= 4:
        return "Weak Accept"
    else:
        return "Reject"


def review_comment(score: int) -> str:
    """Return a short canned comment consistent with the review score."""
    if score >= 8:
        return "Excellent methodology. Highly recommended."
    elif score >= 5:
        return "Solid contribution, but needs more experiments."
    else:
        return "Lacks novelty and weak evaluation. Reject."


# =============================================================================
# STEP 1 — LOAD & FILTER RAW DBLP DATA
# =============================================================================

def load_dblp_data() -> pd.DataFrame:
    """
    Read DBLP article and inproceedings CSVs in chunks, apply year and
    venue filters, and return a single concatenated DataFrame.

    Real data origin:
      - paper_id, title, authors, venue, year, cites, doi  <- directly from DBLP.

    Synthetic / corrected fields introduced here:
      - pages : replaced with realistic random ranges (DBLP pages are noisy).
      - doi   : synthetic fallback when the 'ee' field has no doi.org link.
    """
    print(f"[1/9] Loading and filtering DBLP data (MIN_YEAR={MIN_YEAR})...")

    cols_article = [
        'key:string', 'title:string', 'year:int', 'author:string[]',
        'journal:string', 'cite:string[]', 'volume:string', 'pages:string', 'ee:string[]',
    ]
    cols_inproc = [
        'key:string', 'title:string', 'year:int', 'author:string[]',
        'booktitle:string', 'cite:string[]', 'pages:string', 'ee:string[]',
    ]

    # Load header files produced by the DBLP XML-to-CSV conversion tool.
    try:
        with open('output_csv/dblp_article_header.csv', 'r', encoding='utf-8') as f:
            article_headers = f.read().strip().split(';')
        with open('output_csv/dblp_inproceedings_header.csv', 'r', encoding='utf-8') as f:
            inproc_headers = f.read().strip().split(';')
    except FileNotFoundError:
        raise SystemExit("[!] Header files not found in 'output_csv/'. Aborting.")

    chunks = []

    # --- Journal articles ---
    print("     -> Reading dblp_article.csv ...")
    try:
        for chunk in pd.read_csv(
            'output_csv/dblp_article.csv', sep=';', names=article_headers,
            usecols=cols_article, dtype=str, on_bad_lines='skip', chunksize=100_000,
        ):
            chunk = chunk.rename(columns={
                'key:string': 'paper_id',   'title:string': 'title',
                'year:int': 'year',          'author:string[]': 'authors',
                'journal:string': 'venue',   'volume:string': 'volume',
                'cite:string[]': 'cites',    'pages:string': 'pages',
                'ee:string[]': 'ee',
            })
            chunk['type'] = 'Journal'
            chunk['year'] = pd.to_numeric(chunk['year'], errors='coerce')
            chunk = chunk.dropna(subset=['year', 'venue', 'paper_id'])
            chunks.append(chunk[
                (chunk['year'] >= MIN_YEAR) &
                chunk['venue'].str.contains(REGEX_PATTERN, case=False, na=False)
            ].copy())
    except FileNotFoundError:
        print("     [!] dblp_article.csv not found — skipping journal articles.")

    # --- Conference / workshop inproceedings ---
    print("     -> Reading dblp_inproceedings.csv ...")
    try:
        for chunk in pd.read_csv(
            'output_csv/dblp_inproceedings.csv', sep=';', names=inproc_headers,
            usecols=cols_inproc, dtype=str, on_bad_lines='skip', chunksize=100_000,
        ):
            chunk = chunk.rename(columns={
                'key:string': 'paper_id',      'title:string': 'title',
                'year:int': 'year',             'author:string[]': 'authors',
                'booktitle:string': 'venue',    'cite:string[]': 'cites',
                'pages:string': 'pages',        'ee:string[]': 'ee',
            })
            chunk['type']   = 'Conference'
            chunk['volume'] = 'N/A'
            chunk['year']   = pd.to_numeric(chunk['year'], errors='coerce')
            chunk = chunk.dropna(subset=['year', 'venue', 'paper_id'])
            chunks.append(chunk[
                (chunk['year'] >= MIN_YEAR) &
                chunk['venue'].str.contains(REGEX_PATTERN, case=False, na=False)
            ].copy())
    except FileNotFoundError:
        print("     [!] dblp_inproceedings.csv not found — skipping inproceedings.")

    if not chunks:
        raise SystemExit("[!] No data loaded after filtering. Check input files and REGEX_PATTERN.")

    df = pd.concat(chunks, ignore_index=True).dropna(subset=['authors', 'title'])

    # Sanitise text fields that may contain line-break characters.
    df['title'] = df['title'].str.replace(r'[\n\r\u2028\u2029]+', ' ', regex=True)

    # DOI: extract real one when available, generate synthetic fallback otherwise.
    df['doi'] = df['ee'].apply(extract_doi)

    # Pages: DBLP page ranges are often missing or inconsistent; replace all
    # with synthetic but realistic page ranges (8-20 pages per paper).
    df['pages'] = [
        f"{s}-{s + random.randint(8, 20)}"
        for s in (random.randint(1, 500) for _ in range(len(df)))
    ]

    # Extract the short venue acronym from the DBLP key path.
    # e.g. "journals/vldb/Smith15" -> segment [1] = "vldb" -> "VLDB"
    df['raw_acronym'] = df['paper_id'].apply(
        lambda x: str(x).split('/')[1].upper() if len(str(x).split('/')) > 1 else 'UNK'
    )

    print(f"     ✓ {len(df):,} papers loaded after filtering.")
    return df


# =============================================================================
# STEP 2 — BUILD JOURNAL / VOLUME NODES AND RELATIONSHIPS
# =============================================================================

def build_journal_schema(df_papers: pd.DataFrame):
    """
    Derive Journal and Volume nodes from the journal-article subset.

    Volume deduplication:
      A Volume is defined as the unique combination of (venue, year).
      This mirrors real-world semantics: a journal publishes one or a few
      volumes per year, not one per paper.

    Relationships produced:
      (Paper)  -[:PUBLISHED_IN_VOLUME]-> (Volume)
      (Volume) -[:BELONGS_TO]->          (Journal)

    Synthetic fields on Journal nodes:
      publisher, issn, impact_factor  (not available in the DBLP dump).
    """
    df_j = df_papers[df_papers['type'] == 'Journal'].copy()

    # ── Journal nodes: one per unique venue string ───────────────────────────
    df_nodes_journals = df_j[['venue', 'raw_acronym']].drop_duplicates('venue').copy()
    df_nodes_journals['journal_id']    = [f"jour_{uuid.uuid4().hex[:6]}" for _ in range(len(df_nodes_journals))]
    df_nodes_journals['name']          = df_nodes_journals['raw_acronym'].map(VENUE_FULL_NAMES).fillna(df_nodes_journals['venue'])
    df_nodes_journals['acronym']       = df_nodes_journals['raw_acronym']
    df_nodes_journals['publisher']     = random.choices(PUBLISHERS, k=len(df_nodes_journals))          # synthetic
    df_nodes_journals['issn']          = [f"{random.randint(1000,9999)}-{random.randint(1000,9999)}"   # synthetic
                                           for _ in range(len(df_nodes_journals))]
    df_nodes_journals['impact_factor'] = [round(random.uniform(1.5, 12.0), 2)                          # synthetic
                                           for _ in range(len(df_nodes_journals))]
    df_nodes_journals = df_nodes_journals[['journal_id', 'name', 'acronym', 'publisher', 'issn', 'impact_factor']]

    # Lookup: venue_string -> journal_id
    venue_to_acronym = dict(zip(df_j['venue'], df_j['raw_acronym']))
    acronym_to_jid   = dict(zip(df_nodes_journals['acronym'], df_nodes_journals['journal_id']))
    venue_to_jid     = {v: acronym_to_jid.get(venue_to_acronym.get(v)) for v in df_j['venue'].unique()}

    # ── Volume nodes: ONE per (venue, year) ───
    df_vol_keys = df_j[['venue', 'year']].drop_duplicates().copy()
    df_vol_keys['volume_id'] = [f"vol_{uuid.uuid4().hex[:8]}" for _ in range(len(df_vol_keys))]

    # Carry the first real volume number found for each (venue, year) group,
    # falling back to '1' when DBLP does not provide one.
    vol_number_map = (
        df_j.groupby(['venue', 'year'])['volume']
        .first()
        .reset_index()
        .rename(columns={'volume': 'volumeNumber'})
    )
    df_vol_keys = df_vol_keys.merge(vol_number_map, on=['venue', 'year'], how='left')
    df_vol_keys['volumeNumber'] = df_vol_keys['volumeNumber'].fillna('1')

    df_nodes_volumes = df_vol_keys[['volume_id', 'volumeNumber', 'year']].copy()

    # ── Relationships ────────────────────────────────────────────────────────
    # Paper -> Volume: each paper joins to its (venue, year) volume
    df_j = df_j.merge(df_vol_keys[['venue', 'year', 'volume_id']], on=['venue', 'year'], how='left')
    df_rels_paper_vol = df_j[['paper_id', 'volume_id']].copy()

    # Volume -> Journal
    df_rels_vol_journal = df_vol_keys[['volume_id', 'venue']].copy()
    df_rels_vol_journal['journal_id'] = df_rels_vol_journal['venue'].map(venue_to_jid)
    df_rels_vol_journal = df_rels_vol_journal[['volume_id', 'journal_id']].dropna()

    return df_nodes_journals, df_nodes_volumes, df_rels_paper_vol, df_rels_vol_journal


# =============================================================================
# STEP 3 — BUILD CONFERENCE / WORKSHOP / EDITION NODES AND RELATIONSHIPS
# =============================================================================

def build_conference_schema(df_papers: pd.DataFrame):
    """
    Derive Conference, Workshop, and Edition nodes from conference papers.

    Edition deduplication:
      An Edition is defined as the unique combination of (venue, year).
      This mirrors real-world semantics: SIGMOD 2019 is ONE edition regardless
      of how many papers it contains.

    Relationships produced:
      (Paper)   -[:PRESENTED_IN]-> (Edition)
      (Edition) -[:EDITION_OF]->   (Conference)   [separate file]
      (Edition) -[:EDITION_OF]->   (Workshop)     [separate file]

    The edition->venue split into two files keeps ID spaces unambiguous for
    neo4j-admin (Conference and Workshop are different node label spaces).

    Synthetic fields:
      city  on Edition nodes  (DBLP does not store location).
      field on Conference / Workshop nodes.
    """
    df_c = df_papers[df_papers['type'] == 'Conference'].copy()
    df_c['is_workshop'] = df_c['venue'].str.contains('Workshop', case=False, na=False)

    # ── Edition nodes: ONE per (venue, year) ────────────────────────────────
    df_ed_keys = df_c[['venue', 'year', 'is_workshop']].drop_duplicates(subset=['venue', 'year']).copy()
    df_ed_keys['edition_id'] = [f"ed_{uuid.uuid4().hex[:8]}" for _ in range(len(df_ed_keys))]
    df_ed_keys['city']       = [random.choice(CITIES) for _ in range(len(df_ed_keys))]  # synthetic

    df_nodes_editions = df_ed_keys[['edition_id', 'year', 'city']].copy()

    # Join each paper to its (venue, year) edition
    df_c = df_c.merge(df_ed_keys[['venue', 'year', 'edition_id']], on=['venue', 'year'], how='left')

    # ── Conference nodes (non-workshop venues only) ──────────────────────────
    df_conf_raw = (
        df_c[~df_c['is_workshop']][['venue', 'raw_acronym']]
        .drop_duplicates('venue').copy()
    )
    df_conf_raw['conf_id'] = [f"conf_{uuid.uuid4().hex[:6]}" for _ in range(len(df_conf_raw))]
    df_conf_raw['name']    = df_conf_raw['raw_acronym'].map(VENUE_FULL_NAMES).fillna(df_conf_raw['venue'])
    df_conf_raw['acronym'] = df_conf_raw['raw_acronym']
    df_conf_raw['field']   = [random.choice(FIELDS_LIST) for _ in range(len(df_conf_raw))]  # synthetic
    df_nodes_conferences   = df_conf_raw[['conf_id', 'name', 'acronym', 'field']].copy()

    # ── Workshop nodes ────────────────────────────────────────────────────────
    df_work_raw = (
        df_c[df_c['is_workshop']][['venue', 'raw_acronym']]
        .drop_duplicates('venue').copy()
    )
    df_work_raw['workshop_id'] = [f"ws_{uuid.uuid4().hex[:6]}" for _ in range(len(df_work_raw))]
    df_work_raw['name']        = df_work_raw['venue']
    df_work_raw['acronym']     = df_work_raw['raw_acronym']
    df_work_raw['field']       = [random.choice(FIELDS_LIST) for _ in range(len(df_work_raw))]  # synthetic
    df_nodes_workshops         = df_work_raw[['workshop_id', 'name', 'acronym', 'field']].copy()

    # ── Relationships ────────────────────────────────────────────────────────
    df_rels_paper_edition = df_c[['paper_id', 'edition_id']].copy()

    conf_venue_to_id = dict(zip(df_conf_raw['venue'], df_conf_raw['conf_id']))
    work_venue_to_id = dict(zip(df_work_raw['venue'], df_work_raw['workshop_id']))

    # Edition -> Conference  (non-workshop editions only)
    df_ed_conf = df_ed_keys[~df_ed_keys['is_workshop']][['edition_id', 'venue']].copy()
    df_ed_conf['conf_id'] = df_ed_conf['venue'].map(conf_venue_to_id)
    df_rels_edition_conf  = df_ed_conf[['edition_id', 'conf_id']].dropna()

    # Edition -> Workshop  (workshop editions only)
    df_ed_ws = df_ed_keys[df_ed_keys['is_workshop']][['edition_id', 'venue']].copy()
    df_ed_ws['workshop_id']  = df_ed_ws['venue'].map(work_venue_to_id)
    df_rels_edition_ws       = df_ed_ws[['edition_id', 'workshop_id']].dropna()

    return (
        df_nodes_conferences, df_nodes_workshops, df_nodes_editions,
        df_rels_paper_edition, df_rels_edition_conf, df_rels_edition_ws,
    )


# =============================================================================
# STEP 4 — BUILD AUTHOR / ORGANIZATION NODES AND RELATIONSHIPS
# =============================================================================

def build_author_schema(df_papers: pd.DataFrame):
    """
    Derive Author and Organization nodes, plus WROTE and AFFILIATED_TO
    relationships.

    Real data:
      - Author names and their order within each paper come from DBLP.
      - Position 0 within the author list is labelled 'Main Author'
        (corresponding author); all others are 'Co-author'.

    Synthetic fields:
      - email   : deterministically derived from the author name.
      - h_index : random integer 1-60 (placeholder; could be enriched via
                  the OpenAlex API in a separate pipeline step).
      - org_id  : randomly sampled from the 20-entry ORG_NAMES seed list,
                  satisfying the A.3 affiliation requirement with realistic
                  institution names.
    """
    df_papers = df_papers.copy()
    df_papers['authors_list'] = df_papers['authors'].str.split('|')

    # Explode the author list so each row is one (paper, author) pair.
    df_exp = df_papers[['paper_id', 'authors_list']].explode('authors_list').dropna()
    df_exp['author_name'] = df_exp['authors_list'].str.strip()
    df_exp = df_exp[df_exp['author_name'] != '']

    # Assign role based on position within the per-paper author list.
    df_exp['position'] = df_exp.groupby('paper_id').cumcount()
    df_exp['role']     = df_exp['position'].apply(
        lambda x: 'Main Author' if x == 0 else 'Co-author'
    )

    # ── Author nodes ─────────────────────────────────────────────────────────
    unique_names   = df_exp['author_name'].unique()
    author_records = [
        {
            'author_id': f'auth_{i}',
            'name':      name,
            'email':     make_email(name),       # synthetic
            'h_index':   random.randint(1, 60),  # synthetic
        }
        for i, name in enumerate(unique_names)
    ]
    df_nodes_authors = pd.DataFrame(author_records)
    auth_name_to_id  = dict(zip(df_nodes_authors['name'], df_nodes_authors['author_id']))

    # ── WROTE relationship ────────────────────────────────────────────────────
    df_rels_wrote              = df_exp.copy()
    df_rels_wrote['author_id'] = df_rels_wrote['author_name'].map(auth_name_to_id)
    df_rels_wrote              = df_rels_wrote[['author_id', 'paper_id', 'role']]

    # ── Organization nodes (synthetic seed list) ─────────────────────────────
    org_records = [
        {
            'org_id': f'org_{i}',
            'name':   name,
            'type':   'Company' if any(k in name for k in ['Research', 'AI', 'Services', 'AWS'])
                      else 'University',
        }
        for i, name in enumerate(ORG_NAMES)
    ]
    df_nodes_orgs = pd.DataFrame(org_records)

    # ── AFFILIATED_TO relationship (random, synthetic) ────────────────────────
    df_rels_affiliated = pd.DataFrame([
        {'author_id': r['author_id'], 'org_id': random.choice(org_records)['org_id']}
        for r in author_records
    ])

    return df_nodes_authors, df_nodes_orgs, df_rels_wrote, df_rels_affiliated


# =============================================================================
# STEP 5 — BUILD KEYWORD NODES, HAS_KEYWORD RELATIONSHIPS, AND ABSTRACTS
# =============================================================================

def build_keyword_schema(df_papers: pd.DataFrame):
    """
    Assign 2-4 keywords to every paper (synthetic) and generate a templated
    abstract referencing those keywords.

    The first 7 entries in KEYWORD_LABELS are the exact keywords that define
    the 'database community' in Part C of the lab. Assigning them uniformly
    across the paper set ensures that query C.2 (90% threshold) will classify
    all filtered venues as database-community venues, which is the intended
    behaviour for a lab with synthetic keyword data.

    Returns:
      df_nodes_keywords     — Keyword node table.
      df_rels_has_keyword   — HAS_KEYWORD edge table.
      theme_buckets         — {keyword_label: [paper_id, ...]} for citation sampling.
      paper_primary_theme   — {paper_id: primary_keyword_label}.
      abstracts             — list of abstract strings aligned with df_papers row order.
    """
    df_nodes_keywords = pd.DataFrame([
        {'keyword_id': f'kw_{i}', 'label': label}
        for i, label in enumerate(KEYWORD_LABELS)
    ])
    kw_to_id = dict(zip(df_nodes_keywords['label'], df_nodes_keywords['keyword_id']))

    theme_buckets: dict       = {kw: [] for kw in KEYWORD_LABELS}
    paper_primary_theme: dict = {}
    rels_has_keyword          = []
    abstracts                 = []

    for p_id, title in zip(df_papers['paper_id'], df_papers['title']):
        assigned = random.sample(KEYWORD_LABELS, k=random.randint(2, 4))
        primary  = assigned[0]
        theme_buckets[primary].append(p_id)
        paper_primary_theme[p_id] = primary

        for kw in assigned:
            rels_has_keyword.append({'paper_id': p_id, 'keyword_id': kw_to_id[kw]})

        abstracts.append(
            f"This paper explores novel approaches in {', '.join(assigned)}. "
            f"Specifically, we investigate the challenges associated with: {title}"
        )

    df_rels_has_keyword = pd.DataFrame(rels_has_keyword)
    return df_nodes_keywords, df_rels_has_keyword, theme_buckets, paper_primary_theme, abstracts


# =============================================================================
# STEP 6 — BUILD CITATION RELATIONSHIPS (HYBRID: REAL + SYNTHETIC TOP-UP)
# =============================================================================

def build_citation_schema(
    df_papers: pd.DataFrame,
    theme_buckets: dict,
    paper_primary_theme: dict,
) -> pd.DataFrame:
    """
    Build CITES relationships using a two-tier strategy:

    Tier 1 — Real citations:
      DBLP encodes citation keys in the 'cite' field. We retain those that
      resolve to another paper present in our filtered subgraph.
      Note: when filtering to a narrow year window, internal overlap is low,
      so real citations may be close to zero — this is expected and correct.

    Tier 2 — Synthetic top-up:
      Every paper is guaranteed a minimum of 5 outgoing citations (target
      5-10). Synthetic citations are sampled from papers sharing the same
      primary keyword and are always directed at older papers (year <= citing
      paper year) to preserve chronological consistency.

    The combined result gives every paper enough citations for Part B (top
    cited, h-index) and Part C (top-100 papers by citation count) to work.
    """
    df_papers = df_papers.copy()
    df_papers['cites_list'] = df_papers['cites'].str.split('|')
    df_exp = df_papers[['paper_id', 'cites_list']].explode('cites_list').dropna()
    df_exp['cites_list'] = df_exp['cites_list'].str.strip()

    # Keep only citations that resolve within the filtered subgraph.
    valid_ids    = set(df_papers['paper_id'])
    df_rels_real = (
        df_exp[df_exp['cites_list'].isin(valid_ids)]
        .rename(columns={'paper_id': 'from_paper', 'cites_list': 'to_paper'})
        .copy()
    )

    real_counts    = df_rels_real.groupby('from_paper').size().to_dict()
    year_dict      = df_papers.set_index('paper_id')['year'].to_dict()
    paper_ids_list = df_papers['paper_id'].tolist()

    synthetic = []
    for pid in paper_ids_list:
        p_year   = year_dict[pid]
        needed   = max(0, random.randint(5, 10) - real_counts.get(pid, 0))
        pool     = theme_buckets[paper_primary_theme[pid]]
        added    = 0
        attempts = 0
        while added < needed and attempts < 50:
            target = random.choice(pool)
            if target != pid and year_dict[target] <= p_year:
                synthetic.append({'from_paper': pid, 'to_paper': target})
                added += 1
            attempts += 1

    df_rels_syn = pd.DataFrame(synthetic) if synthetic else pd.DataFrame(columns=['from_paper', 'to_paper'])

    df_rels_cites = (
        pd.concat([df_rels_real, df_rels_syn]).drop_duplicates(subset=['from_paper', 'to_paper'])
        if not df_rels_syn.empty else df_rels_real
    )

    print(f"     ✓ {len(df_rels_real):,} real internal citations retained.")
    print(f"     ✓ {len(df_rels_syn):,} synthetic thematic citations added.")
    print(f"     ✓ {len(df_rels_cites):,} total CITES relationships.")
    return df_rels_cites


# =============================================================================
# STEP 7 — BUILD REVIEW RELATIONSHIPS
# =============================================================================

def build_review_schema(
    df_papers: pd.DataFrame,
    df_nodes_authors: pd.DataFrame,
    df_rels_wrote: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign exactly 3 reviewers per paper (synthetic, per A.3 requirements).

    Constraints enforced:
      - A reviewer must not be one of the paper's own authors.
      - Each review record carries: review_id, score (1-10), decision, comments.

    Design note:
      The review is stored as a rich relationship (Author)-[:REVIEWED]->(Paper)
      rather than a separate Review node. This is the simpler approach and
      satisfies all lab queries.
    """
    paper_to_authors = df_rels_wrote.groupby('paper_id')['author_id'].apply(set).to_dict()
    all_author_ids   = tuple(df_nodes_authors['author_id'].tolist())

    records = []
    for paper_id in df_papers['paper_id']:
        excluded  = paper_to_authors.get(paper_id, set())
        reviewers: set = set()
        while len(reviewers) < 3:
            cand = random.choice(all_author_ids)
            if cand not in excluded:
                reviewers.add(cand)
        for rid in reviewers:
            score = random.randint(1, 10)
            records.append({
                'author_id': rid,
                'paper_id':  paper_id,
                'review_id': f"rev_{uuid.uuid4().hex[:8]}",
                'score':     score,
                'decision':  review_decision(score),
                'comments':  review_comment(score),
            })

    return pd.DataFrame(records)


# =============================================================================
# STEP 8 — EXPORT RAW CSVs
# =============================================================================

def export_raw_csvs(dataframes: dict):
    """
    Write all node and relationship DataFrames to Neo4j/ as plain CSV files.

    These files serve two purposes:
      1. Human-readable inspection before import.
      2. Source for Cypher LOAD CSV (suitable for smaller datasets or
         quick ad-hoc loading without neo4j-admin).
    """
    os.makedirs(DIR_RAW, exist_ok=True)
    for filename, df in dataframes.items():
        path = os.path.join(DIR_RAW, filename)
        df.to_csv(path, index=False)
        print(f"     -> {path}  ({len(df):,} rows)")


# =============================================================================
# STEP 9 — PREPARE NEO4J-ADMIN IMPORT CSVs
# =============================================================================

def prepare_neo4j_admin_csvs(dataframes: dict):
    """
    Rewrite the raw CSVs with neo4j-admin header annotations and save them
    to Neo4j_import/.

    Header annotation rules applied:
      - Node primary key      ->  column_name:ID(Label)
      - Relationship start    ->  :START_ID(Label)
      - Relationship end      ->  :END_ID(Label)
      - Integer columns       ->  column_name:int
      - Float columns         ->  column_name:float
      (Unlabelled columns default to string, which is correct for all others.)

    IMPORTANT: Edition->Conference and Edition->Workshop are kept as two
    separate relationship files. They share the same :START_ID(Edition) but
    resolve to different END_ID spaces (Conference vs Workshop). Merging them
    into one file would cause neo4j-admin to throw an ID resolution error.
    """
    os.makedirs(DIR_IMPORT, exist_ok=True)

    # ── Node header maps ──────────────────────────────────────────────────────
    node_renames = {
        'nodes_papers.csv': {
            'paper_id': 'paper_id:ID(Paper)',
            'year':     'year:int',
        },
        'nodes_authors.csv': {
            'author_id': 'author_id:ID(Author)',
            'h_index':   'h_index:int',
        },
        'nodes_journals.csv': {
            'journal_id':    'journal_id:ID(Journal)',
            'impact_factor': 'impact_factor:float',
        },
        'nodes_conferences.csv': {
            'conf_id': 'conf_id:ID(Conference)',
        },
        'nodes_workshops.csv': {
            'workshop_id': 'workshop_id:ID(Workshop)',
        },
        'nodes_editions.csv': {
            'edition_id': 'edition_id:ID(Edition)',
            'year':       'year:int',
        },
        'nodes_volumes.csv': {
            'volume_id': 'volume_id:ID(Volume)',
            'year':      'year:int',
        },
        'nodes_organizations.csv': {
            'org_id': 'org_id:ID(Organization)',
        },
        'nodes_keywords.csv': {
            'keyword_id': 'keyword_id:ID(Keyword)',
        },
    }

    # ── Relationship header maps ──────────────────────────────────────────────
    rel_renames = {
        'rels_wrote.csv': {
            'author_id': ':START_ID(Author)',
            'paper_id':  ':END_ID(Paper)',
        },
        'rels_cites.csv': {
            'from_paper': ':START_ID(Paper)',
            'to_paper':   ':END_ID(Paper)',
        },
        'rels_has_keyword.csv': {
            'paper_id':   ':START_ID(Paper)',
            'keyword_id': ':END_ID(Keyword)',
        },
        'rels_affiliated_to.csv': {
            'author_id': ':START_ID(Author)',
            'org_id':    ':END_ID(Organization)',
        },
        'rels_reviewed.csv': {
            'author_id': ':START_ID(Author)',
            'paper_id':  ':END_ID(Paper)',
            'score':     'score:int',
        },
        'rels_paper_volume.csv': {
            'paper_id':  ':START_ID(Paper)',
            'volume_id': ':END_ID(Volume)',
        },
        'rels_volume_journal.csv': {
            'volume_id':  ':START_ID(Volume)',
            'journal_id': ':END_ID(Journal)',
        },
        'rels_paper_edition.csv': {
            'paper_id':   ':START_ID(Paper)',
            'edition_id': ':END_ID(Edition)',
        },
        # These two MUST remain separate files (different END_ID label spaces).
        'rels_edition_conference.csv': {
            'edition_id': ':START_ID(Edition)',
            'conf_id':    ':END_ID(Conference)',
        },
        'rels_edition_workshop.csv': {
            'edition_id':  ':START_ID(Edition)',
            'workshop_id': ':END_ID(Workshop)',
        },
    }

    all_renames = {**node_renames, **rel_renames}

    for filename, rename_map in all_renames.items():
        src = os.path.join(DIR_RAW, filename)
        dst = os.path.join(DIR_IMPORT, filename)
        if not os.path.exists(src):
            print(f"     [!] Skipping {filename} — not found in {DIR_RAW}/")
            continue
        df = pd.read_csv(src)
        df.rename(columns=rename_map, inplace=True)
        df.to_csv(dst, index=False)
        print(f"     -> {dst}  ({len(df):,} rows, {len(df.columns)} cols)")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def generate_graph_dataset():
    print("=" * 60)
    print("  SDM Lab — Property Graph Dataset Generator")
    print("=" * 60)

    # ── 1. Load & filter raw DBLP data ───────────────────────────────
    df_papers = load_dblp_data()

    # ── 2. Journal / Volume schema ───────────────────────────────────
    print("\n[2/9] Building journal / volume schema...")
    df_nodes_journals, df_nodes_volumes, df_rels_paper_vol, df_rels_vol_journal = (
        build_journal_schema(df_papers)
    )
    print(f"     ✓ {len(df_nodes_journals)} journals,  {len(df_nodes_volumes)} volumes.")

    # ── 3. Conference / Workshop / Edition schema ─────────────────────
    print("\n[3/9] Building conference / workshop / edition schema...")
    (
        df_nodes_conferences, df_nodes_workshops, df_nodes_editions,
        df_rels_paper_edition, df_rels_edition_conf, df_rels_edition_ws,
    ) = build_conference_schema(df_papers)
    print(
        f"     ✓ {len(df_nodes_conferences)} conferences,  "
        f"{len(df_nodes_workshops)} workshops,  "
        f"{len(df_nodes_editions):,} editions."
    )

    # ── 4. Author / Organization schema ──────────────────────────────
    print("\n[4/9] Building author / organization schema...")
    df_nodes_authors, df_nodes_orgs, df_rels_wrote, df_rels_affiliated = (
        build_author_schema(df_papers)
    )
    print(f"     ✓ {len(df_nodes_authors):,} authors,  {len(df_nodes_orgs)} organizations.")

    # ── 5. Keywords & abstracts ───────────────────────────────────────
    print("\n[5/9] Assigning keywords and generating abstracts...")
    (
        df_nodes_keywords, df_rels_has_keyword,
        theme_buckets, paper_primary_theme, abstracts,
    ) = build_keyword_schema(df_papers)
    df_papers['abstract'] = abstracts
    df_papers['abstract'] = df_papers['abstract'].str.replace(
        r'[\n\r\u2028\u2029]+', ' ', regex=True
    )
    print(f"     ✓ {len(df_nodes_keywords)} keywords,  {len(df_rels_has_keyword):,} HAS_KEYWORD edges.")

    # ── 6. Citations ──────────────────────────────────────────────────
    print("\n[6/9] Processing citations (real + synthetic top-up)...")
    df_rels_cites = build_citation_schema(df_papers, theme_buckets, paper_primary_theme)

    # ── 7. Reviews ────────────────────────────────────────────────────
    print("\n[7/9] Assigning reviewers (3 per paper, no self-review)...")
    df_rels_reviewed = build_review_schema(df_papers, df_nodes_authors, df_rels_wrote)
    print(f"     ✓ {len(df_rels_reviewed):,} REVIEWED relationships ({len(df_papers) * 3:,} expected).")

    # ── 8. Export raw CSVs ────────────────────────────────────────────
    print(f"\n[8/9] Exporting raw CSVs to {DIR_RAW}/...")

    # Ensure correct types before exporting.
    df_papers['year']                    = df_papers['year'].astype(int)
    df_nodes_volumes['year']             = df_nodes_volumes['year'].astype(int)
    df_nodes_editions['year']            = df_nodes_editions['year'].astype(int)
    df_nodes_authors['h_index']          = df_nodes_authors['h_index'].astype(int)
    df_rels_reviewed['score']            = df_rels_reviewed['score'].astype(int)
    df_nodes_journals['impact_factor']   = df_nodes_journals['impact_factor'].round(2)

    raw_dataframes = {
        # ── Nodes ──
        'nodes_papers.csv':        df_papers[['paper_id', 'title', 'abstract', 'year', 'pages', 'doi']],
        'nodes_authors.csv':       df_nodes_authors,
        'nodes_keywords.csv':      df_nodes_keywords,
        'nodes_organizations.csv': df_nodes_orgs,
        'nodes_journals.csv':      df_nodes_journals,
        'nodes_volumes.csv':       df_nodes_volumes,
        'nodes_conferences.csv':   df_nodes_conferences,
        'nodes_workshops.csv':     df_nodes_workshops,
        'nodes_editions.csv':      df_nodes_editions,
        # ── Relationships ──
        'rels_wrote.csv':               df_rels_wrote,
        'rels_cites.csv':               df_rels_cites,
        'rels_has_keyword.csv':         df_rels_has_keyword,
        'rels_affiliated_to.csv':       df_rels_affiliated,
        'rels_reviewed.csv':            df_rels_reviewed,
        'rels_paper_volume.csv':        df_rels_paper_vol,
        'rels_volume_journal.csv':      df_rels_vol_journal,
        'rels_paper_edition.csv':       df_rels_paper_edition,
        'rels_edition_conference.csv':  df_rels_edition_conf,
        'rels_edition_workshop.csv':    df_rels_edition_ws,
    }
    export_raw_csvs(raw_dataframes)

    # ── 9. Prepare neo4j-admin import CSVs ───────────────────────────
    print(f"\n[9/9] Preparing neo4j-admin annotated CSVs in {DIR_IMPORT}/...")
    prepare_neo4j_admin_csvs(raw_dataframes)

    # ── Summary ───────────────────────────────────────────────────────
    total_nodes = sum(len(df) for k, df in raw_dataframes.items() if k.startswith('nodes_'))
    total_rels  = sum(len(df) for k, df in raw_dataframes.items() if k.startswith('rels_'))
    print("\n" + "=" * 60)
    print(f"  Done!  {total_nodes:,} nodes  |  {total_rels:,} relationships")
    print(f"  Raw CSVs    -> {DIR_RAW}/")
    print(f"  Import CSVs -> {DIR_IMPORT}/")
    print("=" * 60)


if __name__ == '__main__':
    generate_graph_dataset()
