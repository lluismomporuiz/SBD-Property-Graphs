"""
FormatCSV.py  (Part A.2)
========================
Reads raw DBLP CSV exports, filters to the database / data-mining subgraph,
and produces two sets of output CSVs that represent the A.1 graph schema:

  Neo4j/          -- plain CSVs for human inspection / LOAD CSV.
  Neo4j_import/   -- neo4j-admin annotated CSVs for bulk import.

A.1 schema (no affiliations, no review properties -- those are added in A.3):
  Nodes    : Paper, Author, Keyword, Journal, Volume,
             Conference, Workshop, Edition
  Edges    : WROTE {role}, CITES, HAS_KEYWORD,
             REVIEWED (no properties),
             PUBLISHED_IN_VOLUME, BELONGS_TO,
             PRESENTED_IN, EDITION_OF

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
MIN_YEAR = 2015

# Venue filter: journals and conferences in the DB / DM domain.
REGEX_PATTERN = (
    r'VLDB|SIGMOD|ICDE|KDD|TODS|TKDE|WSDM|WWW|Workshop|'
    r'Trans\. Database Syst\.|Trans\. Knowl\. Data Eng\.|'
    r'Inf\. Syst\.|Data Knowl\. Eng\.|Knowl\. Based Syst\.|'
    r'Trans\. Web|World Wide Web|Expert Syst\.'
)

# Full names for the most common venue acronyms found in DBLP keys.
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

# Synthetic data pools used for fields not present in the DBLP dump.
FIELDS_LIST = [
    'Databases', 'Data Mining', 'Artificial Intelligence',
    'Machine Learning', 'Information Retrieval', 'Data Science',
]
CITIES = [
    'San Francisco', 'New York', 'London', 'Berlin', 'Tokyo',
    'Sydney', 'Paris', 'Barcelona', 'Toronto', 'Singapore',
]
PUBLISHERS = ['ACM', 'IEEE', 'Springer', 'Elsevier', 'VLDB Endowment']

# Keywords for the graph. The first 7 match exactly the database community
# definition required by Part C so that query C.2 works correctly.
KEYWORD_LABELS = [
    'Data Management', 'Indexing', 'Data Modeling', 'Big Data',
    'Data Processing', 'Data Storage', 'Data Querying',   # DB community (C.1)
    'Machine Learning', 'Graph Databases', 'Optimization',
    'Deep Learning', 'Natural Language Processing',
]

# Output directories
DIR_RAW    = 'Neo4j'         # plain CSVs (inspection / LOAD CSV)
DIR_IMPORT = 'Neo4j_import'  # neo4j-admin annotated CSVs


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_doi(ee_str: str) -> str:
    """
    Extract a DOI from the DBLP 'ee' field (multiple URLs separated by '|').
    Returns a synthetic fallback DOI when no doi.org link is found.
    """
    if pd.isna(ee_str):
        return f"10.fake/{uuid.uuid4().hex[:8]}"
    for link in str(ee_str).split('|'):
        if 'doi.org' in link:
            return link.replace('https://doi.org/', '').strip()
    return f"10.fake/{uuid.uuid4().hex[:8]}"


def make_email(name: str) -> str:
    """Derive a plausible academic e-mail address from an author name."""
    clean = re.sub(r'[^a-zA-Z\s]', '', name).lower().replace(' ', '.')
    return f"{clean}@university.edu"


# =============================================================================
# STEP 1 -- LOAD AND FILTER RAW DBLP DATA
# =============================================================================

def load_dblp_data() -> pd.DataFrame:
    """
    Read dblp_article.csv and dblp_inproceedings.csv in 100k-row chunks,
    apply the year and venue regex filters, and return a single DataFrame.

    Real fields kept: paper_id (DBLP key), title, year, authors, venue,
                      volume, cites, doi.
    Synthetic replacements: pages (DBLP page data is unreliable).
    """
    print(f"[1/8] Loading DBLP data (MIN_YEAR={MIN_YEAR})...")

    cols_article = [
        'key:string', 'title:string', 'year:int', 'author:string[]',
        'journal:string', 'cite:string[]', 'volume:string', 'pages:string', 'ee:string[]',
    ]
    cols_inproc = [
        'key:string', 'title:string', 'year:int', 'author:string[]',
        'booktitle:string', 'cite:string[]', 'pages:string', 'ee:string[]',
    ]

    try:
        with open('output_csv/dblp_article_header.csv', 'r', encoding='utf-8') as fh:
            article_headers = fh.read().strip().split(';')
        with open('output_csv/dblp_inproceedings_header.csv', 'r', encoding='utf-8') as fh:
            inproc_headers = fh.read().strip().split(';')
    except FileNotFoundError:
        raise SystemExit("[ERROR] Header files not found in output_csv/. Aborting.")

    chunks = []

    print("     -> Reading dblp_article.csv ...")
    try:
        for chunk in pd.read_csv(
            'output_csv/dblp_article.csv', sep=';', names=article_headers,
            usecols=cols_article, dtype=str, on_bad_lines='skip', chunksize=100_000,
        ):
            chunk = chunk.rename(columns={
                'key:string': 'paper_id',  'title:string': 'title',
                'year:int':   'year',      'author:string[]': 'authors',
                'journal:string': 'venue', 'volume:string': 'volume',
                'cite:string[]': 'cites',  'pages:string': 'pages',
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
        print("     [WARN] dblp_article.csv not found -- skipping journal articles.")

    print("     -> Reading dblp_inproceedings.csv ...")
    try:
        for chunk in pd.read_csv(
            'output_csv/dblp_inproceedings.csv', sep=';', names=inproc_headers,
            usecols=cols_inproc, dtype=str, on_bad_lines='skip', chunksize=100_000,
        ):
            chunk = chunk.rename(columns={
                'key:string': 'paper_id',     'title:string': 'title',
                'year:int':   'year',         'author:string[]': 'authors',
                'booktitle:string': 'venue',  'cite:string[]': 'cites',
                'pages:string': 'pages',      'ee:string[]': 'ee',
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
        print("     [WARN] dblp_inproceedings.csv not found -- skipping inproceedings.")

    if not chunks:
        raise SystemExit("[ERROR] No data loaded after filtering. Check inputs.")

    df = pd.concat(chunks, ignore_index=True).dropna(subset=['authors', 'title'])
    df['title']       = df['title'].str.replace(r'[\n\r\u2028\u2029]+', ' ', regex=True)
    df['doi']         = df['ee'].apply(extract_doi)
    df['pages']       = [
        f"{s}-{s + random.randint(8, 20)}"
        for s in (random.randint(1, 500) for _ in range(len(df)))
    ]
    df['raw_acronym'] = df['paper_id'].apply(
        lambda x: str(x).split('/')[1].upper() if len(str(x).split('/')) > 1 else 'UNK'
    )

    print(f"     [OK] {len(df):,} papers loaded after filtering.")
    return df


# =============================================================================
# STEP 2 -- JOURNAL AND VOLUME SCHEMA
# =============================================================================

def build_journal_schema(df_papers: pd.DataFrame):
    """
    Build Journal and Volume nodes from journal-type papers.

    One Volume per (venue, year) pair -- not one per paper.
    Journal nodes carry publisher and issn as synthetic attributes;
    impact_factor is intentionally omitted (computed by query B.3).

    Returns: df_nodes_journals, df_nodes_volumes,
             df_rels_paper_vol, df_rels_vol_journal
    """
    df_j = df_papers[df_papers['type'] == 'Journal'].copy()

    # Journal nodes -- one per unique venue string.
    df_nodes_journals = df_j[['venue', 'raw_acronym']].drop_duplicates('venue').copy()
    df_nodes_journals['journal_id'] = [f"jour_{uuid.uuid4().hex[:6]}" for _ in range(len(df_nodes_journals))]
    df_nodes_journals['name']       = df_nodes_journals['raw_acronym'].map(VENUE_FULL_NAMES).fillna(df_nodes_journals['venue'])
    df_nodes_journals['acronym']    = df_nodes_journals['raw_acronym']
    df_nodes_journals['publisher']  = random.choices(PUBLISHERS, k=len(df_nodes_journals))   # synthetic
    df_nodes_journals['issn']       = [
        f"{random.randint(1000,9999)}-{random.randint(1000,9999)}"
        for _ in range(len(df_nodes_journals))
    ]  # synthetic
    df_nodes_journals = df_nodes_journals[['journal_id', 'name', 'acronym', 'publisher', 'issn']]

    venue_to_acronym = dict(zip(df_j['venue'], df_j['raw_acronym']))
    acronym_to_jid   = dict(zip(df_nodes_journals['acronym'], df_nodes_journals['journal_id']))
    venue_to_jid     = {v: acronym_to_jid.get(venue_to_acronym.get(v)) for v in df_j['venue'].unique()}

    # Volume nodes -- one per (venue, year).
    df_vol_keys = df_j[['venue', 'year']].drop_duplicates().copy()
    df_vol_keys['volume_id'] = [f"vol_{uuid.uuid4().hex[:8]}" for _ in range(len(df_vol_keys))]
    vol_number_map = (
        df_j.groupby(['venue', 'year'])['volume'].first()
        .reset_index().rename(columns={'volume': 'volumeNumber'})
    )
    df_vol_keys = df_vol_keys.merge(vol_number_map, on=['venue', 'year'], how='left')
    df_vol_keys['volumeNumber'] = df_vol_keys['volumeNumber'].fillna('1')
    df_nodes_volumes = df_vol_keys[['volume_id', 'volumeNumber', 'year']].copy()
    df_nodes_volumes['year'] = df_nodes_volumes['year'].astype(int)

    # Relationships.
    df_j = df_j.merge(df_vol_keys[['venue', 'year', 'volume_id']], on=['venue', 'year'], how='left')
    df_rels_paper_vol = df_j[['paper_id', 'volume_id']].copy()

    df_rels_vol_journal = df_vol_keys[['volume_id', 'venue']].copy()
    df_rels_vol_journal['journal_id'] = df_rels_vol_journal['venue'].map(venue_to_jid)
    df_rels_vol_journal = df_rels_vol_journal[['volume_id', 'journal_id']].dropna()

    return df_nodes_journals, df_nodes_volumes, df_rels_paper_vol, df_rels_vol_journal


# =============================================================================
# STEP 3 -- CONFERENCE, WORKSHOP, AND EDITION SCHEMA
# =============================================================================

def build_conference_schema(df_papers: pd.DataFrame):
    """
    Build Conference, Workshop, and Edition nodes from inproceedings papers.

    One Edition per (venue, year) pair.
    Workshops are identified by the substring 'Workshop' in the venue name.
    The EDITION_OF relationship is split into two files to keep Conference
    and Workshop ID spaces separate for neo4j-admin import.

    Returns: df_nodes_conferences, df_nodes_workshops, df_nodes_editions,
             df_rels_paper_edition, df_rels_edition_conf, df_rels_edition_ws
    """
    df_c = df_papers[df_papers['type'] == 'Conference'].copy()
    df_c['is_workshop'] = df_c['venue'].str.contains('Workshop', case=False, na=False)

    # Edition nodes -- one per (venue, year).
    df_ed_keys = df_c[['venue', 'year', 'is_workshop']].drop_duplicates(subset=['venue', 'year']).copy()
    df_ed_keys['edition_id'] = [f"ed_{uuid.uuid4().hex[:8]}" for _ in range(len(df_ed_keys))]
    df_ed_keys['city']       = [random.choice(CITIES) for _ in range(len(df_ed_keys))]  # synthetic
    df_nodes_editions = df_ed_keys[['edition_id', 'year', 'city']].copy()
    df_nodes_editions['year'] = df_nodes_editions['year'].astype(int)

    df_c = df_c.merge(df_ed_keys[['venue', 'year', 'edition_id']], on=['venue', 'year'], how='left')

    # Conference nodes.
    df_conf_raw = df_c[~df_c['is_workshop']][['venue', 'raw_acronym']].drop_duplicates('venue').copy()
    df_conf_raw['conf_id'] = [f"conf_{uuid.uuid4().hex[:6]}" for _ in range(len(df_conf_raw))]
    df_conf_raw['name']    = df_conf_raw['raw_acronym'].map(VENUE_FULL_NAMES).fillna(df_conf_raw['venue'])
    df_conf_raw['acronym'] = df_conf_raw['raw_acronym']
    df_conf_raw['field']   = [random.choice(FIELDS_LIST) for _ in range(len(df_conf_raw))]  # synthetic
    df_nodes_conferences   = df_conf_raw[['conf_id', 'name', 'acronym', 'field']].copy()

    # Workshop nodes.
    df_work_raw = df_c[df_c['is_workshop']][['venue', 'raw_acronym']].drop_duplicates('venue').copy()
    df_work_raw['workshop_id'] = [f"ws_{uuid.uuid4().hex[:6]}" for _ in range(len(df_work_raw))]
    df_work_raw['name']        = df_work_raw['venue']
    df_work_raw['acronym']     = df_work_raw['raw_acronym']
    df_work_raw['field']       = [random.choice(FIELDS_LIST) for _ in range(len(df_work_raw))]  # synthetic
    df_nodes_workshops         = df_work_raw[['workshop_id', 'name', 'acronym', 'field']].copy()

    # Relationships.
    df_rels_paper_edition = df_c[['paper_id', 'edition_id']].copy()

    df_ed_conf = df_ed_keys[~df_ed_keys['is_workshop']][['edition_id', 'venue']].copy()
    df_ed_conf['conf_id'] = df_ed_conf['venue'].map(dict(zip(df_conf_raw['venue'], df_conf_raw['conf_id'])))
    df_rels_edition_conf  = df_ed_conf[['edition_id', 'conf_id']].dropna()

    df_ed_ws = df_ed_keys[df_ed_keys['is_workshop']][['edition_id', 'venue']].copy()
    df_ed_ws['workshop_id'] = df_ed_ws['venue'].map(dict(zip(df_work_raw['venue'], df_work_raw['workshop_id'])))
    df_rels_edition_ws      = df_ed_ws[['edition_id', 'workshop_id']].dropna()

    return (
        df_nodes_conferences, df_nodes_workshops, df_nodes_editions,
        df_rels_paper_edition, df_rels_edition_conf, df_rels_edition_ws,
    )


# =============================================================================
# STEP 4 -- AUTHOR SCHEMA
# =============================================================================

def build_author_schema(df_papers: pd.DataFrame):
    """
    Build Author nodes and WROTE relationships from the paper author lists.

    Author order within each paper is preserved from DBLP: position 0 is
    labelled 'Main Author' (corresponding author); all others are 'Co-author'.

    h_index is intentionally excluded from Author nodes -- it is computed
    on-the-fly by query B.4 using the citation graph.

    Real fields : name (from DBLP author list).
    Synthetic   : email (derived from name).

    Returns: df_nodes_authors, df_rels_wrote
    """
    df_papers = df_papers.copy()
    df_papers['authors_list'] = df_papers['authors'].str.split('|')

    df_exp = df_papers[['paper_id', 'authors_list']].explode('authors_list').dropna()
    df_exp['author_name'] = df_exp['authors_list'].str.strip()
    df_exp = df_exp[df_exp['author_name'] != '']

    df_exp['position'] = df_exp.groupby('paper_id').cumcount()
    df_exp['role']     = df_exp['position'].apply(
        lambda x: 'Main Author' if x == 0 else 'Co-author'
    )

    unique_names   = df_exp['author_name'].unique()
    author_records = [
        {
            'author_id': f'auth_{i}',
            'name':      name,
            'email':     make_email(name),   # synthetic
        }
        for i, name in enumerate(unique_names)
    ]
    df_nodes_authors = pd.DataFrame(author_records)
    auth_name_to_id  = dict(zip(df_nodes_authors['name'], df_nodes_authors['author_id']))

    df_rels_wrote              = df_exp.copy()
    df_rels_wrote['author_id'] = df_rels_wrote['author_name'].map(auth_name_to_id)
    df_rels_wrote              = df_rels_wrote[['author_id', 'paper_id', 'role']]

    return df_nodes_authors, df_rels_wrote


# =============================================================================
# STEP 5 -- KEYWORD AND ABSTRACT SCHEMA
# =============================================================================

def build_keyword_schema(df_papers: pd.DataFrame):
    """
    Assign 2-4 synthetic keywords per paper and generate a templated abstract.

    Returns: df_nodes_keywords, df_rels_has_keyword,
             theme_buckets, paper_primary_theme, abstracts
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

    return (
        df_nodes_keywords,
        pd.DataFrame(rels_has_keyword),
        theme_buckets,
        paper_primary_theme,
        abstracts,
    )


# =============================================================================
# STEP 6 -- CITATION SCHEMA (REAL + SYNTHETIC TOP-UP)
# =============================================================================

def build_citation_schema(
    df_papers: pd.DataFrame,
    theme_buckets: dict,
    paper_primary_theme: dict,
) -> pd.DataFrame:
    """
    Build CITES relationships.

    Tier 1: real DBLP cite keys that resolve within the filtered subgraph.
    Tier 2: synthetic thematic citations to guarantee 5-10 per paper,
            always pointing to older papers (year <= citing paper year).
    """
    df_papers = df_papers.copy()
    df_papers['cites_list'] = df_papers['cites'].str.split('|')
    df_exp = df_papers[['paper_id', 'cites_list']].explode('cites_list').dropna()
    df_exp['cites_list'] = df_exp['cites_list'].str.strip()

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

    df_rels_syn = (
        pd.DataFrame(synthetic) if synthetic
        else pd.DataFrame(columns=['from_paper', 'to_paper'])
    )
    df_rels_cites = (
        pd.concat([df_rels_real, df_rels_syn]).drop_duplicates(subset=['from_paper', 'to_paper'])
        if not df_rels_syn.empty else df_rels_real
    )

    duplicates_removed = len(df_rels_real) + len(df_rels_syn) - len(df_rels_cites)
    print(f"     [OK] {len(df_rels_real):,} real + {len(df_rels_syn):,} synthetic "
          f"- {duplicates_removed:,} duplicates = {len(df_rels_cites):,} total CITES relationships.")
    return df_rels_cites


# ========================
# STEP 7 -- REVIEW SCHEMA
# ========================

def build_review_schema(
    df_papers: pd.DataFrame,
    df_nodes_authors: pd.DataFrame,
    df_rels_wrote: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign exactly 3 reviewers per paper.

    Constraint: a reviewer cannot be an author of the paper being reviewed.

    At this stage (A.2) the REVIEWED relationship carries no properties.
    Review content (score, decision, comments) is added in part A.3 via
    Cypher queries that evolve the graph in-place.

    Returns a DataFrame with columns: author_id, paper_id.
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
            records.append({'author_id': rid, 'paper_id': paper_id})

    return pd.DataFrame(records)


# =============================================================================
# STEP 8 -- EXPORT CSVs
# =============================================================================

def export_raw_csvs(dataframes: dict) -> None:
    """Write all DataFrames to Neo4j/ as plain CSV files."""
    os.makedirs(DIR_RAW, exist_ok=True)
    for filename, df in dataframes.items():
        path = os.path.join(DIR_RAW, filename)
        df.to_csv(path, index=False)
        print(f"     -> {path}  ({len(df):,} rows)")


def prepare_neo4j_admin_csvs(dataframes: dict) -> None:
    """
    Rewrite the raw CSVs with neo4j-admin header annotations to Neo4j_import/.

    Annotations applied:
      - Node ID columns   -> column:ID(Label)
      - Relationship ends -> :START_ID(Label) / :END_ID(Label)
      - Integer columns   -> column:int
    """
    os.makedirs(DIR_IMPORT, exist_ok=True)

    node_renames = {
        'nodes_papers.csv': {
            'paper_id': 'paper_id:ID(Paper)',
            'year':     'year:int',
        },
        'nodes_authors.csv': {
            'author_id': 'author_id:ID(Author)',
        },
        'nodes_journals.csv': {
            'journal_id': 'journal_id:ID(Journal)',
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
        'nodes_keywords.csv': {
            'keyword_id': 'keyword_id:ID(Keyword)',
        },
    }

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
        'rels_reviewed.csv': {
            'author_id': ':START_ID(Author)',
            'paper_id':  ':END_ID(Paper)',
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
        'rels_edition_conference.csv': {
            'edition_id': ':START_ID(Edition)',
            'conf_id':    ':END_ID(Conference)',
        },
        'rels_edition_workshop.csv': {
            'edition_id':  ':START_ID(Edition)',
            'workshop_id': ':END_ID(Workshop)',
        },
    }

    for filename, rename_map in {**node_renames, **rel_renames}.items():
        src = os.path.join(DIR_RAW, filename)
        dst = os.path.join(DIR_IMPORT, filename)
        if not os.path.exists(src):
            print(f"     [WARN] Skipping {filename} -- not found in {DIR_RAW}/")
            continue
        df = pd.read_csv(src)
        df.rename(columns=rename_map, inplace=True)
        df.to_csv(dst, index=False)
        print(f"     -> {dst}  ({len(df):,} rows)")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def generate_graph_dataset() -> None:
    print("=" * 60)
    print("  SDM Lab A.2 -- Property Graph CSV Generator")
    print("=" * 60)

    df_papers = load_dblp_data()

    print("\n[2/8] Building journal / volume schema...")
    df_nodes_journals, df_nodes_volumes, df_rels_paper_vol, df_rels_vol_journal = (
        build_journal_schema(df_papers)
    )
    print(f"     [OK] {len(df_nodes_journals)} journals,  {len(df_nodes_volumes)} volumes.")

    print("\n[3/8] Building conference / workshop / edition schema...")
    (
        df_nodes_conferences, df_nodes_workshops, df_nodes_editions,
        df_rels_paper_edition, df_rels_edition_conf, df_rels_edition_ws,
    ) = build_conference_schema(df_papers)
    print(f"     [OK] {len(df_nodes_conferences)} conferences,  "
          f"{len(df_nodes_workshops)} workshops,  "
          f"{len(df_nodes_editions):,} editions.")

    print("\n[4/8] Building author schema...")
    df_nodes_authors, df_rels_wrote = build_author_schema(df_papers)
    print(f"     [OK] {len(df_nodes_authors):,} authors.")

    print("\n[5/8] Assigning keywords and generating abstracts...")
    (
        df_nodes_keywords, df_rels_has_keyword,
        theme_buckets, paper_primary_theme, abstracts,
    ) = build_keyword_schema(df_papers)
    df_papers['abstract'] = abstracts
    df_papers['abstract'] = df_papers['abstract'].str.replace(
        r'[\n\r\u2028\u2029]+', ' ', regex=True
    )
    print(f"     [OK] {len(df_nodes_keywords)} keywords,  "
          f"{len(df_rels_has_keyword):,} HAS_KEYWORD edges.")

    print("\n[6/8] Processing citations...")
    df_rels_cites = build_citation_schema(df_papers, theme_buckets, paper_primary_theme)

    print("\n[7/8] Assigning reviewers (3 per paper, no self-review)...")
    df_rels_reviewed = build_review_schema(df_papers, df_nodes_authors, df_rels_wrote)
    print(f"     [OK] {len(df_rels_reviewed):,} REVIEWED relationships.")

    # Type safety before export.
    df_papers['year']         = df_papers['year'].astype(int)
    df_nodes_volumes['year']  = df_nodes_volumes['year'].astype(int)
    df_nodes_editions['year'] = df_nodes_editions['year'].astype(int)

    print(f"\n[8/8] Exporting CSVs...")
    raw_dataframes = {
        'nodes_papers.csv':       df_papers[['paper_id', 'title', 'abstract', 'year', 'pages', 'doi']],
        'nodes_authors.csv':      df_nodes_authors,
        'nodes_keywords.csv':     df_nodes_keywords,
        'nodes_journals.csv':     df_nodes_journals,
        'nodes_volumes.csv':      df_nodes_volumes,
        'nodes_conferences.csv':  df_nodes_conferences,
        'nodes_workshops.csv':    df_nodes_workshops,
        'nodes_editions.csv':     df_nodes_editions,
        'rels_wrote.csv':             df_rels_wrote,
        'rels_cites.csv':             df_rels_cites,
        'rels_has_keyword.csv':       df_rels_has_keyword,
        'rels_reviewed.csv':          df_rels_reviewed,
        'rels_paper_volume.csv':      df_rels_paper_vol,
        'rels_volume_journal.csv':    df_rels_vol_journal,
        'rels_paper_edition.csv':     df_rels_paper_edition,
        'rels_edition_conference.csv': df_rels_edition_conf,
        'rels_edition_workshop.csv':  df_rels_edition_ws,
    }
    export_raw_csvs(raw_dataframes)
    prepare_neo4j_admin_csvs(raw_dataframes)

    total_nodes = sum(len(df) for k, df in raw_dataframes.items() if k.startswith('nodes_'))
    total_rels  = sum(len(df) for k, df in raw_dataframes.items() if k.startswith('rels_'))
    print("\n" + "=" * 60)
    print(f"  Done.  {total_nodes:,} nodes  |  {total_rels:,} relationships")
    print(f"  Raw CSVs    -> {DIR_RAW}/")
    print(f"  Import CSVs -> {DIR_IMPORT}/")
    print("=" * 60)


if __name__ == '__main__':
    generate_graph_dataset()