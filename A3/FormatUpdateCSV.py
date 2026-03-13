"""
FormatUpdateCSV.py  (Part A.3)
==============================
Generates the three CSV files required to evolve the A.2 graph into the A.3
schema without reimporting from scratch.

Changes introduced in A.3:
  1. New node   : Organization  {org_id, name, type}
  2. New edge   : (Author)-[:AFFILIATED_TO]->(Organization)
  3. New props  : REVIEWED edge gains {review_id, score, decision, comments}

This script reads the Author and REVIEWED tables already produced by
FormatCSV.py (in Neo4j/) and derives the update payloads from them.
No DBLP data is re-read; all new data is synthetic.

Output files (written to Neo4j_update/)
----------------------------------------
  update_organizations.csv      -- Organization nodes to CREATE
  update_affiliated_to.csv      -- AFFILIATED_TO edges to CREATE
  update_reviewed_props.csv     -- Properties to SET on existing REVIEWED edges

Usage
-----
  1. Ensure FormatCSV.py has already been run (Neo4j/ directory must exist).
  2. python FormatUpdateCSV.py
  3. Run UploadUpdateCSV.py to push the changes into Neo4j.
"""

import os
import random
import uuid
import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

# Directory produced by FormatCSV.py (source of existing IDs).
DIR_RAW    = 'Neo4j'

# Directory where the A.3 update CSVs will be written.
DIR_UPDATE = 'Neo4j_update'

# Seed list of organizations used to synthesise author affiliations.
ORG_NAMES = [
    "MIT",
    "Stanford University",
    "CMU",
    "UC Berkeley",
    "Oxford",
    "Cambridge",
    "ETH Zurich",
    "EPFL",
    "Tsinghua University",
    "NUS",
    "Google Research",
    "Microsoft Research",
    "IBM Research",
    "Meta AI",
    "Amazon Web Services",
    "UPC BarcelonaTech",
    "University of Toronto",
    "University of Washington",
    "Cornell University",
    "Max Planck Institute",
]

# Type is derived from the name: entries containing 'Research', 'AI', 'Services', or 'AWS' are classified as 'Company'; everything else as 'University'.
COMPANY_KEYWORDS = {'Research', 'AI', 'Services', 'AWS'}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def org_type(name: str) -> str:
    """Classify an organization as 'Company' or 'University' based on its name."""
    return 'Company' if any(kw in name for kw in COMPANY_KEYWORDS) else 'University'


def review_decision(score: int) -> str:
    """Map a numeric review score (1-10) to a textual acceptance decision."""
    if score >= 7:
        return "Accept"
    elif score >= 4:
        return "Weak Accept"
    else:
        return "Reject"


def review_comment(score: int) -> str:
    """Return a short canned reviewer comment consistent with the score."""
    if score >= 8:
        return "Excellent methodology and clear contribution. Highly recommended."
    elif score >= 5:
        return "Solid work with minor issues. Needs additional experiments."
    else:
        return "Insufficient novelty and weak empirical evaluation. Reject."


# =============================================================================
# STEP 1 -- BUILD ORGANIZATION NODES
# =============================================================================

def build_organization_nodes() -> pd.DataFrame:
    """
    Create a DataFrame of Organization nodes from the ORG_NAMES seed list.

    Each organization receives a stable synthetic ID (org_0, org_1, ...) so
    that UploadUpdateCSV.py can match them when creating AFFILIATED_TO edges.

    Returns: df_nodes_orgs  with columns [org_id, name, type]
    """
    records = [
        {
            'org_id': f'org_{i}',
            'name':   name,
            'type':   org_type(name),
        }
        for i, name in enumerate(ORG_NAMES)
    ]
    return pd.DataFrame(records)


# =============================================================================
# STEP 2 -- BUILD AFFILIATED_TO RELATIONSHIPS
# =============================================================================

def build_affiliated_to(df_nodes_orgs: pd.DataFrame) -> pd.DataFrame:
    """
    Assign one random organization to every author in the existing graph.

    Reads Neo4j/nodes_authors.csv to obtain the full set of author IDs
    already present in the database.  Each author is mapped to a single
    organization (one affiliation per author, for simplicity).

    Parameters
    ----------
    df_nodes_orgs : pd.DataFrame
        Organization node table (needed for its org_id values).

    Returns
    -------
    df_rels_affiliated : pd.DataFrame
        Columns: [author_id, org_id]
    """
    authors_path = os.path.join(DIR_RAW, 'nodes_authors.csv')
    if not os.path.exists(authors_path):
        raise FileNotFoundError(
            f"[ERROR] {authors_path} not found. Run FormatCSV.py first."
        )

    df_authors = pd.read_csv(authors_path, usecols=['author_id'])
    org_ids    = df_nodes_orgs['org_id'].tolist()

    df_rels_affiliated = pd.DataFrame({
        'author_id': df_authors['author_id'],
        'org_id':    [random.choice(org_ids) for _ in range(len(df_authors))],
    })
    return df_rels_affiliated


# =============================================================================
# STEP 3 -- BUILD REVIEWED EDGE PROPERTY UPDATES
# =============================================================================

def build_reviewed_properties() -> pd.DataFrame:
    """
    Generate synthetic review properties for every REVIEWED edge that
    already exists in the graph.

    Reads Neo4j/rels_reviewed.csv to obtain the (author_id, paper_id) pairs
    that identify each existing REVIEWED relationship, then attaches:
      - review_id : globally unique synthetic identifier
      - score     : integer 1-10
      - decision  : derived from score ('Accept' / 'Weak Accept' / 'Reject')
      - comments  : short textual reviewer comment derived from score

    These values will be SET on the matching relationships in Neo4j by
    UploadUpdateCSV.py -- no new relationships are created.

    Returns
    -------
    df_reviewed_props : pd.DataFrame
        Columns: [author_id, paper_id, review_id, score, decision, comments]
    """
    reviewed_path = os.path.join(DIR_RAW, 'rels_reviewed.csv')
    if not os.path.exists(reviewed_path):
        raise FileNotFoundError(
            f"[ERROR] {reviewed_path} not found. Run FormatCSV.py first."
        )

    df = pd.read_csv(reviewed_path, usecols=['author_id', 'paper_id'])

    scores    = [random.randint(1, 10) for _ in range(len(df))]
    df['review_id'] = [f"rev_{uuid.uuid4().hex[:8]}" for _ in range(len(df))]
    df['score']     = scores
    df['decision']  = [review_decision(s) for s in scores]
    df['comments']  = [review_comment(s)  for s in scores]

    return df[['author_id', 'paper_id', 'review_id', 'score', 'decision', 'comments']]


# =============================================================================
# EXPORT
# =============================================================================

def export_update_csvs(
    df_nodes_orgs: pd.DataFrame,
    df_rels_affiliated: pd.DataFrame,
    df_reviewed_props: pd.DataFrame,
) -> None:
    """
    Write the three update DataFrames to Neo4j_update/ as plain CSV files.

    Parameters
    ----------
    df_nodes_orgs       : Organization node table.
    df_rels_affiliated  : AFFILIATED_TO relationship table.
    df_reviewed_props   : REVIEWED property update table.
    """
    os.makedirs(DIR_UPDATE, exist_ok=True)

    files = {
        'update_organizations.csv':  df_nodes_orgs,
        'update_affiliated_to.csv':  df_rels_affiliated,
        'update_reviewed_props.csv': df_reviewed_props,
    }
    for filename, df in files.items():
        path = os.path.join(DIR_UPDATE, filename)
        df.to_csv(path, index=False)
        print(f"     -> {path}  ({len(df):,} rows)")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def generate_update_csvs() -> None:
    print("=" * 60)
    print("  SDM Lab A.3 -- Graph Evolution CSV Generator")
    print("=" * 60)
    print()

    print("[1/3] Building Organization nodes...")
    df_nodes_orgs = build_organization_nodes()
    print(f"     [OK] {len(df_nodes_orgs)} organizations.")

    print("\n[2/3] Building AFFILIATED_TO relationships...")
    df_rels_affiliated = build_affiliated_to(df_nodes_orgs)
    print(f"     [OK] {len(df_rels_affiliated):,} AFFILIATED_TO edges.")

    print("\n[3/3] Generating REVIEWED edge properties...")
    df_reviewed_props = build_reviewed_properties()
    print(f"     [OK] {len(df_reviewed_props):,} REVIEWED edges enriched.")

    print(f"\n[INFO] Exporting update CSVs to {DIR_UPDATE}/...")
    export_update_csvs(df_nodes_orgs, df_rels_affiliated, df_reviewed_props)

    print("\n" + "=" * 60)
    print("  Done.  Run UploadUpdateCSV.py to apply changes to Neo4j.")
    print("=" * 60)


if __name__ == '__main__':
    generate_update_csvs()