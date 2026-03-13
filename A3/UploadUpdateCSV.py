"""
UploadUpdateCSV.py  (Part A.3)
===============================
Evolves the existing A.2 Neo4j graph into the A.3 schema by executing
Cypher statements through the official Neo4j Python driver.
 
No data is reimported. The three changes are applied incrementally:
  1. CREATE  Organization nodes       (from update_organizations.csv)
  2. CREATE  AFFILIATED_TO edges      (from update_affiliated_to.csv)
  3. SET     REVIEWED edge properties (from update_reviewed_props.csv)
 
Each operation uses batched Cypher (UNWIND + MERGE / MATCH) to avoid
one-round-trip-per-row overhead and to stay within driver memory limits.
 
Temporary indexes on Author.author_id and Paper.paper_id are created
before the update and dropped afterwards to accelerate the MATCH lookups
in steps 2 and 3. If those indexes already exist (e.g. created manually),
the script detects this and skips creation / deletion for them.
 
Requirements
------------
  - The Neo4j database must be RUNNING.
  - FormatUpdateCSV.py must have been run first (Neo4j_update/ must exist).
 
Configuration
-------------
  All paths and credentials are read from a .env file in the project root.
"""

import csv
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
import os
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# =============================================================================
# CONFIGURATION -- edit these values to match your local installation
# =============================================================================

load_dotenv()
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
DATABASE_NAME  = os.getenv("NEO4J_DATABASE")

# Directory to save the CSV to update.
DIR_UPDATE = Path('Neo4j_update')

# Batch size fr efficient Cypher query operations.
BATCH_SIZE = 2000

# Temporary indexes created by this script.
TEMP_INDEXES = [
    (
        "tmp_author_id_idx",
        "CREATE INDEX tmp_author_id_idx FOR (a:Author) ON (a.author_id)",
        "DROP INDEX tmp_author_id_idx",
    ),
    (
        "tmp_paper_id_idx",
        "CREATE INDEX tmp_paper_id_idx FOR (p:Paper) ON (p.paper_id)",
        "DROP INDEX tmp_paper_id_idx",
    ),
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
 
def read_csv_as_dicts(filepath: Path) -> list[dict]:
    """
    Read a CSV file and return its rows as a list of plain dictionaries.
    Integer-coercible values are cast to int so Cypher receives native types.
    """
    rows = []
    with open(filepath, newline='', encoding='utf-8') as fh:
        for row in csv.DictReader(fh):
            coerced = {}
            for k, v in row.items():
                try:
                    coerced[k] = int(v)
                except (ValueError, TypeError):
                    coerced[k] = v
            rows.append(coerced)
    return rows
 
 
def batched(iterable: list, size: int):
    """Yield successive non-overlapping slices of length *size*."""
    for start in range(0, len(iterable), size):
        yield iterable[start: start + size]
 
 
def run_batched(session, query: str, rows: list[dict], batch_size: int = BATCH_SIZE) -> int:
    """
    Execute *query* in batches using UNWIND.
 
    The query must accept a parameter named $batch (list of dicts).
    Returns the total number of rows processed.
    """
    processed = 0
    for batch in batched(rows, batch_size):
        session.run(query, batch=batch)
        processed += len(batch)
    return processed
 
 
def _existing_index_names(session) -> set[str]:
    """Return the set of index names currently present in the database."""
    result = session.run("SHOW INDEXES YIELD name")
    return {row["name"] for row in result}
 
 
# =============================================================================
# STEP 1 -- CONNECT AND VERIFY
# =============================================================================
 
def connect(uri: str, user: str, password: str):
    """
    Open a Neo4j driver and verify connectivity.
    Retries for up to 30 seconds to tolerate a database that is still starting.
    """
    driver   = GraphDatabase.driver(uri, auth=(user, password))
    deadline = time.time() + 30
    while True:
        try:
            driver.verify_connectivity()
            print(f"[OK] Connected to Neo4j at {uri}.")
            return driver
        except ServiceUnavailable:
            if time.time() >= deadline:
                print(f"[ERROR] Could not reach Neo4j at {uri} after 30 seconds.")
                sys.exit(1)
            print("     Database not yet reachable -- retrying in 5 seconds...")
            time.sleep(5)
 
 
# =============================================================================
# STEP 2 -- TEMPORARY INDEXES
# =============================================================================
 
def create_temp_indexes(session) -> list[str]:
    """
    Create temporary indexes to accelerate MATCH lookups during the update.
 
    Skips any index whose name already exists so the script remains idempotent
    when run more than once. Returns the list of index names that were
    actually created (and should therefore be dropped at the end).
    """
    existing = _existing_index_names(session)
    created  = []
 
    print("[INFO] Creating temporary indexes...")
    for name, create_stmt, _ in TEMP_INDEXES:
        if name in existing:
            print(f"       {name} already exists -- skipping.")
        else:
            session.run(create_stmt)
            created.append(name)
            print(f"       {name} created.")
 
    if created:
        # Wait for all indexes to reach ONLINE state before proceeding.
        print("       Waiting for indexes to come online...")
        deadline = time.time() + 60
        while time.time() < deadline:
            result = session.run(
                "SHOW INDEXES YIELD name, state WHERE name IN $names",
                names=created,
            )
            states = {row["name"]: row["state"] for row in result}
            if all(s == "ONLINE" for s in states.values()):
                break
            time.sleep(1)
        print("       [OK] All temporary indexes are online.")
 
    return created
 
 
def drop_temp_indexes(session, created_names: list[str]) -> None:
    """
    Drop only the temporary indexes that were created by this script run.
    Indexes that already existed before the script started are left intact.
    """
    if not created_names:
        return
 
    drop_map = {name: drop for name, _, drop in TEMP_INDEXES}
    print("\n[INFO] Dropping temporary indexes...")
    for name in created_names:
        session.run(drop_map[name])
        print(f"       {name} dropped.")
 
 
# =============================================================================
# STEP 3 -- CREATE ORGANIZATION NODES
# =============================================================================
 
def load_organizations(session, rows: list[dict]) -> None:
    """
    MERGE Organization nodes so the operation is idempotent.
    Running UploadUpdateCSV.py more than once will not create duplicates.
    """
    query = """
    UNWIND $batch AS row
    MERGE (o:Organization {org_id: row.org_id})
    SET   o.name = row.name,
          o.type = row.type
    """
    total = run_batched(session, query, rows)
    print(f"     [OK] {total:,} Organization nodes merged.")
 
 
# =============================================================================
# STEP 4 -- CREATE AFFILIATED_TO RELATIONSHIPS
# =============================================================================
 
def load_affiliated_to(session, rows: list[dict]) -> None:
    """
    MERGE AFFILIATED_TO relationships between existing Author nodes and the
    newly created Organization nodes.
 
    MERGE is used on the relationship to remain idempotent; the direction
    and the (author_id, org_id) pair together uniquely identify the edge.
    """
    query = """
    UNWIND $batch AS row
    MATCH (a:Author       {author_id: row.author_id})
    MATCH (o:Organization {org_id:    row.org_id})
    MERGE (a)-[:AFFILIATED_TO]->(o)
    """
    total = run_batched(session, query, rows)
    print(f"     [OK] {total:,} AFFILIATED_TO relationships merged.")
 
 
# =============================================================================
# STEP 5 -- SET PROPERTIES ON EXISTING REVIEWED EDGES
# =============================================================================
 
def load_reviewed_properties(session, rows: list[dict]) -> None:
    """
    SET the new properties (review_id, score, decision, comments) on the
    REVIEWED relationships that were created without properties in A.2.
 
    The (author_id, paper_id) pair uniquely identifies each relationship
    because build_review_schema in FormatCSV.py enforces at most one
    REVIEWED edge per (author, paper) pair.
 
    No new relationships are created; only existing ones are updated.
    """
    query = """
    UNWIND $batch AS row
    MATCH (a:Author)-[r:REVIEWED]->(p:Paper)
    WHERE a.author_id = row.author_id
      AND p.paper_id  = row.paper_id
    SET r.review_id = row.review_id,
        r.score     = row.score,
        r.decision  = row.decision,
        r.comments  = row.comments
    """
    total = run_batched(session, query, rows)
    print(f"     [OK] {total:,} REVIEWED relationships updated with properties.")
 
 
# =============================================================================
# STEP 6 -- POST-UPDATE VERIFICATION
# =============================================================================
 
def verify_update(session) -> None:
    """
    Run lightweight count queries to confirm all three changes were applied.
    """
    checks = [
        ("Organization nodes",    "MATCH (o:Organization) RETURN count(o) AS n"),
        ("AFFILIATED_TO edges",   "MATCH ()-[r:AFFILIATED_TO]->() RETURN count(r) AS n"),
        ("REVIEWED with score",   "MATCH ()-[r:REVIEWED]->() WHERE r.score IS NOT NULL RETURN count(r) AS n"),
        ("REVIEWED without score","MATCH ()-[r:REVIEWED]->() WHERE r.score IS NULL RETURN count(r) AS n"),
    ]
    print("\n[INFO] Verification counts:")
    for label, cypher in checks:
        result = session.run(cypher).single()
        count  = result['n'] if result else 0
        print(f"     {label:30s}  {count:>12,}")
 
 
# =============================================================================
# MAIN
# =============================================================================
 
def main() -> None:
    print("=" * 60)
    print("  SDM Lab A.3 -- Incremental Graph Evolution")
    print("=" * 60)
    print()
 
    # Validate that update CSVs exist.
    required_files = [
        DIR_UPDATE / 'update_organizations.csv',
        DIR_UPDATE / 'update_affiliated_to.csv',
        DIR_UPDATE / 'update_reviewed_props.csv',
    ]
    missing = [str(f) for f in required_files if not f.exists()]
    if missing:
        print("[ERROR] The following update CSV files are missing:")
        for m in missing:
            print(f"        {m}")
        print("        Run FormatUpdateCSV.py first.")
        sys.exit(1)
 
    # Load CSVs into memory.
    print("[INFO] Reading update CSVs...")
    rows_orgs     = read_csv_as_dicts(DIR_UPDATE / 'update_organizations.csv')
    rows_affil    = read_csv_as_dicts(DIR_UPDATE / 'update_affiliated_to.csv')
    rows_reviewed = read_csv_as_dicts(DIR_UPDATE / 'update_reviewed_props.csv')
    print(f"     Organizations     : {len(rows_orgs):>10,} rows")
    print(f"     AFFILIATED_TO     : {len(rows_affil):>10,} rows")
    print(f"     REVIEWED updates  : {len(rows_reviewed):>10,} rows")
 
    # Connect.
    print()
    driver = connect(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
 
    start = time.time()
    created_indexes = []
 
    try:
        with driver.session(database=DATABASE_NAME) as session:
 
            created_indexes = create_temp_indexes(session)
            print()
 
            print("[1/3] Creating Organization nodes...")
            load_organizations(session, rows_orgs)
 
            print("\n[2/3] Creating AFFILIATED_TO relationships...")
            load_affiliated_to(session, rows_affil)
 
            print("\n[3/3] Setting properties on REVIEWED relationships...")
            load_reviewed_properties(session, rows_reviewed)
 
            verify_update(session)
 
            drop_temp_indexes(session, created_indexes)
 
    except Exception:
        # If the update fails mid-way, still attempt to clean up indexes.
        if created_indexes:
            print("\n[WARN] Update failed -- attempting to clean up temporary indexes...")
            try:
                with driver.session(database=DATABASE_NAME) as session:
                    drop_temp_indexes(session, created_indexes)
            except Exception as cleanup_exc:
                print(f"[WARN] Could not drop temporary indexes: {cleanup_exc}")
                print("       Drop them manually:")
                print("         DROP INDEX tmp_author_id_idx;")
                print("         DROP INDEX tmp_paper_id_idx;")
        raise
 
    finally:
        driver.close()
 
    elapsed = time.time() - start
    print(f"\n[OK] Graph evolution completed in {elapsed:.1f} seconds.")
    print("     The graph now conforms to the A.3 schema.")
 
 
if __name__ == '__main__':
    main()