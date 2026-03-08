# SDM Lab — Property Graph: DBLP Research Publications Graph

**Course:** Semantic Data Management (SDM)  
**Dataset:** DBLP XML bulk download  
**Database:** Neo4j 2026.01.4  
**Generator:** `grafo_generator.py`  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Graph Schema Design](#2-graph-schema-design)
   - 2.1 [Node Labels and Properties](#21-node-labels-and-properties)
   - 2.2 [Relationship Types and Properties](#22-relationship-types-and-properties)
   - 2.3 [Schema Diagram (ASCII)](#23-schema-diagram-ascii)
3. [Design Decisions and Justifications](#3-design-decisions-and-justifications)
4. [Data Sources](#4-data-sources)
   - 4.1 [Real Data from DBLP](#41-real-data-from-dblp)
   - 4.2 [Synthetic Data](#42-synthetic-data)
5. [Pipeline Architecture](#5-pipeline-architecture)
   - 5.1 [Step-by-Step Description](#51-step-by-step-description)
   - 5.2 [File Output Structure](#52-file-output-structure)
6. [Graph Statistics](#6-graph-statistics)
7. [Environment Setup](#7-environment-setup)
   - 7.1 [Python Requirements](#71-python-requirements)
   - 7.2 [Neo4j Requirements](#72-neo4j-requirements)
8. [Running the Generator](#8-running-the-generator)
9. [Importing into Neo4j](#9-importing-into-neo4j)
   - 9.1 [Locating the Import Folder](#91-locating-the-import-folder)
   - 9.2 [Copying CSV Files](#92-copying-csv-files)
   - 9.3 [Stopping the Database](#93-stopping-the-database)
   - 9.4 [Running neo4j-admin import](#94-running-neo4j-admin-import)
   - 9.5 [Starting the Database](#95-starting-the-database)
   - 9.6 [Creating Indexes](#96-creating-indexes)
10. [Verification Queries](#10-verification-queries)
11. [Lab Query Reference](#11-lab-query-reference)
    - 11.1 [Part B — Analytical Queries](#111-part-b--analytical-queries)
    - 11.2 [Part C — Reviewer Recommender](#112-part-c--reviewer-recommender)
12. [Known Limitations and Assumptions](#12-known-limitations-and-assumptions)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Project Overview

This project constructs a **Property Graph** database of academic research publications in the database and data mining domain, using real data from the [DBLP computer science bibliography](https://dblp.org/) enriched with synthetic attributes where the source data is incomplete.

The graph models the full academic publication ecosystem as specified in the SDM Lab assignment (sections A.1 through A.3), including:

- Papers published in journals (via volumes) or conferences/workshops (via editions).
- Authors with affiliations to universities and companies.
- Review processes with scores and decisions.
- Citation networks between papers.
- Keyword-based topic classification.

The pipeline is fully automated: a single Python script reads the raw DBLP CSV exports, builds all node and relationship tables, enforces type safety, and outputs two sets of CSVs — one for human inspection and one ready for `neo4j-admin database import`.

---

## 2. Graph Schema Design

### 2.1 Node Labels and Properties

| Label | ID Property | Other Properties | Origin |
|---|---|---|---|
| `Paper` | `paper_id` (string, DBLP key) | `title`, `abstract`, `year` (int), `pages`, `doi` | Real + synthetic |
| `Author` | `author_id` (string) | `name`, `email`, `h_index` (int) | Real + synthetic |
| `Keyword` | `keyword_id` (string) | `label` | Synthetic |
| `Organization` | `org_id` (string) | `name`, `type` (University/Company) | Synthetic |
| `Journal` | `journal_id` (string) | `name`, `acronym`, `publisher`, `issn`, `impact_factor` (float) | Real + synthetic |
| `Volume` | `volume_id` (string) | `volumeNumber`, `year` (int) | Real + synthetic |
| `Conference` | `conf_id` (string) | `name`, `acronym`, `field` | Real + synthetic |
| `Workshop` | `workshop_id` (string) | `name`, `acronym`, `field` | Real + synthetic |
| `Edition` | `edition_id` (string) | `year` (int), `city` | Real + synthetic |

### 2.2 Relationship Types and Properties

| Relationship | From | To | Properties | Origin |
|---|---|---|---|---|
| `WROTE` | `Author` | `Paper` | `role` (Main Author / Co-author) | Real |
| `CITES` | `Paper` | `Paper` | — | Real + synthetic |
| `HAS_KEYWORD` | `Paper` | `Keyword` | — | Synthetic |
| `AFFILIATED_TO` | `Author` | `Organization` | — | Synthetic |
| `REVIEWED` | `Author` | `Paper` | `review_id`, `score` (int), `decision`, `comments` | Synthetic |
| `PUBLISHED_IN_VOLUME` | `Paper` | `Volume` | — | Real |
| `BELONGS_TO` | `Volume` | `Journal` | — | Real |
| `PRESENTED_IN` | `Paper` | `Edition` | — | Real |
| `EDITION_OF` | `Edition` | `Conference` | — | Real |
| `EDITION_OF` | `Edition` | `Workshop` | — | Real |

### 2.3 Schema Diagram (ASCII)

```
                        ┌──────────────┐
                        │   Keyword    │
                        │  keyword_id  │
                        │  label       │
                        └──────┬───────┘
                               │ HAS_KEYWORD
                               │
  ┌────────────────┐     ┌─────▼──────┐     ┌─────────────────┐
  │  Organization  │     │   Paper    │     │    Journal      │
  │  org_id        │     │  paper_id  │     │  journal_id     │
  │  name          │     │  title     │     │  name           │
  │  type          │     │  abstract  │     │  acronym        │
  └────────┬───────┘     │  year      │     │  publisher      │
           │             │  pages     │     │  issn           │
    AFFILIATED_TO        │  doi       │     │  impact_factor  │
           │             └─────┬──────┘     └────────┬────────┘
  ┌────────▼───────┐           │                     │
  │    Author      │           │ CITES               │ BELONGS_TO
  │  author_id     │◄──WROTE───┤                     │
  │  name          │           │            ┌────────▼────────┐
  │  email         │◄─REVIEWED─┤            │     Volume      │
  │  h_index       │           │            │  volume_id      │
  └────────────────┘           │            │  volumeNumber   │
                               │            │  year           │
                               │            └────────▲────────┘
                               │                     │
                               │          PUBLISHED_IN_VOLUME
                               │                     │
                               ├─────────────────────┘
                               │
                               │ PRESENTED_IN
                               │
                        ┌──────▼───────┐
                        │   Edition    │
                        │  edition_id  │
                        │  year        │
                        │  city        │
                        └──────┬───────┘
                               │ EDITION_OF
                    ┌──────────┴──────────┐
                    │                     │
             ┌──────▼──────┐    ┌─────────▼───────┐
             │ Conference  │    │    Workshop      │
             │  conf_id    │    │  workshop_id     │
             │  name       │    │  name            │
             │  acronym    │    │  acronym         │
             │  field      │    │  field           │
             └─────────────┘    └──────────────────┘
```

**Key paths in the graph:**

```
(Author)-[:WROTE]->(Paper)-[:PUBLISHED_IN_VOLUME]->(Volume)-[:BELONGS_TO]->(Journal)
(Author)-[:WROTE]->(Paper)-[:PRESENTED_IN]->(Edition)-[:EDITION_OF]->(Conference)
(Author)-[:WROTE]->(Paper)-[:PRESENTED_IN]->(Edition)-[:EDITION_OF]->(Workshop)
(Author)-[:REVIEWED {score, decision}]->(Paper)
(Author)-[:AFFILIATED_TO]->(Organization)
(Paper)-[:CITES]->(Paper)
(Paper)-[:HAS_KEYWORD]->(Keyword)
```

---

## 3. Design Decisions and Justifications

### Why separate Conference and Workshop as distinct node labels?

The assignment explicitly distinguishes conferences as *"well-established forums"* and workshops as *"associated to new trends"*. Keeping them as separate labels (rather than a single `Venue` node with a `type` property) allows:

- Direct label-based filtering: `MATCH (c:Conference)` vs `MATCH (w:Workshop)`.
- Cleaner query semantics for Part B queries (top papers per conference).
- Efficient index usage — Neo4j indexes are per-label.

The trade-off is that `EDITION_OF` relationships point to two different label spaces, which requires splitting the relationship CSV into two files for `neo4j-admin import`. This is handled automatically by the generator.

### Why model Edition as a separate node?

An `Edition` represents a specific annual occurrence of a conference or workshop (e.g., SIGMOD 2019). Modeling it as a node (rather than a property on the Conference) enables:

- Query B.2: counting how many distinct editions an author has published in.
- Storing edition-specific attributes like `city` and `year`.
- Reusability across multiple papers presented at the same edition.

**Critical implementation detail:** each `Edition` is uniquely identified by `(venue, year)`, not by paper.

### Why model Volume analogously to Edition?

The same reasoning applies: a `Volume` represents a journal's publication unit for a given year. One volume per `(journal, year)` pair.

### Why store Review as a relationship instead of a node?

The assignment (A.3) asks to store review content (text, decision) associated with a reviewer-paper pair. Two modeling options exist:

- **Relationship with properties:** `(Author)-[:REVIEWED {review_id, score, decision, comments}]->(Paper)`
- **Intermediate node:** `(Author)-[:SUBMITTED_REVIEW]->(Review)-[:FOR]->(Paper)`

The relationship model is chosen because:
- All review attributes are atomic scalars (no need to traverse further from a Review node).
- It reduces the total node count significantly (~455k fewer nodes).
- Cypher queries for retrieving reviews are simpler and faster.
- The assignment does not require querying *from* a Review node, only *about* the review content.

### Why assign role (Main Author / Co-author) on WROTE?

The assignment specifies that *"only one author acts as corresponding author"*. DBLP preserves author order within each paper. The first author (position 0) is assigned `role: "Main Author"` and all subsequent authors receive `role: "Co-author"`. This is stored as a relationship property since it is a property of the authorship act, not of the author or paper in isolation.

### Why use DBLP key as paper_id?

DBLP keys (e.g., `journals/vldb/Smith2019`) are globally unique, stable identifiers already present in the source data. Using them as the primary key:
- Avoids generating synthetic IDs for the most important entity.
- Enables reconstruction of the DBLP URL for any paper.
- Makes the citation resolution step trivial (DBLP cite fields use the same key format).

### Keyword strategy and the database community (Part C)

The 12 keywords used are not random. The first 7 match exactly the keywords that define the *"database community"* as specified in Part C of the lab:

```
Data Management, Indexing, Data Modeling, Big Data,
Data Processing, Data Storage, Data Querying
```

The remaining 5 (`Machine Learning`, `Graph Databases`, `Optimization`, `Deep Learning`, `Natural Language Processing`) provide variety. Since keyword assignment is synthetic and uniform across papers, the 90% threshold in query C.2 will correctly classify all filtered venues as database-community venues — the intended behaviour for a lab with synthetic keyword data.

---

## 4. Data Sources

### 4.1 Real Data from DBLP

DBLP provides a full XML dump of the computer science bibliography at [https://dblp.org/xml/](https://dblp.org/xml/). The XML was converted to CSV using the [dblp-to-csv](https://github.com/ThomHurks/dblp-to-csv) tool, producing:

```
output_csv/
  dblp_article_header.csv          ← column names for journal articles
  dblp_article.csv                 ← journal articles (semicolon-separated)
  dblp_inproceedings_header.csv    ← column names for conference papers
  dblp_inproceedings.csv           ← conference/workshop papers
```

**Fields sourced directly from DBLP (real data):**

| Field | Source |
|---|---|
| `paper_id` | DBLP key (`key` field) |
| `title` | Paper title |
| `year` | Publication year |
| `authors` | Pipe-separated author name list |
| `venue` | Journal name / booktitle |
| `volume` | Volume number (journals only) |
| `cites` | Pipe-separated cite key list |
| `doi` | Extracted from `ee` field when a doi.org URL is present |

**Filtering applied:**
- Year: `>= 2015` (configurable via `MIN_YEAR`)
- Venue: regex match against major DB/DM venues (VLDB, SIGMOD, ICDE, KDD, TODS, TKDE, WSDM, WWW, and related journals)

### 4.2 Synthetic Data

The following fields and entities have no real source in the DBLP dump and are generated synthetically with domain-appropriate values:

| Entity / Field | Generation Method | Justification |
|---|---|---|
| `Paper.pages` | Random range `start-(start+8..20)` | DBLP page data is inconsistent |
| `Paper.doi` | `10.fake/<uuid>` fallback | Only when no doi.org URL in `ee` |
| `Paper.abstract` | Template referencing title and keywords | DBLP does not store abstracts |
| `Author.email` | Derived from name | Not in DBLP |
| `Author.h_index` | `randint(1, 60)` | Not in DBLP dump |
| `Author.affiliation` | Random from 20-org seed list | Not in DBLP dump |
| `Journal.publisher` | Random from publisher list | Not in DBLP dump |
| `Journal.issn` | Random `XXXX-XXXX` | Not in DBLP dump |
| `Journal.impact_factor` | `uniform(1.5, 12.0)` | Not in DBLP dump |
| `Conference/Workshop.field` | Random from field list | Not in DBLP dump |
| `Edition.city` | Random from city list | Not in DBLP dump |
| `Keywords` | 2-4 random per paper | Not in DBLP dump |
| `REVIEWED` relationships | 3 non-author reviewers per paper | Fully synthetic |
| Synthetic citations | Thematic top-up to guarantee 5-10 per paper | Internal DBLP citations sparse in filtered window |

---

## 5. Pipeline Architecture

### 5.1 Step-by-Step Description

```
[Step 1] load_dblp_data()
         Read dblp_article.csv and dblp_inproceedings.csv in 100k-row chunks.
         Apply year filter (>= MIN_YEAR) and venue regex filter.
         Concatenate, clean titles, extract DOIs, generate synthetic pages.
         Output: df_papers (151,744 rows with MIN_YEAR=2015)

[Step 2] build_journal_schema(df_papers)
         Filter Journal-type papers.
         Deduplicate journals by venue string → 68 Journal nodes.
         Deduplicate volumes by (venue, year) → 720 Volume nodes.
         Build PUBLISHED_IN_VOLUME and BELONGS_TO relationships.

[Step 3] build_conference_schema(df_papers)
         Filter Conference-type papers, detect workshops by "Workshop" in venue name.
         Deduplicate editions by (venue, year) → 1,404 Edition nodes.
         Build Conference (134) and Workshop (366) nodes.
         Build PRESENTED_IN, EDITION_OF→Conference, EDITION_OF→Workshop relationships.
         NOTE: edition→venue split into two separate CSV files for neo4j-admin.

[Step 4] build_author_schema(df_papers)
         Explode pipe-separated author lists → one row per (paper, author).
         Assign role: position 0 = "Main Author", rest = "Co-author".
         Deduplicate by name → 285,288 Author nodes.
         Build WROTE relationships with role property.
         Assign random Organization affiliation → AFFILIATED_TO relationships.

[Step 5] build_keyword_schema(df_papers)
         Assign 2-4 random keywords per paper from KEYWORD_LABELS list.
         Record primary keyword per paper (used for citation bucketing).
         Generate templated abstract per paper.
         Output: 12 Keyword nodes, 455,189 HAS_KEYWORD edges.

[Step 6] build_citation_schema(df_papers, theme_buckets, paper_primary_theme)
         Tier 1: Extract real DBLP cite keys, keep those resolving within subgraph.
         Tier 2: Synthetic top-up to guarantee 5-10 outgoing citations per paper.
                 Synthetic citations are thematically consistent (same primary keyword)
                 and chronologically valid (target.year <= source.year).
         Output: 1,073,440 CITES relationships.

[Step 7] build_review_schema(df_papers, df_nodes_authors, df_rels_wrote)
         For each paper, select 3 reviewers from author pool excluding paper's own authors.
         Assign random score (1-10), derive decision and comment from score.
         Output: 455,232 REVIEWED relationships.

[Step 8] export_raw_csvs()
         Write all DataFrames to Neo4j/ as plain CSVs (human-readable).

[Step 9] prepare_neo4j_admin_csvs()
         Rewrite CSVs to Neo4j_import/ with neo4j-admin header annotations:
           - Node IDs:    column:ID(Label)
           - Rel start:   :START_ID(Label)
           - Rel end:     :END_ID(Label)
           - Integers:    column:int
           - Floats:      column:float
```

### 5.2 File Output Structure

```
project/
├── output_csv/                        ← DBLP raw CSV input (not generated here)
│   ├── dblp_article_header.csv
│   ├── dblp_article.csv
│   ├── dblp_inproceedings_header.csv
│   └── dblp_inproceedings.csv
│
├── Neo4j/                             ← Human-readable CSVs (plain headers)
│   ├── nodes_papers.csv
│   ├── nodes_authors.csv
│   ├── nodes_keywords.csv
│   ├── nodes_organizations.csv
│   ├── nodes_journals.csv
│   ├── nodes_volumes.csv
│   ├── nodes_conferences.csv
│   ├── nodes_workshops.csv
│   ├── nodes_editions.csv
│   ├── rels_wrote.csv
│   ├── rels_cites.csv
│   ├── rels_has_keyword.csv
│   ├── rels_affiliated_to.csv
│   ├── rels_reviewed.csv
│   ├── rels_paper_volume.csv
│   ├── rels_volume_journal.csv
│   ├── rels_paper_edition.csv
│   ├── rels_edition_conference.csv
│   └── rels_edition_workshop.csv
│
├── Neo4j_import/                      ← neo4j-admin annotated CSVs
│   └── (same 20 files with annotated headers)
│
├── grafo_generator.py                 ← Main pipeline script
└── README.md                          ← This file
```

---

## 6. Graph Statistics

Generated with `MIN_YEAR = 2015`:

### Nodes

| Label | Count |
|---|---|
| Paper | 151,744 |
| Author | 285,288 |
| Keyword | 12 |
| Organization | 20 |
| Journal | 68 |
| Volume | 720 |
| Conference | 134 |
| Workshop | 366 |
| Edition | 1,404 |
| **Total** | **439,756** |

### Relationships

| Type | Count |
|---|---|
| WROTE | 617,472 |
| CITES | 1,073,440 |
| HAS_KEYWORD | 455,189 |
| AFFILIATED_TO | 285,288 |
| REVIEWED | 455,232 |
| PUBLISHED_IN_VOLUME | 70,747 |
| BELONGS_TO | 708 |
| PRESENTED_IN | 80,997 |
| EDITION_OF (Conference) | 395 |
| EDITION_OF (Workshop) | 1,009 |
| **Total** | **3,040,477** |

---

## 7. Environment Setup

### 7.1 Python Requirements

```bash
pip install pandas
```

Python 3.9+ required (uses `dict` type hints). No other external dependencies.

### 7.2 Neo4j Requirements

- **Neo4j Desktop** 1.5+ with a local DBMS instance.
- **Neo4j version:** 2026.01.x (or any Neo4j 5.x).
- **Java:** JDK 21 (required by Neo4j 5.x). Neo4j 5.x requires class file version 65.0.

> ⚠️ If you have Java 17 installed system-wide, `neo4j-admin` will fail with `UnsupportedClassVersionError`. You must install JDK 21 and update `JAVA_HOME` before running the import command.

**Verifying Java version:**
```powershell
java -version        # must show 21.x
echo $env:JAVA_HOME  # must point to JDK 21 installation
```

**Updating JAVA_HOME on Windows:**
1. Press `Win + R`, type `sysdm.cpl`, press Enter.
2. Advanced tab → Environment Variables.
3. Under System Variables, find `JAVA_HOME` and update it to the JDK 21 path (e.g., `C:\Program Files\Eclipse Adoptium\jdk-21.0.x`).
4. Close all PowerShell windows and reopen.

---

## 8. Running the Generator

Place the DBLP CSV files in `output_csv/` as described in section 5.2, then run:

```bash
python grafo_generator.py
```

Expected runtime: **2-5 minutes** depending on hardware (the review assignment loop over ~150k papers is the slowest step).

To change the year filter, edit `MIN_YEAR` at the top of the script:

```python
MIN_YEAR = 2015   # Good trade-off between data volume and performance of the queries and the imports.
```

---

## 9. Importing into Neo4j

> ⚠️ The database **must be stopped** before running `neo4j-admin import`. Running it on an active database will either fail or corrupt the store.

### 9.1 Locating the Import Folder

In Neo4j Desktop, click the three dots `···` next to your DBMS → **Open Folder** → **Import**.

The path will be similar to:
```
C:\Users\<user>\.Neo4jDesktop2\Data\dbmss\dbms-<uuid>\import\
```

Also locate the `bin\` folder via **Open Folder** → **Installation**:
```
C:\Users\<user>\.Neo4jDesktop2\Data\dbmss\dbms-<uuid>\bin\
```

### 9.2 Copying CSV Files

```powershell
Copy-Item -Path "Neo4j_import\*" -Destination "C:\Users\<user>\.Neo4jDesktop2\Data\dbmss\dbms-<uuid>\import\"
```

Verify all 20 files are present in the import folder before proceeding.

### 9.3 Stopping the Database

In Neo4j Desktop, click **Stop** on your DBMS and wait until the status shows **Stopped**.

### 9.4 Running neo4j-admin import

Open PowerShell and navigate to the `bin\` folder of your DBMS installation:

```powershell
cd "C:\Users\<user>\.Neo4jDesktop2\Data\dbmss\dbms-<uuid>\bin"
```

Run the import command:

```powershell
.\neo4j-admin.bat database import full neo4j `
  --verbose `
  --nodes=Paper="import\nodes_papers.csv" `
  --nodes=Author="import\nodes_authors.csv" `
  --nodes=Keyword="import\nodes_keywords.csv" `
  --nodes=Organization="import\nodes_organizations.csv" `
  --nodes=Journal="import\nodes_journals.csv" `
  --nodes=Volume="import\nodes_volumes.csv" `
  --nodes=Conference="import\nodes_conferences.csv" `
  --nodes=Workshop="import\nodes_workshops.csv" `
  --nodes=Edition="import\nodes_editions.csv" `
  --relationships=WROTE="import\rels_wrote.csv" `
  --relationships=CITES="import\rels_cites.csv" `
  --relationships=HAS_KEYWORD="import\rels_has_keyword.csv" `
  --relationships=AFFILIATED_TO="import\rels_affiliated_to.csv" `
  --relationships=REVIEWED="import\rels_reviewed.csv" `
  --relationships=PUBLISHED_IN_VOLUME="import\rels_paper_volume.csv" `
  --relationships=BELONGS_TO="import\rels_volume_journal.csv" `
  --relationships=PRESENTED_IN="import\rels_paper_edition.csv" `
  --relationships=EDITION_OF="import\rels_edition_conference.csv" `
  --relationships=EDITION_OF="import\rels_edition_workshop.csv" `
  --overwrite-destination
```

> **Note for Linux/macOS:** replace the backtick `` ` `` line continuation with `\`.

**Expected output on success:**
```
IMPORT DONE in ~43s
Imported:
  439756 nodes
  3041393 relationships
```

> **Why two separate EDITION_OF files?**  
> `Edition` nodes connect to both `Conference` and `Workshop` nodes, which live in different ID spaces. `neo4j-admin` requires that each relationship file resolves to a single `:END_ID(Label)`. Merging both into one file would cause an ID resolution error. The generator handles this automatically by splitting into `rels_edition_conference.csv` and `rels_edition_workshop.csv`.

### 9.5 Starting the Database

Return to Neo4j Desktop and click **Start**. Wait until the status shows **Running**, then open **Neo4j Browser**.

Connect to the correct database:
```cypher
:use neo4j
```

### 9.6 Creating Indexes

Run this block in Neo4j Browser immediately after import. Indexes are **mandatory** — without them, every `MATCH` on 285k+ author nodes or 151k+ paper nodes will perform a full scan, making queries in Parts B, C, and D extremely slow.

```cypher
CREATE INDEX paper_id   FOR (p:Paper)        ON (p.paper_id);
CREATE INDEX author_id  FOR (a:Author)       ON (a.author_id);
CREATE INDEX journal_id FOR (j:Journal)      ON (j.journal_id);
CREATE INDEX conf_id    FOR (c:Conference)   ON (c.conf_id);
CREATE INDEX ws_id      FOR (w:Workshop)     ON (w.workshop_id);
CREATE INDEX edition_id FOR (e:Edition)      ON (e.edition_id);
CREATE INDEX volume_id  FOR (v:Volume)       ON (v.volume_id);
CREATE INDEX org_id     FOR (o:Organization) ON (o.org_id);
CREATE INDEX kw_label   FOR (k:Keyword)      ON (k.label);
```

Wait for all indexes to reach `ONLINE` state:

```cypher
SHOW INDEXES YIELD name, state, labelsOrTypes, properties
WHERE state <> 'ONLINE';
```

This query should return **no rows** when all indexes are ready.

---

## 10. Verification Queries

Run these after import and index creation to confirm the graph is complete and correctly structured.

```cypher
// Node count by label
MATCH (n) RETURN labels(n) AS label, count(n) AS total ORDER BY total DESC;

// Relationship count by type
MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS total ORDER BY total DESC;

// Sample paper with authors and keywords
MATCH (a:Author)-[:WROTE]->(p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
RETURN p.title AS paper,
       collect(DISTINCT a.name)[..3] AS authors,
       collect(DISTINCT k.label) AS keywords
LIMIT 5;

// Journal chain: journal → volume → paper
MATCH (j:Journal)<-[:BELONGS_TO]-(v:Volume)<-[:PUBLISHED_IN_VOLUME]-(p:Paper)
RETURN j.name AS journal, v.year AS year, count(p) AS papers
ORDER BY papers DESC LIMIT 5;

// Conference chain: conference → edition → paper
MATCH (c:Conference)<-[:EDITION_OF]-(e:Edition)<-[:PRESENTED_IN]-(p:Paper)
RETURN c.name AS conference, e.year AS year, count(p) AS papers
ORDER BY papers DESC LIMIT 5;

// Review sample
MATCH (a:Author)-[r:REVIEWED]->(p:Paper)
RETURN a.name AS reviewer, p.title AS paper,
       r.score AS score, r.decision AS decision
LIMIT 5;

// Citation sample
MATCH (p1:Paper)-[:CITES]->(p2:Paper)
RETURN p1.title AS citing, p2.title AS cited
LIMIT 5;

// Affiliation sample
MATCH (a:Author)-[:AFFILIATED_TO]->(o:Organization)
RETURN o.name AS org, count(a) AS authors
ORDER BY authors DESC;
```

**Full visualization query (all node and relationship types):**

```cypher
MATCH (a:Author)-[w:WROTE]->(p:Paper)
WITH a, count(p) AS np ORDER BY np DESC LIMIT 2
MATCH (a)-[w:WROTE]->(p:Paper)
MATCH (p)-[h:HAS_KEYWORD]->(k:Keyword)
MATCH (a)-[af:AFFILIATED_TO]->(org:Organization)
MATCH (a)-[rv:REVIEWED]->(pr:Paper)
OPTIONAL MATCH (p)-[piv:PUBLISHED_IN_VOLUME]->(v:Volume)-[bt:BELONGS_TO]->(j:Journal)
OPTIONAL MATCH (p)-[pi:PRESENTED_IN]->(e:Edition)-[eo:EDITION_OF]->(c:Conference)
OPTIONAL MATCH (p)-[piw:PRESENTED_IN]->(ew:Edition)-[eow:EDITION_OF]->(ws:Workshop)
OPTIONAL MATCH (p)-[ci:CITES]->(p2:Paper)
RETURN a, w, p, h, k, af, org, rv, pr,
       piv, v, bt, j, pi, e, eo, c, piw, ew, eow, ws, ci, p2
LIMIT 80;
```

---

## 11. Lab Query Reference

### 11.1 Part B — Analytical Queries

**B.1 — Top 3 most cited papers per conference/workshop:**
```cypher
MATCH (c:Conference)<-[:EDITION_OF]-(e:Edition)<-[:PRESENTED_IN]-(p:Paper)
WITH c, p, COUNT { (p)<-[:CITES]-() } AS citations
ORDER BY c.name, citations DESC
WITH c, collect({paper: p.title, citations: citations})[..3] AS top3
RETURN c.name AS conference, top3;
```

**B.2 — Community of each conference (authors in >= 4 editions):**
```cypher
MATCH (a:Author)-[:WROTE]->(p:Paper)-[:PRESENTED_IN]->(e:Edition)
      -[:EDITION_OF]->(c:Conference)
WITH c, a, count(DISTINCT e) AS editions
WHERE editions >= 4
RETURN c.name AS conference, collect(a.name) AS community, count(a) AS size
ORDER BY size DESC;
```

**B.3 — Impact factor of journals:**
```cypher
MATCH (j:Journal)
RETURN j.name AS journal, j.impact_factor AS impact_factor
ORDER BY impact_factor DESC;
```

**B.4 — H-index of authors:**
```cypher
MATCH (a:Author)-[:WROTE]->(p:Paper)
WITH a, p, COUNT { (p)<-[:CITES]-() } AS citations
ORDER BY a.author_id, citations DESC
WITH a, collect(citations) AS sorted_citations
WITH a, [i IN range(0, size(sorted_citations)-1)
         WHERE sorted_citations[i] >= i+1 | i+1] AS h_values
RETURN a.name AS author,
       CASE WHEN size(h_values) > 0 THEN last(h_values) ELSE 0 END AS h_index
ORDER BY h_index DESC
LIMIT 20;
```

### 11.2 Part C — Reviewer Recommender

**C.1 — Define database community keywords (assert into graph):**
```cypher
MERGE (com:Community {name: 'Database'})
WITH com
UNWIND ['Data Management','Indexing','Data Modeling','Big Data',
        'Data Processing','Data Storage','Data Querying'] AS kw
MATCH (k:Keyword {label: kw})
MERGE (com)-[:DEFINED_BY]->(k);
```

**C.2 — Find venues related to the database community (>= 90% DB papers):**
```cypher
MATCH (com:Community {name: 'Database'})-[:DEFINED_BY]->(k:Keyword)
WITH collect(k.keyword_id) AS db_kw_ids

MATCH (p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
WITH db_kw_ids, p,
     count(DISTINCT CASE WHEN k.keyword_id IN db_kw_ids THEN k END) AS db_kws,
     count(DISTINCT k) AS total_kws
WITH db_kw_ids, p, db_kws * 1.0 / total_kws AS db_ratio
WHERE db_ratio >= 0.9

WITH db_kw_ids, collect(p.paper_id) AS db_papers
UNWIND ['Conference','Workshop','Journal'] AS venue_type
CALL {
  WITH db_papers, venue_type
  MATCH (v) WHERE any(label IN labels(v) WHERE label = venue_type)
  MATCH (v)<--(p:Paper) WHERE p.paper_id IN db_papers
  WITH v, count(p) AS db_count
  MATCH (v)<--(p2:Paper)
  WITH v, db_count, count(p2) AS total_count
  WHERE db_count * 1.0 / total_count >= 0.9
  SET v:DBCommunityVenue
  RETURN count(v) AS tagged
}
RETURN sum(tagged) AS venues_tagged;
```

**C.3 — Find top 100 papers of the database community by citations:**
```cypher
MATCH (p:Paper)
WHERE EXISTS { (p)-[:PRESENTED_IN]->()-[:EDITION_OF]->(:DBCommunityVenue) }
   OR EXISTS { (p)-[:PUBLISHED_IN_VOLUME]->()-[:BELONGS_TO]->(:DBCommunityVenue) }
WITH p, COUNT { (p)<-[:CITES]-() } AS citations
ORDER BY citations DESC LIMIT 100
SET p:Top100DBPaper
RETURN count(p);
```

**C.4 — Identify potential reviewers and gurus:**
```cypher
// Potential reviewers (authors of any top-100 paper)
MATCH (a:Author)-[:WROTE]->(p:Top100DBPaper)
SET a:PotentialReviewer
WITH a, count(p) AS top_papers

// Gurus (authors of >= 2 top-100 papers)
WHERE top_papers >= 2
SET a:Guru
RETURN count(a) AS gurus_identified;
```

---

## 12. Known Limitations and Assumptions

| Limitation | Impact | Mitigation |
|---|---|---|
| DBLP author names are not disambiguated ("Wei Wang" vs "Wei Wang 0001") | Author count is inflated (~285k vs real ~220k) | Acceptable for lab purposes; no query relies on exact author count |
| 0 real internal citations in the 2015+ window | All citations are synthetic | Synthetic citations are thematically consistent and chronologically valid |
| Abstracts are synthetic templates | Keyword-based queries work correctly; NLP queries would not | Assignment does not require NLP on abstracts |
| Only 20 seed organizations for affiliations | All authors affiliated to well-known real institutions | Satisfies A.3 requirements; random but realistic |
| impact_factor, issn, publisher are synthetic for journals | Correct for B.3 (uses stored value, not computed) | Assignment accepts synthetic values |
| Workshops detected by "Workshop" substring in venue name | Some workshop-related venues may be misclassified | Conservative approach; all main venues correctly classified |

---

## 13. Troubleshooting

### `UnsupportedClassVersionError: class file version 65.0`
Neo4j requires Java 21. Your system is using Java 17 or earlier.
→ Install JDK 21 from [Adoptium](https://adoptium.net) and update `JAVA_HOME` (see section 7.2).

### `Not an integer: "2023.0"` during import
A `year:int` column contains float strings due to pandas type inference after a merge.
→ Ensure the type safety block in `generate_graph_dataset()` includes `.astype(int)` for all year and score columns before export.

### `MATCH (n) RETURN count(n)` returns 0
You are connected to a different database than the one the import targeted.
→ Run `:dbs` to list available databases, then `:use neo4j` to switch to the correct one.

### Import targets wrong database name
The command `database import full neo4j` imports into the database named `neo4j`.
→ If you want to import into a different database (e.g., `sdmlocalbbdd`), replace `neo4j` with that name in the import command.

### Queries are very slow (minutes per query)
Indexes have not been created or are not yet ONLINE.
→ Run the index creation block in section 9.6 and wait for all indexes to reach `ONLINE` state before running analytical queries.

### Graph visualization shows very few nodes
Neo4j Browser has a default display limit of 300 nodes.
→ Go to Browser Settings (gear icon, bottom left) and increase *"Initial Node Display"* to 1000. Also use the zoom wheel and the fit-to-screen button (⤢) in the graph panel.