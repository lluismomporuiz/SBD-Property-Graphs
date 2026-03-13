# SDM Lab — Property Graph: DBLP Research Publications

**Course:** Semantic Data Management (SDM) — Facultat d'Informàtica de Barcelona (UPC)
**Dataset:** DBLP XML bulk download
**Database:** Neo4j Community Edition 2026.x

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Graph Schema Design](#3-graph-schema-design)
   - 3.1 [Node Labels and Properties](#31-node-labels-and-properties)
   - 3.2 [Relationship Types and Properties](#32-relationship-types-and-properties)
   - 3.3 [Schema Diagram](#33-schema-diagram)
   - 3.4 [Key Paths](#34-key-paths)
4. [Design Decisions and Justifications](#4-design-decisions-and-justifications)
5. [Data Sources](#5-data-sources)
   - 5.1 [Real Data from DBLP](#51-real-data-from-dblp)
   - 5.2 [Synthetic Data](#52-synthetic-data)
6. [Graph Statistics](#6-graph-statistics)
7. [Environment Setup](#7-environment-setup)
   - 7.1 [Python Requirements](#71-python-requirements)
   - 7.2 [Neo4j Community Edition](#72-neo4j-community-edition)
   - 7.3 [Configuration (.env)](#73-configuration-env)
8. [Pipeline: Part A.2 — Initial Graph Load](#8-pipeline-part-a2--initial-graph-load)
   - 8.1 [Step 1 — Generate CSVs (FormatCSV.py)](#81-step-1--generate-csvs-formatcsvpy)
   - 8.2 [Step 2 — Bulk Import (UploadCSV.py)](#82-step-2--bulk-import-uploadcsvpy)
9. [Pipeline: Part A.3 — Graph Evolution](#9-pipeline-part-a3--graph-evolution)
   - 9.1 [Step 1 — Generate Update CSVs (FormatUpdateCSV.py)](#91-step-1--generate-update-csvs-formatupdatecsvpy)
   - 9.2 [Step 2 — Apply Updates (UploadUpdateCSV.py)](#92-step-2--apply-updates-uploadupdatecsvpy)
10. [Verification Queries](#10-verification-queries)
11. [Managing the Neo4j Service](#11-managing-the-neo4j-service)
12. [Known Limitations and Assumptions](#12-known-limitations-and-assumptions)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Project Overview

This project constructs a **Property Graph** database of academic research publications in the database and data mining domain, using real data from the [DBLP computer science bibliography](https://dblp.org/) enriched with synthetic attributes where the source data is incomplete.

The graph models the full academic publication ecosystem as specified in the SDM Lab assignment (sections A.1 through A.3):

- Papers published in journals (via volumes) or conferences/workshops (via editions).
- Authors with affiliations to universities and companies.
- Review processes with scores and decisions.
- Citation networks between papers.
- Keyword-based topic classification.

The pipeline is split into two parts matching the lab structure:

- **Part A.2** — generates and bulk-imports the base graph from DBLP data.
- **Part A.3** — evolves the live graph incrementally to add organizations, affiliations, and review properties.

---

## 2. Repository Structure

```
SBD-Property-Graphs/
│
├── A2/
│   ├── FormatCSV.py
│   └── UploadCSV.py
│
├── A3/
│   ├── FormatUpdateCSV.py
│   └── UploadUpdateCSV.py
│
├── StartStopBBDD.py
│
├── output_csv/
│   ├── dblp_article_header.csv
│   ├── dblp_article.csv
│   ├── dblp_inproceedings_header.csv
│   └── dblp_inproceedings.csv
│
├── Neo4j/
├── Neo4j_import/
├── Neo4j_update/
│
├── .env
├── .env.example
└── README.md
```

> **Note:** `Neo4j/`, `Neo4j_import/`, `Neo4j_update/` and `output_csv/` are generated at runtime and should be listed in `.gitignore`.

---

## 3. Graph Schema Design

### 3.1 Node Labels and Properties

| Label | ID Property | Other Properties | Origin |
|---|---|---|---|
| `Paper` | `paper_id` (DBLP key) | `title`, `abstract`, `year` (int), `pages`, `doi` | Real + synthetic |
| `Author` | `author_id` | `name`, `email` | Real + synthetic |
| `Keyword` | `keyword_id` | `label` | Synthetic |
| `Organization` | `org_id` | `name`, `type` (University / Company) | Synthetic |
| `Journal` | `journal_id` | `name`, `acronym`, `publisher`, `issn` | Real + synthetic |
| `Volume` | `volume_id` | `volumeNumber`, `year` (int) | Real + synthetic |
| `Conference` | `conf_id` | `name`, `acronym`, `field` | Real + synthetic |
| `Workshop` | `workshop_id` | `name`, `acronym`, `field` | Real + synthetic |
| `Edition` | `edition_id` | `year` (int), `city` | Real + synthetic |


### 3.2 Relationship Types and Properties

| Relationship | From | To | Properties | Origin |
|---|---|---|---|---|
| `WROTE` | `Author` | `Paper` | `role` (Main Author / Co-author) | Real |
| `CITES` | `Paper` | `Paper` | — | Real + synthetic |
| `HAS_KEYWORD` | `Paper` | `Keyword` | — | Synthetic |
| `AFFILIATED_TO` | `Author` | `Organization` | — | Synthetic |
| `REVIEWED` | `Author` | `Paper` | `review_id`, `score` (int 1–10), `decision`, `comments` | Synthetic |
| `PUBLISHED_IN_VOLUME` | `Paper` | `Volume` | — | Real |
| `BELONGS_TO` | `Volume` | `Journal` | — | Real |
| `PRESENTED_IN` | `Paper` | `Edition` | — | Real |
| `EDITION_OF` | `Edition` | `Conference` | — | Real |
| `EDITION_OF` | `Edition` | `Workshop` | — | Real |

> **Schema evolution (A.2 → A.3):** `REVIEWED` is created in A.2 without properties. The properties `review_id`, `score`, `decision` and `comments` are added in A.3 by `UploadUpdateCSV.py` without reimporting. `Organization` nodes and `AFFILIATED_TO` edges are also new in A.3.

### 3.3 Schema Diagram

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
    AFFILIATED_TO        │  doi       │     └────────┬────────┘
           │             └─────┬──────┘              │
  ┌────────▼───────┐           │                     │ BELONGS_TO
  │    Author      │           │ CITES               │
  │  author_id     │◄──WROTE───┤            ┌────────▼────────┐
  │  name          │           │            │     Volume      │
  │  email         │◄─REVIEWED─┤            │  volume_id      │
  └────────────────┘           │            │  volumeNumber   │
                               │            │  year           │
                               │            └────────▲────────┘
                               │                     │
                               │        PUBLISHED_IN_VOLUME
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

### 3.4 Key Paths

```cypher
(Author)-[:WROTE]->(Paper)-[:PUBLISHED_IN_VOLUME]->(Volume)-[:BELONGS_TO]->(Journal)
(Author)-[:WROTE]->(Paper)-[:PRESENTED_IN]->(Edition)-[:EDITION_OF]->(Conference)
(Author)-[:WROTE]->(Paper)-[:PRESENTED_IN]->(Edition)-[:EDITION_OF]->(Workshop)
(Author)-[:REVIEWED {score, decision}]->(Paper)
(Author)-[:AFFILIATED_TO]->(Organization)
(Paper)-[:CITES]->(Paper)
(Paper)-[:HAS_KEYWORD]->(Keyword)
```

---

## 4. Design Decisions and Justifications

### Why separate Conference and Workshop as distinct node labels?

The assignment distinguishes conferences as *"well-established forums"* and workshops as *"associated with new trends"*. Keeping them as separate labels allows direct label-based filtering (`MATCH (c:Conference)` vs `MATCH (w:Workshop)`) and cleaner query semantics. The trade-off is that `EDITION_OF` must be split into two CSV files for `neo4j-admin` import, which `FormatCSV.py` handles automatically.

### Why model Edition as a separate node?

An `Edition` represents a specific annual occurrence of a conference or workshop (e.g., SIGMOD 2019). Modeling it as a node enables storing edition-specific attributes (`city`, `year`) and querying how many distinct editions an author has published in — required by Part B.2.

### Why split EDITION_OF into two CSV files?

`neo4j-admin database import` resolves relationship endpoints by label-scoped ID spaces. Since `Conference` and `Workshop` are different labels, a single `EDITION_OF` file would cause ID resolution conflicts. Two separate files, each referencing one label, is the correct approach.

### Why use MIN_YEAR = 2015?

Filtering to papers from 2015 onwards keeps the graph at a manageable size (~150k papers) while covering a decade of recent research. Including the full DBLP history would produce millions of papers with little additional value for the lab queries.

---

## 5. Data Sources

### 5.1 Real Data from DBLP

The following fields are taken directly from the DBLP dump without modification:

| Field | Source |
|---|---|
| `paper_id` | DBLP key (e.g., `journals/pvldb/Smith19`) |
| `title` | Title field |
| `year` | Year field |
| `authors` | Author list (pipe-separated) |
| `venue` | Journal name or booktitle |
| `volume` | Volume number (journals only) |
| `doi` | Extracted from the `ee` field (doi.org URLs) |
| `cites` | Cite keys (used to build real CITES relationships) |
| Author `name` | From the author list |

The DBLP XML dump is converted to CSV using the [dblp-to-csv tool](https://github.com/ThomHurks/dblp-to-csv)
(script `xml_to_csv.py`). The conversion is automated by `export_to_csv.py`, a wrapper script in the project
root that invokes `xml_to_csv.py` with the correct arguments and paths:
```powershell
python export_to_csv.py
```

The tool generates CSV files for all DBLP entity types (articles, inproceedings, proceedings, books, etc.)
into `output_csv/`. Of all the files produced, only four are consumed by `FormatCSV.py`:

| File | Content |
|---|---|
| `dblp_article_header.csv` | Column headers for journal articles |
| `dblp_article.csv` | Journal article records |
| `dblp_inproceedings_header.csv` | Column headers for conference/workshop papers |
| `dblp_inproceedings.csv` | Conference and workshop paper records |

The input files (`dblp.xml` and `dblp.dtd`) must be placed in `InputData/` before running the conversion.
Both files can be downloaded from [https://dblp.org/xml/](https://dblp.org/xml/).
> **Note:** The full `dblp.xml` dump is approximately 4GB and the conversion can take several minutes.

### 5.2 Synthetic Data

The following fields are generated synthetically because DBLP does not provide them:

| Field | Generation Method |
|---|---|
| `abstract` | Template: *"This paper explores novel approaches in {keywords}..."* |
| `pages` | Random `start-(start+8..20)` |
| `doi` | Fake DOI (`10.fake/<uuid>`) when no doi.org link is found in `ee` |
| Author `email` | Derived from name: `firstname.lastname@university.edu` |
| `HAS_KEYWORD` edges | 2–4 random keywords per paper from `KEYWORD_LABELS` |
| `CITES` edges | Synthetic thematic citations to guarantee 5–10 per paper, always pointing to older papers |
| `REVIEWED` edges | 3 random non-author reviewers per paper |
| `review_id`, `score`, `decision`, `comments` | Random values; score 1–10; decision accept/reject/revision |
| `Organization` nodes | 20 seed institutions (mix of universities and companies) |
| `AFFILIATED_TO` edges | One random organization per author |
| `city` (Edition) | Random from a pool of 10 cities |
| `publisher`, `issn` (Journal) | Random from predefined pools |
| `field` (Conference/Workshop) | Random from a predefined fields list |

---

## 6. Graph Statistics

All counts are based on `MIN_YEAR = 2015` and the standard DBLP 2025 dump.

### Nodes

| Label | Count |
|---|---|
| Author | 285,288 |
| Paper | 151,744 |
| Edition | 1,404 |
| Volume | 720 |
| Workshop | 366 |
| Conference | 134 |
| Journal | 68 |
| Keyword | 12 |
| Organization | 20 |
| **Total** | **439,756** |

### Relationships

| Type | Count |
|---|---|
| CITES | ~1,073,892 |
| WROTE | 617,472 |
| HAS_KEYWORD | ~455,195 |
| REVIEWED | 455,232 |
| PRESENTED_IN | 80,997 |
| PUBLISHED_IN_VOLUME | 70,747 |
| AFFILIATED_TO | 285,288 |
| EDITION_OF | 1,404 |
| BELONGS_TO | 708 |
| **Total** | **~3,040,935** |

> CITES and HAS_KEYWORD counts vary slightly between runs due to random synthetic generation.

## 7. Environment Setup

### 7.1 Python Requirements

**Required packages:**
```
neo4j
python-dotenv
pandas
lxml
```

Any standard Python 3.10+ environment works. The instructions below cover the two most common setups.

---

**Option A — uv (recommended for this project)**

`uv` is a fast Python package manager that handles virtual environments and dependencies automatically.
All scripts in this README use `uv run`, but it is not mandatory.

Install uv:
```powershell
pip install uv
```

Key commands:
```powershell
uv init --python 3.10   # Initialise a new project with Python 3.10 (run once)
uv add <library>        # Add a dependency and install it
uv sync                 # Install all dependencies from pyproject.toml
uv run <file.py>        # Run a script inside the managed environment
uv remove               # Remove a library from pyproject.toml
```

---

**Option B — pip + venv (standard approach)**
```powershell
python -m venv .venv                          # Create a virtual environment
.venv\Scripts\Activate.ps1                    # Activate it (PowerShell)
pip install neo4j python-dotenv pandas lxml   # Install dependencies
python A2/FormatCSV.py                        # Run scripts directly with python
```

With this approach, replace every `uv run <script>` in this README with `python <script>`.

### 7.2 Neo4j Community Edition

The project requires **Neo4j Community Edition** installed directly (not Neo4j Desktop), so that the Windows service can be managed programmatically via `net start` / `net stop`.

**Installation steps:**

1. Download the Windows ZIP from [https://neo4j.com/deployment-center](https://neo4j.com/deployment-center) → Graph Database Self-Managed → Community → Windows.

2. Extract to a permanent location, e.g. `C:\neo4j\`.

3. Set the initial password **before the first start**:
   ```powershell
   C:\neo4j\bin\neo4j-admin.bat dbms set-initial-password your_password_here
   ```

4. Install Neo4j as a Windows service (run PowerShell as Administrator):
   ```powershell
   C:\neo4j\bin\neo4j.bat windows-service install
   ```

5. Start the service:
   ```powershell
   net start neo4j
   ```

6. Verify the Browser is accessible at [http://localhost:7474](http://localhost:7474).
   Connect with username `neo4j` and the password set in step 3.
   The Bolt connector listens on `bolt://localhost:7687`.

7. Verify the Java version (Neo4j requires Java 21):
   ```powershell
   java -version
   ```
   If Java 21 is not installed, download the JDK from [https://adoptium.net](https://adoptium.net).

**Useful service commands** (require Administrator):
```powershell
net start neo4j           # Start the service
net stop neo4j            # Stop the service
sc.exe query neo4j        # Check current service status (Running / Stopped)

# Control autostart behaviour
sc.exe config neo4j start= auto     # Start automatically with Windows (default after install)
sc.exe config neo4j start= demand   # Start only when explicitly requested (saves RAM on boot)
```

### 7.3 Configuration (.env)

Create a `.env` file in the project root by copying `.env.example` and filling in your values:

```dotenv
# Neo4j connection
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here
NEO4J_DATABASE=neo4j

# Neo4j Community Edition installation root
# Update this path if you extracted the ZIP to a different location
NEO4J_DBMS_ROOT=C:\neo4j
```

> **Security:** Never commit `.env` to git. Verify that `.env` is listed in `.gitignore`.

---

## 8. Pipeline: Part A.2 — Initial Graph Load

All Part A.2 scripts live in the `A2/` folder and must be run from the **project root**:

```powershell
uv run A2/FormatCSV.py
uv run A2/UploadCSV.py   # requires Administrator
```

### 8.1 Step 1 — Generate CSVs (FormatCSV.py)

Reads the raw DBLP CSV exports from `output_csv/` and produces two sets of output files:

- `Neo4j/` — plain CSVs for human inspection and LOAD CSV usage.
- `Neo4j_import/` — same data with neo4j-admin annotated headers (`:ID`, `:START_ID`, `:END_ID`, `:int`, etc.) ready for bulk import.

**Key configuration constants** (edit at the top of the file):

| Constant | Default | Description |
|---|---|---|
| `MIN_YEAR` | `2015` | Only papers from this year onwards are included |
| `KEYWORD_LABELS` | 12 labels | Assigned randomly to papers; the first 7 are the DB community keywords required by Part C |
| `REGEX_PATTERN` | VLDB, SIGMOD, KDD… | Venue filter applied to the journal and booktitle fields |

**Output files generated in both `Neo4j/` and `Neo4j_import/`:**

| File | Content |
|---|---|
| `nodes_papers.csv` | Paper nodes |
| `nodes_authors.csv` | Author nodes |
| `nodes_keywords.csv` | Keyword nodes |
| `nodes_journals.csv` | Journal nodes |
| `nodes_volumes.csv` | Volume nodes |
| `nodes_conferences.csv` | Conference nodes |
| `nodes_workshops.csv` | Workshop nodes |
| `nodes_editions.csv` | Edition nodes |
| `rels_wrote.csv` | WROTE relationships |
| `rels_cites.csv` | CITES relationships |
| `rels_has_keyword.csv` | HAS_KEYWORD relationships |
| `rels_reviewed.csv` | REVIEWED relationships (no properties in A.2) |
| `rels_paper_volume.csv` | PUBLISHED_IN_VOLUME relationships |
| `rels_volume_journal.csv` | BELONGS_TO relationships |
| `rels_paper_edition.csv` | PRESENTED_IN relationships |
| `rels_edition_conference.csv` | EDITION_OF → Conference relationships |
| `rels_edition_workshop.csv` | EDITION_OF → Workshop relationships |

### 8.2 Step 2 — Bulk Import (UploadCSV.py)

Automates the full import pipeline without any manual steps:

1. **Validates** all paths and credentials from `.env`.
2. **Copies** the 17 annotated CSV files from `Neo4j_import/` to `C:\neo4j\import\`, creating the directory if it does not exist.
3. **Stops** the Neo4j Windows service (`net stop neo4j`) and waits until Bolt is unreachable before proceeding.
4. **Runs** `neo4j-admin database import full` with all node and relationship files and `--overwrite-destination` to allow re-runs.
5. **Starts** the Neo4j Windows service (`net start neo4j`) and waits until Bolt accepts connections.
6. **Verifies** the import by running count queries for every node label and relationship type, flagging any that returned zero rows.

**Requirements:**
- VS Code (or the terminal running the script) must be open as Administrator.
- `FormatCSV.py` must have been run first.

**Re-running:** The script is fully re-runnable. `--overwrite-destination` wipes and reimports the database from scratch on each run.

**Note on EDITION_OF split:** `neo4j-admin` resolves relationship endpoints by label-scoped ID spaces. Because `Conference` and `Workshop` are different labels, `EDITION_OF` is deliberately split into `rels_edition_conference.csv` and `rels_edition_workshop.csv`. Merging them into a single file would cause ID resolution failures.

---

## 9. Pipeline: Part A.3 — Graph Evolution

All Part A.3 scripts live in the `A3/` folder and must be run from the **project root**:

```powershell
uv run A3/FormatUpdateCSV.py
uv run A3/UploadUpdateCSV.py
```

The A.3 pipeline does **not** reimport the database. It evolves the live graph incrementally using batched Cypher statements via the Python driver. Neo4j must be running when executing these scripts.

### 9.1 Step 1 — Generate Update CSVs (FormatUpdateCSV.py)

Reads `Neo4j/nodes_authors.csv` and `Neo4j/rels_reviewed.csv` (outputs of `FormatCSV.py`) and generates three update files in `Neo4j_update/`:

| File | Content |
|---|---|
| `update_organizations.csv` | 20 Organization nodes with `org_id`, `name`, `type` |
| `update_affiliated_to.csv` | One `AFFILIATED_TO` edge per author (285,288 rows) |
| `update_reviewed_props.csv` | `review_id`, `score`, `decision`, `comments` for each existing REVIEWED edge |

**Organization seed list:** MIT, Stanford, CMU, UC Berkeley, Oxford, Cambridge, ETH Zurich, EPFL, Tsinghua, NUS, Google Research, Microsoft Research, IBM Research, Meta AI, Amazon Web Services, UPC BarcelonaTech, U Toronto, U Washington, Cornell, Max Planck Institute.

**Type classification:** names containing *Research*, *AI*, *Services* or *AWS* → `Company`; all others → `University`.

### 9.2 Step 2 — Apply Updates (UploadUpdateCSV.py)

Connects to the running Neo4j instance and applies the three A.3 changes in order:

1. **MERGE** 20 Organization nodes (idempotent).
2. **MERGE** 285,288 AFFILIATED_TO relationships (Author → Organization, idempotent).
3. **SET** `review_id`, `score`, `decision`, `comments` on all 455,232 REVIEWED relationships.

**Temporary indexes:** The script automatically creates `tmp_author_id_idx` (on `Author.author_id`) and `tmp_paper_id_idx` (on `Paper.paper_id`) before the update to accelerate MATCH lookups, then drops them when finished. If those indexes already exist, the script detects this and skips creation and deletion for them. If the script is interrupted mid-run, it attempts to clean up the indexes automatically; if that fails, it prints the manual DROP commands.

**Idempotency:** All operations use `MERGE` or `SET`, so the script can be run multiple times safely without creating duplicates or corrupting data.

**Performance:** With the temporary indexes in place, the full update completes in approximately 45–60 seconds on a standard laptop.

---

## 10. Verification Queries

After completing the full pipeline (A.2 + A.3), run these queries in the Neo4j Browser at [http://localhost:7474](http://localhost:7474) to confirm the graph is correct.

**Node and relationship counts:**
```cypher
MATCH (n)
RETURN labels(n)[0] AS label, count(n) AS total
ORDER BY total DESC;
```
```cypher
MATCH ()-[r]->()
RETURN type(r) AS rel, count(r) AS total
ORDER BY total DESC;
```

**A.3 schema verification — all REVIEWED edges should have a score:**
```cypher
MATCH ()-[r:REVIEWED]->()
RETURN
  count(r)              AS total_reviewed,
  count(r.score)        AS with_score,
  count(*) - count(r.score) AS without_score;
```

**A.3 schema verification — every author should have an affiliation:**
```cypher
MATCH (a:Author)
RETURN
  count(a)                           AS total_authors,
  count { (a)-[:AFFILIATED_TO]->() } AS with_affiliation;
```

**Quick spot check (sample traversal across all relationship types):**
```cypher
MATCH (a:Author)-[:WROTE]->(p:Paper)-[:HAS_KEYWORD]->(k:Keyword)
MATCH (a)-[:AFFILIATED_TO]->(o:Organization)
RETURN a.name, p.title, k.label, o.name
LIMIT 10;
```

---

## 11. Managing the Neo4j Service

Neo4j Community Edition runs as a Windows service and consumes approximately 500MB–1GB of RAM while idle. Stop the service when not working on the project to free up resources.

**`StartStopBBDD.py`** (project root) provides a single-command toggle:

```powershell
uv run StartStopBBDD.py   # requires Administrator
```

- If Neo4j is **running** → stops the service immediately.
- If Neo4j is **stopped** → starts the service and waits until Bolt is accepting connections before returning.

The script reads `NEO4J_URI`, `NEO4J_USER` and `NEO4J_PASSWORD` from `.env` and must be run from a terminal with Administrator privileges.

**Manual service commands** (alternative to the toggle script):
```powershell
net start neo4j     # Start
net stop neo4j      # Stop
sc query neo4j      # Check status (Running / Stopped / Paused)
```

**Autostart configuration:**
```powershell
# Disable autostart with Windows (recommended to save RAM on boot)
sc config neo4j start= demand

# Re-enable autostart
sc config neo4j start= auto
```

---

## 12. Known Limitations and Assumptions

| Limitation | Impact | Mitigation |
|---|---|---|
| DBLP author names are not disambiguated (e.g. "Wei Wang" vs "Wei Wang 0001") | Author count is slightly inflated | Acceptable for lab purposes; no query relies on exact author count |
| 0 real internal citations within the 2015+ filtered window | All CITES relationships are synthetic | Synthetic citations are thematically consistent and chronologically valid (never forward-citing) |
| Abstracts are synthetic templates | Keyword-based queries work correctly; NLP queries would not | The assignment does not require NLP on abstracts |
| Only 20 seed organizations for affiliations | All 285k authors are affiliated to one of 20 well-known institutions | Satisfies A.3 requirements; random but realistic distribution |
| `publisher` and `issn` are synthetic for journals | Structural correctness is maintained | The assignment accepts synthetic values for these fields |
| Workshops are detected by the substring "Workshop" in the venue name | A small number of workshop-related venues may be misclassified | Conservative approach; all main conference venues are correctly classified |
| `score` is a random integer (1–10) with no correlation to `decision` | Review data is not semantically consistent | Acceptable for synthetic data; the assignment does not require semantic consistency |

---

## 13. Troubleshooting

### `Error de sistema 5 / Acceso denegado` when running UploadCSV.py or StartStopBBDD.py
`net stop` and `net start` require Administrator privileges.
→ Open VS Code as Administrator (right-click the shortcut → Run as administrator). If VS Code is installed in user scope, it will show a warning about automatic updates being disabled — this is cosmetic and does not affect functionality.

### `UnsupportedClassVersionError: class file version 65.0`
Neo4j requires Java 21. Your system is using an older JDK.
→ Install JDK 21 from [https://adoptium.net](https://adoptium.net) and set `JAVA_HOME`:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.x.x"
```

### `neo4j-admin.bat not found` / `FileNotFoundError`
`NEO4J_DBMS_ROOT` in `.env` is pointing to the wrong directory.
→ Verify that `C:\neo4j\bin\neo4j-admin.bat` exists. Update `NEO4J_DBMS_ROOT` accordingly.

### `MATCH (n) RETURN count(n)` returns 0 after import
You are connected to a different database than the import target.
→ Run `:dbs` in the Browser to list available databases, then `:use neo4j` to switch to the correct one.

### `ServiceUnavailable` when running UploadUpdateCSV.py
Neo4j is not running.
→ Start the service first:
```powershell
uv run StartStopBBDD.py
# or
net start neo4j
```
