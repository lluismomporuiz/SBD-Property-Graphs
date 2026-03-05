# Semantic Data Management Lab

Graph database for research publications built using **Neo4j**.

This project processes the **DBLP Computer Science Bibliography dataset** and transforms it into a property graph that can be imported into Neo4j.

---

# Dataset

This project uses the **DBLP XML dataset**.

Download it from:

https://dblp.org/xml/

Required files:

- `dblp.xml`
- `dblp.dtd`

Place them in the following directory:

```
data/raw/
```

Example structure:

```
project/
│
├── data
│   ├── raw
│   │   ├── dblp.xml
│   │   └── dblp.dtd
│   │
│   └── processed
│
├── scripts
│
└── README.md
```

The dataset files are **not included in this repository** because they are very large.

---

# Preprocessing

Convert the XML dataset to CSV files using:

```
python scripts/xml_to_csv.py data/raw/dblp.xml data/raw/dblp.dtd data/processed/dblp.csv
```

This script extracts publication data and generates CSV files that can later be imported into Neo4j.

---

# Pipeline

The data processing pipeline consists of the following steps:

1. **Filter XML dataset**
2. **Transform XML → CSV**
3. **Add synthetic data**
4. **Import into Neo4j**

---

# Technologies Used

- Python
- Neo4j
- XML processing (`lxml`)
- Git / GitHub