# Semantic Data Management Lab

Graph database for research publications using Neo4j.

## Dataset

This project uses the **DBLP Computer Science Bibliography dataset**.

Download it from:

https://dblp.org/xml/

Required files:

* `dblp.xml`
* `dblp.dtd`

Place them in:

```
data/raw/
```

The files are not included in the repository because they are very large.

Then run:

python scripts/xml_to_csv.py data/raw/dblp.xml data/raw/dblp.dtd data/processed/dblp.csv

## Pipeline
1. Filter XML
2. Transform to CSV
3. Add synthetic data
4. Import into Neo4j