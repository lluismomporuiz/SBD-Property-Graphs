#!/usr/bin/env python3
"""
run_xml_to_csv.py
=================
Wrapper script that converts the DBLP XML bulk dump into Neo4j-compatible
CSV files by invoking the xml_to_csv.py converter with the correct arguments.

Expected project layout
-----------------------
  project/
    InputData/
      dblp.xml          <- DBLP XML bulk dump
      dblp.dtd          <- DBLP document type definition
    output_csv/         <- created automatically if absent
    xml_to_csv.py       <- upstream XML-to-CSV converter (ThomHurks/dblp-to-csv)
    export_to_csv.py   <- this script

Usage
-----
  python export_to_csv.py

Output
------
  output_csv/
    dblp_article_header.csv
    dblp_article.csv
    dblp_inproceedings_header.csv
    dblp_inproceedings.csv
    (additional entity files depending on xml_to_csv.py configuration)

The generated CSVs use semicolons as delimiters and include Neo4j-annotated
headers (e.g. key:string, year:int) compatible with LOAD CSV and
neo4j-admin database import.
"""

import subprocess
import sys
import time
from pathlib import Path


def validate_inputs(input_xml: Path, input_dtd: Path) -> None:
    """
    Check that the required input files exist before starting the conversion.
    Exits with code 1 if any file is missing.

    Parameters
    ----------
    input_xml : Path
        Path to the DBLP XML dump file.
    input_dtd : Path
        Path to the DBLP DTD schema file required by the XML parser.
    """
    if not input_xml.exists():
        print(f"[ERROR] XML file not found: {input_xml}")
        sys.exit(1)

    if not input_dtd.exists():
        print(f"[ERROR] DTD file not found: {input_dtd}")
        sys.exit(1)


def ensure_output_dir(output_dir: Path) -> None:
    """
    Create the output directory if it does not already exist.

    Parameters
    ----------
    output_dir : Path
        Directory where the generated CSV files will be written.
    """
    if not output_dir.exists():
        print(f"[INFO] Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        print(f"[INFO] Output directory already exists: {output_dir}")


def build_command(
    project_root: Path,
    input_xml: Path,
    input_dtd: Path,
    output_base: Path,
) -> list[str]:
    """
    Construct the subprocess command that invokes xml_to_csv.py.

    The --neo4j flag instructs the converter to annotate column headers with
    Neo4j type suffixes (e.g. :string, :int, :string[]) and to use the
    semicolon delimiter expected by neo4j-admin database import.

    Parameters
    ----------
    project_root : Path
        Root directory of the project (used to locate xml_to_csv.py).
    input_xml : Path
        Absolute path to dblp.xml.
    input_dtd : Path
        Absolute path to dblp.dtd.
    output_base : Path
        Base output path; xml_to_csv.py appends entity names and suffixes
        automatically (e.g. output_csv/dblp_article.csv).

    Returns
    -------
    list[str]
        Command and arguments ready to pass to subprocess.run().
    """
    return [
        sys.executable,                        # reuse the current Python interpreter
        str(project_root / "xml_to_csv.py"),   # upstream converter script
        str(input_xml),
        str(input_dtd),
        str(output_base),
        "--neo4j",                             # enable Neo4j-compatible header annotations
    ]


def run_conversion(command: list[str]) -> None:
    """
    Execute the XML-to-CSV conversion subprocess and report elapsed time.

    The subprocess inherits stdout/stderr from the parent process so that
    progress messages from xml_to_csv.py are visible in real time.

    Parameters
    ----------
    command : list[str]
        The command to execute, as returned by build_command().

    Raises
    ------
    SystemExit
        On subprocess failure, missing executable, or keyboard interrupt.
    """
    print("[INFO] Starting XML to CSV conversion...")
    print(f"       Source XML : {command[2]}")
    print(f"       Source DTD : {command[3]}")
    print(f"       Output base: {command[4]}")
    print()

    try:
        start_time = time.time()

        subprocess.run(
            command,
            check=True,       # raises CalledProcessError on non-zero exit code
            capture_output=False,  # stream output directly to the terminal
            text=True,
        )

        elapsed = time.time() - start_time
        print(f"\n[OK] Conversion completed in {elapsed:.2f} seconds.")

    except subprocess.CalledProcessError as exc:
        # xml_to_csv.py exited with a non-zero return code
        print(f"\n[ERROR] Conversion failed with exit code {exc.returncode}.")
        print("        Check the output above for details from xml_to_csv.py.")
        sys.exit(1)

    except FileNotFoundError:
        # The xml_to_csv.py script itself could not be found
        print("\n[ERROR] xml_to_csv.py could not be located.")
        print("        Ensure the file exists in the project root directory.")
        sys.exit(1)

    except KeyboardInterrupt:
        print("\n[WARN] Conversion interrupted by user (Ctrl+C).")
        sys.exit(1)


def print_summary(output_dir: Path) -> None:
    """
    Print a brief summary of the output location and next steps after a
    successful conversion.

    Parameters
    ----------
    output_dir : Path
        Directory where the generated CSV files were written.
    """
    print(f"[INFO] Output CSV files written to: {output_dir}")
    print()
    print("Next steps:")
    print("  1. Inspect the generated CSVs in output_csv/ to verify content.")
    print("  2. Run grafo_generator.py to build the filtered, enriched graph CSVs.")
    print("  3. Copy Neo4j_import/ to your Neo4j instance's import/ folder.")
    print("  4. Run neo4j-admin database import full (see README.md for the full command).")


def main() -> None:
    """
    Entry point. Resolves all paths relative to this script's location,
    validates inputs, creates the output directory, runs the conversion,
    and prints a summary.
    """
    # Resolve paths relative to this file so the script works regardless of
    # the current working directory when it is invoked.
    project_root = Path(__file__).parent
    input_xml    = project_root / "InputData" / "dblp.xml"
    input_dtd    = project_root / "InputData" / "dblp.dtd"
    output_dir   = project_root / "output_csv"
    output_base  = output_dir  / "dblp.csv"   # xml_to_csv.py appends entity suffixes

    validate_inputs(input_xml, input_dtd)
    ensure_output_dir(output_dir)

    command = build_command(project_root, input_xml, input_dtd, output_base)
    run_conversion(command)
    print_summary(output_dir)


if __name__ == "__main__":
    main()