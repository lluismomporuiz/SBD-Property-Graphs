"""
StartStopBBDD.py
================
Toggle the Neo4j Windows service on and off with a single command.

  - If Neo4j is running  -> stops the service.
  - If Neo4j is stopped  -> starts the service and waits until Bolt is ready.

Usage
-----
  uv run StartStopBBDD.py   (must be run as Administrator)

Configuration
-------------
  Reads NEO4J_URI, NEO4J_USER and NEO4J_PASSWORD from the .env file in the
  project root. No other variables are required.
"""

import os
import subprocess
import sys
import time
from dotenv import load_dotenv
from neo4j import GraphDatabase

# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Poll interval and timeout, in seconds.
POLL_INTERVAL  = 3
START_TIMEOUT  = 60

# Force UTF-8 output on Windows.
if sys.platform == "win32":
    os.system("chcp 65001 > nul")


# =============================================================================
# HELPERS
# =============================================================================

def is_running() -> bool:
    """Return True if Neo4j is accepting Bolt connections, False otherwise."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        driver.close()
        return True
    except Exception:
        return False


def run_service_command(action: str) -> None:
    """
    Execute 'net start neo4j' or 'net stop neo4j'.

    Exit code 2 means the service was already in the desired state, which is
    treated as a non-fatal warning. Any other non-zero code is a hard error
    (typically exit code 5 = access denied -- run as Administrator).
    """
    try:
        subprocess.run(["net", action, "neo4j"], check=True)
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 2:
            print(f"[WARN] Service was already {'running' if action == 'start' else 'stopped'}.")
        else:
            print(f"[ERROR] 'net {action} neo4j' exited with code {exc.returncode}.")
            if exc.returncode == 5:
                print("        Access denied -- run VS Code (or this terminal) as Administrator.")
            sys.exit(1)


def wait_until_online() -> None:
    """Block until Bolt accepts connections or the timeout expires."""
    print(f"[INFO] Waiting for Neo4j to come online (timeout: {START_TIMEOUT}s)...")
    deadline = time.time() + START_TIMEOUT
    while time.time() < deadline:
        if is_running():
            print("[OK] Neo4j is online and accepting connections.")
            return
        print(f"       Not yet reachable -- retrying in {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)
    print("[ERROR] Neo4j did not come online within the timeout.")
    print("        Check the Neo4j logs for errors.")
    sys.exit(1)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 50)
    print("  Neo4j Service Toggle")
    print("=" * 50)

    if is_running():
        print("[INFO] Neo4j is currently RUNNING -- stopping...")
        run_service_command("stop")
        print("[OK] Neo4j stopped.")
    else:
        print("[INFO] Neo4j is currently STOPPED -- starting...")
        run_service_command("start")
        wait_until_online()

    print("=" * 50)


if __name__ == "__main__":
    main()