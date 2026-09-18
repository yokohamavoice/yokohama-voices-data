"""Create a vote matrix from v2 files or verified v3 pages with bounded RAM."""
import argparse
import csv
import hashlib
import json
import re
import sqlite3
import tempfile
from pathlib import Path

TABLES = ("opinions", "responses", "presentations")
EXPECTED = {"agree": -1, "disagree": 1, "pass": 0, "unrelated": None}


def exact(value, fields):
    return isinstance(value, dict) and set(value) == set(fields)


def ref_valid(ref, table, kind):
    if not exact(ref, ("part", "sha256", "row_count", "byte_length")):
        raise ValueError("Invalid page reference fields")
    if not re.fullmatch(rf"{kind}-{table}-(?:[0-9]{{9}}|[0-9]{{12}})", ref["part"]):
        raise ValueError("Invalid part name")
    if not re.fullmatch(r"[a-f0-9]{64}", ref["sha256"]):
        raise ValueError("Invalid part hash")
    if any(type(ref[k]) is not int or ref[k] < 0 for k in ("row_count", "byte_length")):
        raise ValueError("Invalid part counts")


def read_part(directory, ref, table, kind):
    ref_valid(ref, table, kind)
    suffix = ".json" if kind == "i" else ".jsonl"
    raw = (directory / "parts" / (ref["part"] + suffix)).read_bytes()
    if len(raw) != ref["byte_length"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
        raise ValueError("Page length/hash mismatch")
    return raw.decode("utf-8", errors="strict")


def paged_rows(dataset, directory, table):
    summary = dataset["tables"][table]
    if not exact(summary, ("rows", "pages", "index")):
        raise ValueError("Invalid table summary")
    if any(type(summary[k]) is not int or summary[k] < 0 for k in ("rows", "pages")):
        raise ValueError("Invalid table counts")
    index, rows, pages = summary["index"], 0, 0
    visited = set()
    while index is not None:
        if index["part"] in visited:
            raise ValueError("Cyclic page index")
        visited.add(index["part"])
        node = json.loads(read_part(directory, index, table, "i"))
        if not exact(node, ("schema_version", "kind", "release_id", "table", "pages", "next")):
            raise ValueError("Invalid index fields")
        if node["schema_version"] != 3 or node["kind"] != "page_index" or node["release_id"] != dataset["release"]["id"] or node["table"] != table:
            raise ValueError("Page index identity mismatch")
        if not isinstance(node["pages"], list) or not 1 <= len(node["pages"]) <= 256 or len(node["pages"]) != index["row_count"]:
            raise ValueError("Invalid index page count")
        for ref in node["pages"]:
            if ref["part"] in visited:
                raise ValueError("Repeated data page")
            visited.add(ref["part"])
            content = read_part(directory, ref, table, "d")
            lines = content.split("\n")
            if lines and lines[-1] == "":
                lines.pop()
            if len(lines) != ref["row_count"] or any(not line.strip() for line in lines):
                raise ValueError("JSONL row count mismatch")
            rows += len(lines)
            pages += 1
            if rows > summary["rows"] or pages > summary["pages"]:
                raise ValueError("Page totals exceed root")
            for line in lines:
                yield json.loads(line)
        index = node["next"]
    if rows != summary["rows"] or pages != summary["pages"]:
        raise ValueError("Incomplete table")


def jsonl_rows(file):
    with file.open(encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def source_rows(root, manifest):
    if manifest.get("schema_version", 2) == 2:
        return lambda table: jsonl_rows(root / "data" / (table + ".jsonl"))
    if manifest.get("schema_version") != 3 or manifest.get("format") != "jsonl-pages-v1" or manifest.get("complete") is not True:
        raise ValueError("Incomplete or unsupported latest dataset")
    dataset_file = (root / "data" / manifest["dataset_path"]).resolve()
    if not dataset_file.is_relative_to((root / "releases").resolve()):
        raise ValueError("Dataset path leaves release archive")
    raw = dataset_file.read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        raise ValueError("Root hash mismatch")
    dataset = json.loads(raw)
    if not exact(dataset, ("schema_version", "format", "release", "updated_through", "tags", "sources", "tables")) or dataset["schema_version"] != 3 or dataset["format"] != "jsonl-pages-v1" or not exact(dataset["tables"], TABLES):
        raise ValueError("Invalid version 3 root")
    if dataset["release"]["id"] != manifest["release_id"]:
        raise ValueError("Release ID mismatch")
    return lambda table: paged_rows(dataset, dataset_file.parent, table)


def build_matrix(root):
    root = root.resolve()
    manifest_file = root / "data" / "manifest.json"
    manifest = json.loads(manifest_file.read_text()) if manifest_file.exists() else {"schema_version": 2}
    rows = source_rows(root, manifest)
    opinions = {}
    for opinion in rows("opinions"):
        if not exact(opinion, ("id", "tag_id", "text", "kind", "source_ids", "created_date")) or not isinstance(opinion["id"], str) or not opinion["id"] or opinion["id"] in opinions:
            raise ValueError("Invalid/duplicate public opinion")
        opinions[opinion["id"]] = None
    opinion_ids = sorted(opinions)
    for index, opinion_id in enumerate(opinion_ids):
        opinions[opinion_id] = index
    output = root / "analysis" / "output"
    output.mkdir(parents=True, exist_ok=True)
    destination = output / "vote_matrix.csv"
    temporary_output = output / "vote_matrix.csv.tmp"
    session_count = 0
    with tempfile.TemporaryDirectory(prefix="yokohama-matrix-") as temp:
        db = sqlite3.connect(str(Path(temp) / "responses.sqlite"))
        try:
            db.execute("PRAGMA temp_store=FILE")
            db.execute("CREATE TABLE votes(session_id TEXT NOT NULL, opinion_index INTEGER NOT NULL, value INTEGER, PRIMARY KEY(session_id,opinion_index)) WITHOUT ROWID")
            batch = []
            for response in rows("responses"):
                if not exact(response, ("session_id", "opinion_id", "response", "analysis_value")) or not isinstance(response["session_id"], str) or not response["session_id"] or response["opinion_id"] not in opinions or response["response"] not in EXPECTED or response["analysis_value"] != EXPECTED[response["response"]]:
                    raise ValueError("Invalid public response")
                batch.append((response["session_id"], opinions[response["opinion_id"]], response["analysis_value"]))
                if len(batch) >= 1000:
                    db.executemany("INSERT INTO votes VALUES(?,?,?)", batch)
                    db.commit()
                    batch.clear()
            if batch:
                db.executemany("INSERT INTO votes VALUES(?,?,?)", batch)
                db.commit()
            with temporary_output.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(["session_id"] + opinion_ids)
                current_session, current_row = None, None
                for session, column, value in db.execute("SELECT session_id,opinion_index,value FROM votes ORDER BY session_id,opinion_index"):
                    if session != current_session:
                        if current_row is not None:
                            writer.writerow([current_session] + current_row)
                        current_session, current_row = session, [None] * len(opinion_ids)
                        session_count += 1
                    current_row[column] = value
                if current_row is not None:
                    writer.writerow([current_session] + current_row)
            temporary_output.replace(destination)
        finally:
            db.close()
            temporary_output.unlink(missing_ok=True)
    print(f"Wrote {session_count} sessions x {len(opinion_ids)} opinions to {destination}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    build_matrix(parser.parse_args().root)
