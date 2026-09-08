#!/usr/bin/env python3
"""Prove the run journal records what retention needs -- and does not record
what it shouldn't. Every check below is paired with a case that makes it fail if
the logic were wrong; a guard that only ever says yes proves nothing.
"""
import importlib.util
import json
import os
import sys
import tempfile
import time

SRC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "vcephfs_transcoder.py")
spec = importlib.util.spec_from_file_location("tc", SRC)
tc = importlib.util.module_from_spec(spec)
sys.argv = ["tc"]
spec.loader.exec_module(tc)

fails = []


def check(name, cond):
    print(("  PASS  " if cond else "  FAIL  ") + name)
    if not cond:
        fails.append(name)


def touch(path, mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write("x")
    if mtime is not None:
        os.utime(path, (mtime, mtime))
    return os.stat(path, follow_symlinks=False)


vol = tempfile.mkdtemp()

# artifact A, with a nested artifact B inside it, and loose data under neither
touch(os.path.join(vol, "artA", "RETENTION"))
touch(os.path.join(vol, "artA", "sub", "deep", "RETENTION"))   # nested artifact B
artA = os.path.join(vol, "artA")
artB = os.path.join(vol, "artA", "sub", "deep")

OLD = time.time() - 86400 * 30
NEW = time.time() - 86400 * 5

f_a = os.path.join(vol, "artA", "data1.parquet")
f_a2 = os.path.join(vol, "artA", "sub", "data2.parquet")
f_b = os.path.join(vol, "artA", "sub", "deep", "data3.parquet")
f_loose = os.path.join(vol, "loose", "data4.parquet")

st_a = touch(f_a, OLD)
st_a2 = touch(f_a2, NEW)          # newest under A, but NOT under B
st_b = touch(f_b, OLD)
st_loose = touch(f_loose, NEW)

# pre_rctime is a CephFS xattr; off Ceph it is None. Substitute a known value so
# the capture-ordering property is testable here.
rctime_reads = []
def fake_rctime(p):
    rctime_reads.append(p)
    return 1000.0 + len(rctime_reads)
tc.read_rctime = fake_rctime

j = tc.RunJournal(enabled=True, stop_at=[vol])
for path, st in ((f_a, st_a), (f_a2, st_a2), (f_b, st_b), (f_loose, st_loose)):
    j.note_file(path, st)

print("artifact attribution:")
check("file directly in A -> A", j._artifact_root(os.path.dirname(f_a)) == artA)
check("file in A/sub (no RETENTION there) -> A",
      j._artifact_root(os.path.dirname(f_a2)) == artA)
check("file in the NESTED artifact -> B, not A",
      j._artifact_root(os.path.dirname(f_b)) == artB)
check("file under no artifact -> None",
      j._artifact_root(os.path.dirname(f_loose)) is None)
check("loose file produced no record", os.path.join(vol, "loose") not in j._artifacts)

print("\nrecorded values:")
check("A and B both recorded", set(j._artifacts) == {artA, artB})
check("A's max_file_mtime is the NEWEST file under A",
      abs(j._artifacts[artA]["max_file_mtime"] - NEW) < 1)
check("B's max_file_mtime is its own file, not A's newer one",
      abs(j._artifacts[artB]["max_file_mtime"] - OLD) < 1)
check("pre_rctime captured for each artifact",
      all(r["pre_rctime"] is not None for r in j._artifacts.values()))
check("pre_rctime read exactly once per artifact", len(rctime_reads) == 2)

print("\ncapture happens on FIRST sighting (before we would modify anything):")
before = j._artifacts[artA]["pre_rctime"]
j.note_file(f_a, st_a)          # sighting the same artifact again
check("re-sighting does not re-read rctime", len(rctime_reads) == 2)
check("pre_rctime unchanged by later sightings",
      j._artifacts[artA]["pre_rctime"] == before)

print("\nnegative control -- would a broken attribution be caught?")
j2 = tc.RunJournal(enabled=True, stop_at=[vol])
j2.note_file(f_b, st_b)
check("test data really does distinguish A from B (nested marker exists)",
      j2._artifact_root(os.path.dirname(f_b)) != artA)

print("\ndisabled journal records nothing:")
j3 = tc.RunJournal(enabled=False, stop_at=[vol])
j3.note_file(f_a, st_a)
check("--no-run-journal produces no records", not j3._artifacts)

print("\nstop_at keeps the walk-up inside the volume:")
outside = tempfile.mkdtemp()
touch(os.path.join(outside, "RETENTION"))
nested_vol = os.path.join(outside, "vol")
f_out = os.path.join(nested_vol, "x", "d.parquet")
st_out = touch(f_out, OLD)
j4 = tc.RunJournal(enabled=True, stop_at=[nested_vol])
check("does not attribute to an artifact ABOVE the volume root",
      j4._artifact_root(os.path.dirname(f_out)) is None)
j5 = tc.RunJournal(enabled=True, stop_at=[])       # no boundary -> it escapes
check("without stop_at it WOULD escape (so the guard is doing work)",
      j5._artifact_root(os.path.dirname(f_out)) == outside)

print("\njournal file:")
class A:
    min_size = 131072
j.write([vol], A())
jp = os.path.join(vol, tc.RUN_JOURNAL_NAME)
check("journal written at the volume root", os.path.exists(jp))
rows = [json.loads(l) for l in open(jp)]
check("one row per artifact", len(rows) == 2)
check("rows carry what retention needs",
      all({"artifact", "pre_rctime", "max_file_mtime", "start", "end", "run"}
          <= set(r) for r in rows))
check("end >= start", all(r["end"] >= r["start"] for r in rows))
check("min_size recorded", all(r["min_size"] == 131072 for r in rows))

j.write([vol], A())
check("a second run APPENDS rather than truncating",
      len([json.loads(l) for l in open(jp)]) == 4)

# ---------------------------------------------------------------------------
# Regression guard for the cm#3999 review finding: the --paths-from branch ends
# in `return` from INSIDE the `with ThreadPoolExecutor(...)` block, so a journal
# write placed after that block is unreachable and every record is silently
# dropped. --paths-from is the mode the revisit passes use. This is a structural
# check because reproducing it functionally would mean standing up the executor,
# the regulator and a mount table; the shape is what broke and the shape is what
# is pinned.
import ast as _ast


def _mentions_paths_from(node):
    return any(isinstance(n, _ast.Attribute) and n.attr == "paths_from"
               for n in _ast.walk(node))


def _finally_covers_returns(source):
    """True when the --paths-from early return is covered by a finally that
    calls run_journal.write.

    Deliberately specific on both halves. An earlier version accepted any
    Return inside any try whose finally held any `.write` attribute, which an
    unrelated `fh.write` would have satisfied -- it pinned a family of shapes
    rather than this regression.
    """
    tree = _ast.parse(source)
    fn = [n for n in _ast.walk(tree)
          if isinstance(n, _ast.FunctionDef) and n.name == "process_files"]
    if not fn:
        return False
    for t in [n for n in fn[0].body if isinstance(n, _ast.Try)]:
        journal_write = any(
            isinstance(n, _ast.Call)
            and isinstance(n.func, _ast.Attribute)
            and n.func.attr == "write"
            and isinstance(n.func.value, _ast.Name)
            and n.func.value.id == "run_journal"
            for n in _ast.walk(_ast.Module(body=t.finalbody, type_ignores=[])))
        if not journal_write:
            continue
        for node in _ast.walk(t):
            if (isinstance(node, _ast.If)
                    and _mentions_paths_from(node.test)
                    and any(isinstance(x, _ast.Return)
                            for x in _ast.walk(node))):
                return True
    return False


print("\ncm#3999 regression -- the journal write survives an early return:")
_src = open(SRC).read()
check("process_files has try/finally with run_journal.write, enclosing a return",
      _finally_covers_returns(_src))

# inject the pre-fix shape and prove the guard rejects it
_broken = """
def process_files(args):
    roots_seen = []
    with ThreadPoolExecutor() as executor:
        if args.paths_from:
            process_paths(args)
            return
        for d in args.dirs:
            process_dir(args, d)

    if run_journal is not None:
        run_journal.write(roots_seen, args)
"""
check("guard REJECTS the pre-fix shape (write after the with-block)",
      not _finally_covers_returns(_broken))

# The reviewer's case: an unrelated try/finally holding some other .write and
# some other return must NOT satisfy the guard.
_decoy = """
def process_files(args):
    roots_seen = []
    try:
        with ThreadPoolExecutor() as executor:
            if args.paths_from:
                process_paths(args)
                return
            for d in args.dirs:
                process_dir(args, d)
    finally:
        fh.write('unrelated')
"""
check("guard REJECTS an unrelated .write in the finally",
      not _finally_covers_returns(_decoy))

# A finally that does call run_journal.write, but with the paths-from return
# left outside the try, must also be rejected.
_escaped = """
def process_files(args):
    roots_seen = []
    if args.paths_from:
        process_paths(args)
        return
    try:
        with ThreadPoolExecutor() as executor:
            for d in args.dirs:
                process_dir(args, d)
    finally:
        run_journal.write(roots_seen, args)
"""
check("guard REJECTS the paths-from return escaping the try",
      not _finally_covers_returns(_escaped))

# ---------------------------------------------------------------------------
# Cross-repo format contract. The consumer is Voleon/infra,
# jenkins/scripts/retention_path_policy.py (transcode_pinned_rctime), which
# reads these key names and types out of the journal. It lives in a different
# repo and cannot import this one, so the two halves are pinned by this test
# and its mirror there -- not by prose in a PR description. Changing a key name
# or a value type here silently turns the consumer into a no-op: every mismatch
# degrades to "trust rctime", with no error and no log line.
print("\ncross-repo journal contract (consumer: infra retention_path_policy):")

_contract_dir = tempfile.mkdtemp()
_art = os.path.join(_contract_dir, "artifact")
os.makedirs(_art)
open(os.path.join(_art, tc.RETENTION_MARKER), "w").write("30d\n")
_data = os.path.join(_art, "d.parquet")
_st = touch(_data, OLD)

tc.read_rctime = lambda p: 1757000000.123456
_j = tc.RunJournal(enabled=True, stop_at=[_contract_dir])
_j.note_file(_data, _st)


class _CArgs:
    min_size = 131072


_j.write([_contract_dir], _CArgs())
_line = open(os.path.join(_contract_dir, tc.RUN_JOURNAL_NAME)).readline()
_row = json.loads(_line)

check("journal filename is the name the consumer looks for",
      tc.RUN_JOURNAL_NAME == ".vcephfs-transcode-runs.jsonl")
check("emits every key the consumer reads",
      {"artifact", "start", "end", "pre_rctime", "max_file_mtime"} <= set(_row))
check("artifact is an absolute path string",
      isinstance(_row["artifact"], str) and os.path.isabs(_row["artifact"]))
check("start/end are numeric epoch seconds, not ISO strings",
      all(isinstance(_row[k], (int, float)) for k in ("start", "end"))
      and _row["start"] > 1_600_000_000)
check("pre_rctime and max_file_mtime are numeric or null",
      all(_row[k] is None or isinstance(_row[k], (int, float))
          for k in ("pre_rctime", "max_file_mtime")))
check("one JSON object per line", len(_line.splitlines()) == 1)

# The consumer keys on the artifact path. Prove the producer writes the same
# spelling the walk would hand a consumer standing in the same tree.
check("artifact key matches the directory as walked",
      _row["artifact"] == _art)

print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILURES: {fails}"))
sys.exit(1 if fails else 0)
