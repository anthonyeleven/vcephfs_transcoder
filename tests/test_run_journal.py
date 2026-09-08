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


def _finally_covers_returns(source):
    """True when process_files wraps its body in try/finally and the finally
    calls run_journal.write, with the early return(s) enclosed."""
    tree = _ast.parse(source)
    fn = [n for n in _ast.walk(tree)
          if isinstance(n, _ast.FunctionDef) and n.name == "process_files"]
    if not fn:
        return False
    tries = [n for n in fn[0].body if isinstance(n, _ast.Try)]
    if not tries:
        return False
    covered = 0
    for t in tries:
        writes = [n for n in _ast.walk(_ast.Module(body=t.finalbody, type_ignores=[]))
                  if isinstance(n, _ast.Attribute) and n.attr == "write"]
        if not writes:
            continue
        covered += len([n for n in _ast.walk(t) if isinstance(n, _ast.Return)])
    return covered > 0


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

print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILURES: {fails}"))
sys.exit(1 if fails else 0)
