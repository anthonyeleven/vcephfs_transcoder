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

print("\npinned_at -- which write rctime is showing:")
check("an artifact only READ has pinned_at null",
      j._artifacts[artA]["pinned_at"] is None
      and j._artifacts[artB]["pinned_at"] is None)


class _Clock:
    """Hand-cranked time.time() so ordering is asserted, not raced."""

    def __init__(self, *values):
        self.values = list(values)

    def time(self):
        return self.values.pop(0) if len(self.values) > 1 else self.values[0]


real_time = tc.time
tc.time = _Clock(5000.0, 6000.0)
j.note_write(f_a)
check("note_write claims the artifact the file belongs to",
      j._artifacts[artA]["pinned_at"] == 5000.0)
check("and does not touch a sibling artifact",
      j._artifacts[artB]["pinned_at"] is None)
j.note_write(f_a2)                 # also under A, later
check("the LAST write wins -- that is the one rctime ends up showing",
      j._artifacts[artA]["pinned_at"] == 6000.0)

# Two workers under one artifact take different stripe locks, so they are not
# ordered against each other and their clock samples can arrive out of order.
# pinned_at must track the newest sample, never the last one to arrive: if it
# slid backwards it would sit behind the write the MDS stamped into rctime, and
# the consumer would stop recognising its own pin. Sampling outside the lock and
# assigning unconditionally -- the previous shape -- fails this check at 4000.0.
tc.time = _Clock(9000.0, 4000.0)
j.note_write(f_a)
check("a newer write advances pinned_at", j._artifacts[artA]["pinned_at"] == 9000.0)
j.note_write(f_a2)
check("an out-of-order sample never drags pinned_at backwards",
      j._artifacts[artA]["pinned_at"] == 9000.0)
tc.time = real_time

check("pinned_at is a per-write time, NOT the run start (the whole point)",
      j._artifacts[artA]["pinned_at"] != j.started)

j.note_write(f_loose)
check("a write under no artifact records nothing",
      os.path.join(vol, "loose") not in j._artifacts)

j6 = tc.RunJournal(enabled=False, stop_at=[vol])
j6.note_file(f_a, st_a)
j6.note_write(f_a)
check("--no-run-journal records no writes either", not j6._artifacts)

print("\nnegative control -- would a missing note_write be caught?")
j7 = tc.RunJournal(enabled=True, stop_at=[vol])
j7.note_file(f_b, st_b)
check("without note_write the artifact stays unclaimed (so the check bites)",
      j7._artifacts[artB]["pinned_at"] is None)

print("\njournal file:")
class A:
    min_size = 131072
j.write([vol], A())
jp = os.path.join(vol, tc.RUN_JOURNAL_NAME)
check("journal written at the volume root", os.path.exists(jp))
rows = [json.loads(l) for l in open(jp)]
check("one row per artifact", len(rows) == 2)
check("rows carry what retention needs",
      all({"artifact", "pre_rctime", "max_file_mtime", "pinned_at",
           "start", "end", "run"} <= set(r) for r in rows))
by_artifact = {r["artifact"]: r for r in rows}
check("a written artifact serializes pinned_at as a number",
      isinstance(by_artifact[artA]["pinned_at"], float))
check("a read-only artifact serializes pinned_at as null",
      by_artifact[artB]["pinned_at"] is None)
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
# Cross-repo format contract. The consumer is a retention policy script in a
# separate private repo (jenkins/scripts/retention_path_policy.py,
# transcode_pinned_rctime), which
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
# The consumer only recovers for an artifact this run actually WROTE to, so the
# contract line -- which is what the infra fixture is copied from -- has to be
# the shape produced by a real replace, not by a read-only visit.
_j.note_write(_data)


class _CArgs:
    min_size = 131072


_j.write([_contract_dir], _CArgs())
_line = open(os.path.join(_contract_dir, tc.RUN_JOURNAL_NAME)).readline()
_row = json.loads(_line)

check("journal filename is the name the consumer looks for",
      tc.RUN_JOURNAL_NAME == ".vcephfs-transcode-runs.jsonl")
check("emits every key the consumer reads",
      {"artifact", "start", "end", "pre_rctime", "max_file_mtime",
       "pinned_at"} <= set(_row))
check("pinned_at is the numeric per-write timestamp the consumer matches on",
      isinstance(_row["pinned_at"], float)
      and _row["start"] <= _row["pinned_at"] <= _row["end"])
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
print("\n-- the contract line, copied verbatim into the infra fixture --")
print(_line.rstrip("\n").replace(_art, "@ARTIFACT@"))

check("artifact key matches the directory as walked",
      _row["artifact"] == _art)

# ---------------------------------------------------------------------------
# Durability. write() needs Python to unwind, so SIGKILL, the OOM killer or a
# power loss used to lose the whole run's records. The checkpoint survives.
print("\ncheckpoint survives a kill:")


def _wait_for(pred, timeout=5.0):
    deadline = time.time() + timeout
    while not pred() and time.time() < deadline:
        time.sleep(0.02)
    return pred()


_ck = tempfile.mkdtemp()
_ca = os.path.join(_ck, "art")
touch(os.path.join(_ca, tc.RETENTION_MARKER))
_cf = os.path.join(_ca, "f.parquet")
_cst = touch(_cf, OLD)
tc.read_rctime = lambda p: 1757000000.5
_cj = tc.RunJournal(enabled=True, stop_at=[_ck])
_cj.note_file(_cf, _cst)
_cj.note_write(_cf)
_partial = _cj._partial_path(_ck)
_cjournal = os.path.join(_ck, tc.RUN_JOURNAL_NAME)
_cj.start_checkpoints([_ck], _CArgs(), every_s=0.2, poll_s=0.05)

check("a checkpoint is written with write() never called",
      _wait_for(lambda: os.path.exists(_partial)))
_prow = json.loads(open(_partial).readline())
check("checkpoint rows carry every contract key",
      {"artifact", "start", "end", "pre_rctime", "max_file_mtime",
       "pinned_at"} <= set(_prow))
check("checkpoint rows are marked partial", _prow.get("partial") is True)
check("checkpoint keeps the pre-transcode rctime",
      _prow["pre_rctime"] == 1757000000.5)
check("checkpoint keeps the pin", isinstance(_prow["pinned_at"], float))
check("no temp file is left beside it", not os.path.exists(_partial + ".tmp"))
check("the shared journal is untouched until write()",
      not os.path.exists(_cjournal))

_m = os.stat(_partial).st_mtime_ns
time.sleep(0.5)
check("an unchanged run is not rewritten", os.stat(_partial).st_mtime_ns == _m)

_cj.write([_ck], _CArgs())
_frows = [json.loads(x) for x in open(_cjournal)]
check("write() appends the real rows",
      len(_frows) == 1 and "partial" not in _frows[0])
check("write() removes the checkpoint", not os.path.exists(_partial))
_cj.note_write(_cf)
time.sleep(0.4)
check("no checkpoint lands after write()", not os.path.exists(_partial))

# Record changes trigger too, well before a long interval.
_ck2 = tempfile.mkdtemp()
touch(os.path.join(_ck2, "art", tc.RETENTION_MARKER))
_cf2 = os.path.join(_ck2, "art", "f.parquet")
_cst2 = touch(_cf2, OLD)
_cj2 = tc.RunJournal(enabled=True, stop_at=[_ck2])
_cj2.start_checkpoints([_ck2], _CArgs(), every_s=3600, every_changes=2,
                       poll_s=0.05)
time.sleep(0.3)
check("nothing to record, nothing written",
      not os.path.exists(_cj2._partial_path(_ck2)))
_cj2.note_file(_cf2, _cst2)
_cj2.note_write(_cf2)
check("enough record changes checkpoint before the interval",
      _wait_for(lambda: os.path.exists(_cj2._partial_path(_ck2))))

# A final write that fails leaves the checkpoint as the only record.
os.makedirs(os.path.join(_ck2, tc.RUN_JOURNAL_NAME))
_cj2.write([_ck2], _CArgs())
check("a failed final write keeps the checkpoint",
      os.path.exists(_cj2._partial_path(_ck2)))

# A checkpoint that fails is retried, not forgotten until the next change.
_ck3 = tempfile.mkdtemp()
touch(os.path.join(_ck3, "art", tc.RETENTION_MARKER))
_cf3 = os.path.join(_ck3, "art", "f.parquet")
_cj3 = tc.RunJournal(enabled=True, stop_at=[_ck3])
_cj3.note_file(_cf3, touch(_cf3, OLD))
os.makedirs(_cj3._partial_path(_ck3) + ".tmp")        # open(tmp, "w") fails
_cj3.checkpoint([_ck3], _CArgs())
check("a failed checkpoint stays due", _cj3._changes > 0)


def _art_journal():
    vroot = tempfile.mkdtemp()
    arts = []
    for name in ("a1", "a2"):
        touch(os.path.join(vroot, name, tc.RETENTION_MARKER))
        f = os.path.join(vroot, name, "f.parquet")
        arts.append((f, touch(f, OLD)))
    return vroot, arts, tc.RunJournal(enabled=True, stop_at=[vroot])


# A checkpoint cut short (ENOSPC after open) drops its temp.
_ck4, _a4, _cj4 = _art_journal()
_cj4.note_file(*_a4[0])
_real_fsync = tc.os.fsync


def _enospc(fd):
    raise OSError(28, "No space left on device")


tc.os.fsync = _enospc
try:
    _cj4.checkpoint([_ck4], _CArgs())
finally:
    tc.os.fsync = _real_fsync
check("a checkpoint cut short leaves no temp",
      not os.path.exists(_cj4._partial_path(_ck4) + ".tmp"))
check("... and stays due", _cj4._changes > 0)

# An artifact's first pin checkpoints in seconds, not at the next interval:
# until then its pre_rctime is in memory only.
print("\nfirst pin checkpoints promptly:")
_ck5, _a5, _cj5 = _art_journal()
_p5 = _cj5._partial_path(_ck5)
_cj5.start_checkpoints([_ck5], _CArgs(), every_s=3600, every_changes=10 ** 6,
                       poll_s=3600, pin_debounce_s=0.1)
_cj5.note_file(*_a5[0])
time.sleep(0.4)
check("a first sighting alone waits for the interval", not os.path.exists(_p5))
_cj5.note_write(_a5[0][0])
check("a first pin checkpoints within the debounce",
      _wait_for(lambda: os.path.exists(_p5), timeout=3.0))
_m5 = os.stat(_p5).st_mtime_ns
_cj5.note_write(_a5[0][0])
time.sleep(0.4)
check("a later pin of the same artifact waits for the interval",
      os.stat(_p5).st_mtime_ns == _m5)
_cj5.write([_ck5], _CArgs())

# ... but not back to back when a checkpoint is expensive.
_ck6, _a6, _cj6 = _art_journal()
_cj6._ckpt_cost = 100.0
_cj6.start_checkpoints([_ck6], _CArgs(), every_s=3600, every_changes=10 ** 6,
                       poll_s=3600, pin_debounce_s=0.05)
_cj6.note_file(*_a6[0])
_cj6.note_write(_a6[0][0])
time.sleep(0.5)
check("pin checkpoints are spaced by the last one's cost",
      not os.path.exists(_cj6._partial_path(_ck6)))
_cj6._stop.set()
_cj6._wake.set()

print("\nfinal write durability:")
# The append is fsynced before the checkpoint is unlinked.
_ck7, _a7, _cj7 = _art_journal()
_cj7.note_file(*_a7[0])
_cj7.note_write(_a7[0][0])
_cj7.checkpoint([_ck7], _CArgs())
_events = []
_real_unlink = tc.os.unlink
tc.os.fsync = lambda fd: (_events.append("fsync"), _real_fsync(fd))[1]
tc.os.unlink = lambda p: (_events.append("unlink " + p), _real_unlink(p))[1]
try:
    _cj7.write([_ck7], _CArgs())
finally:
    tc.os.fsync, tc.os.unlink = _real_fsync, _real_unlink
check("the journal is fsynced before its checkpoint is removed",
      _events == ["fsync", "unlink " + _cj7._partial_path(_ck7)])

# A failed append brings the checkpoint up to date: the periodic one misses
# every artifact since, and a short run never wrote one at all.
_ck8, _a8, _cj8 = _art_journal()
_cj8.note_file(*_a8[0])
_cj8.note_write(_a8[0][0])
_cj8.checkpoint([_ck8], _CArgs())
_cj8.note_file(*_a8[1])
_cj8.note_write(_a8[1][0])
os.makedirs(os.path.join(_ck8, tc.RUN_JOURNAL_NAME))
_cj8.write([_ck8], _CArgs())
_rows8 = [json.loads(x) for x in open(_cj8._partial_path(_ck8))]
check("a failed final write leaves the CURRENT rows in the checkpoint",
      len(_rows8) == 2 and all(r.get("partial") is True for r in _rows8))
_ck9, _a9, _cj9 = _art_journal()
_cj9.note_file(*_a9[0])
os.makedirs(os.path.join(_ck9, tc.RUN_JOURNAL_NAME))
_cj9.write([_ck9], _CArgs())
check("... including a run that never checkpointed",
      os.path.exists(_cj9._partial_path(_ck9)))

check("the walker skips the journal, every checkpoint and their temps",
      all(tc._is_journal_file(n) for n in (
          tc.RUN_JOURNAL_NAME, os.path.basename(_partial),
          os.path.basename(_partial) + ".tmp"))
      and not tc._is_journal_file("f.parquet"))


print("\n" + ("ALL PASS" if not fails else f"{len(fails)} FAILURES: {fails}"))
sys.exit(1 if fails else 0)
