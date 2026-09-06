#!/usr/bin/env python3
"""Unit tests for vcephfs_transcoder.py. Run: python3 test_vcephfs_transcoder.py

Covers the pure, decision-making parts added or changed by the copy_file_range /
replace-lock / subtree-pruning work. These are the places where a silent wrong
answer is expensive: a default that flips back, a lock that stops excluding, a
prune that skips more data than intended, or a warning that stops being true.
"""
import argparse
import importlib.util
import os
import sys
import threading
import time
import unittest

# The script under test lives at the repository root.
HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "..", "vcephfs_transcoder.py")
spec = importlib.util.spec_from_file_location("vct", os.path.abspath(TARGET))
vct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vct)


class Args:
    """Stand-in for the argparse namespace where only a few fields matter."""

    def __init__(self, **kw):
        self.prune_small_subtrees = False
        self.min_size = 0
        self.prune_subtree_max_bytes = 1 << 30
        self.prune_budget_bytes = 100 << 30
        self.dirs = []
        self.regulate_prometheus_url = None
        self.regulate_query = None
        self.regulate_pause_ms = 150.0
        self.regulate_slo_ms = 75.0
        self.regulate_period_s = 30
        self.regulate_floor_ms = 0
        self.regulate_quiet_ticks = 10
        self.__dict__.update(kw)


class CopyFileRangeDefault(unittest.TestCase):
    """The default is off. Rebuild the same flag pair main() uses.

    A regression here is silent: the tool keeps working and simply gets slower on
    small files, which is the whole reason the default was flipped.
    """

    def parser(self):
        p = argparse.ArgumentParser()
        p.add_argument("--no-copy-file-range", dest="no_copy_file_range",
                       action="store_true", default=True)
        p.add_argument("--copy-file-range", dest="no_copy_file_range",
                       action="store_false")
        return p

    def test_no_flag_disables(self):
        self.assertTrue(self.parser().parse_args([]).no_copy_file_range)

    def test_explicit_disable(self):
        self.assertTrue(
            self.parser().parse_args(["--no-copy-file-range"]).no_copy_file_range)

    def test_opt_back_in(self):
        self.assertFalse(
            self.parser().parse_args(["--copy-file-range"]).no_copy_file_range)

    def test_real_parser_agrees(self):
        """Guard against the module's own flags drifting from the pair above."""
        with open(os.path.abspath(TARGET)) as fh:
            src = fh.read()
        self.assertIn('"--no-copy-file-range"', src)
        self.assertIn('"--copy-file-range"', src)
        self.assertIn("default=True", src)


class ReplaceLockStriping(unittest.TestCase):
    """Per-path exclusion must survive; unrelated paths must not serialize."""

    def test_same_path_same_lock(self):
        a = vct._replace_lock_for("/vol/a/b/c.gz")
        b = vct._replace_lock_for("/vol/a/b/c.gz")
        self.assertIs(a, b)

    def test_same_path_mutually_excludes(self):
        order = []

        def worker(tag):
            with vct._replace_lock_for("/same/file"):
                order.append(("in", tag))
                time.sleep(0.05)
                order.append(("out", tag))

        t1 = threading.Thread(target=worker, args=("A",))
        t2 = threading.Thread(target=worker, args=("B",))
        t1.start()
        time.sleep(0.01)
        t2.start()
        t1.join()
        t2.join()
        # Critical sections must not interleave: in/out of one tag, then the other.
        self.assertEqual(order[0][1], order[1][1], order)

    def test_distinct_paths_can_run_concurrently(self):
        base = "/same/file"
        other = next(
            (f"/other/{i}" for i in range(1000)
             if vct._replace_lock_for(f"/other/{i}") is not vct._replace_lock_for(base)),
            None,
        )
        self.assertIsNotNone(other, "no path hashed to a different stripe")
        order = []

        def worker(tag, path):
            with vct._replace_lock_for(path):
                order.append(("in", tag))
                time.sleep(0.05)
                order.append(("out", tag))

        t1 = threading.Thread(target=worker, args=("A", base))
        t2 = threading.Thread(target=worker, args=("B", other))
        t1.start()
        time.sleep(0.01)
        t2.start()
        t1.join()
        t2.join()
        self.assertNotEqual(order[0][1], order[1][1], order)

    def test_striping_spreads(self):
        seen = {id(vct._replace_lock_for(f"/p/{i}")) for i in range(2000)}
        # hash() is PYTHONHASHSEED-randomized, so the mapping is per-run; only the
        # spread is asserted, not which stripe any path lands on.
        self.assertGreater(len(seen), 8)


class PruneInertWarning(unittest.TestCase):
    """min_size is runtime-mutable, so this is re-checked, not just startup."""

    def test_warns_when_inert(self):
        self.assertIsNotNone(
            vct._prune_inert_warning(Args(prune_small_subtrees=True, min_size=0)))

    def test_silent_when_active(self):
        self.assertIsNone(
            vct._prune_inert_warning(Args(prune_small_subtrees=True, min_size=131072)))

    def test_silent_when_pruning_off(self):
        self.assertIsNone(
            vct._prune_inert_warning(Args(prune_small_subtrees=False, min_size=0)))


class PruneBudget(unittest.TestCase):
    """The budget bounds aggregate loss; the per-subtree ceiling does not."""

    def test_budget_stops_pruning(self):
        st = vct.Stats()
        budget, each, taken = 1000, 200, 0
        for _ in range(20):
            if st.prune_budget_left(budget) > each:
                st.note_pruned_subtree(each)
                taken += 1
        self.assertLessEqual(st.bytes_pruned, budget)
        self.assertLess(taken, 20, "budget never engaged")

    def test_without_the_guard_it_overruns(self):
        """Control: proves the assertion above is not vacuous."""
        st = vct.Stats()
        for _ in range(20):
            st.note_pruned_subtree(200)
        self.assertGreater(st.bytes_pruned, 1000)

    def test_counters_track(self):
        st = vct.Stats()
        st.note_pruned_subtree(512)
        st.note_pruned_subtree(512)
        self.assertEqual(st.subtrees_pruned, 2)
        self.assertEqual(st.bytes_pruned, 1024)


class RuntimeConfigKeys(unittest.TestCase):
    def test_prune_keys_present(self):
        for k in ("prune_subtree_max_bytes", "prune_budget_bytes"):
            self.assertIn(k, vct.RuntimeConfig.KEYS)

    def test_prune_keys_parse(self):
        out, errs = vct.RuntimeConfig._parse(
            "prune_subtree_max_bytes = 2147483648\nprune_budget_bytes = 0\n")
        self.assertEqual(errs, [])
        self.assertEqual(out["prune_subtree_max_bytes"], 2147483648)
        self.assertEqual(out["prune_budget_bytes"], 0)

    def test_negative_rejected(self):
        _, errs = vct.RuntimeConfig._parse("prune_budget_bytes = -1\n")
        self.assertTrue(errs)

    def test_unknown_key_rejected(self):
        """A typo must not be silently ignored, or a tuning edit does nothing."""
        _, errs = vct.RuntimeConfig._parse("prune_bugdet_bytes = 5\n")
        self.assertTrue(errs)


class HelpRenders(unittest.TestCase):
    """--help must not crash.

    argparse runs the epilog through `text % dict(prog=...)`. The epilog embeds
    config_example(), so a single literal % in a config comment ("10% increments")
    is read as a format spec and raises TypeError. That shipped once; this is the
    guard.
    """

    def test_help_does_not_raise(self):
        import subprocess
        r = subprocess.run([sys.executable, os.path.abspath(TARGET), "--help"],
                           capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr[-500:])
        self.assertNotIn("Traceback", r.stderr)
        self.assertIn("runtime signals:", r.stdout)

    def test_percent_survives_into_help(self):
        """A literal % from the config example must render as one %, not vanish."""
        import subprocess
        r = subprocess.run([sys.executable, os.path.abspath(TARGET), "--help"],
                           capture_output=True, text=True)
        self.assertIn("10% increments", r.stdout)

    def test_new_flags_documented(self):
        import subprocess
        r = subprocess.run([sys.executable, os.path.abspath(TARGET), "--help"],
                           capture_output=True, text=True)
        for flag in ("--copy-file-range", "--source-pool",
                     "--prune-small-subtrees", "--prune-subtree-max-bytes",
                     "--prune-budget-bytes"):
            self.assertIn(flag, r.stdout, f"{flag} missing from --help")


class RegulatorConfig(unittest.TestCase):
    """The disabled paths matter most: anyone without Prometheus must be fine."""

    def test_keys_registered(self):
        for k in ("regulate_prometheus_url", "regulate_query", "regulate_pause_ms",
                  "regulate_slo_ms", "regulate_period_s", "regulate_quiet_ticks"):
            self.assertIn(k, vct.RuntimeConfig.KEYS)

    def test_query_with_equals_and_braces_parses(self):
        """A PromQL expression is full of '=' and '{'; the parser splits on the
        FIRST '=' only, so this must survive intact."""
        q = ('1e3 * sum(increase(m_sum{a="b",c=~"d\\.e\\..*"}[1m]))'
             ' / sum(increase(m_count{a="b"}[1m]))')
        out, errs = vct.RuntimeConfig._parse("regulate_query = " + q + "\n")
        self.assertEqual(errs, [])
        self.assertEqual(out["regulate_query"], q)

    def test_bad_thresholds_rejected(self):
        for line in ("regulate_pause_ms = 0\n", "regulate_pause_ms = -5\n",
                     "regulate_period_s = 0\n"):
            _, errs = vct.RuntimeConfig._parse(line)
            self.assertTrue(errs, f"accepted {line!r}")

    def test_empty_url_means_none(self):
        out, errs = vct.RuntimeConfig._parse("regulate_prometheus_url =\n")
        self.assertEqual(errs, [])
        self.assertIsNone(out["regulate_prometheus_url"])


class RegulatorVolume(unittest.TestCase):
    def test_query_without_placeholder_passes_through(self):
        a = Args(regulate_query="up", dirs=["/nowhere"])
        q, why = vct._resolve_query(a)
        self.assertEqual(q, "up")
        self.assertIsNone(why)

    def test_no_query_is_disabled_not_crash(self):
        a = Args(regulate_query=None, dirs=["/nowhere"])
        q, why = vct._resolve_query(a)
        self.assertIsNone(q)
        self.assertIn("no regulate_query", why)

    def test_placeholder_without_resolvable_volume_disables(self):
        """Must refuse rather than guess -- querying the wrong filesystem would
        regulate against a volume this job is not touching, silently."""
        a = Args(regulate_query="x{volume}y", dirs=["/definitely/not/a/ceph/mount"])
        q, why = vct._resolve_query(a)
        self.assertIsNone(q)
        self.assertIn("{volume}", why)

    def test_volume_is_escaped_for_regex_and_for_promql(self):
        """A dot in the name must not widen the match, and the backslash that
        stops it has to survive PromQL's string layer as well.

        re.escape() alone gives a\\.b, and inside a PromQL double-quoted string
        that is "unknown escape sequence U+002E" -- an HTTP 400 at every poll,
        so the regulator silently never comes up.
        """
        real = vct._mds_namespace_for
        vct._mds_namespace_for = lambda d: "a.b"
        try:
            a = Args(regulate_query='m{d=~"mds\\.{volume}\\..*"}', dirs=["/x"])
            q, why = vct._resolve_query(a)
        finally:
            vct._mds_namespace_for = real
        self.assertIsNone(why)
        self.assertIn(r"a\\.b", q)
        # and not the single-backslash form, which is the one that 400s
        self.assertNotIn(r"a\.b", q.replace(r"a\\.b", "<v>"))

    def test_plain_volume_name_is_untouched(self):
        real = vct._mds_namespace_for
        vct._mds_namespace_for = lambda d: "myvol"
        try:
            a = Args(regulate_query="x{volume}y", dirs=["/x"])
            q, why = vct._resolve_query(a)
        finally:
            vct._mds_namespace_for = real
        self.assertEqual(q, "xmyvoly")

    def test_dirs_on_different_volumes_disables(self):
        real = vct._mds_namespace_for
        seq = iter(["one", "two"])
        vct._mds_namespace_for = lambda d: next(seq)
        try:
            a = Args(regulate_query="{volume}", dirs=["/a", "/b"])
            q, why = vct._resolve_query(a)
        finally:
            vct._mds_namespace_for = real
        self.assertIsNone(q)
        self.assertIn("exactly one", why)


class RegulatorAutofs(unittest.TestCase):
    """Mount-point name != filesystem name, and the mount may not exist yet."""

    def test_reads_mds_namespace_not_the_path(self):
        """On the real fleet, several distinct mount points can live on one
        filesystem, and a mount point can differ from its filesystem name. Deriving the
        name from the path would query a filesystem that does not exist."""
        src = open(os.path.abspath(TARGET)).read()
        self.assertIn("mds_namespace=", src)
        # must not fall back to basename-of-path anywhere in the resolver
        fn = src.split("def _mds_namespace_for")[1].split("\ndef ")[0]
        self.assertNotIn("basename", fn)
        self.assertIn("/proc/self/mounts", fn)

    def test_triggers_the_automount_before_reading(self):
        """A bare stat is not enough -- autofs answers a stat of the mount point
        without mounting. Verified on the fleet: stat returned an inode and the
        ceph mount still did not appear."""
        src = open(os.path.abspath(TARGET)).read()
        fn = src.split("def _mds_namespace_for")[1].split("\ndef ")[0]
        self.assertIn("scandir", fn)

    def test_unmountable_path_returns_none(self):
        self.assertIsNone(vct._mds_namespace_for("/definitely/not/here/at/all"))


class RegulatorBehavior(unittest.TestCase):
    """Drive the decision logic with a stubbed sample()."""

    def _reg(self, floor=20):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=75.0,
                 regulate_period_s=30, regulate_floor_ms=floor,
                 regulate_quiet_ticks=3, dirs=["/x"])
        return vct.Regulator(a, "q")

    def test_floor_ratchets_then_releases(self):
        r = self._reg(floor=20)
        for _ in range(12):
            r._note_pause()
        ratcheted = r.floor_ms
        self.assertGreater(ratcheted, 20, "ratchet did not fire")
        r._pauses = []
        for _ in range(8):
            r._last_decay = time.time() - vct.REG_FLOOR_DECAY_S - 1
            r._maybe_decay_floor()
        self.assertEqual(r.floor_ms, 20, "floor never returned to baseline")

    def test_floor_never_below_baseline(self):
        r = self._reg(floor=20)
        for _ in range(12):
            r._note_pause()
        for _ in range(20):
            r._pauses = []
            r._last_decay = time.time() - 99999
            r._maybe_decay_floor()
        self.assertEqual(r.floor_ms, 20)

    def test_control_ratchet_alone_does_not_return(self):
        """Without the decay the floor stays up -- proves the release does work."""
        r = self._reg(floor=20)
        for _ in range(12):
            r._note_pause()
        self.assertGreater(r.floor_ms, 20)

    def test_bad_samples_raise_rather_than_mislead(self):
        r = self._reg()
        for payload in ('{"status":"error"}',
                        '{"status":"success","data":{"result":[]}}',
                        '{"status":"success","data":{"result":[{"value":[0,"1"]},'
                        '{"value":[0,"2"]}]}}',
                        '{"status":"success","data":{"result":[{"value":[0,"-1"]}]}}',
                        '{"status":"success","data":{"result":[{"value":[0,"NaN"]}]}}'):
            with self.subTest(payload=payload[:40]):
                r_ = self._reg()
                r_._payload = payload
                import io, json as _j
                class _R:
                    def __init__(self, t): self.t = t
                    def read(self): return self.t.encode()
                    def __enter__(self): return self
                    def __exit__(self, *a): return False
                real = vct.urllib.request.urlopen
                vct.urllib.request.urlopen = lambda *a, **k: _R(payload)
                try:
                    with self.assertRaises(Exception):
                        r_.sample()
                finally:
                    vct.urllib.request.urlopen = real


class RegulateConfigRouting(unittest.TestCase):
    """A regulate_* key that parses but is never copied onto args is invisible.

    This is the bug these tests exist for: regulate_prometheus_url and
    regulate_query validated cleanly and were then discarded, so a fully
    configured job still logged "Self-regulation disabled (no
    regulate_prometheus_url)" and ran unthrottled. Nothing failed; the feature
    simply was not there.
    """

    @staticmethod
    def _routed():
        return {k for k, _ in vct.REGULATE_APPLY}

    @staticmethod
    def _declared():
        return {k for k in vct.RuntimeConfig.KEYS if k.startswith("regulate_")}

    def test_every_declared_key_is_routed(self):
        missing = self._declared() - self._routed()
        self.assertEqual(missing, set(),
                         "accepted from the config file but never applied: %s"
                         % sorted(missing))

    def test_every_routed_key_is_declared(self):
        """The other direction: routing a key the parser rejects is dead code."""
        extra = self._routed() - self._declared()
        self.assertEqual(extra, set(),
                         "applied but not accepted from the config file: %s"
                         % sorted(extra))

    def test_the_invariant_catches_a_dropped_key(self):
        """Control. Remove a key from the routing table and the check must fail,
        otherwise the two tests above would pass against any table at all."""
        crippled = tuple(e for e in vct.REGULATE_APPLY
                         if e[0] != "regulate_prometheus_url")
        routed = {k for k, _ in crippled}
        self.assertTrue(self._declared() - routed,
                        "invariant did not notice a missing key")

    def test_routing_lands_the_url_and_query_on_args(self):
        """Walk the same table _apply_config walks, against a real parse."""
        text = ("regulate_prometheus_url = http://prom.example/api/v1/query\n"
                'regulate_query = 1e3 * avg(x{a="b"})\n'
                "regulate_pause_ms = 200\n"
                "regulate_floor_ms = 25\n")
        cfg, errs = vct.RuntimeConfig._parse(text)
        self.assertEqual(errs, [])
        a = Args()
        for k, _label in vct.REGULATE_APPLY:
            if k in cfg:
                setattr(a, k, cfg[k])
        self.assertEqual(a.regulate_prometheus_url,
                         "http://prom.example/api/v1/query")
        self.assertEqual(a.regulate_query, '1e3 * avg(x{a="b"})')
        self.assertEqual(a.regulate_pause_ms, 200.0)
        self.assertEqual(a.regulate_floor_ms, 25)

    def test_floor_ms_accepted_and_range_checked(self):
        cfg, errs = vct.RuntimeConfig._parse("regulate_floor_ms = 20\n")
        self.assertEqual(errs, [])
        self.assertEqual(cfg["regulate_floor_ms"], 20)
        for bad in ("regulate_floor_ms = -1\n",
                    "regulate_floor_ms = %d\n" % (vct.DELAY_MAX_MS + 1)):
            _, errs = vct.RuntimeConfig._parse(bad)
            self.assertTrue(errs, "accepted %r" % bad)

    def test_set_floor_base_moves_the_baseline(self):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_floor_ms=20, dirs=["/x"])
        r = vct.Regulator(a, "q")
        self.assertEqual(r._floor_base, 20)
        r.set_floor_base(50)
        self.assertEqual(r._floor_base, 50)
        self.assertGreaterEqual(r.floor_ms, 50,
                                "floor left sitting below its own baseline")

    def test_example_config_documents_every_key(self):
        """--help prints this; a key absent from it is undiscoverable."""
        ex = vct.config_example("vol")
        for k in self._declared():
            self.assertIn(k, ex, "%s missing from the example config" % k)


class PathsFromList(unittest.TestCase):
    """--paths-from replaces the walk, so its reader and its containment check
    are the only things standing between a stale or wrong list and the data."""

    def _write(self, data):
        import tempfile
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        self.addCleanup(os.unlink, path)
        return path

    def test_newline_delimited(self):
        p = self._write(b"/a/one\n/a/two\n/a/three\n")
        self.assertEqual(list(vct._iter_listed_paths(p)),
                         ["/a/one", "/a/two", "/a/three"])

    def test_nul_delimited_detected(self):
        """A list from find -print0 must not be read as one enormous path."""
        p = self._write(b"/a/one\x00/a/two\x00")
        self.assertEqual(list(vct._iter_listed_paths(p)), ["/a/one", "/a/two"])

    def test_path_containing_newline_survives_nul_mode(self):
        p = self._write(b"/a/we\nird\x00/a/two\x00")
        self.assertEqual(list(vct._iter_listed_paths(p)), ["/a/we\nird", "/a/two"])

    def test_blank_lines_and_crlf(self):
        p = self._write(b"/a/one\r\n\n/a/two\n\n")
        self.assertEqual(list(vct._iter_listed_paths(p)), ["/a/one", "/a/two"])

    def test_no_trailing_separator(self):
        p = self._write(b"/a/one\n/a/last")
        self.assertEqual(list(vct._iter_listed_paths(p)), ["/a/one", "/a/last"])

    def test_entry_spanning_the_read_boundary(self):
        """Reads are chunked, so an entry straddling a chunk edge is the case
        that silently truncates paths if the buffering is wrong."""
        names = ["/vol/%06d/%s" % (i, "x" * 90) for i in range(4000)]
        p = self._write(("\n".join(names) + "\n").encode())
        got = list(vct._iter_listed_paths(p))
        self.assertEqual(got, names)
        self.assertTrue(len(("\n".join(names)).encode()) > (1 << 16),
                        "test data too small to cross a chunk boundary")

    def test_undecodable_bytes_round_trip(self):
        """A path the filesystem accepts need not be valid UTF-8; it must still
        be usable, not dropped."""
        p = self._write(b"/a/bad\xff\n")
        got = list(vct._iter_listed_paths(p))
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0].encode("utf-8", "surrogateescape"), b"/a/bad\xff")

    def test_under_roots(self):
        roots = ["/vol/ab"]
        self.assertTrue(vct._under_roots("/vol/ab", roots))
        self.assertTrue(vct._under_roots("/vol/ab/x/y", roots))
        self.assertFalse(vct._under_roots("/vol/abc", roots),
                         "sibling sharing a name prefix must not be included")
        self.assertFalse(vct._under_roots("/other/ab/x", roots))

    def test_under_roots_trailing_slash(self):
        self.assertTrue(vct._under_roots("/vol/ab/x", ["/vol/ab/"]))

    def test_naive_prefix_would_be_wrong(self):
        """Control: the bug the helper exists to prevent. If _under_roots ever
        degrades to a bare startswith, this documents what breaks."""
        self.assertTrue("/vol/abc".startswith("/vol/ab"))
        self.assertFalse(vct._under_roots("/vol/abc", ["/vol/ab"]))


class Crossover(unittest.TestCase):
    """Generalized source scheme: comparing against 3x replication only is wrong."""

    def test_alloc_replicated(self):
        self.assertEqual(vct.alloc_bytes(16384, ("rep", 3), 4096), 49152)
        self.assertEqual(vct.alloc_bytes(16384, ("rep", 2), 4096), 32768)

    def test_alloc_ec_pads_to_stripe_rows_then_min_alloc(self):
        # 4+2, su 4k: one stripe row of 16 KiB, 6 shards of 4 KiB
        self.assertEqual(vct.alloc_bytes(16384, ("ec", 4, 2, 4096), 4096), 24576)
        # same file on 16k media: each shard rounds to 16 KiB
        self.assertEqual(vct.alloc_bytes(16384, ("ec", 4, 2, 4096), 16384), 98304)

    def test_effective_granularity_is_max_of_su_and_min_alloc(self):
        a = vct.alloc_bytes(16384, ("ec", 4, 2, 4096), 16384)
        b = vct.alloc_bytes(16384, ("ec", 4, 2, 16384), 16384)
        c = vct.alloc_bytes(16384, ("ec", 4, 2, 16384), 4096)
        self.assertEqual(a, b)
        self.assertEqual(b, c)

    def test_crossover_depends_on_the_source_scheme(self):
        ec63 = ("ec", 6, 3, 4096)
        self.assertEqual(vct.size_crossover(("rep", 3), ec63, 4096), 16384)
        self.assertEqual(vct.size_crossover(("rep", 2), ec63, 4096), 20480)

    def test_ec_beats_r3_but_loses_to_r2_in_the_gap(self):
        """The case an R3-only formula gets wrong."""
        for size in (16384, 32768):
            ec = vct.alloc_bytes(size, ("ec", 6, 3, 4096), 4096)
            self.assertLess(ec, vct.alloc_bytes(size, ("rep", 3), 4096))
            self.assertGreater(ec, vct.alloc_bytes(size, ("rep", 2), 4096))

    def test_min_alloc_moves_the_crossover(self):
        ec42 = ("ec", 4, 2, 4096)
        self.assertEqual(vct.size_crossover(("rep", 3), ec42, 4096), 12288)
        self.assertEqual(vct.size_crossover(("rep", 3), ec42, 16384), 36864)

    def test_no_crossover_when_target_is_the_source(self):
        ec42 = ("ec", 4, 2, 4096)
        self.assertIsNone(vct.size_crossover(ec42, ec42, 4096))

    def test_probe_degrades_without_ceph(self):
        """Must never be load-bearing: no ceph CLI -> None, not an exception."""
        self.assertIsNone(vct._pool_scheme("definitely-not-a-pool-xyzzy"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
