#!/usr/bin/env python3
"""Unit tests for vcephfs_transcoder.py. Run: python3 test_vcephfs_transcoder.py

Covers the pure, decision-making parts added or changed by the copy_file_range /
replace-lock / subtree-pruning work. These are the places where a silent wrong
answer is expensive: a default that flips back, a lock that stops excluding, a
prune that skips more data than intended, or a warning that stops being true.
"""
import argparse
import errno
import logging
import signal
import importlib.util
import os
import sys
import shlex
import shutil
import threading
import time
import tempfile
import types
import unittest
import unittest.mock

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
        self.regulate_max_threads = 0
        self.threads = 1
        self.__dict__.update(kw)


class CopyFileRangeDefault(unittest.TestCase):
    """The default is off. Rebuild the same flag pair main() uses.

    A regression here is silent: the tool keeps working and simply gets slower on
    small files, which is the whole reason for the default.
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
        a = vct._replace_lock_for("/shared/ceph/myvol/a/b/c.gz")
        b = vct._replace_lock_for("/shared/ceph/myvol/a/b/c.gz")
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
                     "--prune-budget-bytes", "--no-regulate"):
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
        """On the real fleet, mount points feeds2/pits/feeds3 all live on the
        filesystem named 'feeds', and eartheq lives on 'eartheq2'. Deriving the
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


class RegulatorDisableIsAudible(unittest.TestCase):
    """An omitted --config must not be the quietest way to run unregulated.

    start_regulator declines to start in four places, and all four log at
    WARNING. The no-URL path is the one an omitted --config lands on, and by
    far the likeliest of the four; at INFO it would be indistinguishable from
    a normal start in a multi-gigabyte log. Reverting the level to INFO must
    fail these tests, or they are not testing anything.

    --no-regulate also has to DISABLE regulation, not merely silence the
    message: a flag read only when no URL is configured would be ignored by
    every job that has one, which is the same quiet surprise in a new place.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2

    def tearDown(self):
        vct.thread_count, vct.file_delay_ms = self._saved

    def test_omitted_config_warns(self):
        a = Args(regulate_prometheus_url=None, no_regulate=False, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertTrue(
            any("no regulate_prometheus_url" in m for m in got.output),
            "an omitted --config did not warn: %r" % (got.output,))

    def test_explicit_opt_out_does_not_warn(self):
        a = Args(regulate_prometheus_url=None, no_regulate=True, dirs=["/x"])
        with self.assertLogs(level="INFO") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertFalse(
            [r for r in got.records if r.levelno >= logging.WARNING],
            "--no-regulate is deliberate and must stay quiet: %r" % (got.output,))
        self.assertTrue(any("--no-regulate" in m for m in got.output),
                        "opt-out should name itself: %r" % (got.output,))

    def test_missing_attr_behaves_as_not_opted_out(self):
        """An older namespace with no no_regulate attribute must still warn."""
        a = Args(regulate_prometheus_url=None, dirs=["/x"])
        self.assertFalse(hasattr(a, "no_regulate"),
                         "Args grew a no_regulate default; this test no longer "
                         "covers the missing-attribute case")
        with self.assertLogs(level="WARNING"):
            self.assertIsNone(vct.start_regulator(a))

    def test_opt_out_overrides_a_configured_url(self):
        """--no-regulate must win over a URL, and say that it is doing so.

        regulate_query is deliberately left unset: if the flag were ignored,
        control would fall through to _resolve_query and warn about the missing
        query instead, so asserting on the message -- not merely on the None
        return -- is what makes this test fail if the ordering regresses. It
        also keeps the fall-through off the network.
        """
        url = "http://prometheus.invalid/api/v1/query"
        a = Args(regulate_prometheus_url=url, no_regulate=True, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertTrue(
            any("--no-regulate" in m and url in m for m in got.output),
            "the flag must override the URL and name it: %r" % (got.output,))

    def test_opt_out_override_does_not_log_url_credentials(self):
        url = "http://bob:hunter2@prometheus.invalid/api/v1/query"
        a = Args(regulate_prometheus_url=url, no_regulate=True, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertFalse(any("hunter2" in m for m in got.output),
                         "credentials logged: %r" % (got.output,))
        self.assertTrue(any("prometheus.invalid" in m for m in got.output),
                        "host should still be named: %r" % (got.output,))

    def test_missing_config_file_is_named_as_missing(self):
        a = Args(regulate_prometheus_url=None, no_regulate=False, dirs=["/x"],
                 config="/nonexistent/vcephfs-transcoder.conf")
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertTrue(any("which does not exist" in m for m in got.output),
                        "a missing --config file was not named: %r" % (got.output,))

    def test_existing_config_without_url_says_so(self):
        import tempfile
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, path)
        a = Args(regulate_prometheus_url=None, no_regulate=False, dirs=["/x"],
                 config=path)
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a))
        self.assertTrue(
            any("has no regulate_prometheus_url" in m and path in m
                for m in got.output),
            "an existing --config without the URL was not called out: %r"
            % (got.output,))


class _StubRegulator:
    """Stands in for a live regulator. Non-None is the whole contract here;
    set_floor_base exists so a stray regulate_floor_ms key cannot AttributeError."""

    def set_floor_base(self, ms):
        self.base = ms


class RegulatorLateConfigChangeIsAudible(unittest.TestCase):
    """A late config change has to be reported by what it actually does.

    Adding the URL to a running job's --config after an unregulated start
    changes nothing, and an INFO line alone would read like success.
    But the reverse error is just as bad: regulate_prometheus_url IS live on a
    running regulator, because sample() rebuilds the URL from args every tick,
    so advising a restart for it sends the operator to do something pointless.
    Only regulate_query is frozen at construction. The startup read, before
    start_regulator() has run, must stay quiet either way."""

    URL = "http://prometheus.invalid/api/v1/query"

    def setUp(self):
        self._saved = (vct.regulator, vct.regulator_started,
                       vct.thread_count, vct.file_delay_ms)
        vct.regulator = None
        # start_regulator() reports these in its decline messages.
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2

    def tearDown(self):
        (vct.regulator, vct.regulator_started,
         vct.thread_count, vct.file_delay_ms) = self._saved

    def test_url_added_after_unregulated_start_warns(self):
        vct.regulator_started = True
        a = Args(regulate_prometheus_url=None, regulate_query="q", dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertEqual(a.regulate_prometheus_url, self.URL)
        self.assertTrue(
            any("started unregulated" in m and "restart the job" in m
                for m in got.output),
            "a late URL after an unregulated start did not warn: %r"
            % (got.output,))

    def test_late_change_names_every_missing_precondition(self):
        """Both missing preconditions, not just the first one found.

        _regulator_decline_reason() stopped at the first reason, so an
        unregulated run missing BOTH the URL and the query reported only the
        URL. The operator set it, reloaded, and only then heard about the
        query -- one round trip per missing precondition.
        """
        vct.regulator_started = True
        a = Args(regulate_prometheus_url=None, regulate_query=None,
                 dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_period_s": 45})
        blob = " ".join(got.output)
        self.assertIn("regulate_prometheus_url", blob,
                      "the missing URL was not named: %r" % (got.output,))
        self.assertIn("regulate_query", blob,
                      "the missing query was not named too: %r" % (got.output,))

    def test_startup_read_does_not_warn(self):
        vct.regulator_started = False
        a = Args(regulate_prometheus_url=None, dirs=["/x"])
        with self.assertLogs(level="INFO") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertFalse(
            [r for r in got.records if r.levelno >= logging.WARNING],
            "the startup config read must not warn: %r" % (got.output,))

    def test_url_change_on_running_regulator_does_not_advise_restart(self):
        """The URL is re-read every sample, so a restart would be pointless."""
        vct.regulator_started = True
        vct.regulator = _StubRegulator()
        a = Args(regulate_prometheus_url="http://old.invalid/q", dirs=["/x"])
        with self.assertLogs(level="INFO") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertEqual(a.regulate_prometheus_url, self.URL)
        self.assertFalse(
            [r for r in got.records if r.levelno >= logging.WARNING],
            "a live URL change must not warn: %r" % (got.output,))
        self.assertFalse(
            any("restart the job" in m for m in got.output),
            "must not advise a restart for a key read every sample: %r"
            % (got.output,))
        self.assertTrue(
            any("no restart needed" in m for m in got.output),
            "should say the running regulator picks it up: %r" % (got.output,))

    def test_query_change_on_running_regulator_advises_restart(self):
        """regulate_query is copied into Regulator.query and never re-read."""
        vct.regulator_started = True
        vct.regulator = _StubRegulator()
        a = Args(regulate_query="old", dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_query": "new"})
        self.assertTrue(
            any("fixed when the regulator starts" in m and "restart" in m
                for m in got.output),
            "a late query change must still advise a restart: %r"
            % (got.output,))

    def test_url_cleared_on_running_regulator_warns(self):
        """Clearing it breaks every sample; that must not pass as routine."""
        vct.regulator_started = True
        vct.regulator = _StubRegulator()
        a = Args(regulate_prometheus_url=self.URL, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": ""})
        self.assertTrue(
            any("cleared under a running regulator" in m for m in got.output),
            "clearing the URL under a running regulator did not warn: %r"
            % (got.output,))

    def test_unregulated_by_flag_says_drop_the_flag(self):
        """A plain restart keeps --no-regulate and so changes nothing."""
        vct.regulator_started = True
        vct.regulator = None
        a = Args(regulate_prometheus_url=None, no_regulate=True, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertTrue(
            any("--no-regulate" in m and "restart without it" in m
                for m in got.output),
            "must say the restart has to drop the flag: %r" % (got.output,))

    def test_cleared_url_sample_names_the_cause(self):
        """sample() must name the cause, not surface a bare NoneType + str TypeError."""
        a = Args(regulate_prometheus_url=self.URL, regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=75.0,
                 regulate_period_s=30, regulate_floor_ms=20,
                 regulate_quiet_ticks=3, dirs=["/x"])
        r = vct.Regulator(a, "q")
        a.regulate_prometheus_url = None
        with self.assertRaises(ValueError) as caught:
            r.sample()
        self.assertIn("is empty", str(caught.exception))

    def test_url_cleared_on_unregulated_run_does_not_advise_restart(self):
        """Clearing a key nothing is using leaves nothing to take effect."""
        vct.regulator_started = True
        vct.regulator = None
        for flag in (False, True):
            a = Args(regulate_prometheus_url=self.URL, no_regulate=flag,
                     dirs=["/x"])
            with self.assertLogs(level="INFO") as got:
                vct._apply_regulate_keys(a, {"regulate_prometheus_url": ""})
            self.assertFalse(
                [r for r in got.records if r.levelno >= logging.WARNING],
                "clearing a key on an unregulated run must not advise a "
                "restart (no_regulate=%s): %r" % (flag, got.output))

    def test_other_key_after_declined_start_warns(self):
        """Fixing slo_ms/pause_ms after the regulator declined changes nothing.

        URL and query are both set, so the regulator declined on slo_ms >=
        pause_ms and a restart with the fix really would start it."""
        vct.regulator_started = True
        vct.regulator = None
        a = Args(regulate_prometheus_url=self.URL, regulate_query="q",
                 regulate_slo_ms=150.0, regulate_pause_ms=150.0, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_slo_ms": 75.0})
        self.assertTrue(
            any("regulate soft target" in m and "started unregulated" in m
                and "restart the job to apply" in m
                for m in got.output),
            "a late slo_ms fix after a declined start did not warn: %r"
            % (got.output,))

    def test_other_key_with_no_url_says_restart_is_not_enough(self):
        """With no URL, a restart comes back unregulated: advise none."""
        vct.regulator_started = True
        vct.regulator = None
        a = Args(regulate_prometheus_url=None, regulate_query="q", dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_slo_ms": 50.0})
        self.assertFalse(
            any("restart the job for this to take effect" in m
                for m in got.output),
            "advised a restart that cannot help: %r" % (got.output,))
        self.assertTrue(
            any("would still decline" in m
                and "no regulate_prometheus_url is set" in m
                for m in got.output),
            "should name the missing URL: %r" % (got.output,))

    def test_url_added_without_query_names_the_query(self):
        """A URL alone is not enough; the query is still missing."""
        vct.regulator_started = True
        vct.regulator = None
        a = Args(regulate_prometheus_url=None, regulate_query=None, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertTrue(
            any("would still decline" in m and "regulate_query" in m
                for m in got.output),
            "should name the missing query: %r" % (got.output,))
        self.assertFalse(
            any("restart the job for this to take effect" in m
                for m in got.output),
            "advised a restart that cannot help: %r" % (got.output,))

    def test_url_credentials_are_not_logged(self):
        """Basic-auth userinfo must never reach the log, stored or not.

        At STARTUP the URL is deliberately stored and start_regulator() does
        the refusing (see test_config_credentialed_url_is_refused_by_startup):
        refusing here as well left args.regulate_prometheus_url None, so
        start_regulator() fell through to its "no regulate_prometheus_url"
        branch and told the operator a --config file that plainly has one does
        not. Either way the credential must not be logged."""
        vct.regulator_started = False
        a = Args(regulate_prometheus_url=None, dirs=["/x"])
        secret = "http://bob:hunter2@prom.invalid:9090/api/v1/query"
        with self.assertLogs(level="INFO") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": secret})
        self.assertFalse(any("hunter2" in m for m in got.output),
                         "password logged: %r" % (got.output,))
        self.assertTrue(any("prom.invalid:9090" in m for m in got.output),
                        "host should still be logged: %r" % (got.output,))

    def test_late_credentialed_url_is_refused(self):
        """Once a regulator is running, a credentialed URL is not stored."""
        vct.regulator_started = True
        vct.regulator = _StubRegulator()
        a = Args(regulate_prometheus_url="http://old.invalid/q", dirs=["/x"])
        secret = "http://bob:hunter2@prom.invalid:9090/api/v1/query"
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": secret})
        self.assertEqual(a.regulate_prometheus_url, "http://old.invalid/q",
                         "a credentialed URL replaced a working one")
        self.assertFalse(any("hunter2" in m for m in got.output),
                         "password logged: %r" % (got.output,))

    def test_config_credentialed_url_is_refused_by_startup(self):
        """A --config URL must reach start_regulator()'s refusal, not its
        "no regulate_prometheus_url" branch.

        poll() runs before process_files(), so refusing at apply time left the
        value None and start_regulator() then reported a file with a URL in it
        as having none -- and the HTTPBasicAuthHandler advice only ever fired
        for a URL given on the command line."""
        vct.regulator_started = False
        vct.regulator = None
        secret = "http://bob:hunter2@prom.invalid:9090/api/v1/query"
        a = Args(regulate_prometheus_url=None, regulate_query="q",
                 dirs=["/x"], config="/nonexistent/tc.conf")
        with self.assertLogs(level="INFO") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": secret})
            self.assertIsNone(vct.start_regulator(a))
        self.assertFalse(
            any("has no regulate_prometheus_url" in m for m in got.output),
            "claimed the config has no URL when it has a credentialed one: %r"
            % (got.output,))
        self.assertTrue(
            any("embedded credentials" in m and "user:password@" in m
                for m in got.output),
            "startup should explain the credential refusal: %r" % (got.output,))
        self.assertFalse(any("hunter2" in m for m in got.output),
                         "password logged: %r" % (got.output,))

    def test_unparseable_credentialed_url_fails_closed(self):
        """urlsplit() raises on some URLs urllib still sends (item A).

        "http://bob:hunter2@[prom/..." -- an unbalanced bracket. urlsplit()
        raises, so deriving credentials from it returned nothing: both
        rejection layers admitted the URL and redaction had nothing to scrub,
        and urllib's InvalidURL("nonnumeric port: 'hunter2@[prom'") put the
        password in the log at WARNING."""
        bad = "http://bob:hunter2@[prom/api/v1/query"
        self.assertTrue(vct._url_has_userinfo(bad),
                        "unparseable credentialed URL not detected")
        self.assertIn("hunter2", vct._url_credentials(bad))
        self.assertNotIn(
            "hunter2",
            vct._redact_text("nonnumeric port: 'hunter2@[prom'", bad),
            "password survived redaction for an unparseable URL")
        self.assertNotIn("hunter2", str(vct._loggable_url(bad)))

    def test_percent_encoded_password_is_redacted(self):
        """Request._parse() unquotes the host, so the DECODED form appears."""
        url = "http://bob:p%40ss@prom.invalid/api/v1/query"
        out = vct._redact_text("nonnumeric port: 'p@ss@prom.invalid'", url)
        self.assertNotIn("p@ss", out, "decoded password survived: %r" % out)

    def test_redaction_is_longest_first(self):
        """A username that is a prefix of the password must not half-redact."""
        url = "http://bob:bobSecret@prom.invalid/api/v1/query"
        out = vct._redact_text("nonnumeric port: 'bobSecret@prom.invalid'", url)
        self.assertNotIn("bobSecret", out, "password survived: %r" % out)
        self.assertNotIn("Secret", out,
                         "password was only partly redacted: %r" % out)

    def test_one_reload_adding_url_and_query_is_not_contradictory(self):
        """Deciding per key mid-loop read a half-applied namespace.

        The URL is handled first, while args.regulate_query is still None, so
        the operator got "set regulate_query, then restart" for the URL and
        "restart the job for this to take effect" for the query -- in the same
        reload, for the one edit the startup warning had asked them to make."""
        vct.regulator_started = True
        vct.regulator = None
        a = Args(regulate_prometheus_url=None, regulate_query=None, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(
                a, {"regulate_prometheus_url": self.URL, "regulate_query": "q"})
        self.assertFalse(any("still has no" in m for m in got.output),
                         "half-applied advice survived: %r" % (got.output,))
        self.assertFalse(any("would still decline" in m for m in got.output),
                         "both keys were set, nothing should decline: %r"
                         % (got.output,))
        self.assertEqual(
            len([r for r in got.records if r.levelno >= logging.WARNING]), 1,
            "should warn once for the whole reload: %r" % (got.output,))

    def test_loggable_url_leaves_plain_urls_alone(self):
        self.assertEqual(vct._loggable_url(self.URL), self.URL)
        self.assertIsNone(vct._loggable_url(None))

    def test_period_change_reaches_a_running_loop(self):
        """regulate_period_s is read every tick, not once before the loop."""
        a = Args(regulate_prometheus_url=None, regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=75.0,
                 regulate_period_s=30, regulate_floor_ms=20,
                 regulate_quiet_ticks=3, dirs=["/x"])
        r = vct.Regulator(a, "q")
        waits = []

        class _Exit:
            def is_set(self):
                return len(waits) >= 2

            def wait(self, t):
                waits.append(t)
                a.regulate_period_s = 60

        saved = vct.do_exit
        vct.do_exit = _Exit()
        try:
            with self.assertLogs(level="WARNING"):
                r.run()
        finally:
            vct.do_exit = saved
        self.assertEqual(waits, [30, 60],
                         "a mid-run period change was not picked up")



class RegulatorUrlCredentialsNeverLogged(unittest.TestCase):
    """A password in regulate_prometheus_url must never reach the log.

    urllib.request.urlopen() does not use URL userinfo for authentication: it
    passes "user:pw@host" to http.client as the HOSTNAME. With no explicit
    port that dies in _get_hostport() as
    InvalidURL("nonnumeric port: 'pw@host'"); with one it fails DNS on the
    same string. Either way the exception text carries the PASSWORD -- not a
    "user:pw@" pair that a shape-matching regex would catch -- and both
    "first sample failed (%s)" and "no usable sample (%s)" logged it verbatim
    at WARNING.

    Two layers, tested separately: such a URL is refused where it is applied,
    and any error text is still scrubbed against the URL's known credentials.
    Removing either must fail a test here.
    """

    PW = "hunter2"
    URL = "http://bob:hunter2@prom/api/v1/query"

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms,
                       vct.regulator, vct.regulator_started)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2
        vct.regulator = None

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms,
         vct.regulator, vct.regulator_started) = self._saved

    def _no_secret(self, records, where):
        leaked = [m for m in records if self.PW in m]
        self.assertFalse(leaked, "password reached the log via %s: %r"
                         % (where, leaked))

    def test_credentialed_url_is_rejected_at_startup(self):
        a = Args(regulate_prometheus_url=self.URL, regulate_query="q",
                 no_regulate=False, dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            self.assertIsNone(vct.start_regulator(a),
                              "a credentialed URL started a regulator")
        self.assertTrue(any("credentials" in m for m in got.output),
                        "rejection was not explained: %r" % (got.output,))
        self._no_secret(got.output, "start_regulator")

    def test_credentialed_url_is_not_applied_late(self):
        vct.regulator_started = True
        vct.regulator = _StubRegulator()
        keep = "http://prom.invalid/api/v1/query"
        a = Args(regulate_prometheus_url=keep, regulate_query="q", dirs=["/x"])
        with self.assertLogs(level="WARNING") as got:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": self.URL})
        self.assertEqual(a.regulate_prometheus_url, keep,
                         "a credentialed URL was stored on args")
        self.assertTrue(any("NOT" in m and "credentials" in m
                            for m in got.output),
                        "silent refusal: %r" % (got.output,))
        self._no_secret(got.output, "_apply_regulate_keys")

    def test_malformed_port_with_userinfo_does_not_raise(self):
        """.port raises ValueError here; urlsplit() does not, so it escaped."""
        bad = "http://u:%s@prom:90x0/api/v1/query" % self.PW
        got = vct._loggable_url(bad)          # must not raise
        self.assertNotIn(self.PW, str(got))
        vct.regulator_started = True
        a = Args(regulate_prometheus_url="http://prom.invalid/", dirs=["/x"])
        with self.assertLogs(level="INFO") as logs:
            vct._apply_regulate_keys(a, {"regulate_prometheus_url": bad})
        self._no_secret(logs.output, "_loggable_url via _apply_regulate_keys")

    def test_redact_text_scrubs_the_password_not_a_shape(self):
        """The real CPython message holds "pw@host", not "user:pw@"."""
        msg = "nonnumeric port: '%s@prom'" % self.PW
        out = vct._redact_text(msg, self.URL)
        self.assertNotIn(self.PW, out)
        self.assertIn("<redacted>", out)

    def test_run_loop_redacts_credentials_in_error_text(self):
        """The hold path logs the exception; scrub it against the URL."""
        a = Args(regulate_prometheus_url=self.URL, regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=5, regulate_floor_ms=0,
                 regulate_quiet_ticks=3, dirs=["/x"],
                 regulate_max_threads=6)
        reg = vct.Regulator(a, "q")
        was_set = vct.do_exit.is_set()
        vct.do_exit.clear()

        def boom():
            # Stop the loop after this one iteration, then fail the way
            # http.client does for a credentialed URL.
            vct.do_exit.set()
            raise Exception("nonnumeric port: '%s@prom'" % self.PW)

        reg.sample = boom
        try:
            with self.assertLogs(level="WARNING") as got:
                reg.run()
        finally:
            if was_set:
                vct.do_exit.set()
            else:
                vct.do_exit.clear()
        self.assertTrue(any("no usable sample" in m for m in got.output),
                        "the hold path did not log: %r" % (got.output,))
        self._no_secret(got.output, "Regulator.run")


class CommandLineCredentialsRedacted(unittest.TestCase):
    """A credential on the command line must not reach the log or `ps`.

    main() logs the whole command line in its Starting:/Finished: lines, and
    _report_state() logs _amended_cmdline() on every signal and every "Config
    reloaded" -- which also feeds setproctitle(). The first attempt at this
    scrubbed the JOINED string against the value found under the full
    "--regulate-prometheus-url" spelling, and was wrong three ways, one per
    test below:

      - main()'s parser is NOT built with allow_abbrev=False, so argparse
        accepts any unique prefix. "--regulate-prom URL" set the value while
        the scrubber matched nothing.
      - Replacing the credential substrings across the whole joined line hit
        every other occurrence, so username "data" rewrote an unrelated
        "--tmpdir /data/tmp".
      - shlex.join() quotes first, so a password containing an apostrophe was
        re-quoted and the raw-substring replace missed it.

    Redaction is per-token now, before the join, so the flag spelling and the
    password's contents are both irrelevant.
    """

    PW = "hunter2"

    def setUp(self):
        self._saved = (list(sys.argv), vct.thread_count, vct.min_age_days,
                       vct.file_delay_ms)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.min_age_days = 1
        vct.file_delay_ms = 2

    def tearDown(self):
        (argv, vct.thread_count, vct.min_age_days,
         vct.file_delay_ms) = self._saved
        sys.argv = argv

    def _cmdline(self, argv):
        sys.argv = list(argv)
        return vct._amended_cmdline()

    def test_abbreviated_flag_is_redacted(self):
        out = self._cmdline(["tc.py", "--regulate-prom",
                             "http://bob:%s@prom/api/v1/query" % self.PW,
                             "/vol"])
        self.assertNotIn(self.PW, out,
                         "an abbreviated flag leaked the password: %r" % out)
        self.assertIn("<redacted>@prom", out,
                      "the URL was not redacted at all: %r" % out)

    def test_joined_flag_form_is_redacted(self):
        out = self._cmdline(
            ["tc.py",
             "--regulate-prometheus-url=http://bob:%s@prom/q" % self.PW,
             "/vol"])
        self.assertNotIn(self.PW, out,
                         "the --flag=URL form leaked the password: %r" % out)

    def test_password_with_a_quote_is_redacted(self):
        pw = "hun'ter"
        out = self._cmdline(["tc.py", "--regulate-prometheus-url",
                             "http://bob:%s@prom/q" % pw, "/vol"])
        self.assertNotIn(pw, out,
                         "a quoted password survived the join: %r" % out)
        self.assertNotIn("hun", out,
                         "part of the password survived: %r" % out)

    def test_unrelated_argument_is_not_mangled(self):
        """Username "data" must not rewrite --tmpdir /data/tmp."""
        out = self._cmdline(["tc.py", "--regulate-prometheus-url",
                             "http://data:%s@prom/q" % self.PW,
                             "--tmpdir", "/data/tmp", "/vol"])
        self.assertNotIn(self.PW, out,
                         "the password leaked: %r" % out)
        self.assertIn("/data/tmp", out,
                      "an unrelated argument was rewritten: %r" % out)


class UserinfoBoundaryFailsClosed(unittest.TestCase):
    """A password containing "/", "?" or "#" must not hide its own "@".

    Cutting the netloc at the first of "/?#" BEFORE looking for "@", as
    urllib does to find where the host ends, fails OPEN: in
    "http://bob:pa/ss@prom/api/v1/query" the "@" falls past the cut, so the
    URL reads as credential-free. Neither refusal layer fires,
    _loggable_url() returns it untouched, and the FULL password reaches the
    startup "Config: regulate Prometheus URL None -> ..." INFO line, the
    Starting:/Finished: lines, every _report_state() line and the process
    title. urllib's own _splithost() stops at the same "/", so sample() then
    raises InvalidURL("nonnumeric port: 'pa'") and leaks the password's
    prefix too.

    The boundary is the LAST "@" anywhere after "://" -- deliberately not
    urllib's host boundary, because the secret to protect is what the operator
    typed, not what urllib would transmit. Generated and base64 passwords
    carry these characters routinely.
    """

    BAD = ("http://bob:pa/ss@prom/api/v1/query",
           "http://bob:pa?ss@prom/api/v1/query",
           "http://bob:pa#ss@prom/api/v1/query")

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms,
                       vct.regulator, vct.regulator_started)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2
        vct.regulator = None
        vct.regulator_started = False

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms,
         vct.regulator, vct.regulator_started) = self._saved

    def test_detected_as_credentialed(self):
        for url in self.BAD:
            with self.subTest(url=url):
                self.assertTrue(vct._url_has_userinfo(url),
                                "userinfo hidden by a separator in the password")

    def test_not_logged_by_loggable_url(self):
        for url in self.BAD:
            with self.subTest(url=url):
                self.assertNotIn("ss", str(vct._loggable_url(url)).split("@")[0],
                                 "password survived _loggable_url()")
                self.assertNotIn("pa", str(vct._loggable_url(url)).split("@")[0],
                                 "password survived _loggable_url()")

    def test_not_logged_on_the_command_line(self):
        for url in self.BAD:
            with self.subTest(url=url):
                out = " ".join(vct._redact_argv(
                    ["tc", "--regulate-prom", url, "--tmpdir", "/data/tmp"]))
                self.assertNotIn("pa/ss", out)
                self.assertNotIn("pa?ss", out)
                self.assertNotIn("pa#ss", out)
                self.assertIn("/data/tmp", out,
                              "an unrelated argument was mangled")

    def test_not_logged_by_the_startup_config_line(self):
        """This INFO line carried the whole password before the fix."""
        for url in self.BAD:
            with self.subTest(url=url):
                pw = url.split(":", 2)[2].split("@")[0]
                a = Args(regulate_prometheus_url=None, regulate_query="q",
                         dirs=["/x"], config="/nonexistent/tc.conf")
                with self.assertLogs(level="INFO") as got:
                    vct._apply_regulate_keys(
                        a, {"regulate_prometheus_url": url})
                leaked = [m for m in got.output if pw in m]
                self.assertFalse(leaked,
                                 "password reached the log: %r" % (leaked,))

    def test_clean_url_with_a_path_is_still_clean(self):
        """The fix must not make every URL look credentialed."""
        for url in ("http://prom/api/v1/query",
                    "https://prom.example:9090/api/v1/query?x=1"):
            with self.subTest(url=url):
                self.assertFalse(vct._url_has_userinfo(url))
                self.assertEqual(vct._loggable_url(url), url)
                self.assertEqual(vct._url_credentials(url), ())


class PercentEncodedAtFailsClosed(unittest.TestCase):
    """A percent-encoded "@" must not hide userinfo either.

    _url_userinfo() matched only a LITERAL "@". But Request._parse() runs
    unquote() on the host, so "http://bob:pw%40prom/api/v1/query" reaches
    http.client as "bob:pw@prom" and dies in _get_hostport() with
    InvalidURL("nonnumeric port: 'pw@prom'"). Before the fix that URL read as
    credential-free: neither refusal layer fired, _loggable_url() returned it
    whole, the startup "Config:" INFO line and Starting:/Finished: logged it
    unchanged, and "pw" was not scrubbed from that exception text.

    The encoded form is consulted ONLY when no literal "@" is present, so a
    URL that already has one keeps its old boundary and a later "%40" in the
    path cannot move it.
    """

    BAD = ("http://bob:pw%40prom/api/v1/query",
           "http://bob:pw%40prom:9090/api/v1/query")

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms,
                       vct.regulator, vct.regulator_started)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2
        vct.regulator = None
        vct.regulator_started = False

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms,
         vct.regulator, vct.regulator_started) = self._saved

    def test_detected_as_credentialed(self):
        for url in self.BAD:
            with self.subTest(url=url):
                self.assertTrue(vct._url_has_userinfo(url),
                                "a percent-encoded @ hid the userinfo")

    def test_detection_and_logging_share_a_boundary(self):
        """The two must never disagree, in EITHER direction.

        A URL detected as credentialed but emitted intact is worse than one
        never detected, so this asserts the invariant rather than checking the
        two functions apart.
        """
        for url in self.BAD + ("http://bob:pa/ss@prom/api/v1/query",
                               "http://prom/api/v1/query"):
            with self.subTest(url=url):
                detected = vct._url_has_userinfo(url)
                logged = str(vct._loggable_url(url))
                if detected:
                    self.assertNotEqual(logged, url,
                                        "detected as credentialed but logged intact")
                    self.assertIn("<redacted>@", logged)
                else:
                    self.assertEqual(logged, url,
                                     "rewrote a URL it did not consider credentialed")

    def test_not_logged_by_loggable_url(self):
        for url in self.BAD:
            with self.subTest(url=url):
                before = str(vct._loggable_url(url)).split("@")[0]
                self.assertNotIn("pw", before, "password survived _loggable_url()")
                self.assertNotIn("bob", before, "username survived _loggable_url()")

    def test_loggable_url_keeps_the_operators_remainder(self):
        """It must redact their URL, not emit a decoded rewrite of it."""
        out = str(vct._loggable_url("http://bob:pw%40prom/api/v1/query"))
        self.assertEqual(out, "http://<redacted>@prom/api/v1/query")

    def test_error_text_is_scrubbed(self):
        """The urllib exception that actually carries the credential."""
        url = "http://bob:pw%40prom/api/v1/query"
        out = vct._redact_text("nonnumeric port: 'pw@prom'", url)
        self.assertNotIn("'pw@", out, "password survived the error-text scrub")

    def test_not_logged_on_the_command_line(self):
        for url in self.BAD:
            with self.subTest(url=url):
                out = " ".join(vct._redact_argv(
                    ["tc", "--regulate-prom", url, "--tmpdir", "/data/tmp"]))
                self.assertNotIn("pw%40", out)
                self.assertIn("/data/tmp", out, "an unrelated argument was mangled")

    def test_not_logged_by_the_startup_config_line(self):
        for url in self.BAD:
            with self.subTest(url=url):
                a = Args(regulate_prometheus_url=None, regulate_query="q",
                         dirs=["/x"], config="/nonexistent/tc.conf")
                with self.assertLogs(level="INFO") as got:
                    vct._apply_regulate_keys(a, {"regulate_prometheus_url": url})
                leaked = [m for m in got.output if "pw%40" in m]
                self.assertFalse(leaked, "URL reached the log: %r" % (leaked,))

    def test_a_literal_at_keeps_its_boundary(self):
        """%40 later in the path must not move the split."""
        url = "http://bob:pw@prom/api/v1/query%40x"
        self.assertEqual(str(vct._loggable_url(url)),
                         "http://<redacted>@prom/api/v1/query%40x")


class SchemelessCredentialsRedactedInArgv(unittest.TestCase):
    """_redact_argv() accepts what detection accepts, for URL-shaped tokens.

    NOT exact parity, and the gap is deliberate: a scheme-less value whose
    userinfo is a username only ("tok@prom:9090/api") is refused by
    start_regulator() and NOT redacted here, because a bare "user@host"
    carries no secret and redacting every "@" would mangle ordinary
    arguments. Scheme-less tokens also have to look like an authority at all,
    so PromQL is not rewritten. See _redact_argv().

    It gated on "://", which is NARROWER than _url_has_userinfo(): a value
    with no scheme is still detected, so
    "--regulate-prometheus-url bob:pw@prom:9090/api" was refused by
    start_regulator() while the token went unredacted into Starting:,
    Finished:, every _report_state() line and the process title.

    The ":" inside the userinfo is what keeps ordinary arguments intact --
    "user@host" alone carries no secret. A path holding both a ":" and an "@"
    is over-redacted, which is the side to err on.
    """

    def test_scheme_less_credentialed_token_is_redacted(self):
        out = vct._redact_argv(
            ["tc", "--regulate-prometheus-url", "bob:pw@prom:9090/api"])
        self.assertNotIn("pw", " ".join(out).split("@")[0],
                         "a scheme-less credential reached the command line")
        self.assertIn("<redacted>@prom:9090/api", out)

    def test_ordinary_arguments_are_not_mangled(self):
        for tok in ("/data/tmp", "/data/x@y", "user@host", "bob@example.com",
                    "--tmpdir", "/shared/ceph/vol", "600"):
            with self.subTest(tok=tok):
                self.assertEqual(vct._redact_argv([tok]), [tok],
                                 "an ordinary argument was rewritten")

    def test_detection_and_redaction_accept_the_same_inputs(self):
        """Anything refused for userinfo must also be redacted in argv.

        The first three inputs all contain a literal ":" in the userinfo, so
        on their own they never exercised the case where redaction was
        narrower than detection. The last two are the ones that did: a
        username-only token and an encoded ":". Both were detected and
        refused, then logged in full.
        """
        for tok in ("bob:pw@prom:9090/api", "http://u:pw@h/api",
                    "--regulate-prometheus-url=http://u:pw@h/api",
                    "http://TOKEN@prom/api/v1/query",
                    "http://bob%3Apw@prom/api/v1/query"):
            with self.subTest(tok=tok):
                self.assertTrue(vct._url_has_userinfo(tok))
                self.assertNotEqual(vct._redact_argv([tok]), [tok],
                                    "detected as credentialed but left in argv")

    def test_promql_is_not_redacted(self):
        """--regulate-query goes through the same argv redaction.

        A query carrying an "@" modifier with any ":" before it matched the
        ":"-in-userinfo guard: recording-rule names ("job:metric:p99") and
        subqueries ("[1h:5m]") both hold a ":", so
        "max_over_time(x[1h:5m] @ end())" was logged as "<redacted>@ end())".
        Not a leak, but it destroys output the operator reads.
        """
        for q in ("max_over_time(x[1h:5m] @ end())",
                  "job:metric:p99 @ end()",
                  "job:metric:p99@end()",
                  "sum(rate(http_requests_total[5m])) @ end()"):
            with self.subTest(q=q):
                self.assertEqual(vct._redact_argv([q]), [q],
                                 "PromQL was rewritten as a credential")

    def test_promql_with_a_clean_user_and_host_is_over_redacted(self):
        """Pinned: the guard tests the username and host, not the password.

        "job:mds_lat:p99[5m]@1700000000" has user "job" and host
        "1700000000", so it is redacted. Testing the whole userinfo again
        would spare it, and would let "bob:p(w@prom:9090/api" through whole.
        """
        q = "--regulate-query=job:mds_lat:p99[5m]@1700000000"
        self.assertEqual(vct._redact_argv([q]),
                         ["--regulate-query=<redacted>@1700000000"])

    def test_scheme_less_joined_flag_keeps_its_name(self):
        """_loggable_url() takes its head from "://", which this has none of.

        So "--regulate-prometheus-url=bob:pw@prom:9090/api" was logged as
        "<redacted>@prom:9090/api" -- the credential gone, but the flag name
        gone with it, leaving an unreadable command line.
        """
        tok = "--regulate-prometheus-url=bob:pw@prom:9090/api"
        out = vct._redact_argv([tok])[0]
        self.assertEqual(out,
                         "--regulate-prometheus-url=<redacted>@prom:9090/api")
        self.assertNotIn("pw@", out, "credential survived")

    def test_ipv6_scheme_less_credential_is_still_redacted(self):
        """Brackets stay legal in the host, or IPv6 stops being redacted.

        The authority test that keeps PromQL intact rejects brackets in the
        USERINFO only; an IPv6 literal needs them in the host.
        """
        out = vct._redact_argv(["bob:pw@[::1]:9090/api"])[0]
        self.assertEqual(out, "<redacted>@[::1]:9090/api")

    def test_url_with_a_parenthesised_query_is_still_redacted(self):
        """The authority test must not apply to tokens holding "://".

        A real URL's query string may contain anything, so rejecting on those
        characters would stop redacting a genuine credential.
        """
        out = vct._redact_argv(["http://u:pw@prom/api?q=(x)"])[0]
        self.assertEqual(out, "http://<redacted>@prom/api?q=(x)")

    def test_username_only_scheme_less_value_is_the_documented_gap(self):
        """Detected and refused, but not redacted -- on purpose.

        Pinned so the trade-off is a decision rather than an accident: a bare
        "user@host" has no secret, and redacting it would mangle ordinary
        arguments.
        """
        tok = "tok@prom:9090/api"
        self.assertTrue(vct._url_has_userinfo(tok))
        self.assertEqual(vct._redact_argv([tok]), [tok])

    def test_username_only_token_is_redacted(self):
        """A bare token as the username is a credential with no ":" in it.

        start_regulator() refuses this and prints
        "http://<redacted>@prom/..." in its own warning, while the Starting:
        line a few lines earlier printed the token itself.
        """
        tok = "http://TOKEN@prom/api/v1/query"
        out = vct._redact_argv([tok])[0]
        self.assertNotIn("TOKEN", out, "username-only credential left in argv")
        self.assertEqual(out, "http://<redacted>@prom/api/v1/query")

    def test_encoded_colon_in_userinfo_is_redacted(self):
        """Request._parse() unquotes the host, so "bob%3Apw" is "bob:pw"."""
        tok = "http://bob%3Apw@prom/api/v1/query"
        out = vct._redact_argv([tok])[0]
        self.assertNotIn("pw", out, "encoded-separator credential left in argv")
        self.assertEqual(out, "http://<redacted>@prom/api/v1/query")

    def test_scheme_less_encoded_colon_is_redacted(self):
        """The scheme-less guard tests the decoded userinfo, not the raw one."""
        out = vct._redact_argv(["bob%3Apw@prom:9090/api"])[0]
        self.assertNotIn("pw", out)

    def test_password_holding_promql_characters_is_redacted(self):
        """The authority test never looks at the password.

        If it did, "bob:p(w@prom:9090/api" would read as PromQL: refused by
        start_regulator() for its userinfo, then logged whole in
        Starting:/Finished:, every _report_state() line and ps.
        """
        for pw in ("p(w", "p{w", "p w", "p[w]", 'p"w', "p,w"):
            tok = "bob:%s@prom:9090/api" % pw
            with self.subTest(tok=tok):
                self.assertTrue(vct._url_has_userinfo(tok))
                self.assertEqual(vct._redact_argv([tok]),
                                 ["<redacted>@prom:9090/api"],
                                 "refused for its userinfo, but left in argv")
        line = shlex.join(vct._redact_argv(
            ["tc", "--regulate-prometheus-url=bob:p(w@prom:9090/api"]))
        self.assertNotIn("p(w", line, "the Starting: line carries the password")
        self.assertIn("--regulate-prometheus-url=<redacted>@prom:9090/api", line)

    def test_the_path_is_not_part_of_the_host_test(self):
        """A scheme-less URL's path or query may hold anything, as with "://"."""
        self.assertEqual(vct._redact_argv(["bob:pw@prom:9090/api?x=(y)"]),
                         ["<redacted>@prom:9090/api?x=(y)"])


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


class PathsFromSkipsTheJournal(unittest.TestCase):
    """The walker skips the journal and its checkpoints; a path list must too.

    Replacing the shared journal by temp+rename would race another run's
    append, and a --paths-from list or a pool drain can name it.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self._saved = (vct._path_source, vct.run_journal, vct.file_delay_ms,
                       vct.runtime_config)
        vct.file_delay_ms = 0
        vct.runtime_config = None
        self.noted = []
        test = self

        class Recording:
            def note_file(self, path, st):
                test.noted.append(path)

        vct.run_journal = Recording()

    def tearDown(self):
        (vct._path_source, vct.run_journal, vct.file_delay_ms,
         vct.runtime_config) = self._saved

    def _run(self, paths):
        vct._path_source = lambda args, roots: iter(paths)
        vct.process_paths(Args(max_files=None, source_pool=None), {}, None, {},
                          [self.tmp])

    def test_the_journal_and_its_checkpoints_are_skipped(self):
        names = [vct.RUN_JOURNAL_NAME, vct.RUN_JOURNAL_NAME + ".abc.partial",
                 vct.RUN_JOURNAL_NAME + ".abc.partial.tmp"]
        paths = []
        for n in names:
            paths.append(os.path.join(self.tmp, n))
            open(paths[-1], "w").close()
        self._run(paths)
        self.assertEqual(self.noted, [], "a path list reached the journal")

    def test_an_ordinary_file_is_not(self):
        """Control: the skip is by name, not everything."""
        p = os.path.join(self.tmp, "f.parquet")
        open(p, "w").close()
        saved = vct.get_layout_walking_up
        self.addCleanup(setattr, vct, "get_layout_walking_up", saved)
        vct.get_layout_walking_up = lambda d: None      # off CephFS
        with self.assertLogs(level="ERROR"):
            self._run([p])
        self.assertEqual(self.noted, [p])


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


class ThreadAdaptivity(unittest.TestCase):
    """Regulator-driven thread changes.

    The property that matters is not that it climbs -- it is that a pause can
    never be followed by a climb back to the count that caused it. Without the
    learned ceiling the regulator pauses at N, resumes at N, climbs to N, and
    oscillates forever at the one setting the filesystem has already rejected.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle)
        vct._setproctitle = None      # keep ps(1) out of the unit tests

    def tearDown(self):
        vct.thread_count, vct.file_delay_ms, vct._setproctitle = self._saved

    def _reg(self, threads=1, maxt=0, floor=0):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=75.0,
                 regulate_period_s=30, regulate_floor_ms=floor,
                 regulate_quiet_ticks=3, dirs=["/x"],
                 threads=threads, regulate_max_threads=maxt)
        vct.thread_count = vct.DynamicSemaphore(threads)
        vct.file_delay_ms = floor
        return vct.Regulator(a, "q")

    # -- opt-in ---------------------------------------------------------------
    def test_off_by_default_never_touches_threads(self):
        r = self._reg(threads=2, maxt=0)
        for _ in range(10):
            r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 2)

    def test_control_identical_conditions_do_raise_when_enabled(self):
        """Same latency, same delay, opt-in flipped on. Proves the assertion
        above is held by the opt-in and not by some unrelated precondition."""
        r = self._reg(threads=2, maxt=6)
        r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 3)

    def _operator_writes_before_the_write(self, n):
        """Land an operator write between a regulator read and its write."""
        sem = vct.thread_count
        real = sem.set_limit_if

        def racing(gen, new):
            sem.set_limit_if = real
            sem.set_limit(n)                # SIGUSR1, or a threads edit
            return real(gen, new)

        sem.set_limit_if = racing

    def test_a_raise_does_not_overwrite_an_operator_write(self):
        r = self._reg(threads=2, maxt=6)
        self._operator_writes_before_the_write(10)
        r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 10,
                         "a regulator raise overwrote the operator's threads")

    def test_a_shed_does_not_overwrite_an_operator_write(self):
        r = self._reg(threads=1, maxt=0)
        vct.thread_count.set_limit(4)
        self._operator_writes_before_the_write(9)
        r._tighten(100.0)
        self.assertEqual(vct.thread_count.limit, 9,
                         "a regulator shed overwrote the operator's threads")

    def test_a_pause_racing_an_operator_write_pauses_from_theirs(self):
        r = self._reg(threads=4, maxt=0)
        real = vct.thread_count.set_limit_if
        self._operator_writes_before_the_write(6)
        r._pause(200.0)
        vct.thread_count.set_limit_if = real    # only _pause() races here
        self.assertEqual(vct.thread_count.limit, 0)
        r._resume()
        self.assertEqual(vct.thread_count.limit, 6,
                         "the resume gave back a count the operator replaced")

    # -- the two gates --------------------------------------------------------
    def test_will_not_raise_while_the_delay_is_above_its_floor(self):
        r = self._reg(threads=1, maxt=4, floor=20)
        vct.file_delay_ms = 21
        r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 1, "raised with delay left to give back")
        vct.file_delay_ms = 20
        r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 2, "never raised even at the floor")

    def test_will_not_raise_at_or_above_the_slo(self):
        r = self._reg(threads=1, maxt=4)
        r._maybe_raise_threads(75.0)
        self.assertEqual(vct.thread_count.limit, 1, "raised while at the target")
        r._maybe_raise_threads(74.9)
        self.assertEqual(vct.thread_count.limit, 2, "never raised even under target")

    # -- climb ----------------------------------------------------------------
    def test_one_step_per_interval_and_stops_at_the_ceiling(self):
        r = self._reg(threads=1, maxt=5)
        for expected in (2, 3, 4, 5):
            r._maybe_raise_threads(1.0)
            self.assertEqual(vct.thread_count.limit, expected)
        r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 5, "climbed past the ceiling")

    # -- hysteresis -----------------------------------------------------------
    def test_pause_lowers_the_ceiling_below_the_level_that_paused(self):
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        self.assertEqual(vct.thread_count.limit, 0)
        self.assertEqual(r._thread_ceiling(), 5)

    def test_resume_is_held_under_the_learned_ceiling(self):
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        r._resume()
        self.assertEqual(vct.thread_count.limit, 5,
                         "resumed straight back to the count that just paused")

    def test_cannot_oscillate_back_to_the_level_that_paused(self):
        """The anti-thrash property, end to end."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(5)
        r._pause(200.0)
        r._resume()
        for _ in range(20):
            r._maybe_raise_threads(1.0)
        self.assertLessEqual(vct.thread_count.limit, 4,
                             "climbed back to the count that caused the pause")

    # -- release --------------------------------------------------------------
    # The ratchet without these is a trap: one pause pins the job for the rest
    # of the run. The release tests fail without the release; the negative
    # controls fail if the clock is ignored.
    def _stale(self, r):
        """Push both clocks far enough back to count as sustained quiet."""
        past = time.time() - vct.REG_FLOOR_DECAY_S - 1
        r._last_pressure_at = past
        r._last_ceiling_decay = past

    def test_a_pause_earned_ceiling_is_released_after_sustained_quiet(self):
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        self.assertEqual(r._thread_ceiling(), 5)
        self._stale(r)
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 6,
                         "quiet never gave back any of the learned ceiling")

    def test_a_recent_pause_blocks_the_release(self):
        """The negative control for the test above: identical state, except the
        pause is recent. Without this the test above would also pass on an
        implementation that ignores the clock entirely."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        r._last_ceiling_decay = time.time() - vct.REG_FLOOR_DECAY_S - 1
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 5,
                         "released the ceiling while the pause was still recent")

    def test_a_soft_band_shed_also_blocks_the_release(self):
        """_tighten() lowers the ceiling too, and it is the easier path to get
        wrong: a job that has never paused has both clocks at 0.0, so a ceiling
        shed in the soft band would be handed straight back on the next quiet
        tick -- the climb-straight-back behaviour _tighten exists to prevent.
        Fails if only _pause() sets the clock."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(5)
        vct.file_delay_ms = vct.REG_TIGHTEN_CAP_MS
        r._tighten(80.0)
        self.assertEqual(r._thread_ceiling(), 4, "precondition: _tighten shed a thread")
        r._last_ceiling_decay = time.time() - vct.REG_FLOOR_DECAY_S - 1
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 4,
                         "released a ceiling lowered by _tighten, with no quiet at all")

    def test_a_pause_at_the_operator_floor_still_blocks_the_release(self):
        """A pause that lowers nothing is still evidence.

        Once the ceiling sits at the --threads floor, new = max(level - 1,
        base) == cur for every pause, so a clock gated on the ceiling actually
        moving would never restart -- and the ceiling would be released a poll
        period after a pause. That is the `--threads 1` configuration exactly, so it is
        the one that has to be held. Fails if the clock reset sits inside
        `if new < cur:`."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(2)
        r._pause(200.0)
        self.assertEqual(r._thread_ceiling(), 1, "precondition: pinned at the floor")
        self._stale(r)
        vct.thread_count.set_limit(1)
        r._pause(200.0)
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 1,
                         "released one poll period after a pause at the operator floor")

    def test_a_sustained_pause_keeps_blocking_the_release(self):
        """A pause that lasts longer than the decay interval must not hand back
        a step on the tick it ends.

        _pause() reaches _lower_ceiling() only while thread_count.limit > 0, and
        once paused the limit IS 0, so a clock kept there stops advancing for the
        whole pause. Fails if the assignment sits in _lower_ceiling()."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(2)
        r._pause(200.0)
        self.assertEqual(r._thread_ceiling(), 1, "precondition: pinned at the floor")
        self.assertEqual(vct.thread_count.limit, 0, "precondition: paused")
        self._stale(r)
        r._pause(200.0)
        r._resume()
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 1,
                         "released a step on the tick a sustained pause ended")

    def test_a_soft_band_tick_that_sheds_nothing_still_blocks_the_release(self):
        """At --threads 1 every soft-band tick sheds nothing, because _tighten
        returns early at cur <= base. Those ticks are still pressure. Fails if
        the clock is only set where a knob actually moves."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(2)
        r._pause(200.0)
        r._resume()
        self.assertEqual(vct.thread_count.limit, 1, "precondition: back at the floor")
        self._stale(r)
        vct.file_delay_ms = vct.REG_TIGHTEN_CAP_MS
        r._tighten(80.0)
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 1,
                         "released after a soft-band tick that shed nothing")

    def test_release_is_one_step_per_interval(self):
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        self._stale(r)
        r._maybe_decay_ceiling()
        r._maybe_decay_ceiling()
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 6,
                         "gave back more than one step in a single interval")

    def test_release_stops_at_the_configured_max(self):
        r = self._reg(threads=1, maxt=6)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        for _ in range(20):
            self._stale(r)
            r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 6, "climbed past regulate_max_threads")

    def test_no_release_when_thread_adaptivity_is_off(self):
        r = self._reg(threads=2, maxt=0)
        vct.thread_count.set_limit(2)
        r._pause(200.0)
        self._stale(r)
        r._maybe_decay_ceiling()
        self.assertEqual(r._thread_ceiling(), 0,
                         "touched the ceiling with adaptivity switched off")

    def test_a_pinned_ceiling_clears_on_its_own(self):
        """End to end: a job started --threads 1 pauses
        once at 2, which sets the ceiling to max(2 - 1, 1) = 1, and can never
        climb again however quiet the filesystem gets. With the release it
        recovers without an operator touching regulate_max_threads."""
        r = self._reg(threads=1, maxt=24)
        vct.thread_count.set_limit(2)
        r._pause(200.0)
        r._resume()
        for _ in range(20):
            r._maybe_raise_threads(5.0)
        self.assertEqual(vct.thread_count.limit, 1, "precondition: pinned at 1")
        self._stale(r)
        r._maybe_decay_ceiling()
        r._maybe_raise_threads(5.0)
        self.assertEqual(vct.thread_count.limit, 2,
                         "still pinned after the filesystem went quiet")

    def test_ceiling_never_drops_below_what_the_operator_asked_for(self):
        """--threads is a floor on ambition, not a starting suggestion. The
        ratchet's release (see the decay tests above) only climbs back toward
        regulate_max_threads a step at a time, so a ceiling that could fall
        past the operator's own setting would cost hours to earn back."""
        r = self._reg(threads=4, maxt=8)
        for level in (8, 7, 6, 5, 4, 3, 2):
            vct.thread_count.set_limit(level)
            r._pause(200.0)
        self.assertGreaterEqual(r._thread_ceiling(), 4)

    # -- config ---------------------------------------------------------------
    def test_regulate_max_threads_parsed_and_range_checked(self):
        cfg, errs = vct.RuntimeConfig._parse("regulate_max_threads = 6\n")
        self.assertEqual(errs, [])
        self.assertEqual(cfg["regulate_max_threads"], 6)
        _, errs = vct.RuntimeConfig._parse("regulate_max_threads = -1\n")
        self.assertTrue(errs, "accepted a negative ceiling")

    def test_zero_is_accepted_as_the_disable_value(self):
        cfg, errs = vct.RuntimeConfig._parse("regulate_max_threads = 0\n")
        self.assertEqual(errs, [])
        self.assertEqual(cfg["regulate_max_threads"], 0)

    def test_it_is_live_reloadable(self):
        self.assertIn("regulate_max_threads",
                      [k for k, _ in vct.REGULATE_APPLY])
        self.assertIn("regulate_max_threads", vct.RuntimeConfig.KEYS)



class ThreadCeilingFollowsConfig(unittest.TestCase):
    """regulate_max_threads must work in both directions.

    It is advertised as a live tunable, but the clamp only ever moved the
    ceiling DOWN toward it. On a job that had already climbed to its ceiling,
    raising the knob was therefore a silent no-op -- the operator edits the
    config, the poller logs the new value, and nothing changes. Measured on a
    drain pinned at 6 threads with MDS reply latency at 0.3 ms against
    a 51 ms target.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle)
        vct._setproctitle = None

    def tearDown(self):
        vct.thread_count, vct.file_delay_ms, vct._setproctitle = self._saved

    def _reg(self, threads=1, maxt=6):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=30, regulate_floor_ms=0,
                 regulate_quiet_ticks=3, dirs=["/x"],
                 threads=threads, regulate_max_threads=maxt)
        vct.thread_count = vct.DynamicSemaphore(threads)
        vct.file_delay_ms = 0
        return vct.Regulator(a, "q")

    def test_raising_the_max_lifts_an_established_ceiling(self):
        r = self._reg(threads=1, maxt=6)
        self.assertEqual(r._thread_ceiling(), 6)
        r.args.regulate_max_threads = 12
        self.assertEqual(r._thread_ceiling(), 12,
                         "raising regulate_max_threads did not lift the ceiling")

    def test_a_climbed_job_can_still_climb_further(self):
        """End to end: a job that climbed once can climb again."""
        r = self._reg(threads=1, maxt=6)
        for _ in range(10):
            r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 6)
        r.args.regulate_max_threads = 8
        for _ in range(10):
            r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 8)

    def test_raising_the_max_overrides_a_pause_earned_ceiling(self):
        """An explicit operator instruction beats learned evidence."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        self.assertEqual(r._thread_ceiling(), 5)
        r.args.regulate_max_threads = 10
        self.assertEqual(r._thread_ceiling(), 10)

    def test_a_pause_ceiling_survives_a_steady_config(self):
        """Control for the test above: absent a config CHANGE, nothing is lost."""
        r = self._reg(threads=1, maxt=8)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        for _ in range(5):
            self.assertEqual(r._thread_ceiling(), 5,
                             "a steady config erased the learned ceiling")

    def test_lowering_the_max_still_clamps(self):
        r = self._reg(threads=1, maxt=8)
        self.assertEqual(r._thread_ceiling(), 8)
        r.args.regulate_max_threads = 3
        self.assertEqual(r._thread_ceiling(), 3)

    def test_lowering_never_goes_below_what_the_operator_asked_for(self):
        r = self._reg(threads=4, maxt=8)
        r.args.regulate_max_threads = 1
        self.assertEqual(r._thread_ceiling(), 4)

    def test_zero_still_disables_adaptivity(self):
        r = self._reg(threads=2, maxt=6)
        r.args.regulate_max_threads = 0
        self.assertEqual(r._thread_ceiling(), 0)
        for _ in range(5):
            r._maybe_raise_threads(1.0)
        self.assertEqual(vct.thread_count.limit, 2)


class SoftBandBackoff(unittest.TestCase):
    """Latency between the soft target and the pause line must cost something.

    Counted as `quiet`, it would have the regulator ease the file delay DOWN
    while latency sat past the target it exists to defend, leaving only the
    pause cliff. For a job pushed above its ceiling by SIGUSR1 -- delay
    already at its floor -- the whole band would be a dead zone.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
                       vct.do_exit)
        vct._setproctitle = None

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
         vct.do_exit) = self._saved

    def _reg(self, threads=1, maxt=6, delay_cfg=20, floor=2, delay_now=2):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=30, regulate_floor_ms=floor,
                 regulate_quiet_ticks=3, dirs=["/x"],
                 threads=threads, regulate_max_threads=maxt,
                 file_delay=delay_cfg)
        vct.thread_count = vct.DynamicSemaphore(threads)
        vct.file_delay_ms = delay_now
        return vct.Regulator(a, "q")

    def test_the_cheap_knob_moves_first(self):
        r = self._reg(threads=6, delay_now=2)
        vct.thread_count.set_limit(6)
        r._tighten(100.0)
        self.assertGreater(vct.file_delay_ms, 2, "file delay did not rise")
        self.assertEqual(vct.thread_count.limit, 6,
                         "surrendered a thread before easing off the delay")

    def test_a_thread_goes_back_once_the_delay_is_restored(self):
        r = self._reg(threads=1, delay_now=2)
        vct.thread_count.set_limit(6)
        for _ in range(40):
            r._tighten(100.0)
            if vct.thread_count.limit < 6:
                break
        self.assertEqual(vct.file_delay_ms, 20,
                         "climbed past or stopped short of the configured delay")
        self.assertEqual(vct.thread_count.limit, 5)

    def test_giving_a_thread_back_also_lowers_the_ceiling(self):
        """Otherwise the next quiet spell climbs straight back into it."""
        r = self._reg(threads=1, maxt=8, delay_now=20)
        vct.thread_count.set_limit(6)
        r._tighten(100.0)
        self.assertEqual(vct.thread_count.limit, 5)
        self.assertLessEqual(r._thread_ceiling(), 5)

    def test_never_drops_below_what_the_operator_asked_for(self):
        r = self._reg(threads=4, delay_now=20)
        vct.thread_count.set_limit(4)
        for _ in range(20):
            r._tighten(100.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_the_delay_climb_is_bounded(self):
        r = self._reg(threads=2, delay_cfg=10 ** 6, delay_now=2)
        for _ in range(500):
            r._tighten(100.0)
            if vct.thread_count.limit < 2:
                break
        self.assertLessEqual(vct.file_delay_ms, vct.REG_TIGHTEN_CAP_MS)

    def test_the_loop_routes_the_band_to_tighten_not_ease(self):
        r = self._reg(threads=6)
        calls = []
        # Instance attributes, not class patches -- a class patch would leak
        # into every other Regulator built in this run.
        r._tighten = lambda lat: calls.append(("tighten", lat))
        r._ease = lambda: calls.append(("ease",))
        r.sample = lambda: 100.0

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None

        vct.do_exit = OneShot()
        r.run()
        self.assertEqual([c[0] for c in calls], ["tighten"])

    def test_under_the_target_still_eases(self):
        """Control: the same loop, one number changed."""
        r = self._reg(threads=6)
        calls = []
        r._tighten = lambda lat: calls.append(("tighten", lat))
        r._ease = lambda: calls.append(("ease",))
        r.sample = lambda: 1.0
        r._quiet = r.args.regulate_quiet_ticks - 1

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None

        vct.do_exit = OneShot()
        r.run()
        self.assertEqual([c[0] for c in calls], ["ease"])

    def test_a_floor_above_the_cap_sheds_without_a_delay_step(self):
        """Repeated pauses walk the floor up; past the cap there is no knob.

        _note_pause() ratchets floor_ms as far as REG_FLOOR_CAP_MS and _ease()
        will not go below it, so once the floor passes REG_TIGHTEN_CAP_MS the
        delay cannot climb any further and the first soft-band tick must spend
        itself on concurrency instead. Every other case in this class runs
        with a 2ms floor, so without this the regime is untested.
        """
        floor = vct.REG_TIGHTEN_CAP_MS * 2
        r = self._reg(threads=1, maxt=8, delay_cfg=20, floor=floor,
                      delay_now=floor)
        vct.thread_count.set_limit(6)
        r._tighten(100.0)
        self.assertEqual(vct.file_delay_ms, floor,
                         "climbed the delay past a floor already over the cap")
        self.assertEqual(vct.thread_count.limit, 5,
                         "no cheap knob left, yet no thread was shed")

    def test_ease_will_not_take_the_delay_below_the_floor(self):
        """The invariant that makes the regime above reachable outside a test.

        The test above hands _tighten its precondition directly. This one
        earns it the way a live job does: _ease() decays the delay toward the
        floor and stops there, so once repeated pauses have walked the floor
        past REG_TIGHTEN_CAP_MS the delay can never come back under the climb
        guard, and the next soft-band tick has nothing cheap to spend.
        """
        floor = vct.REG_TIGHTEN_CAP_MS * 2
        r = self._reg(threads=1, maxt=8, delay_cfg=20, floor=floor,
                      delay_now=floor * 4)
        for _ in range(50):
            r._ease()
        self.assertEqual(vct.file_delay_ms, floor,
                         "eased below the floor, or stopped short of it")
        vct.thread_count.set_limit(6)
        r._tighten(100.0)
        self.assertEqual(vct.thread_count.limit, 5,
                         "delay parked at a floor over the cap, yet nothing shed")

    def test_resume_into_the_soft_band_then_backs_off(self):
        """The one interval where _resume and _tighten both fire.

        A paused job whose latency recovers only as far as the soft band is
        resumed and tightened in the same tick. The resume is already held
        under the ceiling the pause lowered, so this must not restore the
        count that paused, and the back-off must then continue from there
        rather than starting over.
        """
        r = self._reg(threads=1, maxt=8, delay_now=2)
        vct.thread_count.set_limit(6)
        r._pause(200.0)
        self.assertEqual(vct.thread_count.limit, 0)

        calls = []
        real_tighten = r._tighten
        r._tighten = lambda lat: (calls.append(lat), real_tighten(lat))[1]
        r.sample = lambda: 100.0

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None

        vct.do_exit = OneShot()
        r.run()

        self.assertEqual(calls, [100.0], "the soft band did not tighten")
        self.assertEqual(vct.thread_count.limit, 5,
                         "resumed to the count that paused, or failed to resume")
        # First tick spends itself on the cheap knob, not on concurrency.
        self.assertGreater(vct.file_delay_ms, 2)

        for _ in range(40):
            real_tighten(100.0)
            if vct.thread_count.limit < 5:
                break
        self.assertEqual(vct.thread_count.limit, 4,
                         "back-off did not continue from the resumed count")

    def test_the_pause_line_still_wins(self):
        r = self._reg(threads=6)
        calls = []
        r._tighten = lambda lat: calls.append(("tighten", lat))
        r._pause = lambda lat: calls.append(("pause", lat))
        r.sample = lambda: 200.0

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None

        vct.do_exit = OneShot()
        r.run()
        self.assertEqual([c[0] for c in calls], ["pause"])



class RegulatorStartsAnyway(unittest.TestCase):
    """A failed FIRST sample must not disable self-regulation for the run.

    Returning None from the startup path on any sample() exception would be
    strictly less tolerant than the poll loop it gates -- that loop logs "no
    usable sample" and holds. Causes are all transient: nan from a query
    window shorter than the volume's request interval, a Prometheus 503 at
    the moment the job starts, or a volume with no MDS traffic yet. Getting
    this wrong pins a volume at 1 thread / 20ms delay, 35-80x slower than its
    siblings.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(1)
        vct.file_delay_ms = 20

    def tearDown(self):
        vct.thread_count, vct.file_delay_ms, vct._setproctitle = self._saved

    def _args(self, **kw):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=30, regulate_floor_ms=0,
                 regulate_quiet_ticks=3, dirs=["/x"], threads=1,
                 regulate_max_threads=6)
        a.__dict__.update(kw)
        return a

    def _start(self, args, sample):
        """start_regulator with sample() stubbed, and the thread never run."""
        started = []
        real_init = vct.Regulator.__init__

        class Stub(vct.Regulator):
            def __init__(self, a, q):
                real_init(self, a, q)

            def sample(self):
                return sample()

            def start(self):
                started.append(True)   # do not actually run the poll loop

        orig = vct.Regulator
        vct.Regulator = Stub
        try:
            return vct.start_regulator(args), started
        finally:
            vct.Regulator = orig

    def test_nan_sample_still_starts(self):
        def boom():
            raise ValueError("query returned nan")
        reg, started = self._start(self._args(), boom)
        self.assertIsNotNone(reg, "a nan first sample disabled the regulator")
        self.assertEqual(started, [True], "regulator was returned but never started")

    def test_transient_query_error_still_starts(self):
        def boom():
            raise OSError("HTTP 503")
        reg, started = self._start(self._args(), boom)
        self.assertIsNotNone(reg, "a transient query error disabled the regulator")
        self.assertEqual(started, [True])

    def test_a_good_sample_still_starts(self):
        """Control: the happy path is unchanged."""
        reg, started = self._start(self._args(), lambda: 0.5)
        self.assertIsNotNone(reg)
        self.assertEqual(started, [True])

    def test_no_url_still_disables(self):
        """Control: an unconfigured regulator is still off, not started blind."""
        reg, started = self._start(self._args(regulate_prometheus_url=None),
                                   lambda: 0.5)
        self.assertIsNone(reg)
        self.assertEqual(started, [])

    def test_slo_not_below_pause_disables(self):
        """An inverted band cannot be regulated, so say so and stay off."""
        reg, started = self._start(
            self._args(regulate_slo_ms=200.0, regulate_pause_ms=150.0),
            lambda: 0.5)
        self.assertIsNone(reg, "an inverted soft band was accepted")
        self.assertEqual(started, [])

    def test_slo_equal_to_pause_disables(self):
        """Equal is also empty -- no band at all between them."""
        reg, started = self._start(
            self._args(regulate_slo_ms=150.0, regulate_pause_ms=150.0),
            lambda: 0.5)
        self.assertIsNone(reg)

    def test_slo_just_below_pause_is_accepted(self):
        """Control for the two above: one unit of band is enough."""
        reg, started = self._start(
            self._args(regulate_slo_ms=149.0, regulate_pause_ms=150.0),
            lambda: 0.5)
        self.assertIsNotNone(reg)


class ReclaimNameGuard(unittest.TestCase):
    """_reclaim_named unlinks; therefore _reclaim_named checks the name.

    Its only caller filters on TMP_RE, so this was not a live bug -- but the
    function's blast radius without the check is "unlink every aged regular
    file in this directory", on a filesystem holding other people's data.
    """

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self._saved_age = vct.TMP_ORPHAN_MIN_AGE_S

    def tearDown(self):
        vct.TMP_ORPHAN_MIN_AGE_S = self._saved_age
        shutil.rmtree(self.d, ignore_errors=True)

    def _aged(self, name):
        """Age is judged by ctime, which no test can backdate, so a file is
        aged by moving the threshold instead."""
        p = os.path.join(self.d, name)
        with open(p, "w") as f:
            f.write("x")
        vct.TMP_ORPHAN_MIN_AGE_S = 0
        return p

    def test_real_data_is_not_unlinked(self):
        keep = self._aged("quarterly-results.parquet")
        vct._reclaim_named(self.d, ["quarterly-results.parquet"])
        self.assertTrue(os.path.exists(keep),
                        "an aged non-temp file was unlinked")

    def test_a_genuine_orphan_is_still_unlinked(self):
        """Control: the guard must not break the function's actual job."""
        orphan = self._aged("." + "a" * 32 + vct.TMP_SUFFIX)
        self.assertTrue(vct.TMP_RE.match(os.path.basename(orphan)),
                        "test fixture does not match TMP_RE")
        vct._reclaim_named(self.d, [os.path.basename(orphan)])
        self.assertFalse(os.path.exists(orphan), "a real orphan survived")

    def test_a_young_orphan_is_left_alone(self):
        p = os.path.join(self.d, "." + "b" * 32 + vct.TMP_SUFFIX)
        with open(p, "w") as f:
            f.write("x")
        vct._reclaim_named(self.d, [os.path.basename(p)])
        self.assertTrue(os.path.exists(p), "a young orphan was unlinked")


class ExampleConfigQueryWindow(unittest.TestCase):
    """The example config is where operators copy their query from.

    A [1m] window returns nan on any volume served fewer than a few requests
    a minute, and that nan is what disabled the regulator on two volumes.
    """

    def test_window_is_not_one_minute(self):
        text = vct.config_example("myvol")
        self.assertIn("mds_lat_sum", text, "example no longer shows a query")
        self.assertNotIn("[1m]", text, "example still recommends a 1m window")
        self.assertIn("[5m]", text)

    def test_the_reason_is_stated(self):
        text = vct.config_example("myvol").lower()
        self.assertIn("nan", text,
                      "example widens the window without saying why")



class HardlinkStagingOrder(unittest.TestCase):
    """A failure partway through relinking must not split the hardlinks.

    The old order renamed the first path, then linked and renamed each
    remaining one. If link number two failed, the first name already pointed at
    the new inode while the rest still pointed at the old one: files that were
    hardlinks to a single inode became two inodes with identical contents, and
    nothing detected or reported it. Staging every link from tmp_file before
    any rename makes that window empty.
    """

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
                       os.link, os.rename, os.chown,
                       vct._apply_and_verify_layout, vct.run_journal)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(1)
        vct.run_journal = None
        vct._apply_and_verify_layout = lambda layout, path: None
        os.chown = lambda *a, **k: None          # unprivileged test runner

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
         os.link, os.rename, os.chown,
         vct._apply_and_verify_layout, vct.run_journal) = self._saved
        shutil.rmtree(self.d, ignore_errors=True)

    class _Layout:
        object_size = 1 << 22

        def diff(self, other):
            return "fake-layout-diff"

    def _args(self):
        return Args(dirs=[self.d], dry_run=False, stage_in_tmpdir=False,
                    tmpdir=self.d, no_copy_file_range=True, threads=1)

    def _three_links(self):
        """One inode, three names, all in the same directory."""
        a = os.path.join(self.d, "a")
        with open(a, "wb") as f:
            f.write(b"payload" * 100)
        b, c = os.path.join(self.d, "b"), os.path.join(self.d, "c")
        os.link(a, b)
        os.link(a, c)
        return [a, b, c]

    def test_a_failed_link_renames_nothing(self):
        paths = self._three_links()
        st = os.lstat(paths[0])
        ino_before = st.st_ino

        real_link = os.link
        calls = {"link": 0}

        def flaky_link(src, dst, **kw):
            calls["link"] += 1
            if calls["link"] == 2:
                raise OSError(28, "No space left on device")
            return real_link(src, dst, **kw)

        renames = []
        real_rename = os.rename

        def spy_rename(src, dst):
            renames.append((src, dst))
            return real_rename(src, dst)

        os.link, os.rename = flaky_link, spy_rename
        with self.assertRaises(OSError):
            vct.process_file(self._args(), paths, st, self._Layout(), self._Layout())

        self.assertEqual(renames, [],
                         "a rename happened even though a link failed: "
                         "the hardlinks are now split across two inodes")
        inos = {os.lstat(p).st_ino for p in paths}
        self.assertEqual(inos, {ino_before},
                         "the three names no longer share one inode")
        self.assertEqual(os.lstat(paths[0]).st_nlink, 3)
        # And nothing hidden is left behind: the staged links are cleaned up
        # by the failure path, not left for the orphan reclaimer to find
        # TMP_ORPHAN_MIN_AGE_S later, as hidden objects on the pool.
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [],
                         "staged temp files survived a failed relink: %s" % leftovers)

    def test_the_happy_path_still_relinks_all_three(self):
        """Control: with no failure, every name ends on the NEW inode."""
        paths = self._three_links()
        st = os.lstat(paths[0])
        ino_before = st.st_ino
        vct.process_file(self._args(), paths, st, self._Layout(), self._Layout())
        inos = {os.lstat(p).st_ino for p in paths}
        self.assertEqual(len(inos), 1, "the names were split across inodes")
        self.assertNotEqual(inos.pop(), ino_before, "nothing was replaced")
        self.assertEqual(os.lstat(paths[0]).st_nlink, 3)
        for p in paths:
            with open(p, "rb") as f:
                self.assertEqual(f.read(), b"payload" * 100)

    def _flaky(self, real, fail_on, calls):
        """real(), except call number fail_on (counted in calls) raises EIO."""
        def f(*a, **kw):
            calls.append(a)
            if len(calls) == fail_on:
                raise OSError(5, "Input/output error")
            return real(*a, **kw)
        return f

    def test_a_failed_second_rename_rejoins_the_original_inode(self):
        """The first name was already on the new inode when the second rename
        failed. The set must not stay split, nor c's staged link be left
        behind."""
        paths = self._three_links()
        st = os.lstat(paths[0])
        os.rename = self._flaky(os.rename, 2, [])
        with self.assertLogs(level="WARNING") as logs:
            with self.assertRaises(OSError):
                vct.process_file(self._args(), paths, st, self._Layout(),
                                 self._Layout())
        self.assertEqual({os.lstat(p).st_ino for p in paths}, {st.st_ino},
                         "the names are split across two inodes")
        self.assertEqual(os.lstat(paths[0]).st_nlink, 3)
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [], "staged links survived: %s"
                         % leftovers)
        self.assertTrue(any("back on the original inode" in m
                            and "Input/output error" in m
                            for m in logs.output), logs.output)

    def test_a_failed_first_rename_leaves_no_staged_links(self):
        paths = self._three_links()
        st = os.lstat(paths[0])
        os.rename = self._flaky(os.rename, 1, [])
        with self.assertRaises(OSError):
            vct.process_file(self._args(), paths, st, self._Layout(),
                             self._Layout())
        self.assertEqual({os.lstat(p).st_ino for p in paths}, {st.st_ino})
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [], "staged links survived: %s"
                         % leftovers)

    def test_a_split_that_cannot_be_rejoined_is_reported(self):
        """Staging b and c are links 1 and 2; link 3 is the rejoin."""
        paths = self._three_links()
        st = os.lstat(paths[0])
        os.rename = self._flaky(os.rename, 2, [])
        os.link = self._flaky(os.link, 3, [])
        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(OSError):
                vct.process_file(self._args(), paths, st, self._Layout(),
                                 self._Layout())
        split = [m for m in logs.output if "SPLIT" in m]
        self.assertEqual(len(split), 1, logs.output)
        a, b, c = paths
        self.assertIn(f"{a} on the new inode, {b}, {c} on the original, "
                      "after a rename raised ([Errno 5] Input/output error)",
                      split[0])
        self.assertNotEqual(os.lstat(a).st_ino, st.st_ino)
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [], "staged links survived: %s"
                         % leftovers)

    def test_a_rename_that_lands_but_raises_is_still_rejoined(self):
        """Which names moved comes from their inodes, not from which renames
        returned: here the first rename takes effect and still raises."""
        paths = self._three_links()
        st = os.lstat(paths[0])
        real_rename = os.rename
        calls = []

        def lands_then_raises(src, dst):
            calls.append(dst)
            real_rename(src, dst)
            if len(calls) == 1:
                raise OSError(5, "Input/output error")
        os.rename = lands_then_raises
        with self.assertLogs(level="WARNING") as logs:
            with self.assertRaises(OSError):
                vct.process_file(self._args(), paths, st, self._Layout(),
                                 self._Layout())
        self.assertEqual({os.lstat(p).st_ino for p in paths}, {st.st_ino},
                         "the name that moved was not rejoined")
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [], "staged links survived: %s"
                         % leftovers)
        self.assertTrue(any("back on the original inode" in m
                            and "Input/output error" in m
                            for m in logs.output), logs.output)

    def _lands_then_raises_on(self, n):
        real_rename = os.rename
        calls = []

        def f(src, dst):
            calls.append(dst)
            real_rename(src, dst)
            if len(calls) == n:
                raise OSError(5, "Input/output error")
        return f

    def _completes(self, paths):
        """process_file() finishes, counted as transcoded, with no ERROR."""
        st = os.lstat(paths[0])
        before = vct.stats.files_transcoded
        with self.assertLogs(level="WARNING") as logs:
            vct.process_file(self._args(), paths, st, self._Layout(),
                             self._Layout())
        self.assertEqual([m for m in logs.output if m.startswith("ERROR")], [])
        self.assertTrue(any("the replace completed" in m
                            and "Input/output error" in m
                            for m in logs.output), logs.output)
        self.assertEqual(vct.stats.files_transcoded, before + 1)
        inos = {os.lstat(p).st_ino for p in paths}
        self.assertEqual(len(inos), 1)
        self.assertNotEqual(inos.pop(), st.st_ino, "nothing was replaced")
        leftovers = [n for n in os.listdir(self.d) if vct.TMP_RE.match(n)]
        self.assertEqual(leftovers, [], "staged links survived: %s"
                         % leftovers)

    def test_a_last_rename_that_lands_but_raises_completes(self):
        """Every name is already on the new inode: nothing is split."""
        os.rename = self._lands_then_raises_on(3)
        self._completes(self._three_links())

    def test_a_single_name_whose_rename_lands_but_raises_completes(self):
        a = os.path.join(self.d, "solo")
        with open(a, "wb") as f:
            f.write(b"payload")
        os.rename = self._lands_then_raises_on(1)
        self._completes([a])

    def test_a_name_someone_else_replaced_is_left_alone(self):
        """b is replaced by an owner write while its rename fails. The rejoin
        must not link the original over it, and must not claim success."""
        paths = self._three_links()
        a, b, c = paths
        st = os.lstat(a)
        real_rename = os.rename
        calls = []

        def owner_replaces_b(src, dst):
            calls.append(dst)
            if len(calls) == 2:
                owner = os.path.join(self.d, "owner.tmp")
                with open(owner, "wb") as f:
                    f.write(b"owner write")
                real_rename(owner, b)
                raise OSError(5, "Input/output error")
            return real_rename(src, dst)
        os.rename = owner_replaces_b
        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(OSError):
                vct.process_file(self._args(), paths, st, self._Layout(),
                                 self._Layout())
        with open(b, "rb") as f:
            self.assertEqual(f.read(), b"owner write",
                             "the owner's b was lost")
        self.assertEqual(os.lstat(a).st_ino, st.st_ino)
        self.assertEqual(os.lstat(c).st_ino, st.st_ino)
        split = [m for m in logs.output if "SPLIT" in m]
        self.assertEqual(len(split), 1, logs.output)
        self.assertIn(f"{b} on neither", split[0])


class _ProcessFileHarness(unittest.TestCase):
    """process_file() off CephFS, with a real RunJournal over one artifact."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.art = os.path.join(self.d, "art")
        os.mkdir(self.art)
        open(os.path.join(self.art, vct.RETENTION_MARKER), "w").close()
        self.src = os.path.join(self.art, "data.parquet")
        with open(self.src, "wb") as f:
            f.write(b"payload" * 100)
        self._saved = (vct.thread_count, vct._setproctitle, os.chown,
                       vct._apply_and_verify_layout, vct.run_journal,
                       vct.read_rctime, vct._copy_file_data)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(1)
        vct._apply_and_verify_layout = lambda layout, path: None
        vct.read_rctime = lambda path: 1.0
        os.chown = lambda *a, **k: None          # unprivileged test runner
        vct.run_journal = vct.RunJournal(enabled=True, stop_at=[self.d])

    def tearDown(self):
        (vct.thread_count, vct._setproctitle, os.chown,
         vct._apply_and_verify_layout, vct.run_journal,
         vct.read_rctime, vct._copy_file_data) = self._saved
        shutil.rmtree(self.d, ignore_errors=True)

    _Layout = HardlinkStagingOrder._Layout

    def _args(self, **kw):
        a = dict(dirs=[self.d], dry_run=False, stage_in_tmpdir=False,
                 tmpdir=self.d, no_copy_file_range=True, threads=1)
        a.update(kw)
        return Args(**a)

    def _process(self, **kw):
        st = os.lstat(self.src)
        vct.run_journal.note_file(self.src, st)
        vct.process_file(self._args(**kw), [self.src], st, self._Layout(),
                         self._Layout())

    def _pinned_at(self):
        return vct.run_journal._artifacts[self.art]["pinned_at"]


class EveryChangeIsClaimed(_ProcessFileHarness):
    """A sibling temp moves its directory's rctime whether or not the replace
    goes through. An exit that left pinned_at null let retention trust an
    rctime that was ours; --dry-run did that on every candidate."""

    def assertClaimed(self):
        pinned = self._pinned_at()
        self.assertIsNotNone(pinned, "the directory changed, but no pin")
        self.assertLessEqual(os.stat(self.art).st_mtime, pinned,
                             "pinned_at is behind the directory's last change")

    def test_dry_run(self):
        ino = os.lstat(self.src).st_ino
        self._process(dry_run=True)
        self.assertEqual(os.lstat(self.src).st_ino, ino, "dry run replaced")
        self.assertClaimed()

    def test_lock_skip(self):
        holder = open(self.src, "rb")
        self.addCleanup(holder.close)
        vct.fcntl.flock(holder, vct.fcntl.LOCK_EX)
        with self.assertLogs(level="WARNING"):
            self._process()
        self.assertClaimed()

    def test_source_changed(self):
        real = self._saved[-1]

        def copy_then_append(ifd, ofd, size, object_size):
            r = real(ifd, ofd, size, object_size)
            with open(self.src, "ab") as f:
                f.write(b"owner write")
            return r
        vct._copy_file_data = copy_then_append
        with self.assertLogs(level="ERROR"):
            self._process()
        self.assertClaimed()

    def test_copy_error(self):
        def eio(*a):
            raise OSError(5, "Input/output error")
        vct._copy_file_data = eio
        with self.assertRaises(OSError):
            self._process()
        self.assertClaimed()

    def test_a_replace_still_claims(self):
        """Control: the path that always claimed still does."""
        self._process()
        self.assertClaimed()

    def test_staging_elsewhere_claims_nothing_it_did_not_touch(self):
        """Control: --stage-in-tmpdir with a dry run never enters the tree."""
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, True)
        self._process(dry_run=True, stage_in_tmpdir=True, tmpdir=tmpdir)
        self.assertIsNone(self._pinned_at())

    def test_the_walks_orphan_sweep_is_claimed(self):
        """An orphan in the artifact root is swept before any of the root's
        files is stat'd: pre_rctime must still be read before the unlink."""
        orphan = os.path.join(self.art, "." + "c" * 32 + vct.TMP_SUFFIX)
        open(orphan, "w").close()
        vct.read_rctime = lambda p: 100.0 if os.path.exists(orphan) else 999.0
        saved = (vct.TMP_ORPHAN_MIN_AGE_S, vct.get_layout_walking_up,
                 vct.CephLayout.__dict__["from_dir"], vct.runtime_config)
        vct.TMP_ORPHAN_MIN_AGE_S = 0
        vct.get_layout_walking_up = lambda d: None      # off CephFS
        vct.CephLayout.from_dir = classmethod(lambda cls, p: None)
        vct.runtime_config = None
        try:
            with self.assertLogs(level="ERROR"):
                vct.process_dir(self._args(max_files=None), self.art, {},
                                None, set(), {})
        finally:
            (vct.TMP_ORPHAN_MIN_AGE_S, vct.get_layout_walking_up,
             from_dir, vct.runtime_config) = saved
            vct.CephLayout.from_dir = from_dir
        self.assertFalse(os.path.exists(orphan), "the orphan was not swept")
        rec = vct.run_journal._artifacts.get(self.art)
        self.assertIsNotNone(rec, "the sweep changed an unrecorded artifact")
        self.assertEqual(rec["pre_rctime"], 100.0,
                         "pre_rctime was read after our own unlink")
        self.assertIsNotNone(rec["pinned_at"])


class ChownBeforeCopystat(_ProcessFileHarness):
    """The kernel clears setuid/setgid (and drops security.capability) on
    chown, so a chown after copystat stripped the bits copystat had copied."""

    def test_setuid_survives_the_replace(self):
        os.chmod(self.src, 0o4755)

        def kernel_chown(path, uid, gid, **kw):
            mode = vct.stat.S_IMODE(os.stat(path).st_mode)
            os.chmod(path, mode & ~(vct.stat.S_ISUID | vct.stat.S_ISGID))
        os.chown = kernel_chown
        ino = os.lstat(self.src).st_ino
        self._process()
        st = os.lstat(self.src)
        self.assertNotEqual(st.st_ino, ino, "nothing was replaced")
        self.assertEqual(vct.stat.S_IMODE(st.st_mode), 0o4755)


class OrphanAgeIsCtime(unittest.TestCase):
    """copystat backdates a finished copy's mtime to its source's before the
    rename, so an mtime age made another job's in-flight temp look a day old.
    cleanup_tmpdir had no age check at all."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        self._saved_age = vct.TMP_ORPHAN_MIN_AGE_S
        self.addCleanup(setattr, vct, "TMP_ORPHAN_MIN_AGE_S", self._saved_age)

    def _temp(self, name, backdate=True):
        p = os.path.join(self.d, name)
        with open(p, "w") as f:
            f.write("x")
        if backdate:
            old = time.time() - 30 * 86400      # what copystat does
            os.utime(p, (old, old))
        return p

    def test_a_backdated_in_flight_temp_survives_the_walk_sweep(self):
        p = self._temp("." + "d" * 32 + vct.TMP_SUFFIX)
        vct._reclaim_named(self.d, [os.path.basename(p)])
        self.assertTrue(os.path.exists(p))

    def test_a_backdated_in_flight_temp_survives_process_file_sweep(self):
        p = self._temp("." + "e" * 32 + vct.TMP_SUFFIX)
        with vct._reclaimed_lock:
            vct._reclaimed_dirs.discard(self.d)
        vct.reclaim_orphans(self.d)
        self.assertTrue(os.path.exists(p))

    def test_cleanup_tmpdir_spares_another_jobs_temps(self):
        live = [self._temp("f" * 32, backdate=False),
                self._temp("." + "f" * 32 + vct.TMP_SUFFIX)]
        vct.cleanup_tmpdir(self.d)
        self.assertEqual([p for p in live if not os.path.exists(p)], [])

    def test_cleanup_tmpdir_still_reclaims_aged_temps(self):
        """Control: only the age changed, not what counts as ours."""
        ours = [self._temp("a" * 32),
                self._temp("." + "a" * 32 + vct.TMP_SUFFIX)]
        theirs = self._temp("results.parquet")
        vct.TMP_ORPHAN_MIN_AGE_S = 0
        vct.cleanup_tmpdir(self.d)
        self.assertEqual([p for p in ours if os.path.exists(p)], [])
        self.assertTrue(os.path.exists(theirs))


class FileDelayWakes(unittest.TestCase):
    """time.sleep(file_delay) could not be interrupted, so at DELAY_MAX_MS an
    exit, or a --config edit lowering the delay, waited up to 10 minutes."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self._saved = (vct._path_source, vct.run_journal, vct.file_delay_ms,
                       vct.runtime_config, vct.do_exit)
        vct.run_journal = None
        vct.runtime_config = None
        vct.do_exit = threading.Event()
        vct.file_delay_ms = vct.DELAY_MAX_MS
        vct._path_source = lambda args, roots: iter(
            [os.path.join(self.tmp, "a"), os.path.join(self.tmp, "b")])

    def tearDown(self):
        (vct._path_source, vct.run_journal, vct.file_delay_ms,
         vct.runtime_config, vct.do_exit) = self._saved

    def _walk_returns_within(self, seconds):
        t = threading.Thread(target=vct.process_paths, daemon=True,
                             args=(Args(max_files=None, source_pool=None), {},
                                   None, {}, [self.tmp]))
        start = time.monotonic()
        t.start()
        t.join(seconds)
        self.assertFalse(t.is_alive(), "the walker is still asleep")
        return time.monotonic() - start

    def test_exit_wakes_the_delay(self):
        threading.Timer(0.3, vct.do_exit.set).start()
        self._walk_returns_within(5)

    def test_a_config_edit_wakes_the_delay(self):
        """The poll inside the sleep is what reads the edit."""
        polls = []

        class Config:
            def poll(self, apply):
                polls.append(time.monotonic())
                if len(polls) >= 2:
                    vct.file_delay_ms = 0   # the operator lowered it
        vct.runtime_config = Config()
        with self.assertLogs(level="INFO"):
            self._walk_returns_within(5)


class ConfigReadWhilePaused(unittest.TestCase):
    """A threads=0 pause must be undoable from --config, as its log line says.

    The walker is the only reader of --config, and a pause parks it inside
    thread_count.acquire(). So "set threads > 0 to resume" did nothing, and no
    other edit was read either, for as long as any pause lasted.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.runtime_config, vct.apply_config)
        fd, self.path = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.unlink, self.path)
        self._write("threads = 0\n", 1000)
        vct.thread_count = vct.DynamicSemaphore(2)
        vct.runtime_config = vct.RuntimeConfig(self.path, poll_seconds=0)
        vct.apply_config = self._apply
        vct._poll_config()                  # the startup read: paused

    def tearDown(self):
        vct.thread_count, vct.runtime_config, vct.apply_config = self._saved

    def _apply(self, cfg):
        if "threads" in cfg:
            vct.thread_count.set_limit(cfg["threads"])

    def _write(self, text, mtime):
        with open(self.path, "w") as fh:
            fh.write(text)
        os.utime(self.path, (mtime, mtime))

    def _acquire_then_edit(self, **kw):
        """Block in acquire() as the walker does, then raise threads in the file."""
        deadline = time.monotonic() + 1.5
        got = []
        t = threading.Thread(target=lambda: got.append(vct.thread_count.acquire(
            cancel=lambda: time.monotonic() > deadline, **kw)))
        t.start()
        time.sleep(0.2)
        self._write("threads = 2\n", 2000)
        t.join(5)
        return got

    def test_raising_threads_in_config_ends_the_pause(self):
        self.assertEqual(vct.thread_count.limit, 0)
        self.assertEqual(self._acquire_then_edit(tick=vct._poll_config), [True],
                         "a paused walker never read the edit that resumes it")
        self.assertEqual(vct.thread_count.limit, 2)

    def test_without_the_poll_the_edit_is_never_seen(self):
        """Control: the old walker -- the same edit, never read."""
        self.assertEqual(self._acquire_then_edit(), [False])
        self.assertEqual(vct.thread_count.limit, 0)

    def test_every_acquire_in_the_walk_polls_config(self):
        with open(os.path.abspath(TARGET)) as fh:
            src = fh.read()
        n = src.count("thread_count.acquire(")
        self.assertGreaterEqual(n, 2)
        self.assertEqual(src.count("tick=_poll_config"), n,
                         "a walker acquire() blocks without reading --config")


class RegulatorPauseAlwaysEnds(unittest.TestCase):
    """A regulator pause must not outlive the evidence for it.

    Only a usable sample resumes a regulator pause, and two things stop one
    arriving: the pause itself, which on a quiet volume removes the only
    requests there were, so the latency ratio goes 0/0 = nan; and a
    Prometheus outage. Either held threads at 0 for good.
    """

    def setUp(self):
        self._saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
                       vct.do_exit)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 20

    def tearDown(self):
        (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
         vct.do_exit) = self._saved

    def _reg(self, blind_s=600):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=30, regulate_floor_ms=0,
                 regulate_quiet_ticks=3, dirs=["/x"], threads=4,
                 regulate_max_threads=0, regulate_blind_resume_s=blind_s)
        return vct.Regulator(a, "q")

    def _tick(self, r, sample):
        """Exactly one pass of Regulator.run()."""
        r.sample = sample

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None

        vct.do_exit = OneShot()
        r.run()

    @staticmethod
    def _nan():
        raise vct.NanSample("query returned nan")

    @staticmethod
    def _down():
        raise OSError("HTTP 503")

    def _paused(self, r):
        r._pause(200.0)
        self.assertEqual(vct.thread_count.limit, 0)

    def _nan_probe(self, r):
        """Pause, then nan for regulate_quiet_ticks (3) samples."""
        self._paused(r)
        for _ in range(3):
            self._tick(r, self._nan)

    def test_nan_after_our_own_pause_resumes(self):
        """At 1 thread: a stalled MDS completes nothing and reads nan too."""
        r = self._reg()
        self._nan_probe(r)
        self.assertEqual(vct.thread_count.limit, 1,
                         "nan resumed the full count, or held the pause")
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 1, "nan drove a climb")

    def test_one_nan_does_not_resume(self):
        r = self._reg()
        self._paused(r)
        self._tick(r, self._nan)
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 0,
                         "nan that had not persisted ended the pause")

    def test_a_sample_breaks_the_nan_streak(self):
        r = self._reg()
        self._paused(r)
        self._tick(r, self._nan)
        self._tick(r, self._nan)
        self._tick(r, lambda: 200.0)        # usable, still over the pause line
        self._tick(r, self._nan)
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 0,
                         "nan from before a usable sample counted")

    def test_a_usable_sample_restores_what_the_pause_took(self):
        """With thread adaptivity off nothing climbs, so without this the job
        stayed at 1 thread for good."""
        r = self._reg()
        self._nan_probe(r)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_a_blind_resume_is_restored_the_same_way(self):
        r = self._reg(blind_s=600)
        self._paused(r)
        self._tick(r, self._down)
        r._blind_since -= 601
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 1)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4,
                         "a blind resume stayed at 1 thread for good")

    def test_a_pause_during_the_probe_keeps_what_is_owed(self):
        r = self._reg()
        self._nan_probe(r)
        self._tick(r, lambda: 200.0)        # pauses again, at 1 thread
        self.assertEqual(vct.thread_count.limit, 0)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4,
                         "a pause at 1 thread shrank what the first one took")

    def test_an_operator_change_forgets_the_probe(self):
        r = self._reg()
        self._nan_probe(r)
        vct.thread_count.set_limit(2)       # SIGUSR1, or a threads edit
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 2,
                         "the regulator overrode an operator thread change")

    def test_nan_while_running_still_holds(self):
        """Control: not paused, nan is still no evidence either way."""
        r = self._reg()
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 4)
        self.assertEqual(vct.file_delay_ms, 20)

    def test_nan_does_not_end_an_operator_pause(self):
        r = self._reg()
        vct.thread_count.set_limit(0)
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 0)

    def test_sample_reports_nan_as_its_own_error(self):
        """Only nan means "no requests"; a negative reading is still bad data."""
        import io
        import json

        def fake(value):
            body = json.dumps({"status": "success", "data": {"result": [
                {"value": [0, value]}]}}).encode()

            class Resp(io.BytesIO):
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return lambda url, timeout=None: Resp(body)

        r = self._reg()
        saved = vct.urllib.request.urlopen
        self.addCleanup(setattr, vct.urllib.request, "urlopen", saved)
        vct.urllib.request.urlopen = fake("NaN")
        self.assertRaises(vct.NanSample, r.sample)
        vct.urllib.request.urlopen = fake("-1")
        with self.assertRaises(ValueError) as cm:
            r.sample()
        self.assertNotIsInstance(cm.exception, vct.NanSample)

    def test_an_outage_during_a_pause_resumes_at_one_thread(self):
        r = self._reg(blind_s=600)
        self._paused(r)
        with self.assertLogs(level="WARNING") as cm:
            self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 0, "resumed before the bound")
        self.assertTrue(any("PAUSED" in m for m in cm.output), cm.output)
        r._blind_since -= 601
        with self.assertLogs(level="ERROR") as cm:
            self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 1,
                         "an outage held the regulator's own pause forever")
        self.assertIn("HTTP 503", "\n".join(cm.output), "the cause was not logged")

    def test_zero_keeps_the_old_hold(self):
        r = self._reg(blind_s=0)
        self._paused(r)
        self._tick(r, self._down)
        r._blind_since -= 10 ** 6
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 0)

    def test_an_outage_while_running_changes_nothing(self):
        """Control: holding is still right when the regulator is not the pause."""
        r = self._reg()
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 4)
        self.assertIsNone(r._blind_since)

    def test_a_usable_sample_restarts_the_blind_clock(self):
        r = self._reg(blind_s=600)
        self._paused(r)
        self._tick(r, self._down)
        r._blind_since -= 500
        self._tick(r, lambda: 200.0)        # usable, still over the pause line
        self.assertIsNone(r._blind_since)
        self._tick(r, self._down)
        r._blind_since -= 500
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 0,
                         "blindness from before a usable sample counted")

    def test_an_operator_resume_forgets_the_regulator_pause(self):
        """Otherwise a later operator pause reads as ours and gets resumed."""
        r = self._reg()
        self._paused(r)
        vct.thread_count.set_limit(3)       # SIGUSR1, or a threads edit
        self._tick(r, lambda: 1.0)
        vct.thread_count.set_limit(0)       # the operator pauses
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 0,
                         "the regulator resumed an operator pause as its own")

    def _alternate(self, r, ticks, step=30):
        """nan, error, nan, error ... with step seconds passing per tick."""
        for i in range(ticks):
            self._tick(r, self._nan if i % 2 == 0 else self._down)
            if r._blind_since is not None:
                r._blind_since -= step

    def test_nan_and_errors_alternating_still_resume(self):
        """They reset each other's clocks, so a Prometheus alternating nan and
        errors reached neither bound and the pause held forever."""
        r = self._reg(blind_s=600)
        self._paused(r)
        self._alternate(r, 6)               # 3 nan: regulate_quiet_ticks
        self.assertEqual(vct.thread_count.limit, 1,
                         "alternating nan and errors held the pause")

    def test_the_blind_bound_caps_a_long_nan_streak(self):
        r = self._reg(blind_s=600)
        r.args.regulate_quiet_ticks = 1000
        self._paused(r)
        self._alternate(r, 30)              # 900 s blind, past the 600 s bound
        self.assertEqual(vct.thread_count.limit, 1,
                         "the blind bound was not a hard upper limit")

    def test_the_blind_bound_caps_pure_nan_too(self):
        r = self._reg(blind_s=600)
        r.args.regulate_quiet_ticks = 1000
        self._paused(r)
        for _ in range(30):                 # 900 s of nan alone
            self._tick(r, self._nan)
            if r._blind_since is not None:
                r._blind_since -= 30
        self.assertEqual(vct.thread_count.limit, 1,
                         "nan alone never reached the blind bound")

    def _operator_writes_after_the_check(self, r, n):
        """Land an operator write right after r's own _paused_by_me()."""
        real = r._paused_by_me

        def racing():
            mine = real()
            if mine:
                vct.thread_count.set_limit(n)   # SIGUSR1, or a threads edit
            return mine

        r._paused_by_me = racing

    def test_a_blind_resume_does_not_overwrite_an_operator_write(self):
        r = self._reg(blind_s=600)
        self._paused(r)
        self._tick(r, self._down)
        r._blind_since -= 601
        self._operator_writes_after_the_check(r, 5)
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 5,
                         "a blind resume overwrote the operator's threads")

    def test_a_nan_resume_does_not_overwrite_an_operator_write(self):
        r = self._reg()
        self._paused(r)
        self._tick(r, self._nan)
        self._tick(r, self._nan)
        self._operator_writes_after_the_check(r, 5)
        self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 5,
                         "a nan resume overwrote the operator's threads")

    def test_a_resume_does_not_overwrite_an_operator_write(self):
        r = self._reg()
        self._paused(r)
        self._operator_writes_after_the_check(r, 2)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 2,
                         "a resume overwrote the operator's threads")

    def test_a_pause_racing_an_operator_write_drops_the_probe_debt(self):
        """The operator replaced the probe; its debt must not come back."""
        r = self._reg()
        self._nan_probe(r)                  # probe at 1, owing 4
        sem = vct.thread_count
        real = sem.set_limit_if

        def racing(gen, new):
            sem.set_limit_if = real
            sem.set_limit(2)                # threads = 2, before the pause
            return real(gen, new)

        sem.set_limit_if = racing
        r._pause(200.0)
        self.assertEqual(vct.thread_count.limit, 0)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 2,
                         "the pause resumed to the probe's old debt")

    def _as_the_regulator(self, r):
        saved = vct.regulator
        vct.regulator = r
        self.addCleanup(setattr, vct, "regulator", saved)

    def test_an_operator_pause_during_a_regulator_pause_stands(self):
        """"threads = 0" read while the regulator already holds 0.

        The walker reads --config during a regulator pause now, so the edit
        arrives while the limit is already 0. A value test skipped it, the
        config reader never passed it again, and the regulator resumed over
        the operator's pause.
        """
        r = self._reg()
        self._as_the_regulator(r)
        self._paused(r)
        with self.assertLogs(level="INFO") as cm:
            vct._apply_config_threads(0)
        self.assertTrue(any("operator's setting" in m
                            and "will not resume or pay back" in m
                            for m in cm.output), cm.output)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 0,
                         "the regulator resumed over threads = 0")

    def test_an_operator_choosing_one_thread_during_the_probe_stands(self):
        r = self._reg()
        self._as_the_regulator(r)
        self._nan_probe(r)
        vct._apply_config_threads(1)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 1,
                         "the probe payback overrode threads = 1")

    def test_sigtstp_during_the_probe_stands(self):
        r = self._reg()
        self._nan_probe(r)
        vct.sigtstp_handler(signal.SIGTSTP, None)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 1,
                         "the probe payback overrode SIGTSTP")

    def test_sigusr2_during_a_regulator_pause_stands(self):
        r = self._reg()
        self._paused(r)
        vct.sigusr2_handler(signal.SIGUSR2, None)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 0,
                         "the regulator resumed over SIGUSR2")

    def test_a_soft_band_sample_pays_the_probe_back_to_threads(self):
        """Over the target is no evidence of room for the full count, but
        --threads is the operator's floor. Held at 1, a volume whose latency
        sits in the soft band stayed below it indefinitely."""
        r = self._reg()
        r.args.threads = 2
        self._nan_probe(r)                  # at 1, owing 4
        self._tick(r, lambda: 60.0)         # over 51 ms, under the pause line
        self.assertEqual(vct.thread_count.limit, 2,
                         "a soft-band sample left the probe below --threads")
        self._tick(r, lambda: 60.0)
        self.assertEqual(vct.thread_count.limit, 2,
                         "a soft-band sample paid back past --threads")
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4,
                         "the rest of the debt was forgotten")

    def test_a_soft_band_sample_keeps_a_one_thread_probe_at_one(self):
        r = self._reg()
        r.args.threads = 1
        self._nan_probe(r)
        self._tick(r, lambda: 60.0)
        self.assertEqual(vct.thread_count.limit, 1)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_a_soft_band_sample_pays_a_debt_within_threads(self):
        r = self._reg()                     # --threads 4, owing 4
        self._nan_probe(r)
        self._tick(r, lambda: 60.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_quiet_ticks_documents_the_nan_resume(self):
        ex = vct.config_example("vol")
        i = ex.index("regulate_quiet_ticks =")
        self.assertIn("nan", ex[max(0, i - 300):i],
                      "the example config does not say quiet_ticks gates it")

    def test_the_bound_is_a_live_config_key(self):
        self.assertIn("regulate_blind_resume_s", vct.RuntimeConfig.KEYS)
        out, errs = vct.RuntimeConfig._parse("regulate_blind_resume_s = 0\n")
        self.assertEqual((out, errs), ({"regulate_blind_resume_s": 0}, []))
        _, errs = vct.RuntimeConfig._parse("regulate_blind_resume_s = -1\n")
        self.assertTrue(errs)

    def test_the_cli_rejects_a_negative_bound(self):
        """As --config does. Negative is truthy, so it resumed blind on the
        second blind tick."""
        import contextlib
        import io
        saved = list(sys.argv)
        self.addCleanup(setattr, sys, "argv", saved)
        sys.argv = ["tc", "--print-config-example", "v",
                    "--regulate-blind-resume-s", "0"]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(vct.main(), 0)
        sys.argv[-1] = "-1"
        err = io.StringIO()
        with contextlib.redirect_stderr(err), \
                self.assertRaises(SystemExit) as cm:
            vct.main()
        self.assertEqual(cm.exception.code, 2)
        self.assertIn("--regulate-blind-resume-s", err.getvalue())


class ExitSignalsUnwindCleanly(unittest.TestCase):
    """SIGTERM and SIGHUP must take the same clean exit as SIGINT.

    Only SIGINT had a handler, so systemd stop, kill(1), a shutdown or a
    closed terminal killed the job outright, and the run journal -- written
    on the way out -- never was.
    """

    SIGS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)

    def setUp(self):
        self._saved = {s: signal.getsignal(s) for s in self.SIGS}
        self._saved_exit = vct.do_exit
        vct.do_exit = threading.Event()
        for s in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(s, signal.SIG_DFL)

    def tearDown(self):
        for s, h in self._saved.items():
            signal.signal(s, h)
        vct.do_exit = self._saved_exit

    def test_each_signal_sets_do_exit(self):
        vct._install_exit_handlers()
        for s in self.SIGS:
            with self.subTest(sig=s.name):
                self.assertIs(signal.getsignal(s), vct._exit_signal_handler,
                              "%s would kill the job without unwinding" % s.name)
                vct.do_exit.clear()
                with self.assertLogs(level="ERROR"):
                    os.kill(os.getpid(), s)
                    for _ in range(200):
                        if vct.do_exit.is_set():
                            break
                        time.sleep(0.01)
                self.assertTrue(vct.do_exit.is_set())

    def test_a_second_signal_escalates(self):
        """A plain kill stopped the job before SIGTERM had a handler.

        Only the signal delivered escalates: a terminal closing during a
        SIGTERM drain still takes the clean exit.
        """
        vct._install_exit_handlers()
        with self.assertLogs(level="ERROR"):
            os.kill(os.getpid(), signal.SIGTERM)
            for _ in range(200):
                if vct.do_exit.is_set():
                    break
                time.sleep(0.01)
        self.assertTrue(vct.do_exit.is_set())
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL,
                         "a second SIGTERM would still wait for the drain")
        self.assertIs(signal.getsignal(signal.SIGHUP), vct._exit_signal_handler)

    def test_the_exit_log_says_what_is_draining(self):
        saved = vct.thread_count
        vct.thread_count = vct.DynamicSemaphore(4)
        self.addCleanup(setattr, vct, "thread_count", saved)
        vct.thread_count.acquire()
        vct.thread_count.acquire()          # two copies in flight
        vct._install_exit_handlers()
        with self.assertLogs(level="ERROR") as cm:
            os.kill(os.getpid(), signal.SIGTERM)
            for _ in range(200):
                if vct.do_exit.is_set():
                    break
                time.sleep(0.01)
        blob = "\n".join(cm.output)
        self.assertIn("draining 2 in-flight copies", blob)
        self.assertIn("send SIGTERM again", blob)

    def test_help_documents_the_drain_and_timeout_k(self):
        import subprocess
        r = subprocess.run([sys.executable, os.path.abspath(TARGET), "--help"],
                           capture_output=True, text=True)
        self.assertIn("exit signals:", r.stdout)
        self.assertIn("timeout -k", r.stdout)

    def test_nohup_is_kept(self):
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        vct._install_exit_handlers()
        self.assertEqual(signal.getsignal(signal.SIGHUP), signal.SIG_IGN,
                         "a job started under nohup would die with its terminal")


class ProcessFilesStartup(unittest.TestCase):
    """What process_files() sets up before the walk. Driven, not read.

    regulator_started must be set, and only AFTER start_regulator():
    _apply_regulate_keys() stays quiet while it is False, which keeps the
    startup config read from warning. Drop the assignment and no late change
    ever warns; move it up and the startup read does.
    """

    def setUp(self):
        import io
        self._saved = (vct.start_regulator, vct.regulator,
                       vct.regulator_started, vct.run_journal, vct.RunJournal)
        vct.regulator_started = False
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        real_open = open

        # Off Linux there is no /proc/self/mounts; an empty walk needs none.
        def fake_open(path, *a, **kw):
            if path == "/proc/self/mounts":
                return io.StringIO("")
            return real_open(path, *a, **kw)

        vct.open = fake_open
        self.addCleanup(vars(vct).pop, "open", None)
        self.seen = []

        def fake_start(args):
            self.seen.append(vct.regulator_started)
            return None

        vct.start_regulator = fake_start

    def tearDown(self):
        (vct.start_regulator, vct.regulator, vct.regulator_started,
         vct.run_journal, vct.RunJournal) = self._saved

    def _run(self, journal=False):
        vct.process_files(Args(
            tmpdir=os.path.join(self.tmp, "t"), dirs=[], run_journal=journal,
            paths_from=None, paths_from_pool=None, stage_in_tmpdir=False))

    def test_regulator_started_is_set(self):
        self._run()
        self.assertTrue(vct.regulator_started,
                        "process_files() never set regulator_started")

    def test_regulator_started_is_set_after_start_regulator(self):
        self._run()
        self.assertEqual(self.seen, [False],
                         "regulator_started was True inside start_regulator()")

    def test_the_journal_checkpoints_from_the_start(self):
        started = []
        real = self._saved[4]

        class Recording(real):
            def start_checkpoints(self, roots, args, **kw):
                started.append(roots)

        vct.RunJournal = Recording
        self._run(journal=True)
        self.assertEqual(len(started), 1,
                         "process_files() never started journal checkpoints")

    def test_a_refused_walk_root_is_skipped_and_counted(self):
        root = os.path.join(self.tmp, "root")
        os.mkdir(root)
        saved = (vct.get_layout_walking_up, vct.stats)

        def restore():
            vct.get_layout_walking_up, vct.stats = saved
        self.addCleanup(restore)
        vct.stats = vct.Stats()

        def walk_up(p):
            raise vct.InvalidLayout(f"ceph.dir.layout of {p} reads back as "
                                    "stripe_unit=0 stripe_count=0 object_size=0 pool=-1")
        vct.get_layout_walking_up = walk_up
        with self.assertLogs(level="ERROR") as logs:
            vct.process_files(Args(
                tmpdir=os.path.join(self.tmp, "t"), dirs=[root], run_journal=False,
                paths_from=None, paths_from_pool=None, stage_in_tmpdir=False))
        self.assertIn(f"Skipping {root}:", "\n".join(logs.output))
        self.assertEqual(vct.stats.subtrees_skipped_layout_invalid, 1)
        self.assertEqual(vct.stats.files_failed, 0)



# --- --target-pool ------------------------------------------------------------

_L = ("stripe_unit=4194304 stripe_count=1 object_size=4194304 "
      "pool=cephfs.v.ec4.2.qlc.data")


class TargetPoolLayout(unittest.TestCase):
    """--target-pool replaces only the pool, and a file already in the target
    must still read as matched however its xattr string is formatted."""

    def test_equality_is_by_field_not_string(self):
        a = vct.CephLayout(_L)
        b = vct.CephLayout("pool=cephfs.v.ec4.2.qlc.data object_size=4194304 "
                           "stripe_count=1 stripe_unit=4194304")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_namespace_distinguishes(self):
        self.assertNotEqual(vct.CephLayout(_L),
                            vct.CephLayout(_L + " pool_namespace=ns"))

    def test_with_pool_changes_only_the_pool(self):
        a = vct.CephLayout(_L)
        t = a.with_pool("cephfs.v.data")
        self.assertEqual(t, vct.CephLayout(_L.replace(
            "cephfs.v.ec4.2.qlc.data", "cephfs.v.data")))
        self.assertEqual((t.stripe_unit, t.stripe_count, t.object_size),
                         (a.stripe_unit, a.stripe_count, a.object_size))
        self.assertIs(a.with_pool(a.pool), a)

    def test_target_layout(self):
        d = vct.CephLayout(_L)
        self.assertIs(vct.target_layout(Args(target_pool=None), d), d,
                      "without --target-pool the directory's layout is the target")
        self.assertIs(vct.target_layout(Args(), d), d)
        self.assertEqual(
            vct.target_layout(Args(target_pool="cephfs.v.data"), d).pool,
            "cephfs.v.data")
        self.assertIsNone(vct.target_layout(Args(target_pool="x"), None))


class TargetPoolGates(unittest.TestCase):
    """The override sends every eligible file to one pool, so the size band is
    all that stops it moving a whole volume to the wrong tier."""

    def setUp(self):
        self._saved = vct._pool_scheme
        vct._pool_scheme = lambda pool: None        # no ceph CLI, as on clients

    def tearDown(self):
        vct._pool_scheme = self._saved

    def test_kind_from_the_name_without_a_cli(self):
        self.assertEqual(vct.target_pool_kind("cephfs.v.ec4.2.qlc.data"),
                         ("ec", "name"))
        self.assertEqual(vct.target_pool_kind("cephfs.v.ec6.3.ssd.data"),
                         ("ec", "name"))
        self.assertEqual(vct.target_pool_kind("cephfs.v.data")[0], "rep")
        self.assertIn("no filesystem pin", vct.target_pool_kind("cephfs.v.data")[1],
                      "an unpinned name match must say so on the startup line")
        self.assertEqual(vct.target_pool_kind("cephfs.vec.data")[0], "rep",
                         "an 'ec' inside a volume name is not a profile")
        for odd in ("cephfs.x.qlc.data", "cephfs.x.ec42.data", "cephfs.x.ec8_3.data"):
            self.assertEqual(vct.target_pool_kind(odd)[0], None,
                             "%s read as replicated by its .data suffix" % odd)

    def test_the_cluster_beats_the_name(self):
        vct._pool_scheme = lambda pool: ("rep", 3)
        self.assertEqual(vct.target_pool_kind("cephfs.v.ec4.2.qlc.data"),
                         ("rep", "cluster"))

    def test_replicated_needs_a_max_size_below_the_floor(self):
        r = vct.target_pool_refusal
        self.assertIsNotNone(r("rep", None, 0, None, "p"))
        self.assertIsNotNone(r("rep", None, 0, vct.YIELD_FLOOR_BYTES, "p"))
        self.assertIsNone(r("rep", None, 0, vct.YIELD_FLOOR_BYTES - 1, "p"))

    def test_ec_needs_a_min_size_at_the_floor(self):
        floor = vct.target_pool_floor("cephfs.v.data", "cephfs.v.ec4.2.qlc.data")
        self.assertEqual(floor, vct.YIELD_FLOOR_BYTES)
        r = vct.target_pool_refusal
        self.assertIsNotNone(r("ec", floor, floor - 1, None, "p"))
        self.assertIsNone(r("ec", floor, floor, None, "p"))

    def test_the_floor_never_drops_below_the_yield_floor(self):
        vct._pool_scheme = lambda pool: (("rep", 3) if pool.endswith(".data")
                                         and ".ec" not in pool
                                         else ("ec", 4, 2, 4096))
        saved = vct._min_alloc_hint
        vct._min_alloc_hint = lambda: 4096
        try:
            self.assertEqual(
                vct.target_pool_floor("cephfs.v.data", "cephfs.v.ec4.2.qlc.data"),
                vct.YIELD_FLOOR_BYTES,
                "a 12 KiB space crossover must not lower the floor")
        finally:
            vct._min_alloc_hint = saved


class PathsFromPoolSweep(unittest.TestCase):
    """--paths-from-pool refuses a pool that is still the write target -- a
    drain cannot converge -- except for a --target-pool small-file sweep."""

    def test_draining_the_write_target_is_refused(self):
        self.assertIsNotNone(vct.write_target_refusal(
            Args(target_pool=None, max_size=None), "P", "/r", "P"))

    def test_a_sweep_is_allowed(self):
        with self.assertLogs(level="INFO"):
            self.assertIsNone(vct.write_target_refusal(
                Args(target_pool="R", max_size=614399), "P", "/r", "P"))

    def test_a_sweep_needs_a_max_size(self):
        self.assertIsNotNone(vct.write_target_refusal(
            Args(target_pool="R", max_size=None), "P", "/r", "P"))

    def test_targeting_the_same_pool_is_still_refused(self):
        self.assertIsNotNone(vct.write_target_refusal(
            Args(target_pool="P", max_size=614399), "P", "/r", "P"))

    def test_another_pool_is_not_a_question(self):
        self.assertIsNone(vct.write_target_refusal(
            Args(target_pool=None, max_size=None), "P", "/r", "Q"))


_TGT = _L.replace("cephfs.v.ec4.2.qlc.data", "cephfs.v.data")
_FAKE_CEPHFS = r"""
import os, time
class Error(Exception):
    def __init__(self, errno_=None, msg=""):
        super().__init__(msg)
        self.errno = errno_
class ObjectNotFound(Error):
    pass
ROOT = os.environ["FAKE_CEPHFS_ROOT"]
HANG = os.environ.get("FAKE_CEPHFS_HANG") or "\0"
def _p(path):
    return os.path.join(ROOT, os.fsdecode(path).lstrip("/"))
def _call(fn, path):
    p = _p(path)
    if HANG in p:
        time.sleep(3600)
    try:
        return fn(p)
    except FileNotFoundError:
        raise ObjectNotFound(2, "not found")
    except OSError as e:
        raise Error(e.errno, str(e))
class LibCephFS:
    def __init__(self, conf=None, conffile=None, auth_id=None):
        self.auth_id = auth_id
    def mount(self, mount_root=None, filesystem_name=None):
        if filesystem_name == b"nosuchfs":
            raise Error(2, "no such filesystem")
    def get_instance_id(self):
        return 4321
    def stat(self, path):
        return _call(os.stat, path)
    def rmdir(self, path):
        return _call(os.rmdir, path)
    def unlink(self, path):
        return _call(os.unlink, path)
    def unmount(self):
        pass
    def shutdown(self):
        pass
"""
_TGT_1M = _TGT.replace("object_size=4194304", "object_size=1048576")
_REAL_RMDIR, _REAL_UNLINK = os.rmdir, os.unlink


class _FakeXattrs:
    """ceph.dir.layout.* off CephFS: os.setxattr records what is set, and
    CephLayout.from_dir reads it back over a base layout."""

    def __init__(self, case, base=_L):
        self.set = {}
        self.base = base
        self.fail = None
        if hasattr(os, "setxattr"):
            case.addCleanup(setattr, os, "setxattr", os.setxattr)
        else:                                   # macOS
            case.addCleanup(delattr, os, "setxattr")
        os.setxattr = self.setxattr
        real = vct.CephLayout.__dict__["from_dir"]
        case.addCleanup(setattr, vct.CephLayout, "from_dir", real)
        vct.CephLayout.from_dir = classmethod(lambda cls, p: self.layout(p))

    def setxattr(self, path, name, value, follow_symlinks=True):
        if self.fail:
            raise self.fail
        self.set.setdefault(path, {})[name.rsplit(".", 1)[1]] = value.decode()

    def layout(self, path):
        vals = dict(kv.split("=", 1) for kv in self.base.split())
        vals.update(self.set.get(path, {}))
        return vct.CephLayout(" ".join(f"{k}={v}" for k, v in vals.items()))


class JobStagingDirs(unittest.TestCase):
    """One staging directory per job, base and target layout -- the base is the
    mount root, a pin boundary or a quota realm's root -- made once and removed
    only at the end of the run: an rmdir of a directory the copier's kernel
    client is renaming out of can wait on a recall of that client's caps."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        saved = (vct.thread_count, vct.removal_session, vct.run_journal,
                 vct._setproctitle, vct.REMOVAL_SESSION_ABORT_TIMEOUT_S)
        self.addCleanup(self._restore, saved)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.removal_session = None
        vct.run_journal = None
        vct._setproctitle = None
        vct.REMOVAL_SESSION_ABORT_TIMEOUT_S = 0.2
        self.x = _FakeXattrs(self)
        self.tgt = vct.CephLayout(_TGT)
        self.mkdirs = []
        real = os.mkdir

        def spy(p, *a, **kw):
            self.mkdirs.append(p)
            return real(p, *a, **kw)
        os.mkdir = spy
        self.addCleanup(setattr, os, "mkdir", real)

    @staticmethod
    def _restore(saved):
        (vct.thread_count, vct.removal_session, vct.run_journal,
         vct._setproctitle, vct.REMOVAL_SESSION_ABORT_TIMEOUT_S) = saved

    def _job(self, session=None, timeout=0.3):
        return vct.JobStaging("7" * 16, self.d, session=session,
                              rmdir_timeout_s=timeout)

    def _no_kernel_rmdir(self):
        """Refuse os.rmdir, and record attempts: a refusal raised inside a
        removal thread is swallowed there, so tests assert on the record."""
        self.kernel_rmdirs = []
        real = os.rmdir

        def refuse(p, *a, **kw):
            self.kernel_rmdirs.append(p)
            raise AssertionError(f"rmdir({p}) through the kernel mount")
        os.rmdir = refuse
        self.addCleanup(setattr, os, "rmdir", real)

    def test_one_directory_for_the_whole_job_at_the_root(self):
        j = self._job()
        a = j.path_for(os.path.join(self.d, "x", "y"), self.tgt)
        b = j.path_for(os.path.join(self.d, "z"), self.tgt)
        self.assertEqual(a, b)
        self.assertEqual(os.path.dirname(a), self.d)
        self.assertRegex(os.path.basename(a), vct.STAGE_RE)
        self.assertEqual(self.mkdirs, [a], "made per directory staged into")
        self.assertEqual(self.x.layout(a), self.tgt)

    def test_a_distinct_target_layout_gets_its_own(self):
        j = self._job()
        other = vct.CephLayout(_TGT_1M)
        a = j.path_for(self.d, self.tgt)
        b = j.path_for(self.d, other)
        self.assertNotEqual(a, b)
        self.assertRegex(os.path.basename(b), vct.STAGE_RE)
        self.assertEqual(self.x.layout(b), other,
                         "only the pool was set on a layout that differs in more")

    def _pins(self, table):
        """ceph.dir.pin / .pin.distributed per path; unlisted paths unpinned."""
        saved = vct._int_xattr
        self.addCleanup(setattr, vct, "_int_xattr", saved)
        vct._int_xattr = lambda p, name, default: table.get(
            (os.path.abspath(p), name.rsplit(".", 1)[1]), default)

    def test_a_distributed_pin_stages_under_each_child(self):
        """A distributed pin hashes each child to its own rank; staging under
        the child keeps the rename on one rank (the header's case A, not B)."""
        self._pins({(self.d, "distributed"): 1})
        for d in (("A", "x", "y"), ("B",)):
            os.makedirs(os.path.join(self.d, *d))
        j = self._job()
        a = j.path_for(os.path.join(self.d, "A", "x", "y"), self.tgt)
        b = j.path_for(os.path.join(self.d, "B"), self.tgt)
        top = j.path_for(self.d, self.tgt)
        self.assertEqual(os.path.dirname(a), os.path.join(self.d, "A"))
        self.assertEqual(os.path.dirname(b), os.path.join(self.d, "B"))
        self.assertEqual(os.path.dirname(top), self.d,
                         "a file directly in the pinned directory")

    def test_an_explicit_pin_stages_inside_it(self):
        deep = os.path.join(self.d, "C", "D")
        self._pins({(deep, "pin"): 1})
        os.makedirs(os.path.join(deep, "E", "F"))
        os.makedirs(os.path.join(self.d, "G"))
        j = self._job()
        p = j.path_for(os.path.join(deep, "E", "F"), self.tgt)
        self.assertEqual(os.path.dirname(p), deep)
        self.assertEqual(os.path.dirname(j.path_for(os.path.join(self.d, "G"),
                                                    self.tgt)), self.d)

    def test_a_quota_realm_deeper_than_the_pin_wins(self):
        self._pins({(self.d, "distributed"): 1})
        realm = os.path.join(self.d, "A", "q")
        saved = vct._has_quota
        self.addCleanup(setattr, vct, "_has_quota", saved)
        vct._has_quota = lambda p: p == realm
        os.makedirs(os.path.join(realm, "z"))
        j = self._job()
        self.assertEqual(j.note_exdev(os.path.join(realm, "z")), realm)
        p = j.path_for(os.path.join(realm, "z"), self.tgt)
        self.assertEqual(os.path.dirname(p), realm)

    def test_the_keepalive_is_joined_before_the_end_of_run_pass(self):
        j = self._job(timeout=2)
        p = j.path_for(self.d, self.tgt)
        order = []
        entered = threading.Event()
        real_once = j.keepalive_once

        def slow_once():
            entered.set()
            time.sleep(0.3)
            real_once()
            order.append("keepalive done")
        j.keepalive_once = slow_once
        real_listdir = os.listdir
        os.listdir = lambda d=".": (order.append("listdir") if d == p else None,
                                    real_listdir(d))[1]
        self.addCleanup(setattr, os, "listdir", real_listdir)
        j.start_keepalive(0.01)
        self.assertTrue(entered.wait(2))
        with self.assertLogs(level="INFO"):
            self.assertEqual(j.remove_all(), [p])
        self.assertEqual(order[:2], ["keepalive done", "listdir"],
                         "the end-of-run pass listed under a running refresh")

    def test_concurrent_first_files_make_it_once(self):
        j = self._job()
        out = []

        def worker():
            for _ in range(50):
                out.append(j.path_for(self.d, self.tgt))
        ts = [threading.Thread(target=worker) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(len(set(out)), 1)
        self.assertEqual(len(self.mkdirs), 1)

    def test_nothing_is_removed_while_the_run_lasts(self):
        sess = _FakeRemovalSession()
        self._no_kernel_rmdir()
        j = self._job(sess)
        for d in ("a", "b", "c"):
            j.path_for(os.path.join(self.d, d), self.tgt)
        self.assertEqual(sess.calls, [])
        self.assertEqual(self.kernel_rmdirs, [])

    def test_the_end_of_run_removal_goes_through_the_session(self):
        sess = _FakeRemovalSession()
        self._no_kernel_rmdir()
        j = self._job(sess)
        p = j.path_for(self.d, self.tgt)
        tmp = os.path.join(p, "." + "e" * 32 + vct.TMP_SUFFIX)
        open(tmp, "w").close()                  # a failed copy's leftover
        with self.assertLogs(level="INFO"):
            self.assertEqual(j.remove_all(), [p])
        self.assertEqual(sess.calls, [p])
        self.assertFalse(os.path.exists(p))
        self.assertEqual(self.kernel_rmdirs, [])

    def test_a_hung_end_of_run_removal_warns_aborts_and_pauses_nothing(self):
        sess = _FakeRemovalSession(hang=True)
        vct.removal_session = sess
        j = self._job(sess, timeout=0.2)
        a = j.path_for(self.d, self.tgt)
        b = j.path_for(self.d, vct.CephLayout(_TGT_1M))
        t0 = time.monotonic()
        with self.assertLogs(level="WARNING") as cm:
            self.assertEqual(j.remove_all(), [])
        self.assertLess(time.monotonic() - t0, 3, "an unbounded removal")
        self.assertTrue(sess.aborted)
        self.assertIsNone(vct.removal_session)
        self.assertEqual(len(sess.calls), 1, "kept removing through an "
                         "aborted session")
        self.assertFalse(any(r.levelno >= logging.CRITICAL for r in cm.records))
        self.assertEqual(vct.thread_count.limit, 4, "paused the job")
        msg = "\n".join(cm.output)
        for p in (a, b):
            self.assertIn(p, msg)
            self.assertTrue(os.path.isdir(p))
        self.assertIn("--reap-staging-min-age", msg)
        self.assertNotIn("walk reclaims", msg, "no walk reclaims it")
        self.assertIn(f"client evict id={sess.ident}", msg)
        self.assertIn("is killed", msg)

    def test_a_failed_layout_is_left_for_the_end_of_run_removal(self):
        sess = _FakeRemovalSession()
        j = self._job(sess)
        self.x.fail = OSError(errno.EIO, "layout apply failed")
        with self.assertRaises(OSError):
            j.path_for(self.d, self.tgt)
        made = self.mkdirs[0]
        self.assertTrue(os.path.isdir(made))
        self.assertEqual(sess.calls, [], "removed mid-run")
        self.x.fail = None
        with self.assertLogs(level="INFO"):
            self.assertEqual(j.remove_all(), [made])

    def _probe(self, landed):
        saved = (vct.get_layout_walking_up, vct.CephLayout.__dict__["from_file"])
        self.addCleanup(self._unprobe, saved)
        vct.get_layout_walking_up = lambda p: vct.CephLayout(_L)
        vct.CephLayout.from_file = classmethod(lambda cls, p: vct.CephLayout(landed))
        sess = _FakeRemovalSession()
        sess.mountpoint = self.d
        j = self._job(sess)
        return j, sess, j.probe("cephfs.v.data")

    @staticmethod
    def _unprobe(saved):
        vct.get_layout_walking_up, from_file = saved
        vct.CephLayout.from_file = from_file

    def test_the_probe_makes_the_job_dir_and_unlinks_only_its_file(self):
        j, sess, why = self._probe(_TGT)
        self.assertIsNone(why)
        p = j.path_for(self.d, self.tgt)
        self.assertEqual(self.mkdirs, [p], "the probe used another directory")
        self.assertEqual(os.listdir(p), [], "the probe file was left")
        self.assertEqual(sess.calls, [], "the probe removed a directory")

    def test_the_probe_never_stats_through_the_session(self):
        """A getattr through the second session of a directory the kernel
        client just made can wait on a recall of that client's caps, as an
        rmdir does."""
        saved = (vct.get_layout_walking_up, vct.CephLayout.__dict__["from_file"])
        self.addCleanup(self._unprobe, saved)
        vct.get_layout_walking_up = lambda p: vct.CephLayout(_L)
        vct.CephLayout.from_file = classmethod(lambda cls, p: vct.CephLayout(_TGT))
        sess = _FakeRemovalSession()
        stats = []
        sess.ino = lambda p: (stats.append(p), os.stat(p).st_ino)[1]
        self.assertIsNone(self._job(sess).probe("cephfs.v.data"))
        self.assertEqual(stats, [], "statted through the session")

    def test_a_refused_start_leaves_its_staging_dir_named(self):
        saved = (vct.get_layout_walking_up, vct.CephLayout.__dict__["from_file"])
        self.addCleanup(self._unprobe, saved)
        self.addCleanup(setattr, vct, "staging", vct.staging)
        vct.get_layout_walking_up = lambda p: vct.CephLayout(_L)
        vct.CephLayout.from_file = classmethod(lambda cls, p: vct.CephLayout(_L))
        sess = _FakeRemovalSession()
        sess.mountpoint = self.d
        vct.removal_session = sess
        with self.assertLogs(level="WARNING") as cm:
            why = vct._start_staging(Args(target_pool="cephfs.v.data"))
        self.assertIn("landed in", why or "")
        self.assertIsNone(vct.staging)
        left = [n for n in os.listdir(self.d) if n.startswith(vct.STAGE_PREFIX)]
        self.assertEqual(len(left), 1)
        self.assertIn(os.path.join(self.d, left[0]), "\n".join(cm.output),
                      "the refused start did not say where it left it")
        self.assertEqual(sess.calls, [], "an rmdir through the session of a "
                         "directory the kernel client just made")

    def test_a_running_job_keeps_its_staging_dirs_fresh(self):
        """A job paused for longer than --reap-staging-min-age stops moving
        its directory's ctime; the keepalive moves it, so a reap leaves it."""
        j = self._job()
        p = j.path_for(self.d, self.tgt)
        before = os.stat(p).st_ctime_ns
        time.sleep(0.05)
        j.keepalive_once()
        self.assertGreater(os.stat(p).st_ctime_ns, before)
        self.assertEqual(os.listdir(p), [], "the keepalive left its temp")
        self.assertEqual(vct._reap_one(p, 3600, []), "young")
        t = j.start_keepalive(0.05)
        time.sleep(0.3)
        mid = os.stat(p).st_ctime_ns
        self.assertGreater(mid, before)
        with self.assertLogs(level="INFO"):
            j.remove_all()
        t.join(2)
        self.assertFalse(t.is_alive(), "the keepalive outlived the run")

    def test_a_hang_on_the_kernel_mount_closes_the_session_too(self):
        """A removal stuck in an fsync, not the rmdir, still closes the
        session: a thread that gets past it later must not send its rmdir
        through a session the run is done with."""
        sess = _FakeRemovalSession()
        j = self._job(sess, timeout=0.2)
        p = j.path_for(self.d, self.tgt)
        go = threading.Event()
        self.addCleanup(go.set)
        real = os.fsync

        def stuck(fd):
            go.wait(5)
            return real(fd)
        os.fsync = stuck
        self.addCleanup(setattr, os, "fsync", real)
        with self.assertLogs(level="WARNING") as cm:
            self.assertEqual(j.remove_all(), [])
        self.assertTrue(sess.aborted, "the session was left open")
        self.assertIn("on the kernel mount", "\n".join(cm.output))
        go.set()
        time.sleep(0.5)                       # let the late thread finish
        self.assertEqual(sess.calls, [], "an rmdir went out after the hang")
        self.assertTrue(os.path.isdir(p))

    def test_a_failed_probe_says_why_and_removes_nothing(self):
        _, sess, why = self._probe(_L)
        self.assertIn("landed in cephfs.v.ec4.2.qlc.data", why or "")
        self.assertEqual(sess.calls, [])
        self.assertEqual(os.listdir(self.mkdirs[0]), [])


class JobStagedReplace(_ProcessFileHarness):
    """The staging path. The temp is BORN in the run's staging directory and
    never given a layout afterwards -- setting one would record an old pool,
    and the MDS keeps a backtrace object there for the life of the file --
    then renamed across directories into place."""

    def setUp(self):
        super().setUp()
        self._saved_staging = (vct.staging, vct._verify_layout, vct._has_quota)
        self.addCleanup(self._restore_staging)
        vct.staging = vct.JobStaging("ab" * 8, self.d)
        self.made = []

        def fake_create(base, layout):          # off CephFS: no layout xattr
            p = os.path.join(base, vct.staging.name_for(layout))
            os.makedirs(p, exist_ok=True)
            self.made.append(p)
            if p not in vct.staging._paths:     # as the real one records it
                vct.staging._paths.append(p)
            return p
        vct.staging._create = fake_create
        self.applied, self.verified = [], []
        vct._apply_and_verify_layout = lambda layout, path: self.applied.append(path)
        vct._verify_layout = lambda layout, path: self.verified.append(path)
        self.renames = []
        real = os.rename

        def spy(src, dst):
            self.renames.append((src, dst))
            return real(src, dst)
        os.rename = spy
        self.addCleanup(setattr, os, "rename", real)
        self.tgt = vct.CephLayout(_TGT)

    def _restore_staging(self):
        vct.staging, vct._verify_layout, vct._has_quota = self._saved_staging

    def _staged(self, stage=True, **kw):
        st = os.lstat(self.src)
        vct.run_journal.note_file(self.src, st)
        vct.process_file(self._args(**kw), [self.src], st, self.tgt,
                         vct.CephLayout(_L), stage)

    def _stage_dir(self, base=None):
        return os.path.join(base or self.d, vct.staging.name_for(self.tgt))

    def test_born_in_the_job_dir_and_renamed_across(self):
        ino = os.lstat(self.src).st_ino
        self._staged()
        self.assertEqual(len(self.renames), 1)
        src, dst = self.renames[0]
        self.assertEqual(os.path.dirname(src), self._stage_dir())
        self.assertEqual(dst, self.src)
        self.assertEqual(self.applied, [],
                         "a layout was set on the staged temp: that records an "
                         "old pool and leaves a backtrace object behind")
        self.assertEqual(self.verified, [src], "the read-back check was skipped")
        self.assertNotEqual(os.lstat(self.src).st_ino, ino)
        with open(self.src, "rb") as f:
            self.assertEqual(f.read(), b"payload" * 100)
        self.assertTrue(os.path.isdir(self._stage_dir()), "removed mid-run")
        self.assertIsNotNone(self._pinned_at())

    def test_unstaged_is_the_sibling_path_unchanged(self):
        """Control: stage=False is today's path -- sibling temp, layout applied."""
        self._staged(stage=False)
        self.assertEqual(self.made, [])
        self.assertEqual(len(self.applied), 1)
        self.assertEqual(os.path.dirname(self.applied[0]), self.art)
        self.assertEqual(os.path.dirname(self.renames[0][0]), self.art)

    def test_a_failed_copy_leaves_no_temp(self):
        def eio(*a):
            raise OSError(5, "Input/output error")
        vct._copy_file_data = eio
        with self.assertRaises(OSError):
            self._staged()
        self.assertEqual(os.listdir(self._stage_dir()), [])
        self.assertEqual(sorted(os.listdir(self.art)),
                         sorted([vct.RETENTION_MARKER, "data.parquet"]))

    def test_a_staged_dry_run_leaves_the_files_directory_alone(self):
        """The temp is not in the file's directory, so nothing there changes
        and nothing is claimed in the journal."""
        before = (sorted(os.listdir(self.art)), os.stat(self.art).st_mtime_ns)
        self._staged(dry_run=True)
        self.assertEqual((sorted(os.listdir(self.art)),
                          os.stat(self.art).st_mtime_ns), before)
        self.assertIsNone(self._pinned_at(),
                          "claimed a directory staging never changed")
        self.assertEqual(os.listdir(self._stage_dir()), [])

    def test_a_staging_dir_inside_an_artifact_claims_it(self):
        """Staged under a pin boundary that is an artifact (or inside one),
        the temp moves that artifact's rctime even when nothing is renamed."""
        vct.staging._pin_base = lambda d: self.art
        self._staged(dry_run=True)
        self.assertTrue(os.path.isdir(self._stage_dir(self.art)))
        self.assertIsNotNone(self._pinned_at(),
                             "the staging directory moved the artifact's "
                             "rctime without a pin")

    def test_hard_links_are_linked_beside_each_name_and_renamed_in_place(self):
        sub = os.path.join(self.art, "sub")
        os.mkdir(sub)
        b = os.path.join(sub, "b")
        os.link(self.src, b)
        st = os.lstat(self.src)
        vct.run_journal.note_file(self.src, st)
        vct.process_file(self._args(), [self.src, b], st, self.tgt,
                         vct.CephLayout(_L), True)
        self.assertEqual(len(self.renames), 2)
        (s1, d1), (s2, d2) = self.renames
        self.assertEqual((os.path.dirname(s1), d1), (self._stage_dir(), self.src))
        self.assertEqual((os.path.dirname(s2), d2), (sub, b),
                         "the extra name was not renamed within its own directory")
        self.assertEqual(os.lstat(self.src).st_ino, os.lstat(b).st_ino)


class JobStagingDirVanished(JobStagedReplace):
    """A staging directory removed under the run (by hand, or by a reap run
    without its age guard) is made again by the next file, not a failure."""

    # Only this test: the inherited ones already run under JobStagedReplace.
    test_born_in_the_job_dir_and_renamed_across = None
    test_unstaged_is_the_sibling_path_unchanged = None
    test_a_failed_copy_leaves_no_temp = None
    test_a_staged_dry_run_leaves_the_files_directory_alone = None
    test_hard_links_are_linked_beside_each_name_and_renamed_in_place = None
    test_a_staging_dir_inside_an_artifact_claims_it = None

    def test_recreated_when_removed_under_the_run(self):
        p = vct.staging.path_for(self.art, self.tgt)
        os.rmdir(p)
        with self.assertLogs(level="WARNING") as cm:
            self._staged()
        self.assertTrue(any("had vanished" in m for m in cm.output))
        self.assertEqual(os.path.dirname(self.renames[0][0]), p)
        self.assertTrue(os.path.isdir(p))


class StagedAcrossAQuotaRealm(JobStagedReplace):
    """A rename across quota realms fails with EXDEV. The copy is retried once,
    staged at the root of the file's quota realm, and later files under that
    realm are staged there directly."""

    test_born_in_the_job_dir_and_renamed_across = None
    test_unstaged_is_the_sibling_path_unchanged = None
    test_a_failed_copy_leaves_no_temp = None
    test_a_staged_dry_run_leaves_the_files_directory_alone = None
    test_hard_links_are_linked_beside_each_name_and_renamed_in_place = None
    test_a_staging_dir_inside_an_artifact_claims_it = None

    def setUp(self):
        super().setUp()
        self.realm = self.art                   # carries a quota
        vct._has_quota = lambda p: p == self.realm
        self.root_stage = self._stage_dir()
        self.realm_stage = self._stage_dir(self.realm)
        spy = os.rename                         # super()'s, which it restores

        def exdev(src, dst):
            if (os.path.dirname(src) == self.root_stage
                    and vct._under_roots(dst, [self.realm])):
                self.renames.append((src, dst))
                raise OSError(errno.EXDEV, "Invalid cross-device link", src)
            return spy(src, dst)
        os.rename = exdev

    def test_restaged_under_the_realm_root_and_retried_once(self):
        before = vct.stats.files_submitted
        with self.assertLogs(level="INFO"):
            self._staged()
        self.assertEqual(len(self.renames), 2)
        (s1, _), (s2, d2) = self.renames
        self.assertEqual(os.path.dirname(s1), self.root_stage)
        self.assertEqual(os.path.dirname(s2), self.realm_stage)
        self.assertEqual(d2, self.src)
        with open(self.src, "rb") as f:
            self.assertEqual(f.read(), b"payload" * 100)
        self.assertEqual(vct.stats.files_submitted, before + 1,
                         "the retry was counted as a second file")
        self.assertEqual(os.listdir(self.root_stage), [],
                         "the first attempt's temp was left")

    def test_later_files_in_the_realm_are_staged_there_directly(self):
        with self.assertLogs(level="INFO"):
            self._staged()
        n = len(self.renames)
        sub = os.path.join(self.realm, "sub")
        os.mkdir(sub)
        g = os.path.join(sub, "g")
        with open(g, "wb") as fh:
            fh.write(b"g" * 10)
        vct.process_file(self._args(), [g], os.lstat(g), self.tgt,
                         vct.CephLayout(_L), True)
        self.assertEqual(len(self.renames), n + 1, "hit EXDEV again")
        self.assertEqual(os.path.dirname(self.renames[-1][0]), self.realm_stage)

    def test_an_exdev_no_quota_explains_is_not_retried(self):
        vct._has_quota = lambda p: False
        with self.assertRaises(OSError) as e:
            self._staged()
        self.assertEqual(e.exception.errno, errno.EXDEV)
        self.assertEqual(len(self.renames), 1)
        self.assertEqual(os.listdir(self.root_stage), [])

    def test_a_second_exdev_fails_the_file(self):
        spy = os.rename

        def always(src, dst):
            if dst == self.src:
                self.renames.append((src, dst))
                raise OSError(errno.EXDEV, "Invalid cross-device link", src)
            return spy(src, dst)
        os.rename = always
        with self.assertLogs(level="INFO"):
            with self.assertRaises(OSError):
                self._staged()
        self.assertEqual(len(self.renames), 2, "not retried exactly once")
        for d in (self.root_stage, self.realm_stage):
            self.assertEqual(os.listdir(d), [], f"a temp was left in {d}")


class _FakeExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, fn, *a):
        self.calls.append(a)

        class F:
            def add_done_callback(self, cb):
                pass
        return F()


_ZERO = "stripe_unit=0 stripe_count=0 object_size=0 pool=-1"


class InvalidLayoutRead(unittest.TestCase):
    """A ceph.dir.layout read can come back as all zeros with pool -1 instead
    of ENODATA. It must never be applied, nor walked past to the parent, and a
    file it reaches is skipped, not failed, so a list chain keeps going."""

    def setUp(self):
        self._saved = (vct._LAYOUT_REREAD_DELAYS_S, vct.stats)
        vct._LAYOUT_REREAD_DELAYS_S = (0, 0, 0)
        vct.stats = vct.Stats()

    def tearDown(self):
        vct._LAYOUT_REREAD_DELAYS_S, vct.stats = self._saved

    def _getxattr(self, answers):
        """os.getxattr answering each path from answers[path], a list consumed
        one read at a time (the last answer repeats); None is ENODATA."""
        reads = []

        def getxattr(path, name, follow_symlinks=True):
            reads.append(path)
            seq = answers[path]
            a = seq.pop(0) if len(seq) > 1 else seq[0]
            if a is None:
                raise OSError(errno.ENODATA, "No data available", path)
            return a.encode("utf-8")
        return getxattr, reads

    def test_valid(self):
        self.assertTrue(vct.CephLayout(_L).valid)
        self.assertFalse(vct.CephLayout(_ZERO).valid)
        self.assertFalse(vct.CephLayout(_ZERO).with_pool("cephfs.v.data").valid)

    def test_a_transient_zero_read_is_read_again(self):
        g, reads = self._getxattr({"/v/d": [_ZERO, _ZERO, _L]})
        with unittest.mock.patch.object(vct.os, "getxattr", g, create=True):
            self.assertEqual(vct.CephLayout.from_dir("/v/d"), vct.CephLayout(_L))
        self.assertEqual(len(reads), 3)

    def test_a_persistent_zero_read_is_refused(self):
        g, reads = self._getxattr({"/v/f": [_ZERO]})
        with unittest.mock.patch.object(vct.os, "getxattr", g, create=True):
            with self.assertRaises(vct.InvalidLayout):
                vct.CephLayout.from_file("/v/f")
        self.assertEqual(len(reads), 1 + len(vct._LAYOUT_REREAD_DELAYS_S))

    def test_enodata_is_still_no_layout(self):
        g, _ = self._getxattr({"/v/d": [None]})
        with unittest.mock.patch.object(vct.os, "getxattr", g, create=True):
            self.assertIsNone(vct.CephLayout.from_dir("/v/d"))

    def test_the_walk_up_does_not_go_past_it(self):
        g, reads = self._getxattr({"/v/a/b": [None], "/v/a": [_ZERO], "/v": [_L]})
        with unittest.mock.patch.object(vct.os, "getxattr", g, create=True):
            with self.assertRaises(vct.InvalidLayout):
                vct.get_layout_walking_up("/v/a/b")
        self.assertNotIn("/v", reads)

    def test_it_is_never_applied(self):
        with unittest.mock.patch.object(vct.os, "setxattr", create=True) as sx:
            with self.assertRaises(vct.InvalidLayout):
                vct.CephLayout(_ZERO).with_pool("cephfs.v.data").apply_file("/v/t")
        sx.assert_not_called()

    def test_no_staging_dir_is_made_for_it(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        j = vct.JobStaging("c" * 16, d)
        with self.assertRaises(vct.InvalidLayout):
            j._create(d, vct.CephLayout(_ZERO).with_pool("cephfs.v.data"))
        self.assertEqual(os.listdir(d), [])

    def test_a_worker_hitting_it_skips_and_names_the_file(self):
        saved = (vct.staging, vct.run_journal)
        self.addCleanup(lambda: (setattr(vct, "staging", saved[0]),
                                 setattr(vct, "run_journal", saved[1])))
        vct.run_journal = None

        class Staging:
            def path_for(self, dirpath, layout):
                raise vct.InvalidLayout("staging layout under /v/a reads back as " + _ZERO)
        vct.staging = Staging()
        with self.assertLogs(level="WARNING") as logs:
            vct.process_file(Args(), ["/v/a/f"], None, vct.CephLayout(_L),
                             vct.CephLayout(_L), stage=True)
        self.assertIn("Skipping /v/a/f:", "\n".join(logs.output))
        self.assertEqual(vct.stats.files_skipped_layout_invalid, 1)
        self.assertEqual(vct.stats.files_failed, 0)

    def test_a_bad_tmpdir_read_is_a_clean_refusal(self):
        saved = vct.get_layout_walking_up
        self.addCleanup(setattr, vct, "get_layout_walking_up", saved)

        def walk_up(p):
            raise vct.InvalidLayout(f"ceph.dir.layout of {p} reads back as {_ZERO}")
        vct.get_layout_walking_up = walk_up
        with self.assertLogs(level="ERROR") as logs:
            with self.assertRaises(SystemExit):
                vct._check_tmpdir_and_target(Args(tmpdir="/v/tmp", target_pool=None))
        self.assertIn("Refusing to start", "\n".join(logs.output))

    def test_a_listed_file_under_it_is_skipped_not_failed(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        f = os.path.join(d, "f")
        with open(f, "wb") as fh:
            fh.write(b"x" * 1000)
        old = time.time() - 30 * 86400
        os.utime(f, (old, old))
        saved = (vct._path_source, vct.run_journal, vct.file_delay_ms,
                 vct.runtime_config, vct.thread_count, vct.staging,
                 vct.get_layout_walking_up)
        self.addCleanup(lambda: (setattr(vct, "_path_source", saved[0]),
                                 setattr(vct, "run_journal", saved[1]),
                                 setattr(vct, "file_delay_ms", saved[2]),
                                 setattr(vct, "runtime_config", saved[3]),
                                 setattr(vct, "thread_count", saved[4]),
                                 setattr(vct, "staging", saved[5]),
                                 setattr(vct, "get_layout_walking_up", saved[6])))
        vct.file_delay_ms = 0
        vct.runtime_config = None
        vct.run_journal = None
        vct.staging = None
        vct.thread_count = vct.DynamicSemaphore(100)

        def walk_up(p):
            raise vct.InvalidLayout(f"ceph.dir.layout of {p} reads back as {_ZERO}")
        vct.get_layout_walking_up = walk_up
        vct._path_source = lambda a, r: iter([f])
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent", dirs=[d],
                    stage_in_tmpdir=False, target_pool="cephfs.v.data")
        ex = _FakeExecutor()
        vct.process_paths(args, {}, ex, {}, [d])
        self.assertEqual(ex.calls, [])
        self.assertEqual(vct.stats.files_skipped_layout_invalid, 1)
        self.assertEqual(vct.stats.files_failed, 0)

        # More files in the same directory: skipped, each named, read once.
        more = []
        for n in ("g", "h", "i"):
            p = os.path.join(d, n)
            with open(p, "wb") as fh:
                fh.write(b"x" * 1000)
            os.utime(p, (old, old))
            more.append(p)
        reads = []
        vct.get_layout_walking_up = lambda p: (reads.append(p), walk_up(p))[1]
        vct._path_source = lambda a, r: iter(more)
        with self.assertLogs(level="WARNING") as logs:
            vct.process_paths(args, {}, ex, {}, [d])
        self.assertEqual(reads, [d])
        self.assertEqual(ex.calls, [])
        self.assertEqual(vct.stats.files_skipped_layout_invalid, 4)
        for p in more:
            self.assertIn(f"Skipping {p}:", "\n".join(logs.output))

    def test_a_walk_skips_the_subtree_and_counts_it(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, True)
        old = time.time() - 30 * 86400
        for rel in ("top", os.path.join("bad", "f"), os.path.join("bad", "sub", "g")):
            p = os.path.join(d, rel)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as fh:
                fh.write(b"x" * 1000)
            os.utime(p, (old, old))
        saved = (vct.run_journal, vct.file_delay_ms, vct.runtime_config,
                 vct.thread_count, vct.staging, vct.get_layout_walking_up,
                 vct.CephLayout.__dict__["from_file"],
                 vct.CephLayout.__dict__["from_dir"])

        def restore():
            (vct.run_journal, vct.file_delay_ms, vct.runtime_config,
             vct.thread_count, vct.staging, vct.get_layout_walking_up,
             from_file, from_dir) = saved
            vct.CephLayout.from_file = from_file
            vct.CephLayout.from_dir = from_dir
        self.addCleanup(restore)
        vct.file_delay_ms = 0
        vct.runtime_config = None
        vct.run_journal = None
        vct.staging = None
        vct.thread_count = vct.DynamicSemaphore(100)
        bad = os.path.join(d, "bad")

        def from_dir(cls, p):
            if p == bad:
                raise vct.InvalidLayout(f"ceph.dir.layout of {p} reads back as {_ZERO}")
            return vct.CephLayout(_L)
        vct.CephLayout.from_dir = classmethod(from_dir)
        vct.get_layout_walking_up = lambda p: from_dir(None, p)
        src = vct.CephLayout(_L.replace("cephfs.v.ec4.2.qlc.data",
                                        "cephfs.v.ec6.3.ssd.data"))
        vct.CephLayout.from_file = classmethod(lambda cls, p: src)
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent", dirs=[d],
                    stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct.process_dir(args, d, {}, ex, set(), {})
        self.assertEqual([c[1][0] for c in ex.calls], [os.path.join(d, "top")])
        self.assertEqual(vct.stats.subtrees_skipped_layout_invalid, 1)
        self.assertEqual(vct.stats.files_skipped_layout_invalid, 0)
        self.assertEqual(vct.stats.files_failed, 0)


class _FakeRemovalSession:
    """Stands in for RemovalSession off CephFS: records each rmdir and unlink
    and does it directly, past any patched os.rmdir; hang=True blocks past any timeout the tests use, and
    abort_hang=True makes abort() block the same way after it frees the
    rmdir (abort_conn waiting on the session's in-flight requests)."""

    def __init__(self, hang=False, ident=4242, abort_hang=False,
                 forever=False, abort_frees=True):
        # forever: the hangs wait with no timeout (the test's cleanup frees
        # them). abort_frees=False: abort() leaves a stuck rmdir stuck, as
        # abort_conn() does in 18.2.8, waiting on it instead.
        self.forever = forever
        self.abort_frees = abort_frees
        self.calls = []
        self.aborted = False
        self.closed = False
        self.hang = hang
        self.abort_hang = abort_hang
        self.ident = ident
        self.who = "client.test"
        self.fsname, self.mountpoint, self.subtree = "v", "/", "/"
        self.unlinks = []
        self._release = threading.Event()
        self._abort_release = threading.Event()

    def fs_path(self, local):
        rel = os.path.relpath(os.path.abspath(local), self.mountpoint)
        if rel == os.pardir or rel.startswith(os.pardir + os.sep):
            raise OSError(errno.EXDEV, f"not under {self.mountpoint}", local)
        return os.path.normpath(os.path.join(self.subtree, rel))

    def ino(self, local):
        self.fs_path(local)
        return os.stat(local).st_ino

    def rmdir(self, path):
        if self.aborted and not self.hang:
            raise OSError(errno.ENOTCONN, "aborted", path)
        self.calls.append(path)
        if self.hang:
            self._release.wait(None if self.forever else 5)
            raise OSError(errno.ENOTCONN, "aborted", path)
        _REAL_RMDIR(path)

    def unlink(self, path):
        self.unlinks.append(path)
        _REAL_UNLINK(path)

    def abort(self):
        self.aborted = True
        if self.abort_frees:
            self._release.set()
        if self.abort_hang:
            self._abort_release.wait(None if self.forever else 5)

    def close(self):
        self.closed = True


class StagingOnlyWithTargetPool(unittest.TestCase):
    """Without --target-pool nothing is staged: the regular replicated -> EC
    path stays exactly what it was. With it, only files whose directory points
    elsewhere are."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        self.f = os.path.join(self.d, "f")
        with open(self.f, "wb") as fh:
            fh.write(b"x" * 1000)
        old = time.time() - 30 * 86400
        os.utime(self.f, (old, old))
        self._saved = (vct._path_source, vct.run_journal, vct.file_delay_ms,
                       vct.runtime_config, vct.thread_count, vct.staging,
                       vct.get_layout_walking_up,
                       vct.CephLayout.__dict__["from_file"],
                       vct.CephLayout.__dict__["from_dir"])
        vct.file_delay_ms = 0
        vct.runtime_config = None
        vct.run_journal = None
        vct.thread_count = vct.DynamicSemaphore(100)
        self.dir_layout = vct.CephLayout(_L)
        vct.get_layout_walking_up = lambda d: self.dir_layout
        vct.CephLayout.from_dir = classmethod(lambda cls, p: self.dir_layout)
        src = vct.CephLayout(_L.replace("cephfs.v.ec4.2.qlc.data",
                                        "cephfs.v.ec6.3.ssd.data"))
        vct.CephLayout.from_file = classmethod(lambda cls, p: src)

    def tearDown(self):
        (vct._path_source, vct.run_journal, vct.file_delay_ms,
         vct.runtime_config, vct.thread_count, vct.staging,
         vct.get_layout_walking_up, from_file, from_dir) = self._saved
        vct.CephLayout.from_file = from_file
        vct.CephLayout.from_dir = from_dir

    def _args(self, **kw):
        a = dict(max_files=None, source_pool=None, min_size=0, max_size=None,
                 process_hardlinks=False, tmpdir="/nonexistent", dirs=[self.d],
                 stage_in_tmpdir=False, target_pool=None)
        a.update(kw)
        return Args(**a)

    def _paths(self, args):
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([self.f])
        vct.process_paths(args, {}, ex, {}, [self.d])
        return ex.calls

    def _walk(self, args):
        ex = _FakeExecutor()
        vct.process_dir(args, self.d, {}, ex, set(), {})
        return ex.calls

    def test_no_target_pool_never_stages(self):
        vct.staging = None
        for run in (self._paths, self._walk):
            calls = run(self._args())
            self.assertEqual(len(calls), 1)
            self.assertFalse(calls[0][5], "%s staged without --target-pool"
                             % run.__name__)
            self.assertEqual(calls[0][3], self.dir_layout)

    def test_target_pool_elsewhere_stages(self):
        vct.staging = vct.JobStaging("c" * 16, self.d)
        for run in (self._paths, self._walk):
            calls = run(self._args(target_pool="cephfs.v.data", max_size=614399))
            self.assertEqual(len(calls), 1)
            self.assertTrue(calls[0][5])
            self.assertEqual(calls[0][3].pool, "cephfs.v.data")

    def test_target_pool_equal_to_the_directory_does_not_stage(self):
        vct.staging = vct.JobStaging("c" * 16, self.d)
        for run in (self._paths, self._walk):
            calls = run(self._args(target_pool="cephfs.v.ec4.2.qlc.data",
                                   min_size=0))
            self.assertEqual(len(calls), 1)
            self.assertFalse(calls[0][5])

    def test_already_in_the_target_is_matched(self):
        vct.staging = vct.JobStaging("c" * 16, self.d)
        there = vct.CephLayout(_L.replace("cephfs.v.ec4.2.qlc.data",
                                          "cephfs.v.data"))
        vct.CephLayout.from_file = classmethod(lambda cls, p: there)
        for run in (self._paths, self._walk):
            self.assertEqual(
                run(self._args(target_pool="cephfs.v.data", max_size=614399)), [])

    def test_a_staging_dir_is_never_walked(self):
        vct.staging = None
        stage = os.path.join(self.d, vct.STAGE_PREFIX + "d" * 16)
        os.mkdir(stage)
        inside = os.path.join(stage, "copy")
        with open(inside, "wb") as fh:
            fh.write(b"y")
        os.utime(inside, (0, 0))
        saved = vct.TMP_ORPHAN_MIN_AGE_S
        vct.TMP_ORPHAN_MIN_AGE_S = 10 ** 9            # not aged: left alone
        try:
            calls = self._walk(self._args())
        finally:
            vct.TMP_ORPHAN_MIN_AGE_S = saved
        self.assertEqual([c[1][0] for c in calls], [self.f])
        self.assertTrue(os.path.exists(inside))

    def test_a_walk_never_removes_a_staging_dir(self):
        """No staging directory is removed mid-run; a killed run's leftover
        is --reap-staging's, never the walk's."""
        vct.staging = None
        stage = os.path.join(self.d, vct.STAGE_PREFIX + "d" * 16)
        os.mkdir(stage)
        tmp = os.path.join(stage, "." + "e" * 32 + vct.TMP_SUFFIX)
        open(tmp, "w").close()
        sess = _FakeRemovalSession()
        saved = (vct.TMP_ORPHAN_MIN_AGE_S, vct.removal_session)
        vct.TMP_ORPHAN_MIN_AGE_S = -1
        vct.removal_session = sess
        try:
            calls = self._walk(self._args())
        finally:
            vct.TMP_ORPHAN_MIN_AGE_S, vct.removal_session = saved
        self.assertEqual([c[1][0] for c in calls], [self.f])
        self.assertTrue(os.path.exists(tmp))
        self.assertEqual(sess.calls, [])

    def test_a_user_dir_with_a_similar_name_is_walked(self):
        """Control: matched on the whole shape, not the prefix."""
        vct.staging = None
        other = os.path.join(self.d, vct.STAGE_PREFIX + "notours")
        os.mkdir(other)
        g = os.path.join(other, "g")
        with open(g, "wb") as fh:
            fh.write(b"z")
        os.utime(g, (0, 0))
        calls = self._walk(self._args())
        self.assertIn(g, [c[1][0] for c in calls])

    def test_a_listed_path_inside_a_staging_dir_is_skipped(self):
        vct.staging = None
        stage = os.path.join(self.d, vct.STAGE_PREFIX + "d" * 16)
        os.mkdir(stage)
        inside = os.path.join(stage, "copy")
        open(inside, "w").close()
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([inside, self.f])
        with self.assertLogs(level="INFO") as cm:
            vct.process_paths(self._args(), {}, ex, {}, [self.d])
        self.assertEqual([c[1][0] for c in ex.calls], [self.f])
        self.assertTrue(any("inside a staging directory" in m for m in cm.output))


class ListedPathsStayOnTheVolume(unittest.TestCase):
    """A listed path is refused when a symlinked directory under its root, or
    another filesystem, would carry it off the volume: lists for one cluster
    reached another through dirs replaced by symlinks into its mount."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.out = tempfile.mkdtemp()
        for t in (self.d, self.out):
            self.addCleanup(shutil.rmtree, t, True)
        os.mkdir(os.path.join(self.d, "sub"))
        old = time.time() - 30 * 86400
        for f in (os.path.join(self.out, "f"), os.path.join(self.d, "sub", "f")):
            with open(f, "wb") as fh:
                fh.write(b"x" * 1000)
            os.utime(f, (old, old))
        os.symlink(self.out, os.path.join(self.d, "away"))
        os.symlink(os.path.join(self.d, "sub"), os.path.join(self.d, "alias"))
        self._saved = (vct._path_source, vct.run_journal, vct.file_delay_ms,
                       vct.runtime_config, vct.thread_count, vct.staging,
                       vct.get_layout_walking_up, vct.stats,
                       vct.CephLayout.__dict__["from_file"])
        vct.file_delay_ms = 0
        vct.runtime_config = None
        vct.run_journal = None
        vct.staging = None
        vct.stats = vct.Stats()
        vct.thread_count = vct.DynamicSemaphore(100)
        self.dir_layout = vct.CephLayout(_L)
        vct.get_layout_walking_up = lambda d: self.dir_layout
        src = vct.CephLayout(_L.replace("cephfs.v.ec4.2.qlc.data",
                                        "cephfs.v.ec6.3.ssd.data"))
        vct.CephLayout.from_file = classmethod(lambda cls, p: src)

    def tearDown(self):
        (vct._path_source, vct.run_journal, vct.file_delay_ms,
         vct.runtime_config, vct.thread_count, vct.staging,
         vct.get_layout_walking_up, vct.stats, from_file) = self._saved
        vct.CephLayout.from_file = from_file

    def _paths(self, *rel):
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent", dirs=[self.d],
                    stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([os.path.join(self.d, *rel)])
        vct.process_paths(args, {}, ex, {}, [self.d])
        return ex.calls

    def test_the_real_path_is_processed(self):
        self.assertEqual(len(self._paths("sub", "f")), 1)
        self.assertEqual(vct.stats.files_outside_root, 0)

    def test_a_symlink_off_the_root_is_not_followed(self):
        self.assertEqual(self._paths("away", "f"), [])
        self.assertEqual(vct.stats.files_outside_root, 1)

    def test_a_symlink_back_into_the_root_is_not_followed_either(self):
        self.assertEqual(self._paths("alias", "f"), [])
        self.assertEqual(vct.stats.files_outside_root, 1)

    def test_a_symlink_in_the_roots_own_path_is_allowed(self):
        link = os.path.join(self.out, "root-alias")
        os.symlink(self.d, link)
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent", dirs=[link],
                    stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([os.path.join(link, "sub", "f")])
        vct.process_paths(args, {}, ex, {}, [link])
        self.assertEqual(len(ex.calls), 1)

    def test_another_filesystem_is_refused(self):
        # A directory turned into a symlink after it was cached looks real
        # here; only the file's st_dev shows where it is.
        f = os.path.join(self.d, "sub", "f")
        real_stat = os.stat

        def stat(p, *a, **kw):
            s = real_stat(p, *a, **kw)
            if p == f:
                s = os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                    s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                    s.st_mtime, s.st_ctime))
            return s
        with unittest.mock.patch.object(vct.os, "stat", stat):
            self.assertEqual(self._paths("sub", "f"), [])
        self.assertEqual(vct.stats.files_outside_root, 1)

    def test_a_root_mounted_again_is_looked_at_again(self):
        # autofs expired the volume and mounted it again: the root and the
        # file are both on the new device, not the one read at the start.
        f = os.path.join(self.d, "sub", "f")
        real_stat = os.stat
        seen = {"root": 0}

        def stat(p, *a, **kw):
            s = real_stat(p, *a, **kw)
            if p == self.d:
                seen["root"] += 1
                if seen["root"] == 1:
                    return s
            elif p != f:
                return s
            return os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                   s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                   s.st_mtime, s.st_ctime))
        with unittest.mock.patch.object(vct.os, "stat", stat):
            self.assertEqual(len(self._paths("sub", "f")), 1)
        self.assertEqual(vct.stats.files_outside_root, 0)

    def test_a_root_that_cannot_be_stated_again_is_its_own_error(self):
        f = os.path.join(self.d, "sub", "f")
        real_stat = os.stat
        seen = {"root": 0}

        def stat(p, *a, **kw):
            if p == self.d:
                seen["root"] += 1
                if seen["root"] > 1:
                    raise OSError(errno.EIO, "Input/output error", p)
            s = real_stat(p, *a, **kw)
            if p != f:
                return s
            return os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                   s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                   s.st_mtime, s.st_ctime))
        with unittest.mock.patch.object(vct.os, "stat", stat):
            with self.assertLogs(level="WARNING") as logs:
                self.assertEqual(self._paths("sub", "f"), [])
        self.assertEqual(vct.stats.files_outside_root, 1)
        self.assertEqual(vct.stats.files_failed, 0)
        self.assertEqual(vct.stats.files_vanished, 0)
        self.assertTrue(any("Could not stat %s again" % self.d in m for m in logs.output))

    def test_a_nested_root_on_its_own_device_is_the_files_root(self):
        # dirs=[d, d/sub] with d/sub a separate mount: a file under d/sub is
        # checked against d/sub's device, not d's, and d is not looked at again.
        sub = os.path.join(self.d, "sub")
        f = os.path.join(sub, "f")
        real_stat = os.stat
        seen = {self.d: 0}

        def stat(p, *a, **kw):
            if p in seen:
                seen[p] += 1
            s = real_stat(p, *a, **kw)
            if p not in (sub, f):
                return s
            return os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                   s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                   s.st_mtime, s.st_ctime))
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent",
                    dirs=[self.d, sub], stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([f])
        with unittest.mock.patch.object(vct.os, "stat", stat):
            vct.process_paths(args, {}, ex, {}, [self.d, sub])
        self.assertEqual(len(ex.calls), 1)
        self.assertEqual(vct.stats.files_outside_root, 0)
        self.assertEqual(seen[self.d], 1)

    def test_a_nested_root_named_through_a_symlink_is_allowed(self):
        # dirs=[d, d/alias] with alias -> sub: the operator named the symlink,
        # so files under it are judged from d/alias, not from d.
        alias = os.path.join(self.d, "alias")
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent",
                    dirs=[self.d, alias], stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([os.path.join(alias, "f")])
        vct.process_paths(args, {}, ex, {}, [self.d, alias])
        self.assertEqual(len(ex.calls), 1)
        self.assertEqual(vct.stats.files_outside_root, 0)

    def test_a_root_that_cannot_be_stated_again_is_said_once(self):
        # Two files miss the root's device and the root keeps failing: both
        # are refused, and the root's error is logged once, not per file.
        f = os.path.join(self.d, "sub", "f")
        real_stat = os.stat
        seen = {"root": 0}

        def stat(p, *a, **kw):
            if p == self.d:
                seen["root"] += 1
                if seen["root"] > 1:
                    raise OSError(errno.EIO, "Input/output error", p)
            s = real_stat(p, *a, **kw)
            if p != f:
                return s
            return os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                   s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                   s.st_mtime, s.st_ctime))
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent", dirs=[self.d],
                    stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([f, f])
        with unittest.mock.patch.object(vct.os, "stat", stat):
            with self.assertLogs(level="WARNING") as logs:
                vct.process_paths(args, {}, ex, {}, [self.d])
        self.assertEqual(ex.calls, [])
        self.assertEqual(vct.stats.files_outside_root, 2)
        self.assertEqual(sum("Could not stat %s again" % self.d in m
                             for m in logs.output), 1)

    def test_only_the_files_own_root_is_looked_at_again(self):
        other = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, other, True)
        f = os.path.join(self.d, "sub", "f")
        real_stat = os.stat
        seen = {self.d: 0, other: 0}

        def stat(p, *a, **kw):
            if p in seen:
                seen[p] += 1
            s = real_stat(p, *a, **kw)
            if p != f:
                return s
            return os.stat_result((s.st_mode, s.st_ino, s.st_dev + 1, s.st_nlink,
                                   s.st_uid, s.st_gid, s.st_size, s.st_atime,
                                   s.st_mtime, s.st_ctime))
        args = Args(max_files=None, source_pool=None, min_size=0, max_size=None,
                    process_hardlinks=False, tmpdir="/nonexistent",
                    dirs=[self.d, other], stage_in_tmpdir=False, target_pool=None)
        ex = _FakeExecutor()
        vct._path_source = lambda a, r: iter([f])
        with unittest.mock.patch.object(vct.os, "stat", stat):
            vct.process_paths(args, {}, ex, {}, [self.d, other])
        self.assertEqual(seen[other], 1)
        self.assertEqual(seen[self.d], 2)

    def test_real_dir_resolves_each_directory_once(self):
        cache = {}
        calls = []
        real = os.path.islink
        with unittest.mock.patch.object(vct.os.path, "islink",
                                        lambda p: calls.append(p) or real(p)):
            vct._real_dir(os.path.join(self.d, "sub"), cache)
            n = len(calls)
            vct._real_dir(os.path.join(self.d, "sub"), cache)
            vct._real_dir(os.path.join(self.d, "alias"), cache)
        self.assertEqual(len(calls), n + 1)
        self.assertEqual(vct._real_dir(os.path.join(self.d, "alias"), cache),
                         os.path.realpath(os.path.join(self.d, "sub")))


class GroupByDirectory(unittest.TestCase):
    """A pool lists in object-hash order, so one directory's files are spread
    through it. Grouped, its files share one staging directory."""

    def setUp(self):
        self.spill = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.spill, True)

    def _grouped(self, paths, run_paths):
        return list(vct.group_by_directory(iter(paths), self.spill, run_paths))

    def _contiguous(self, out):
        seen, last = set(), None
        for p in out:
            d = os.path.dirname(p)
            if d != last:
                self.assertNotIn(d, seen, "directory %s split in two" % d)
                seen.add(d)
                last = d

    def test_groups_with_spills(self):
        paths = ["/v/%s/f%03d" % (d, i) for i in range(40) for d in "abcde"]
        out = self._grouped(paths, run_paths=7)
        self.assertEqual(sorted(out), sorted(paths))
        self._contiguous(out)
        self.assertEqual(os.listdir(self.spill), [], "spill files left behind")

    def test_a_subdirectory_does_not_split_its_parent(self):
        """Sorting on the whole path would put /v/a/b/y between /v/a/a and /v/a/c."""
        out = self._grouped(["/v/a/c", "/v/a/b/y", "/v/a/a"], run_paths=2)
        self._contiguous(out)

    def test_in_memory_when_small(self):
        out = self._grouped(["/v/b/1", "/v/a/1", "/v/b/2"], run_paths=100)
        self._contiguous(out)
        self.assertEqual(os.listdir(self.spill), [])

    def test_undecodable_bytes_round_trip(self):
        bad = b"/v/a/bad\xff".decode("utf-8", "surrogateescape")
        out = self._grouped([bad, "/v/b/1", "/v/a/ok"], run_paths=1)
        self.assertIn(bad, out)

    def test_an_abandoned_merge_cleans_up(self):
        g = vct.group_by_directory(iter(["/v/%d/f" % i for i in range(10)]),
                                   self.spill, 3)
        next(g)
        self.assertNotEqual(os.listdir(self.spill), [])
        g.close()
        self.assertEqual(os.listdir(self.spill), [])



class PoolListingRace(unittest.TestCase):
    """An object listed and then purged before its backtrace is read is gone,
    not an error. Raising killed the whole run part way through the listing --
    seen on a live volume, where a file transcoded away is purged from the pool the
    sweep is reading."""

    def setUp(self):
        import types
        self.mp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.mp, True)
        for n in ("a", "b"):
            open(os.path.join(self.mp, n), "w").close()

        class ObjectNotFound(Exception):
            pass

        class NoData(Exception):
            pass

        class Obj:
            def __init__(self, key):
                self.key = key

        class Ioctx:
            def list_objects(self):
                return [Obj("10000000001.00000000"), Obj("10000000002.00000000"),
                        Obj("10000000003.00000000")]

            def get_xattr(self, key, name):
                if key.startswith("10000000002."):
                    raise ObjectNotFound("Failed to get xattr 'parent'")
                return key.split(".")[0].encode()

            def close(self):
                pass

        class Rados:
            def __init__(self, **kw):
                pass

            def connect(self):
                pass

            def open_ioctx(self, pool):
                return Ioctx()

            def shutdown(self):
                pass

        fake = types.ModuleType("rados")
        fake.Rados, fake.ObjectNotFound, fake.NoData = Rados, ObjectNotFound, NoData
        self._saved = (sys.modules.get("rados"), vct._ceph_connect_args,
                       vct._mount_ancestry_prefix, vct.decode_backtrace,
                       getattr(os, "getxattr", None))
        sys.modules["rados"] = fake
        vct._ceph_connect_args = lambda *a: ("/x.conf", {})
        vct._mount_ancestry_prefix = lambda root: (self.mp, "/")
        names = {0x10000000001: b"a", 0x10000000003: b"b"}
        vct.decode_backtrace = lambda blob: (int(blob, 16), [names[int(blob, 16)]], 0)

        def no_layout(path, name, **kw):
            raise OSError(vct.ENODATA, "No data available")
        os.getxattr = no_layout

    def tearDown(self):
        rados, cca, map_, dbt, gx = self._saved
        if rados is None:
            sys.modules.pop("rados", None)
        else:
            sys.modules["rados"] = rados
        vct._ceph_connect_args, vct._mount_ancestry_prefix = cca, map_
        vct.decode_backtrace = dbt
        if gx is None:
            del os.getxattr
        else:
            os.getxattr = gx

    def test_a_purged_object_is_skipped_not_fatal(self):
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool=None,
                    max_size=None)
        with self.assertLogs(level="INFO") as cm:
            got = list(vct._iter_pool_paths(args, [self.mp]))
        self.assertEqual(got, [os.path.join(self.mp, "a"), os.path.join(self.mp, "b")])
        self.assertTrue(any("1 gone before read" in m for m in cm.output), cm.output)



class TmpdirConflictUnderTargetPool(unittest.TestCase):
    """The tmpdir check guards temps staged IN the tmpdir. A staged
    --target-pool run puts none there, and its usual target -- the replicated
    pool -- is the default pool the tmpdir belongs on, so the check must not
    refuse it."""

    def setUp(self):
        self._saved = vct.get_layout_walking_up
        vct.get_layout_walking_up = lambda d: vct.CephLayout(_L)   # dirs on Q

    def tearDown(self):
        vct.get_layout_walking_up = self._saved

    def _args(self, **kw):
        a = dict(dirs=["/v"], target_pool=None, stage_in_tmpdir=False)
        a.update(kw)
        return Args(**a)

    def test_a_staged_replicated_target_on_the_tmpdir_pool_is_fine(self):
        self.assertEqual(vct.tmpdir_conflicts(
            self._args(target_pool="cephfs.v.data"), "cephfs.v.data"), [])

    def test_stage_in_tmpdir_still_checks_the_target(self):
        self.assertEqual(vct.tmpdir_conflicts(
            self._args(target_pool="cephfs.v.data", stage_in_tmpdir=True),
            "cephfs.v.data"), [("/v", "cephfs.v.data")])

    def test_without_target_pool_it_is_the_old_check(self):
        self.assertEqual(vct.tmpdir_conflicts(self._args(),
                                              "cephfs.v.ec4.2.qlc.data"),
                         [("/v", "cephfs.v.ec4.2.qlc.data")])
        self.assertEqual(vct.tmpdir_conflicts(self._args(), "cephfs.v.data"), [])

    def test_a_refused_root_is_left_out_not_fatal(self):
        def walk_up(d):
            if d == "/w":
                raise vct.InvalidLayout("ceph.dir.layout of /w reads back as "
                                        "stripe_unit=0 stripe_count=0 object_size=0 pool=-1")
            return vct.CephLayout(_L)
        vct.get_layout_walking_up = walk_up
        with self.assertLogs(level="WARNING"):
            self.assertEqual(vct.tmpdir_conflicts(self._args(dirs=["/w", "/v"]),
                                                  "cephfs.v.ec4.2.qlc.data"),
                             [("/v", "cephfs.v.ec4.2.qlc.data")])


class TargetPoolKindFailsClosed(unittest.TestCase):
    """Without the ceph CLI the kind comes from the operator or a positive name
    match. Guessing is unsafe both ways: an EC pool read as replicated is sent
    exactly the small files the yield floor keeps off EC."""

    def setUp(self):
        self._saved = vct._pool_scheme
        vct._pool_scheme = lambda pool: None

    def tearDown(self):
        vct._pool_scheme = self._saved

    def test_an_unrecognized_name_is_refused(self):
        kind, why = vct.target_pool_kind("fastpool")
        self.assertIsNone(kind)
        self.assertIn("--target-pool-kind", why)

    def test_rdata_is_replicated(self):
        self.assertEqual(vct.target_pool_kind("cephfs.myvol.rdata")[0], "rep")

    def test_declared_kind_decides_without_a_cli(self):
        self.assertEqual(vct.target_pool_kind("fastpool", "ec"),
                         ("ec", "--target-pool-kind"))

    def test_a_declared_kind_the_cluster_contradicts_is_refused(self):
        vct._pool_scheme = lambda pool: ("ec", 4, 2, 4096)
        kind, why = vct.target_pool_kind("fastpool", "rep")
        self.assertIsNone(kind)
        self.assertIn("cluster", why)
        self.assertEqual(vct.target_pool_kind("fastpool", "ec"), ("ec", "cluster"))


class GroupingStopsAndYields(unittest.TestCase):
    """The pre-read can take hours on a large pool: an exit must not wait for
    it, and a short spill disk must not fill."""

    def setUp(self):
        self.spill = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.spill, True)

    def test_stop_ends_the_read_and_closes_the_source(self):
        closed = []
        n = [0]

        def source():
            try:
                for i in range(100):
                    n[0] += 1
                    yield "/v/%d/f" % (i % 7)
            finally:
                closed.append(True)

        out = list(vct.group_by_directory(source(), self.spill, 3,
                                          stop=lambda: n[0] >= 10))
        self.assertEqual(out, [])
        self.assertLess(n[0], 100, "kept reading after stop")
        self.assertEqual(closed, [True], "the source was not closed")
        self.assertEqual(os.listdir(self.spill), [])

    def test_short_space_falls_back_to_listing_order(self):
        saved = vct.shutil.disk_usage
        vct.shutil.disk_usage = lambda p: type("U", (), {"free": 0})()
        self.addCleanup(setattr, vct.shutil, "disk_usage", saved)
        paths = ["/v/%s/f%d" % (d, i) for i in range(5) for d in "ab"]
        with self.assertLogs(level="WARNING"):
            out = list(vct.group_by_directory(iter(paths), self.spill, 3))
        self.assertEqual(sorted(out), sorted(paths), "a path was lost or doubled")
        self.assertEqual(os.listdir(self.spill), [])


class WalkSurvivesAVanishedFile(StagingOnlyWithTargetPool):
    """A file listed by os.walk and deleted before its stat is a vanished file,
    as it already was for --paths-from -- not the end of the run."""

    # Only this test: the inherited ones already run under their own class.
    test_no_target_pool_never_stages = None
    test_target_pool_elsewhere_stages = None
    test_target_pool_equal_to_the_directory_does_not_stage = None
    test_already_in_the_target_is_matched = None
    test_a_staging_dir_is_never_walked = None
    test_a_walk_never_removes_a_staging_dir = None
    test_a_user_dir_with_a_similar_name_is_walked = None
    test_a_listed_path_inside_a_staging_dir_is_skipped = None

    def test_deleted_between_listing_and_stat(self):
        vct.staging = None
        other = os.path.join(self.d, "e_gone")
        open(other, "w").close()
        real = os.stat

        def racing_stat(p, *a, **kw):
            if p == other:
                raise FileNotFoundError(2, "No such file or directory", p)
            return real(p, *a, **kw)
        os.stat = racing_stat
        self.addCleanup(setattr, os, "stat", real)
        before = vct.stats.files_vanished
        calls = self._walk(self._args())
        self.assertEqual([c[1][0] for c in calls], [self.f])
        self.assertEqual(vct.stats.files_vanished, before + 1)



class PathsFromSurvivesAVanishedFile(StagingOnlyWithTargetPool):
    """A listed file deleted between its stat and its layout read is vanished,
    as in the walk -- a sweep of a live pool is where this happens most."""

    test_no_target_pool_never_stages = None
    test_target_pool_elsewhere_stages = None
    test_target_pool_equal_to_the_directory_does_not_stage = None
    test_already_in_the_target_is_matched = None
    test_a_staging_dir_is_never_walked = None
    test_a_walk_never_removes_a_staging_dir = None
    test_a_user_dir_with_a_similar_name_is_walked = None
    test_a_listed_path_inside_a_staging_dir_is_skipped = None

    def test_deleted_between_stat_and_layout_read(self):
        vct.staging = None

        def gone(cls, p):
            raise FileNotFoundError(2, "No such file or directory", p)
        vct.CephLayout.from_file = classmethod(gone)
        before = vct.stats.files_vanished
        self.assertEqual(self._paths(self._args()), [])
        self.assertEqual(vct.stats.files_vanished, before + 1)


class LivePoolListingWarns(PoolListingRace):
    """A sweep reads a pool still being written: its end-of-listing checks are
    expected to find fresh files without a backtrace, so WARNING, not ERROR."""

    test_a_purged_object_is_skipped_not_fatal = None

    def _unusable_run(self, root_pool):
        def layout(path, name, **kw):
            return root_pool.encode()
        os.getxattr = layout
        real = vct.decode_backtrace

        def none_for_three(blob):
            if int(blob, 16) == 0x10000000003:
                raise vct.BacktraceError("fresh, not yet flushed")
            return real(blob)
        vct.decode_backtrace = none_for_three
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool="R",
                    max_size=614399)
        with self.assertLogs(level="INFO") as cm:
            list(vct._iter_pool_paths(args, [self.mp]))
        return [r for r in cm.records if "could not be used" in r.getMessage()]

    def test_a_sweep_of_the_write_target_warns(self):
        recs = self._unusable_run("Q")
        self.assertEqual([r.levelno for r in recs], [logging.WARNING])

    def test_a_drain_still_errors(self):
        """Control: off the write target the same finding is still an ERROR."""
        recs = self._unusable_run("S")
        self.assertEqual([r.levelno for r in recs], [logging.ERROR])



class ReplicatedNameMatchesTheFilesystem(unittest.TestCase):
    """With the roots' filesystem known, only ITS default-shaped pool reads as
    replicated: cephfs.qlc.data has the shape but is not myvol's."""

    def setUp(self):
        self._saved = vct._pool_scheme
        vct._pool_scheme = lambda pool: None

    def tearDown(self):
        vct._pool_scheme = self._saved

    def test_only_the_roots_filesystem(self):
        self.assertEqual(vct.target_pool_kind("cephfs.myvol.data", None, {"myvol"}),
                         ("rep", "name, pinned to filesystem myvol"))
        self.assertEqual(vct.target_pool_kind("cephfs.myvol.rdata", None, {"myvol"})[0],
                         "rep")
        self.assertEqual(vct.target_pool_kind("cephfs.a.b.data", None, {"a.b"})[0], "rep",
                         "a filesystem name with a dot")
        self.assertIsNone(vct.target_pool_kind("cephfs.qlc.data", None, {"myvol"})[0])
        self.assertEqual(vct.target_pool_kind("cephfs.qlc.data", "ec", {"myvol"}),
                         ("ec", "--target-pool-kind"))


class PoolListingInheritedRootLayout(PoolListingRace):
    """A root with no layout of its own writes where it inherits from. Both the
    drain refusal and the live-pool level must see that."""

    test_a_purged_object_is_skipped_not_fatal = None

    def setUp(self):
        super().setUp()
        self._saved_glwu = vct.get_layout_walking_up
        vct.get_layout_walking_up = lambda p: vct.CephLayout(
            _L.replace("cephfs.v.ec4.2.qlc.data", "Q"))

    def tearDown(self):
        vct.get_layout_walking_up = self._saved_glwu
        super().tearDown()

    def test_a_drain_of_an_inherited_write_target_is_refused(self):
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool=None,
                    max_size=None)
        with self.assertRaises(SystemExit) as cm:
            list(vct._iter_pool_paths(args, [self.mp]))
        self.assertIn("still the default write target", str(cm.exception))


class VanishedDirectory(StagingOnlyWithTargetPool):
    """A directory removed between being listed and its layout read is skipped,
    in the walk and in --paths-from, not the end of the run."""

    test_no_target_pool_never_stages = None
    test_target_pool_elsewhere_stages = None
    test_target_pool_equal_to_the_directory_does_not_stage = None
    test_already_in_the_target_is_matched = None
    test_a_staging_dir_is_never_walked = None
    test_a_walk_never_removes_a_staging_dir = None
    test_a_user_dir_with_a_similar_name_is_walked = None
    test_a_listed_path_inside_a_staging_dir_is_skipped = None

    def _gone(self, *a, **kw):
        raise FileNotFoundError(2, "No such file or directory")

    def test_walk(self):
        vct.staging = None
        vct.CephLayout.from_dir = classmethod(lambda cls, p: self._gone())
        vct.get_layout_walking_up = lambda d: self._gone()
        with self.assertLogs(level="INFO") as cm:
            self.assertEqual(self._walk(self._args()), [])
        self.assertTrue(any("no longer exists" in m for m in cm.output))

    def test_paths(self):
        vct.staging = None
        vct.get_layout_walking_up = lambda d: self._gone()
        before = vct.stats.files_vanished
        self.assertEqual(self._paths(self._args()), [])
        self.assertEqual(vct.stats.files_vanished, before + 1)



class PoolListingUnreadableRootIsSaid(PoolListingRace):
    """An error other than "no layout here" leaves the root out of a check that
    fails open, so it must be logged, not skipped silently."""

    test_a_purged_object_is_skipped_not_fatal = None

    def test_eacces_is_warned(self):
        def denied(path, name, **kw):
            raise PermissionError(13, "Permission denied")
        os.getxattr = denied
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool=None,
                    max_size=None)
        with self.assertLogs(level="WARNING") as cm:
            list(vct._iter_pool_paths(args, [self.mp]))
        self.assertTrue(any("left out of the write-target check" in m
                            for m in cm.output), cm.output)



class RemovalInAChildProcess(unittest.TestCase):
    """The removal session's libcephfs mount runs in a child process, so a
    call stuck in the MDS is ended by killing the child. An in-process
    abort_conn() waits for the call in 18.2.8 while the client keeps renewing
    its session, so it can neither return nor let the MDS cancel the request.
    The child runs this script with a fake cephfs module on its PYTHONPATH."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        self.root = os.path.join(self.d, "root")
        os.mkdir(self.root)
        pkg = os.path.join(self.d, "fakepkg")
        os.mkdir(pkg)
        with open(os.path.join(pkg, "cephfs.py"), "w") as fh:
            fh.write(_FAKE_CEPHFS)
        saved = (vct._removal_worker_env, vct.removal_session, vct.thread_count,
                 vct.run_journal, vct.REMOVAL_SESSION_ABORT_TIMEOUT_S)
        self.addCleanup(self._restore, saved)
        self.name = vct.STAGE_PREFIX + "ab" * 8
        vct._removal_worker_env = dict(os.environ, PYTHONPATH=pkg,
                                       FAKE_CEPHFS_ROOT=self.root,
                                       FAKE_CEPHFS_HANG=self.name)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.run_journal = None
        vct.REMOVAL_SESSION_ABORT_TIMEOUT_S = 5

    @staticmethod
    def _restore(saved):
        (vct._removal_worker_env, vct.removal_session, vct.thread_count,
         vct.run_journal, vct.REMOVAL_SESSION_ABORT_TIMEOUT_S) = saved

    def _proc(self, fsname="v"):
        p = vct._RemovalProcess({}, None, "x", fsname, 30)
        self.addCleanup(p.abort_conn)
        return p

    def _session(self):
        p = self._proc()
        s = vct.RemovalSession(p, vct._CephfsErrors, "v", self.root, "/",
                               p.ident, "client.x")
        vct.removal_session = s
        return p, s

    def test_removals_go_through_a_child_process(self):
        p, s = self._session()
        self.assertNotEqual(p.pid, os.getpid())
        self.assertEqual(s.ident, 4321)
        sub = os.path.join(self.root, "sub")
        os.mkdir(sub)
        self.assertEqual(s.ino(sub), os.stat(sub).st_ino)
        f = os.path.join(sub, "f")
        open(f, "w").close()
        with self.assertRaises(OSError) as e:
            s.rmdir(sub)
        self.assertEqual(e.exception.errno, errno.ENOTEMPTY)
        s.unlink(f)
        s.rmdir(sub)
        self.assertFalse(os.path.exists(sub))
        with self.assertRaises(FileNotFoundError):
            s.rmdir(sub)
        s.close()
        self.assertIsNotNone(p._p.poll(), "the child outlived close()")

    def test_the_child_outlives_the_jobs_exit_signals(self):
        """systemd's stop sends SIGTERM to the whole unit; the child must
        still serve the job's end-of-run removal, then exit on close()."""
        p, s = self._session()
        os.kill(p.pid, signal.SIGTERM)
        os.kill(p.pid, signal.SIGINT)
        sub = os.path.join(self.root, "sub")
        os.mkdir(sub)
        s.rmdir(sub)
        self.assertFalse(os.path.exists(sub))
        s.close()
        self.assertIsNotNone(p._p.poll())

    def test_a_hung_stat_is_bounded_and_kills_the_child(self):
        saved = vct.REMOVAL_SESSION_MOUNT_TIMEOUT_S
        self.addCleanup(setattr, vct, "REMOVAL_SESSION_MOUNT_TIMEOUT_S", saved)
        vct.REMOVAL_SESSION_MOUNT_TIMEOUT_S = 0.5
        p = self._proc()
        os.mkdir(os.path.join(self.root, self.name))
        out = {}

        def run():
            try:
                p.stat(os.fsencode("/" + self.name))
            except RuntimeError as e:
                out["e"] = e
        th = threading.Thread(target=run, daemon=True)
        th.start()
        th.join(5)
        self.assertFalse(th.is_alive(), "a stat through the session was unbounded")
        self.assertIn("has not answered", str(out.get("e")))
        self.assertFalse(self._alive(p), "the stuck child was not killed")

    def test_the_child_dies_with_a_killed_job(self):
        """kill -9 of the job, the OOM killer, or a second SIGINT while a call
        is stuck: the child must not stay behind renewing its session."""
        import subprocess
        script = os.path.join(self.d, "job.py")
        with open(script, "w") as fh:
            fh.write("import importlib.util, threading, time\n"
                     f"spec = importlib.util.spec_from_file_location('vct', {os.path.abspath(TARGET)!r})\n"
                     "vct = importlib.util.module_from_spec(spec)\n"
                     "spec.loader.exec_module(vct)\n"
                     f"vct._removal_worker_env = {vct._removal_worker_env!r}\n"
                     "p = vct._RemovalProcess({}, None, 'x', 'v', 30)\n"
                     "print(p.pid, flush=True)\n"
                     f"threading.Thread(target=p.rmdir, args=({os.fsencode('/' + self.name)!r},), daemon=True).start()\n"
                     "time.sleep(60)\n")
        os.mkdir(os.path.join(self.root, self.name))
        job = subprocess.Popen([sys.executable, script], stdout=subprocess.PIPE)
        self.addCleanup(job.kill)
        child = int(job.stdout.readline())
        time.sleep(0.5)                       # the rmdir is in flight, stuck
        os.kill(child, 0)                     # alive while the job is
        os.kill(job.pid, signal.SIGKILL)
        job.wait(5)
        deadline = time.monotonic() + 5
        gone = False
        while time.monotonic() < deadline and not gone:
            try:
                os.kill(child, 0)
                time.sleep(0.1)
            except ProcessLookupError:
                gone = True
        if not gone:
            os.kill(child, signal.SIGKILL)
        self.assertTrue(gone, "the removal child outlived the killed job")

    def test_a_failed_mount_is_an_error_and_leaves_no_child(self):
        with self.assertRaises(vct._CephfsErrors.Error):
            vct._RemovalProcess({}, None, "x", "nosuchfs", 30)

    def _alive(self, p):
        return p._p.poll() is None

    def test_a_hung_end_of_run_removal_kills_the_child_and_the_job_goes_on(self):
        p, s = self._session()
        j = vct.JobStaging("ab" * 8, self.root, session=s, rmdir_timeout_s=0.5)
        stage = os.path.join(self.root, self.name)
        os.mkdir(stage)
        j._paths.append(stage)
        ticks = []
        stop = threading.Event()

        def tick():                     # the rest of the job, still running
            while not stop.is_set():
                ticks.append(time.monotonic())
                time.sleep(0.05)
        th = threading.Thread(target=tick, daemon=True)
        th.start()
        t0 = time.monotonic()
        with self.assertLogs(level="WARNING") as cm:
            self.assertEqual(j.remove_all(), [])
        elapsed = time.monotonic() - t0
        stop.set()
        th.join(2)
        self.assertLess(elapsed, 5, "the end-of-run pass waited on the MDS")
        self.assertFalse(self._alive(p), "the stuck child was not killed")
        self.assertGreater(len([t for t in ticks if t >= t0]), 5,
                           "the job stopped while the removal hung")
        self.assertIsNone(vct.removal_session)
        self.assertIn(stage, "\n".join(cm.output))
        self.assertTrue(os.path.isdir(stage))

    def test_a_hung_reap_removal_kills_the_child(self):
        p, _ = self._session()
        stage = os.path.join(self.root, self.name)
        os.mkdir(stage)
        t0 = time.monotonic()
        with self.assertLogs(level="CRITICAL"):
            self.assertEqual(vct._bounded_rmdir_staging(stage, timeout=0.5),
                             "hung")
        self.assertLess(time.monotonic() - t0, 5)
        self.assertFalse(self._alive(p), "the stuck child was not killed")
        self.assertIsNone(vct.removal_session)


class ReapStagingPacedAndGuarded(unittest.TestCase):
    """--reap-staging takes only what no live job can be using, stops on the
    first signal, and is paced like a walk: a reap is MDS load like a walk."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        saved = (vct.thread_count, vct.removal_session, vct.run_journal,
                 vct.file_delay_ms, vct._file_delay, vct._bounded_rmdir_staging)
        self.addCleanup(self._restore, saved)
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.run_journal = None
        vct.file_delay_ms = 0
        vct.removal_session = self.sess = _FakeRemovalSession()
        vct.do_exit.clear()
        self.addCleanup(vct.do_exit.clear)

    @staticmethod
    def _restore(saved):
        (vct.thread_count, vct.removal_session, vct.run_journal,
         vct.file_delay_ms, vct._file_delay, vct._bounded_rmdir_staging) = saved

    def _sd(self, name="a", c="a"):
        p = os.path.join(self.d, name, vct.STAGE_PREFIX + c * 16)
        os.makedirs(p)
        return p

    def _tmp(self, sd):
        p = os.path.join(sd, "." + "e" * 32 + vct.TMP_SUFFIX)
        open(p, "w").close()
        return p

    def _reap(self, min_age=0):
        return vct.reap_staging(Args(dirs=[self.d], paths_from=None,
                                     reap_staging_min_age=min_age))

    def _quiet(self, paths, age=7200):
        """lstat reports paths as unchanged for age seconds: ctime cannot be
        set from userspace."""
        real = os.lstat
        old = time.time() - age

        class Old:
            pass

        def fake(p, *a, **kw):
            st = real(p, *a, **kw)
            if p in paths:
                o = Old()
                o.st_mode, o.st_mtime, o.st_ctime = st.st_mode, old, old
                return o
            return st
        os.lstat = fake
        self.addCleanup(setattr, os, "lstat", real)

    def test_a_recently_changed_directory_is_left(self):
        sd = self._sd()
        with self.assertLogs(level="INFO") as cm:
            self.assertEqual(self._reap(min_age=3600), 0)
        self.assertEqual(self.sess.calls, [])
        self.assertTrue(os.path.isdir(sd))
        self.assertIn("1 changed in the last 3600s", "\n".join(cm.output))

    def test_aged_temps_go_with_it_through_the_session(self):
        sd = self._sd()
        tmp = self._tmp(sd)
        self._quiet({sd, tmp})
        kernel = []
        real = os.unlink
        os.unlink = lambda p, *a, **kw: (kernel.append(p), real(p, *a, **kw))
        self.addCleanup(setattr, os, "unlink", real)
        with self.assertLogs(level="INFO"):
            self.assertEqual(self._reap(min_age=3600), 0)
        self.assertEqual(self.sess.unlinks, [tmp])
        self.assertEqual(self.sess.calls, [sd])
        self.assertNotIn(tmp, kernel, "unlinked through the kernel mount")
        self.assertFalse(os.path.exists(sd))

    def test_a_temp_still_being_written_keeps_its_directory(self):
        sd = self._sd()
        tmp = self._tmp(sd)
        self._quiet({sd})            # the directory is quiet, the copy is not
        with self.assertLogs(level="INFO") as cm:
            self.assertEqual(self._reap(min_age=3600), 0)
        self.assertEqual(self.sess.calls, [])
        self.assertEqual(self.sess.unlinks, [])
        self.assertTrue(os.path.exists(tmp))
        self.assertIn("1 not empty", "\n".join(cm.output))

    def test_the_first_signal_stops_it(self):
        for c in "abcde":                    # one directory: one walk step
            self._sd("x", c)
        real = vct._bounded_rmdir_staging

        def remove_then_signal(*a, **kw):
            r = real(*a, **kw)
            vct.do_exit.set()            # what the first SIGINT's handler does
            return r
        vct._bounded_rmdir_staging = remove_then_signal
        with self.assertLogs(level="INFO") as cm:
            self._reap()
        self.assertEqual(len(self.sess.calls), 1, "kept removing after a signal")
        self.assertIn("stopped by a signal", "\n".join(cm.output))

    def test_a_signal_stops_the_search_for_candidates(self):
        """The walk for candidates can run for hours on a large volume; a
        directory left alone removed nothing to stop after."""
        for i, c in enumerate("abc"):
            self._sd(str(i), c)
        vct.do_exit.set()
        self.assertEqual(list(vct._staging_candidates(
            Args(dirs=[self.d], paths_from=None))), [])

    def test_a_pause_holds_it_and_the_file_delay_spaces_it(self):
        self._sd("a", "a")
        self._sd("b", "b")
        delays = []
        vct.file_delay_ms = 5
        vct._file_delay = lambda: delays.append(1)
        vct.thread_count.set_limit(0)                # threads = 0: paused
        done = threading.Event()

        def run():
            self._reap()
            done.set()
        t = threading.Thread(target=run, daemon=True)
        with self.assertLogs(level="INFO"):
            t.start()
            time.sleep(0.5)
            self.assertEqual(self.sess.calls, [], "removed while paused")
            vct.thread_count.set_limit(1)
            self.assertTrue(done.wait(5), "a resume did not release it")
        self.assertEqual(len(self.sess.calls), 2)
        # One delay per walked directory (the root, a, b) and per removal.
        self.assertEqual(len(delays), 5, "the file delay did not space the "
                         "walk and the removals")

    def test_a_pause_between_removals_holds_the_rest(self):
        """One directory's staging directories come from one walk step; a
        pause that lands between their removals still holds the rest."""
        for c in "abc":
            self._sd("x", c)
        real = vct._bounded_rmdir_staging
        first = []

        def remove_then_pause(*a, **kw):
            r = real(*a, **kw)
            if not first:
                first.append(1)
                vct.thread_count.set_limit(0)
            return r
        vct._bounded_rmdir_staging = remove_then_pause
        done = threading.Event()
        threading.Thread(target=lambda: (self._reap(), done.set()),
                         daemon=True).start()
        with self.assertLogs(level="INFO"):
            time.sleep(0.5)
            self.assertEqual(len(self.sess.calls), 1, "removed while paused")
            vct.thread_count.set_limit(1)
            self.assertTrue(done.wait(5))
        self.assertEqual(len(self.sess.calls), 3)

    def test_the_candidate_walk_is_held_by_a_pause(self):
        """Without --paths-from the walk is the expensive part of a reap."""
        self._sd("a", "a")
        vct.thread_count.set_limit(0)
        got = []
        done = threading.Event()

        def run():
            got.extend(vct._staging_candidates(Args(dirs=[self.d],
                                                    paths_from=None)))
            done.set()
        threading.Thread(target=run, daemon=True).start()
        time.sleep(0.4)
        self.assertEqual(got, [], "walked while paused")
        vct.thread_count.set_limit(1)
        self.assertTrue(done.wait(5))
        self.assertEqual(len(got), 1)


class RegulatorBlindThrottle(unittest.TestCase):
    """A RUNNING job with no usable sample for regulate_blind_throttle_s drops
    to 1 thread and regulate_blind_delay_ms, and a usable sample hands control
    back: through a Prometheus outage a job holding what it had earned runs
    with nothing watching the MDS."""

    def setUp(self):
        saved = (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
                 vct.do_exit, vct.stats)
        self.addCleanup(self._restore, saved)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.file_delay_ms = 2
        vct.stats = vct.Stats()

    @staticmethod
    def _restore(saved):
        (vct.thread_count, vct.file_delay_ms, vct._setproctitle,
         vct.do_exit, vct.stats) = saved

    def _reg(self, throttle_s=600, delay=20, threads=4, maxt=8):
        a = Args(regulate_prometheus_url="http://x", regulate_query="q",
                 regulate_pause_ms=150.0, regulate_slo_ms=51.0,
                 regulate_period_s=30, regulate_floor_ms=2,
                 regulate_quiet_ticks=3, dirs=["/x"], threads=threads,
                 regulate_max_threads=maxt, regulate_blind_resume_s=1800,
                 regulate_blind_throttle_s=throttle_s,
                 regulate_blind_delay_ms=delay)
        return vct.Regulator(a, "q")

    def _tick(self, r, sample):
        r.sample = sample

        class OneShot:
            def __init__(self):
                self.n = 0

            def is_set(self):
                self.n += 1
                return self.n > 1

            def wait(self, _):
                return None
        vct.do_exit = OneShot()
        r.run()

    @staticmethod
    def _down():
        raise OSError("HTTP Error 504: Gateway Time-out")

    @staticmethod
    def _nan():
        raise vct.NanSample("query returned nan")

    def test_a_short_outage_holds(self):
        r = self._reg()
        self._tick(r, self._down)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (4, 2))

    def test_a_long_outage_throttles(self):
        r = self._reg()
        self._tick(r, self._down)
        r._blind_run_since -= 601
        with self.assertLogs(level="ERROR") as cm:
            self._tick(r, self._down)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (1, 20))
        self.assertTrue(any("running BLIND" in m for m in cm.output))

    def test_a_usable_sample_hands_control_back(self):
        r = self._reg()
        self._tick(r, self._down)
        r._blind_run_since -= 601
        self._tick(r, self._down)
        with self.assertLogs(level="WARNING") as cm:
            self._tick(r, lambda: 1.0)
        self.assertIsNone(r._blind_run_since)
        self.assertFalse(r._blind_throttled)
        self.assertTrue(any("usable sample again" in m for m in cm.output))
        self.assertEqual(vct.thread_count.limit, 4, "threads not paid back")

    def _throttled(self, r):
        self._tick(r, self._down)
        r._blind_run_since -= 601
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 1)

    def test_paid_back_with_thread_adaptivity_off(self):
        """regulate_max_threads = 0 is the default: _maybe_raise_threads()
        never adds a thread, so only the debt brings them back."""
        r = self._reg(maxt=0)
        self._throttled(r)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4,
                         "stayed at 1 thread after Prometheus came back")

    def test_a_soft_band_sample_pays_back_to_threads_only(self):
        r = self._reg(maxt=0, threads=2)
        self._throttled(r)
        self._tick(r, lambda: 60.0)              # over the target, under the pause
        self.assertEqual(vct.thread_count.limit, 2)
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_an_operator_write_after_the_throttle_forgets_the_debt(self):
        r = self._reg(maxt=0)
        self._throttled(r)
        vct.thread_count.set_limit(2)            # SIGUSR1 or a threads edit
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 2)

    def test_zero_holds_forever(self):
        r = self._reg(throttle_s=0)
        self._tick(r, self._down)
        r._blind_run_since -= 10 ** 6
        self._tick(r, self._down)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (4, 2))

    def test_an_operator_write_wins(self):
        r = self._reg()
        self._tick(r, self._down)
        r._blind_run_since -= 601
        vct.thread_count.set_limit_if = lambda gen, n: None   # operator raced it
        self._tick(r, self._down)
        self.assertEqual(vct.thread_count.limit, 4)

    def _paused_long(self, sample):
        """A blind clock already past the bound, then an operator pause."""
        r = self._reg()
        self._tick(r, self._down)
        r._blind_run_since -= 601
        vct.thread_count.set_limit(0)            # threads = 0, or SIGUSR2
        logs = []
        handler = logging.Handler()
        handler.emit = logs.append
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)
        self._tick(r, sample)
        return r, [rec.getMessage() for rec in logs]

    def test_a_paused_job_on_a_quiet_volume_is_not_blind(self):
        """A paused job sends the MDS nothing, so a quiet volume reads nan."""
        r, logs = self._paused_long(self._nan)
        self.assertEqual(vct.file_delay_ms, 2, "the operator's later resume "
                         "starts from a delay raised during the pause")
        self.assertFalse(any("running BLIND" in m for m in logs), logs)
        self.assertIsNone(r._blind_run_since)

    def test_an_outage_during_a_pause_is_not_blind_either(self):
        r, logs = self._paused_long(self._down)
        self.assertEqual(vct.file_delay_ms, 2)
        self.assertFalse(any("running BLIND" in m for m in logs), logs)

    def test_the_clock_restarts_when_the_job_runs_again(self):
        r, _ = self._paused_long(self._nan)
        vct.thread_count.set_limit(4)            # SIGUSR1
        self._tick(r, self._down)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (4, 2),
                         "throttled on blindness from before the resume")

    def test_nan_while_files_finish_is_not_blind(self):
        """A nan-only query on a job that keeps finishing files is a window
        too short for its rate, not a stalled MDS."""
        r = self._reg()
        self._tick(r, self._nan)
        r._blind_run_since -= 601
        vct.stats.files_transcoded += 1
        self._tick(r, self._nan)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (4, 2))
        self.assertIsNone(r._blind_run_since)

    def test_an_error_stretch_resets_the_progress_baseline(self):
        """A Prometheus mixing 504s with an occasional nan must still throttle:
        progress is measured since the previous sample, not the previous nan."""
        r = self._reg()
        self._tick(r, self._nan)                 # baseline X
        self._tick(r, self._down)                # the clock starts
        r._blind_run_since -= 595
        vct.stats.files_transcoded += 1          # finished during the 504s
        self._tick(r, self._nan)
        self.assertIsNotNone(r._blind_run_since,
                             "one nan after an error stretch restarted the clock")
        r._blind_run_since -= 10
        self._tick(r, self._down)
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (1, 20),
                         "one nan after an error stretch restarted the clock")

    def test_nan_with_no_progress_still_throttles(self):
        r = self._reg()
        self._tick(r, self._nan)
        r._blind_run_since -= 601
        self._tick(r, self._nan)                 # nothing finished: stalled?
        self.assertEqual((vct.thread_count.limit, vct.file_delay_ms), (1, 20))

    def test_a_throttled_job_on_a_nan_only_query_gets_its_threads_back(self):
        """Paid back to --threads after regulate_quiet_ticks (3) samples of
        nan with files finishing; the rest waits for a usable sample."""
        r = self._reg(maxt=0, threads=2)
        self._throttled(r)
        self._tick(r, self._nan)                 # baseline for the file count
        for _ in range(3):
            vct.stats.files_transcoded += 1
            self._tick(r, self._nan)
        self.assertEqual(vct.thread_count.limit, 2,
                         "a nan-only query held the throttle forever")
        self._tick(r, lambda: 1.0)
        self.assertEqual(vct.thread_count.limit, 4)

    def test_the_keys_parse_and_route(self):
        for k in ("regulate_blind_throttle_s", "regulate_blind_delay_ms"):
            self.assertIn(k, vct.RuntimeConfig.KEYS)
            self.assertIn(k, dict(vct.REGULATE_APPLY))
            self.assertIn(k, vct.REG_DEFAULTS)
            out, errs = vct.RuntimeConfig._parse("%s = 0\n" % k)
            self.assertEqual((out, errs), ({k: 0}, []))
            _, errs = vct.RuntimeConfig._parse("%s = -1\n" % k)
            self.assertTrue(errs)


class ConfigPolledOffTheWalker(unittest.TestCase):
    """--config is read even while the walker is blocked, on a hung
    filesystem call say -- including the edit meant to slow it down."""

    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        self.conf = os.path.join(tmp, "c.conf")
        with open(self.conf, "w") as fh:
            fh.write("threads = 4\n")
        saved = (vct.runtime_config, vct.apply_config, vct.do_exit)
        self.addCleanup(self._restore, saved)
        self.applied = []
        vct.apply_config = lambda cfg: self.applied.append(dict(cfg))
        vct.runtime_config = vct.RuntimeConfig(self.conf, 0.01)
        vct.do_exit = threading.Event()
        self.addCleanup(vct.do_exit.set)
        vct.runtime_config.poll(vct.apply_config)            # startup read

    @staticmethod
    def _restore(saved):
        vct.runtime_config, vct.apply_config, vct.do_exit = saved

    def test_an_edit_applies_with_the_walker_blocked(self):
        t = vct._start_config_poller(0.01)
        with open(self.conf, "w") as fh:
            fh.write("threads = 0\n")
        later = time.time() + 5
        os.utime(self.conf, (later, later))
        deadline = time.monotonic() + 5
        while (time.monotonic() < deadline
               and not any(c.get("threads") == 0 for c in self.applied)):
            time.sleep(0.05)
        self.assertTrue(any(c.get("threads") == 0 for c in self.applied),
                        "the edit was never applied without the walker")
        vct.do_exit.set()
        t.join(3)

    def test_startup_starts_the_poller(self):
        """The thread above is only useful if main() starts it, after the
        startup read so the two cannot race for the first apply."""
        with open(os.path.abspath(TARGET)) as fh:
            src = fh.read()
        i = src.index("runtime_config.poll(apply_config)     # apply once at startup")
        j = src.index("_start_config_poller(args.config_poll_seconds)")
        self.assertLess(i, j)
        self.assertLess(j - i, 200, "poller not started right after the startup read")


class RemovalThroughASecondSession(unittest.TestCase):
    """Staging directories are removed through a separate CephFS session.

    An rmdir through the kernel mount can wait on the MDS recalling caps from
    the very client that sent it. So removals go out on a session that did
    none of the creates and renames, a hang aborts that session instead of
    needing the kernel mount evicted, and with no second session there is no
    staging."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        saved = (vct.thread_count, vct._setproctitle, vct.run_journal,
                 vct.removal_session, vct.staging, vct.file_delay_ms)
        self.addCleanup(self._restore, saved)
        vct._setproctitle = None
        vct.thread_count = vct.DynamicSemaphore(4)
        vct.run_journal = None
        vct.staging = None
        vct.file_delay_ms = 0

    @staticmethod
    def _restore(saved):
        (vct.thread_count, vct._setproctitle, vct.run_journal,
         vct.removal_session, vct.staging, vct.file_delay_ms) = saved

    def _no_kernel_rmdir(self):
        """Refuse os.rmdir, and record attempts: a refusal raised inside a
        removal thread is swallowed there, so tests assert on the record."""
        self.kernel_rmdirs = []
        real = os.rmdir

        def refuse(p, *a, **kw):
            self.kernel_rmdirs.append(p)
            raise AssertionError(f"rmdir({p}) through the kernel mount")
        os.rmdir = refuse
        self.addCleanup(setattr, os, "rmdir", real)

    def test_fs_path_maps_a_subtree_mount(self):
        sess = vct.RemovalSession(None, None, "myvol", "/shared/ceph/myvol",
                                  "/myvol", 7, "client.cephfs")
        self.assertEqual(sess.fs_path("/shared/ceph/myvol"), "/myvol")
        self.assertEqual(sess.fs_path("/shared/ceph/myvol/a/b/.vcephfs-tc-stage-x"),
                         "/myvol/a/b/.vcephfs-tc-stage-x")
        root = vct.RemovalSession(None, None, "t", "/mnt/t", "/", 7, "c")
        self.assertEqual(root.fs_path("/mnt/t/a"), "/a")
        with self.assertRaises(OSError) as e:
            sess.fs_path("/shared/ceph/myvol2/a")
        self.assertEqual(e.exception.errno, errno.EXDEV)

    def test_the_mount_table_wins_over_an_autofs_parent(self):
        """findmnt --target answers /shared/ceph (autofs) for a volume mounted
        at /shared/ceph/myvol; the mount table names the CephFS mount."""
        tab = os.path.join(self.d, "mounts")
        vol = os.path.join(os.path.realpath(self.d), "shared", "ceph", "myvol")
        os.makedirs(os.path.join(vol, "fits"))
        parent = os.path.dirname(vol)
        with open(tab, "w") as fh:
            fh.write(f"auto.ceph {parent} autofs rw,relatime,indirect 0 0\n")
            fh.write(f"10.1.1.1:6789,10.1.1.2:6789:/myvol {vol} ceph "
                     f"rw,name=cephfs,fsid=x,mds_namespace=myvol 0 0\n")
            fh.write(f"10.1.1.1:6789:/ {os.path.join(self.d, 'other')} ceph "
                     f"rw,name=cephfs,mds_namespace=other 0 0\n")
        mp, src, opts = vct._ceph_mount_for(os.path.join(vol, "fits"), tab)
        self.assertEqual(mp, os.path.realpath(vol))
        self.assertEqual(opts["mds_namespace"], "myvol")
        self.assertEqual(vct._source_subtree(src), "/myvol")
        self.assertEqual(vct._mount_mon_ips(src), {"10.1.1.1", "10.1.1.2"})
        self.assertIsNone(vct._ceph_mount_for(parent, tab), "autofs is not CephFS")
        self.assertEqual(vct._source_subtree("cephfs@fsid.myvol=/sub"), "/sub")
        self.assertEqual(vct._source_subtree("10.1.1.1:6789:/"), "/")

    def _fake_open(self, ino_offset=0):
        """RemovalSession.open over a fake cephfs module and an autofs volume
        that mounts only when it is read, not on stat()."""
        vol = os.path.join(os.path.realpath(self.d), "myvol")
        os.mkdir(vol)
        listed = []
        made = {}

        class FS:
            def __init__(self, conf, conffile, auth_id, fsname, timeout):
                made.update(auth_id=auth_id, conf=conf, aborted=False,
                            fsname=fsname.encode())

            def get_instance_id(self):
                return 7

            def stat(self, p):
                made["stat"] = p
                return os.stat_result([0, os.stat(vol).st_ino + ino_offset]
                                      + [0] * 8)

            def abort_conn(self):
                made["aborted"] = True

        self.addCleanup(setattr, vct, "_RemovalProcess", vct._RemovalProcess)
        vct._RemovalProcess = FS
        real_listdir = os.listdir
        self.addCleanup(setattr, os, "listdir", real_listdir)
        os.listdir = lambda p=".": (listed.append(p), real_listdir(p))[1]
        self.addCleanup(setattr, vct, "_ceph_mount_for", vct._ceph_mount_for)
        vct._ceph_mount_for = lambda p, mounts=None: (
            (vol, "10.1.1.1:6789:/myvol",
             {"name": "cephfs", "mds_namespace": "myvol"}) if listed else None)
        saved_id = vct.RemovalSession.__dict__["_identity"]
        self.addCleanup(setattr, vct.RemovalSession, "_identity", saved_id)
        vct.RemovalSession._identity = staticmethod(
            lambda a, r, o, s="": (None, {}, "client.cephfs"))
        return vct.RemovalSession.open(Args(), vol), made, vol

    def test_open_mounts_an_idle_autofs_volume_and_proves_the_mapping(self):
        (s, why), made, vol = self._fake_open()
        self.assertIsNone(why)
        self.assertEqual((made["auth_id"], made["fsname"], made["stat"]),
                         ("cephfs", b"myvol", b"/myvol"))
        self.assertEqual(s.fs_path(os.path.join(vol, "a")), "/myvol/a")
        self.assertEqual(s.ident, 7)

    def test_open_refuses_a_mapping_to_another_inode(self):
        (s, why), made, _ = self._fake_open(ino_offset=1)
        self.assertIsNone(s)
        self.assertIn("different inode", why)
        self.assertTrue(made["aborted"])

    def test_rmdir_and_unlink_raise_builtin_errors(self):
        class FakeCephfs:
            class Error(Exception):
                def __init__(self, errno_=None, msg=""):
                    super().__init__(msg)
                    self.errno = errno_

            class ObjectNotFound(Error):
                pass

        class FS:
            def __init__(self, exc):
                self.exc = exc
                self.paths = []

            def rmdir(self, p):
                self.paths.append(("rmdir", p))
                raise self.exc

            def unlink(self, p):
                self.paths.append(("unlink", p))
                raise self.exc
        for op in ("rmdir", "unlink"):
            for exc, want in ((FakeCephfs.ObjectNotFound(errno.ENOENT),
                               FileNotFoundError),
                              (FakeCephfs.Error(errno.ENOTEMPTY, "not empty"),
                               OSError)):
                fs = FS(exc)
                sess = vct.RemovalSession(fs, FakeCephfs, "v", "/m", "/v", 1, "c")
                with self.assertRaises(want) as cm:
                    getattr(sess, op)("/m/x")
                self.assertEqual(fs.paths, [(op, b"/v/x")])
                if want is OSError:
                    self.assertEqual(cm.exception.errno, errno.ENOTEMPTY)
            dead = vct.RemovalSession(FS(None), FakeCephfs, "v", "/m", "/v", 1, "c")
            dead._dead = True
            with self.assertRaises(OSError):
                getattr(dead, op)("/m/x")

    def test_identity_follows_the_mounts_fsid(self):
        etc = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, etc, True)
        fa, fb = "a" * 8 + "-" + "a" * 4 + "-" + "a" * 4 + "-" + "a" * 4 + "-" + "a" * 12, \
                 "b" * 8 + "-" + "b" * 4 + "-" + "b" * 4 + "-" + "b" * 4 + "-" + "b" * 12
        for stem, fsid, mon in (("one", fa, "10.1.1.1"), ("two", fb, "10.2.2.2")):
            with open(os.path.join(etc, stem + ".conf"), "w") as fh:
                fh.write(f"[global]\n\tfsid = {fsid}\n"
                         f"\tmon_host = [v2:{mon}:3300/0,v1:{mon}:6789/0]\n")
            open(os.path.join(etc, f"{stem}.client.cephfs.keyring"), "w").close()
        src = "10.2.2.2:6789,10.2.2.3:6789:/v"
        import glob as _glob
        real = _glob.glob
        _glob.glob = lambda pat: real(pat.replace("/etc/ceph", etc))
        real_exists = os.path.exists
        os.path.exists = lambda p: real_exists(p.replace("/etc/ceph", etc))
        self.addCleanup(setattr, _glob, "glob", real)
        self.addCleanup(setattr, os.path, "exists", real_exists)
        args = Args(rados_name=None, rados_keyring=None, rados_conffile=None,
                    rados_mon_host=None)
        conf, over, who = vct.RemovalSession._identity(
            args, "/x", {"name": "cephfs", "fsid": fb}, src)
        self.assertTrue(conf.endswith("two.conf"))
        self.assertTrue(over["keyring"].endswith("two.client.cephfs.keyring"))
        self.assertEqual(who, "client.cephfs")
        with self.assertRaises(ValueError):
            vct.RemovalSession._identity(args, "/x", {"name": "nobody", "fsid": fb},
                                         src)
        # mount.ceph without a readable conf reports an all-zero fsid: the
        # cluster is then picked by the monitors in the mount source.
        zero = {"name": "cephfs", "fsid": "00000000-0000-0000-0000-000000000000"}
        conf, over, _ = vct.RemovalSession._identity(args, "/x", zero, src)
        self.assertTrue(conf.endswith("two.conf"), "matched the wrong cluster")
        with self.assertRaises(ValueError):
            vct.RemovalSession._identity(args, "/x", zero, "10.9.9.9:6789:/v")
        explicit = Args(rados_name="client.admin", rados_keyring="/k",
                        rados_conffile="/c.conf", rados_mon_host=None)
        self.assertEqual(vct.RemovalSession._identity(
            explicit, "/x", {"name": "cephfs", "fsid": fb}),
            ("/c.conf", {"keyring": "/k"}, "client.admin"))

    def _reap_args(self, paths_from=None):
        # Age 0: these directories were made a moment ago, and the age guard
        # has its own tests (ReapStagingPacedAndGuarded).
        return Args(dirs=[self.d], paths_from=paths_from,
                    reap_staging_min_age=0)

    def test_reap_removes_empty_staging_dirs_and_leaves_the_rest(self):
        empty = os.path.join(self.d, "a", vct.STAGE_PREFIX + "c" * 16)
        full = os.path.join(self.d, "b", vct.STAGE_PREFIX + "d" * 16)
        lookalike = os.path.join(self.d, "c", vct.STAGE_PREFIX + "nothex")
        for p in (empty, full, lookalike):
            os.makedirs(p)
        open(os.path.join(full, "keep"), "w").close()
        vct.removal_session = sess = _FakeRemovalSession()
        with self.assertLogs(level="INFO") as cm:
            rc = vct.reap_staging(self._reap_args())
        self.assertEqual(rc, 0)
        self.assertEqual(sess.calls, [empty])
        self.assertFalse(os.path.exists(empty))
        self.assertTrue(os.path.exists(os.path.join(full, "keep")))
        self.assertTrue(os.path.isdir(lookalike))
        self.assertTrue(any("2 staging directories found, 1 removed, 1 not empty"
                            in m for m in cm.output))

    def test_reap_from_a_list_reads_only_the_listed_directories(self):
        listed = os.path.join(self.d, "listed")
        unlisted = os.path.join(self.d, "unlisted")
        a = os.path.join(listed, vct.STAGE_PREFIX + "e" * 16)
        b = os.path.join(unlisted, vct.STAGE_PREFIX + "f" * 16)
        os.makedirs(a)
        os.makedirs(b)
        lst = os.path.join(self.d, "dirs.txt")
        with open(lst, "w") as fh:
            fh.write(listed + "\n" + listed + "\n" + "/elsewhere/x\n")
        vct.removal_session = sess = _FakeRemovalSession()
        with self.assertLogs(level="INFO"):
            self.assertEqual(vct.reap_staging(self._reap_args(lst)), 0)
        self.assertEqual(sess.calls, [a], "a listed dir read twice, or a walk")
        self.assertTrue(os.path.isdir(b))

    def test_reap_stops_and_aborts_on_a_hung_removal(self):
        for c in "01":
            os.makedirs(os.path.join(self.d, c, vct.STAGE_PREFIX + c * 16))
        vct.removal_session = sess = _FakeRemovalSession(hang=True)
        saved = vct.STAGE_RMDIR_TIMEOUT_S
        vct.STAGE_RMDIR_TIMEOUT_S = 0.2
        self.addCleanup(setattr, vct, "STAGE_RMDIR_TIMEOUT_S", saved)
        with self.assertLogs(level="CRITICAL"):
            self.assertEqual(vct.reap_staging(self._reap_args()), 1)
        self.assertTrue(sess.aborted)
        self.assertEqual(len(sess.calls), 1, "kept removing after a hang")
        self.assertIsNone(vct.removal_session)

    @unittest.skipUnless(hasattr(signal, "SIGRTMIN"),
                         "main() installs the real-time signal handlers")
    def test_the_cli_reaps_without_a_tmpdir(self):
        """On a transcode host --reap-staging died on the default tmpdir's
        layout before it reached the reap; reaping needs no tmpdir."""
        sd = os.path.join(self.d, "a", vct.STAGE_PREFIX + "a" * 16)
        os.makedirs(sd)
        sess = _FakeRemovalSession()
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(lambda cls, a, r: (sess, None))
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        self._cli_cleanups()
        sys.argv = ["tc", "--reap-staging", "--reap-staging-min-age", "0",
                    "--tmpdir", os.path.join(self.d, "no-such-tmpdir"), self.d]
        with self.assertLogs(level="INFO"), \
                self.assertRaises(SystemExit) as cm:
            vct.main()
        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(sess.calls, [sd])
        self.assertTrue(sess.closed)

    def _cli_cleanups(self):
        """Undo what a reap through main() sets: argv, euid, every signal
        handler it installs, and the regulator and tunable globals."""
        self.addCleanup(setattr, sys, "argv", list(sys.argv))
        self.addCleanup(setattr, os, "geteuid", os.geteuid)
        sigs = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGTSTP,
                signal.SIGUSR1, signal.SIGUSR2]
        sigs += [signal.SIGRTMIN + i for i in range(5)]
        for sig in sigs:
            self.addCleanup(signal.signal, sig, signal.getsignal(sig))
        saved = (vct.regulator, vct.regulator_started, vct.min_age_days,
                 vct.runtime_config, vct.apply_config)

        def restore():
            (vct.regulator, vct.regulator_started, vct.min_age_days,
             vct.runtime_config, vct.apply_config) = saved
        self.addCleanup(restore)
        os.geteuid = lambda: 0

    def _start(self, **kw):
        a = dict(reap_staging=False, target_pool=None, stage_in_tmpdir=False,
                 dirs=[self.d])
        a.update(kw)
        return vct.start_removal_session(Args(**a))

    def test_staging_is_refused_without_a_second_session(self):
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(lambda cls, a, r: (None, "no bindings"))
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        for kw in ({"target_pool": "cephfs.v.data"}, {"reap_staging": True}):
            with self.assertLogs(level="ERROR") as cm:
                with self.assertRaises(SystemExit) as e:
                    self._start(**kw)
            self.assertEqual(e.exception.code, 1)
            msg = "\n".join(cm.output)
            self.assertIn("Refusing to start", msg)
            self.assertIn("no bindings", msg)
        self.assertIsNone(vct.removal_session)

    def test_no_session_is_needed_where_nothing_stages(self):
        def boom(cls, a, r):
            raise AssertionError("opened a session for a run that never stages")
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(boom)
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        self.assertIsNone(self._start())
        self.assertIsNone(self._start(target_pool="cephfs.v.data",
                                      stage_in_tmpdir=True))

    def test_a_staging_run_opens_one_and_logs_it(self):
        sess = _FakeRemovalSession(ident=99)
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(lambda cls, a, r: (sess, None))
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        with self.assertLogs(level="INFO") as cm:
            self.assertIs(self._start(target_pool="cephfs.v.data"), sess)
        self.assertIs(vct.removal_session, sess)
        self.assertTrue(any("client.99" in m for m in cm.output))

    # The reap's abort is bounded; every root must be on the session's mount;
    # --reap-staging journals.

    _BOUNDS = ("REMOVAL_SESSION_ABORT_TIMEOUT_S", "STAGE_RMDIR_TIMEOUT_S")

    def _bound(self, abort=0.2, rmdir=None):
        # getattr: the hollow check runs these against code without them.
        saved = {n: getattr(vct, n, None) for n in self._BOUNDS}
        self.addCleanup(self._unbound, saved)
        vct.REMOVAL_SESSION_ABORT_TIMEOUT_S = abort
        if rmdir is not None:
            vct.STAGE_RMDIR_TIMEOUT_S = rmdir

    @staticmethod
    def _unbound(saved):
        for n, v in saved.items():
            if v is None:
                delattr(vct, n)
            else:
                setattr(vct, n, v)

    def _order(self, sess):
        """Record "critical" and "abort" in the order they happen."""
        order = []
        real_abort = sess.abort

        def abort():
            order.append("abort")
            real_abort()
        sess.abort = abort

        # Wrapped rather than a handler: assertLogs() swaps the root
        # logger's handlers out for its own while it is active.
        real_critical = logging.critical

        def critical(*a, **kw):
            order.append("critical")
            real_critical(*a, **kw)
        logging.critical = critical
        self.addCleanup(setattr, logging, "critical", real_critical)
        return order

    def test_a_hung_reap_logs_before_it_aborts_and_bounds_the_abort(self):
        os.makedirs(os.path.join(self.d, "0", vct.STAGE_PREFIX + "0" * 16))
        sess = _FakeRemovalSession(hang=True, abort_hang=True)
        order = self._order(sess)
        vct.removal_session = sess
        self._bound(rmdir=0.2)
        t0 = time.monotonic()
        with self.assertLogs(level="CRITICAL") as cm:
            self.assertEqual(vct.reap_staging(self._reap_args()), 1)
        self.assertLess(time.monotonic() - t0, 3, "the reap waited on abort()")
        self.assertEqual(order[:2], ["critical", "abort"])
        msg = "\n".join(cm.output)
        self.assertIn("is killed", msg)
        self.assertIn(f"client evict id={sess.ident}", msg)
        self.assertIsNone(vct.removal_session)

    def test_a_root_off_the_sessions_mount_refuses_to_start(self):
        other = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, other, True)
        inside = os.path.join(self.d, "sub")
        os.mkdir(inside)
        sess = _FakeRemovalSession(ident=5)
        sess.mountpoint = self.d               # maps only self.d's mount
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(lambda cls, a, r: (sess, None))
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        self.assertIs(self._start(target_pool="cephfs.v.data",
                                  dirs=[self.d, inside]), sess)
        for kw in ({"target_pool": "cephfs.v.data"}, {"reap_staging": True}):
            sess.closed = False
            with self.assertLogs(level="ERROR") as cm:
                with self.assertRaises(SystemExit) as e:
                    self._start(dirs=[self.d, other], **kw)
            self.assertEqual(e.exception.code, 1)
            self.assertIn(other, "\n".join(cm.output))
            self.assertTrue(sess.closed)
            self.assertIsNone(vct.removal_session)

    def test_a_path_off_the_sessions_mount_is_named(self):
        other = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, other, True)
        sess = _FakeRemovalSession()
        sess.mountpoint = self.d
        self.assertIsNone(vct._session_mismatch(sess, self.d))
        why = vct._session_mismatch(sess, other)
        self.assertIn(other, why)
        self.assertIn(self.d, why)

    def _artifact_with_staging(self):
        art = os.path.join(self.d, "art")
        sd = os.path.join(art, "sub", vct.STAGE_PREFIX + "a" * 16)
        os.makedirs(sd)
        open(os.path.join(art, vct.RETENTION_MARKER), "w").close()
        return art, sd

    def _journal_rows(self):
        import json
        with open(os.path.join(self.d, vct.RUN_JOURNAL_NAME)) as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def test_reap_journals_each_removal_with_a_pin(self):
        art, sd = self._artifact_with_staging()
        vct.removal_session = _FakeRemovalSession()
        vct.run_journal = vct.RunJournal(enabled=True, stop_at=[self.d])
        with self.assertLogs(level="INFO"):
            self.assertEqual(vct.reap_staging(self._reap_args()), 0)
        self.assertFalse(os.path.exists(sd))
        rows = self._journal_rows()
        self.assertEqual([r["artifact"] for r in rows], [art])
        self.assertIsNotNone(rows[0]["pinned_at"], "the reap moved rctime "
                             "without a pinned_at")

    @unittest.skipUnless(hasattr(signal, "SIGRTMIN"),
                         "main() installs the real-time signal handlers")
    def test_the_cli_reap_writes_the_run_journal(self):
        art, sd = self._artifact_with_staging()
        sess = _FakeRemovalSession()
        saved_open = vct.RemovalSession.__dict__["open"]
        vct.RemovalSession.open = classmethod(lambda cls, a, r: (sess, None))
        self.addCleanup(setattr, vct.RemovalSession, "open", saved_open)
        self._cli_cleanups()
        sys.argv = ["tc", "--reap-staging", "--reap-staging-min-age", "0", self.d]
        with self.assertLogs(level="INFO"), \
                self.assertRaises(SystemExit) as cm:
            vct.main()
        self.assertEqual(cm.exception.code, 0)
        self.assertFalse(os.path.exists(sd))
        rows = self._journal_rows()
        self.assertEqual([r["artifact"] for r in rows], [art])
        self.assertIsNotNone(rows[0]["pinned_at"])


    # A hang is handled on the session it went out on; a path reached through
    # a symlink still maps.

    class _SwapThenHang(_FakeRemovalSession):
        """Hangs on rmdir after swapping in a replacement, as a concurrent
        hang would; abort() does not free it."""

        def __init__(self, swap, **kw):
            super().__init__(**kw)
            self.swap = swap
            self.stuck = threading.Event()

        def rmdir(self, path):
            self.calls.append(path)
            self.swap()
            self.stuck.wait(5)
            raise OSError(errno.ENOTCONN, "aborted", path)

    def test_a_second_reap_hang_does_not_abort_the_replacement(self):
        b = _FakeRemovalSession(ident=2)

        def swap():
            vct.removal_session = b
        a = self._SwapThenHang(swap, ident=1)
        self.addCleanup(a.stuck.set)
        vct.removal_session = a
        sd = os.path.join(self.d, vct.STAGE_PREFIX + "b" * 16)
        os.mkdir(sd)
        with self.assertLogs(level="WARNING") as cm:
            self.assertEqual(vct._bounded_rmdir_staging(sd, timeout=0.3),
                             "hung")
        self.assertFalse(b.aborted, "aborted the replacement session")
        self.assertIs(vct.removal_session, b)
        self.assertNotIn("client.2", "\n".join(cm.output))

    def test_the_advice_says_the_process_is_killed_and_how_to_evict(self):
        """An in-process abort_conn() blocks behind a stuck rmdir and the
        session keeps renewing, so no autoclose comes. The advice may name the
        autoclose only because the removal process is killed, which stops the
        renewals."""
        sess = _FakeRemovalSession(ident=5)
        sess.fsname = "myvol"
        msg = vct._stuck_session_advice(sess)
        self.assertIn("is killed", msg)
        self.assertIn("ceph tell mds.myvol:<rank> client evict id=5", msg)
        self.assertNotIn("abort_conn() waits", msg)

    def test_an_end_of_run_hang_stuck_forever_still_returns(self):
        """abort_conn() never frees the rmdir (abort_frees=False) and blocks
        itself (abort_hang); the end-of-run pass still returns, bounded."""
        sess = _FakeRemovalSession(hang=True, abort_hang=True, forever=True,
                                   abort_frees=False, ident=77)
        self.addCleanup(sess._abort_release.set)
        self.addCleanup(sess._release.set)
        vct.removal_session = sess
        self._bound(abort=0.2, rmdir=0.2)
        j = vct.JobStaging("8" * 16, self.d, session=sess, rmdir_timeout_s=0.2)
        p = os.path.join(self.d, j.name_for(vct.CephLayout(_TGT)))
        os.mkdir(p)
        j._paths.append(p)
        t0 = time.monotonic()
        with self.assertLogs(level="WARNING") as cm:
            self.assertEqual(j.remove_all(), [])
        self.assertLess(time.monotonic() - t0, 2, "the end-of-run pass blocked")
        stuck = [t for t in threading.enumerate()
                 if t.name in ("staging-rmdir", "removal-session-abort")
                 and t.is_alive()]
        self.assertGreaterEqual(len(stuck), 2, "the hangs were not real")
        self.assertIsNone(vct.removal_session)
        msg = "\n".join(cm.output)
        self.assertIn("client evict id=77", msg)
        self.assertIn("is killed", msg)

    def test_fs_path_follows_a_symlink_onto_the_mount(self):
        # A mountpoint in /proc/self/mounts is already resolved (on macOS
        # the temp dir itself sits behind /var -> /private/var).
        real = os.path.realpath(os.path.join(self.d, "mnt"))
        os.mkdir(real)
        link = os.path.join(self.d, "data")
        os.symlink(real, link)
        sess = vct.RemovalSession(None, None, "v", real, "/v", 1, "client.c")
        self.assertEqual(sess.fs_path(os.path.join(link, "tmp", "x")),
                         "/v/tmp/x")
        with self.assertRaises(OSError):
            sess.fs_path(os.path.join(self.d, "elsewhere"))


def _encode_backtrace(ino, ancestors, pool=7):
    """An inode_backtrace_t blob; ancestors are (dirino, dname) child-first."""
    import struct
    bps = b""
    for dirino, dname in ancestors:
        body = struct.pack("<Q", dirino) + struct.pack("<I", len(dname)) + dname \
            + struct.pack("<Q", 1)
        bps += bytes([2, 1]) + struct.pack("<I", len(body)) + body
    body = struct.pack("<Q", ino) + struct.pack("<I", len(ancestors)) + bps \
        + struct.pack("<q", pool) + struct.pack("<I", 0)
    return bytes([5, 4]) + struct.pack("<I", len(body)) + body


class BacktraceDirinos(unittest.TestCase):
    """The parent's inode is what locates a file under a renamed directory, so
    the decoder has to keep it, root-first like the dnames."""

    def test_dirinos_line_up_with_dnames(self):
        blob = _encode_backtrace(0x10000000abc, [(0x1000000000b, b"f"),
                                                 (0x1000000000a, b"d"), (1, b"top")])
        bt = vct.decode_backtrace(blob)
        ino, names, pool = bt
        self.assertEqual((ino, names, pool), (0x10000000abc, [b"top", b"d", b"f"], 7))
        self.assertEqual(bt.dirinos, (1, 0x1000000000a, 0x1000000000b))


class FakeLocator(vct.InodeLocator):
    """InodeLocator with the two MDS lookups answered from dicts: dirs maps an
    inode to the directory's current path, live is the set of linked inodes."""

    def __init__(self, dirs=None, live=(), errors=None, rate=1e9, mount="/"):
        super().__init__(mount, rate)
        self.dir_table = dict(dirs or {})
        self.live = set(live)
        self.errors = dict(errors or {})
        self.dir_calls = []

    def _lookup_dir(self, dirino):
        self.dir_calls.append(dirino)
        if dirino in self.errors:
            raise OSError(self.errors[dirino], os.strerror(self.errors[dirino]))
        if dirino not in self.dir_table:
            raise OSError(errno.ESTALE, "Stale file handle")
        return self.dir_table[dirino]

    def _lookup(self, ino):
        if ino in self.errors:
            raise OSError(self.errors[ino], os.strerror(self.errors[ino]))
        if ino not in self.live:
            raise OSError(errno.ESTALE, "Stale file handle")


class LocateByInode(unittest.TestCase):
    """A backtrace path that is missing or holds another inode is located again
    by inode; only ESTALE on the file's own inode makes it gone."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        os.mkdir(os.path.join(self.d, "old"))
        self.f = os.path.join(self.d, "old", "f")
        open(self.f, "w").close()
        self.ino = os.lstat(self.f).st_ino
        os.rename(os.path.join(self.d, "old"), os.path.join(self.d, "new"))
        self.stale = self.f
        self.now = os.path.join(self.d, "new", "f")

    def test_a_path_that_holds_the_inode_needs_no_lookup(self):
        loc = FakeLocator()
        self.assertEqual(loc.locate(self.now, self.ino, 99, "f"), (self.now, "as-is"))
        self.assertEqual(loc.lookups, 0)

    def test_a_file_under_a_renamed_directory_is_found_there(self):
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 99, "f"), (self.now, "parent"))

    def test_another_inode_at_the_old_path_is_not_taken(self):
        os.mkdir(os.path.join(self.d, "old"))
        open(self.stale, "w").close()          # a new file where the old name was
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 99, "f"), (self.now, "parent"))

    def test_a_name_that_holds_another_inode_under_the_parent_is_not_taken(self):
        # A re-run during purge lag: the old inode's name under the moved
        # parent now holds the transcoded copy. Replaced, with no lookup of
        # the old inode itself.
        os.unlink(self.now)
        open(self.now, "w").close()
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 99, "f"), (None, "replaced"))
        self.assertEqual(loc.lookups, 1)

    def test_an_inode_is_gone_only_on_estale(self):
        loc = FakeLocator()
        self.assertEqual(loc.locate(self.stale, self.ino, 0, "f"), (None, "gone"))

    def test_a_live_inode_neither_path_holds_is_unlocatable(self):
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 99, "renamed-too"),
                         (None, "unlocatable"))

    def test_any_other_lookup_error_raises(self):
        loc = FakeLocator(errors={99: errno.EIO})
        with self.assertRaises(OSError):
            loc.locate(self.stale, self.ino, 99, "f")
        loc = FakeLocator(errors={self.ino: errno.EIO})
        with self.assertRaises(OSError):
            loc.locate(self.stale, self.ino, 0, "f")

    def test_a_directory_is_looked_up_once(self):
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        loc.locate(self.stale, self.ino, 99, "f")
        loc.locate(self.stale, self.ino, 99, "f")
        self.assertEqual(loc.dir_calls, [99])
        loc = FakeLocator(live={self.ino})       # a gone directory is cached too
        loc.locate(self.stale, self.ino, 98, "f")
        loc.locate(self.stale, self.ino, 98, "f")
        self.assertEqual(loc.dir_calls, [98])

    def test_lookups_are_paced(self):
        clock = [100.0]
        slept = []

        def sleep(t):
            slept.append(round(t, 6))
            clock[0] += t
        saved = vct.time.monotonic, vct.time.sleep
        vct.time.monotonic, vct.time.sleep = (lambda: clock[0]), sleep
        try:
            loc = FakeLocator(live={self.ino}, rate=4.0)
            for _ in range(3):
                loc.alive(self.ino)
        finally:
            vct.time.monotonic, vct.time.sleep = saved
        self.assertEqual(slept, [0.25, 0.25])
        self.assertEqual(loc.lookups, 3)
        self.assertAlmostEqual(loc.slept, 0.5)

    def test_a_name_replaced_in_an_unrenamed_directory_costs_no_inode_lookup(self):
        os.unlink(self.now)
        open(self.now, "w").close()              # a new inode under the same name
        loc = FakeLocator(dirs={99: os.path.dirname(self.now)}, live={self.ino})
        self.assertEqual(loc.locate(self.now, self.ino, 99, "f"), (None, "replaced"))
        self.assertEqual(loc.lookups, 1)         # the directory only

    def test_a_fresh_directory_at_the_old_name_does_not_hide_the_live_file(self):
        # The common shape: the directory was renamed and a new one made
        # at the old name, holding new files with the same names. The live
        # file is under the renamed directory, and it is found there.
        os.mkdir(os.path.join(self.d, "old"))
        with open(self.stale, "w") as fh:
            fh.write("a different file")
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 99, "f"), (self.now, "parent"))

    def test_a_file_whose_parent_is_gone_costs_no_inode_lookup(self):
        # rm -rf of a tree on the drained pool, re-run during purge lag: the
        # parent's ESTALE is cached and settles every file under it.
        loc = FakeLocator(live={self.ino})
        self.assertEqual(loc.locate(self.stale, self.ino, 98, "f"), (None, "gone"))
        self.assertEqual(loc.locate(self.stale, self.ino, 98, "g"), (None, "gone"))
        self.assertEqual(loc.lookups, 1)         # the directory, once

    def test_rate_zero_says_nothing_and_checks_no_mount(self):
        saved = vct._findmnt

        def no_findmnt(*a, **kw):
            raise AssertionError("lookups are off; the mount is not examined")
        vct._findmnt = no_findmnt
        try:
            self.assertIsNone(vct._inode_locator("/mnt", 0))
        finally:
            vct._findmnt = saved

    def test_a_directory_that_reconnects_off_the_mount_is_not_used(self):
        mnt = os.path.join(self.d, "mnt")
        os.mkdir(mnt)
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino},
                          mount=mnt)
        self.assertEqual(loc.locate(os.path.join(mnt, "old", "f"), self.ino, 99, "f"),
                         (None, "unlocatable"))

    def test_a_match_on_another_device_is_not_taken(self):
        loc = FakeLocator(dirs={99: os.path.join(self.d, "new")}, live={self.ino})
        loc.mount_dev = os.lstat(self.now).st_dev + 1
        self.assertNotEqual(loc.locate(self.stale, self.ino, 99, "f")[1], "parent")

    def test_a_parent_check_error_is_cached(self):
        locked = os.path.join(self.d, "locked")
        os.mkdir(locked)
        os.mkdir(os.path.join(locked, "sub"))
        os.chmod(locked, 0)
        self.addCleanup(os.chmod, locked, 0o755)
        if os.geteuid() == 0:
            self.skipTest("root reads through mode 000")
        import traceback
        paced = []
        depth = []
        loc = FakeLocator()
        for _ in range(3):
            try:
                loc.parent_unchanged(os.path.join(locked, "sub"), 5,
                                     pace=lambda: paced.append(1))
            except OSError as e:
                depth.append(len(traceback.extract_tb(e.__traceback__)))
        self.assertEqual(paced, [1])
        self.assertEqual(len(depth), 3)
        self.assertEqual(depth[1], depth[2])

    def test_a_deleted_name_in_an_unrenamed_directory_costs_no_inode_lookup(self):
        os.unlink(self.now)
        loc = FakeLocator(dirs={99: os.path.dirname(self.now)}, live={self.ino})
        self.assertEqual(loc.locate(self.now, self.ino, 99, "f"), (None, "replaced"))
        self.assertEqual(loc.lookups, 1)         # the directory only

    def test_a_directory_lookup_error_is_cached(self):
        import traceback
        loc = FakeLocator(errors={99: errno.EIO})
        depth = []
        for _ in range(3):
            try:      # not assertRaises: it strips the traceback being measured
                loc.locate(self.stale, self.ino, 99, "f")
            except OSError as e:
                depth.append(len(traceback.extract_tb(e.__traceback__)))
        self.assertEqual(loc.dir_calls, [99])
        self.assertEqual(len(depth), 3)
        self.assertEqual(depth[1], depth[2])     # a cached error does not grow

    def test_a_mount_that_is_not_kernel_cephfs_is_said(self):
        saved = vct._findmnt
        vct._findmnt = lambda field, path: {"FSTYPE": "fuse.ceph-fuse",
                                            "OPTIONS": "rw"}[field]
        try:
            with self.assertLogs(level="WARNING") as cm:
                self.assertIsNone(vct._inode_locator("/mnt", 20))
        finally:
            vct._findmnt = saved
        self.assertTrue(any("not a kernel CephFS mount" in m for m in cm.output),
                        cm.output)

    def test_an_ino32_mount_is_declined(self):
        saved = vct._findmnt, vct.InodeLocator.open
        vct._findmnt = lambda field, path: {"FSTYPE": "ceph",
                                            "OPTIONS": "rw,noatime,ino32"}[field]

        def no_open(*a, **kw):
            raise AssertionError("an ino32 mount must not be looked up")
        vct.InodeLocator.open = no_open
        try:
            with self.assertLogs(level="WARNING") as cm:
                self.assertIsNone(vct._inode_locator("/mnt", 20))
        finally:
            vct._findmnt, vct.InodeLocator.open = saved
        self.assertTrue(any("ino32" in m for m in cm.output), cm.output)

    def test_an_exit_is_not_held_up_by_a_slow_rate(self):
        slept = []
        saved = vct.time.sleep
        vct.time.sleep = lambda t: slept.append(t)
        vct.do_exit.set()
        try:
            loc = FakeLocator(live={self.ino}, rate=0.01)   # 100 s apart
            loc.alive(self.ino)
            loc.alive(self.ino)
        finally:
            vct.do_exit.clear()
            vct.time.sleep = saved
        self.assertEqual(slept, [])

    def test_rate_zero_turns_lookups_off(self):
        def no_cdll(*a, **kw):
            raise AssertionError("rate 0 must not reach open_by_handle_at")
        saved = vct.ctypes.CDLL
        vct.ctypes.CDLL = no_cdll
        try:
            self.assertIsNone(vct.InodeLocator.open(self.d, 0))
        finally:
            vct.ctypes.CDLL = saved


class DeletedDirectoryReadsAsGone(unittest.TestCase):
    """A directory removed between the handle open and the readlink reads back
    as "<path> (deleted)" with no links: that is ESTALE, cached like any gone
    directory. The suffix on a directory that still has links (an unhashed
    alias, or a real name ending that way) keeps the path."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        self.loc = vct.InodeLocator(self.d, 1e9)
        self.loc._open_fd = lambda ino: os.open(self.d, os.O_RDONLY)
        self.named = os.path.join(self.d, "x (deleted)")

    def dir_path(self, dirino, nlink=None):
        with unittest.mock.patch.object(vct.os, "readlink", return_value=self.named):
            if nlink is None:
                return self.loc.dir_path(dirino)
            fake = types.SimpleNamespace(st_nlink=nlink)
            with unittest.mock.patch.object(vct.os, "fstat", return_value=fake):
                return self.loc.dir_path(dirino)

    def test_a_removed_directory_is_gone_and_cached(self):
        self.assertIsNone(self.dir_path(42, nlink=0))
        self.assertIsNone(self.loc._dirs[42])

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "needs /proc/self/fd")
    def test_a_directory_removed_after_the_open_is_gone(self):
        gone = os.path.join(self.d, "gone")
        os.mkdir(gone)

        def open_then_remove(ino):
            fd = os.open(gone, os.O_RDONLY)
            os.rmdir(gone)
            return fd
        self.loc._open_fd = open_then_remove
        self.assertIsNone(self.loc.dir_path(42))
        self.assertIsNone(self.loc._dirs[42])

    def test_a_linked_directory_read_back_as_deleted_keeps_the_path(self):
        self.assertEqual(self.dir_path(42), self.named)

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "needs /proc/self/fd")
    def test_a_live_directory_with_the_suffix_in_its_name_is_kept(self):
        os.mkdir(self.named)
        self.loc._open_fd = lambda ino: os.open(self.named, os.O_RDONLY)
        self.assertEqual(self.loc.dir_path(os.lstat(self.named).st_ino),
                         os.path.realpath(self.named))


class PoolListingFollowsRenamedDirectories(unittest.TestCase):
    """--paths-from-pool emits a file under a renamed directory at its current
    path, skips a deleted one, and reports a live one it cannot name, instead
    of emitting stale paths that are counted vanished."""

    def setUp(self):
        PoolListingRace.setUp(self)              # its fake rados and mount
        self.addCleanup(PoolListingRace.tearDown, self)
        os.mkdir(os.path.join(self.mp, "new"))
        self.moved = os.path.join(self.mp, "new", "m")
        open(self.moved, "w").close()
        ino_m = os.lstat(self.moved).st_ino
        self.inos = {0x10000000001: ino_m, 0x10000000003: 0x777777}
        names = {0x10000000001: [b"old", b"m"], 0x10000000003: [b"old", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x500)
            return bt
        vct.decode_backtrace = dbt
        self.loc = FakeLocator(dirs={0x500: os.path.join(self.mp, "new")},
                               live={ino_m})
        saved = vct._inode_locator, vct.InodeLocator.locate
        self.addCleanup(setattr, vct, "_inode_locator", saved[0])
        self.addCleanup(setattr, vct.InodeLocator, "locate", saved[1])
        vct._inode_locator = lambda mount, rate: self.loc
        # The listing's object names are not the files' st_ino here, so map them.
        real = vct.InodeLocator.locate
        inos = self.inos
        vct.InodeLocator.locate = lambda s, p, ino, d, n: real(s, p, inos[ino], d, n)

    def _run(self):
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool=None,
                    max_size=None)
        with self.assertLogs(level="INFO") as cm:
            got = list(vct._iter_pool_paths(args, [self.mp]))
        return got, cm.output

    def test_a_moved_file_is_emitted_at_its_current_path(self):
        got, out = self._run()
        self.assertEqual(got, [self.moved])
        want = "1 found under a renamed directory, 1 gone (not deletion evidence"
        self.assertTrue(any(want in m for m in out), out)

    def test_a_live_file_it_cannot_name_is_reported_not_emitted(self):
        self.loc.live.add(0x777777)
        got, out = self._run()
        self.assertEqual(got, [self.moved])
        self.assertTrue(any("inode 0x10000000003 is alive" in m for m in out), out)
        self.assertTrue(any("1 alive but not locatable" in m for m in out), out)

    def test_an_all_renamed_sample_passes_the_guard_quietly(self):
        # The residue this is for: every resolved sample is under a renamed
        # directory. Nothing is wrong, so nothing is said about the prefix.
        got, out = self._run()               # no SystemExit: the moved path counts
        self.assertEqual(len(got), 1)
        self.assertFalse(any("check the ancestry prefix" in m for m in out), out)

    def test_an_all_prefix_shifted_sample_is_said(self):
        # What a wrong ancestry prefix produces: every file is at its derived
        # path with a leading component dropped.
        m = os.path.join(self.mp, "m")
        open(m, "w").close()
        self.inos[0x10000000001] = os.lstat(m).st_ino
        names = {0x10000000001: [b"sub", b"m"], 0x10000000003: [b"sub", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x500)
            return bt
        vct.decode_backtrace = dbt
        self.loc.dir_table[0x500] = self.mp
        got, out = self._run()
        self.assertEqual(got, [m])
        self.assertTrue(any("as if the derived paths had extra leading 'sub' "
                            "(e.g. sub/m -> m)" in m for m in out), out)

    def test_a_shift_with_a_path_used_as_listed_is_not_said(self):
        # One path resolves as listed, so the prefix is right; the shifted
        # one is a real move.
        m = os.path.join(self.mp, "m")
        open(m, "w").close()
        stay = os.path.join(self.mp, "stay")
        os.mkdir(stay)
        x = os.path.join(stay, "x")
        open(x, "w").close()
        self.inos.update({0x10000000001: os.lstat(m).st_ino,
                          0x10000000003: os.lstat(x).st_ino})
        names = {0x10000000001: ([b"sub", b"m"], 0x500),
                 0x10000000003: ([b"stay", b"x"], os.lstat(stay).st_ino)}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key][0], 0))
            bt.dirinos = (1, names[key][1])
            return bt
        vct.decode_backtrace = dbt
        self.loc.dir_table[0x500] = self.mp
        got, out = self._run()
        self.assertEqual(got, [m, x])
        self.assertFalse(any("check the ancestry prefix" in m for m in out), out)

    def test_a_shift_with_a_rename_mixed_in_is_still_said(self):
        # A wrong prefix where one sampled directory was also renamed: that
        # sample is not a suffix match, but every sample was relocated.
        m = os.path.join(self.mp, "m")
        open(m, "w").close()
        os.mkdir(os.path.join(self.mp, "renamed"))
        x = os.path.join(self.mp, "renamed", "x")
        open(x, "w").close()
        # The renamed sample is listed first; the example still shows the shift.
        self.inos.update({0x10000000001: os.lstat(x).st_ino,
                          0x10000000003: os.lstat(m).st_ino})
        names = {0x10000000001: ([b"sub", b"orig", b"x"], 0x600),
                 0x10000000003: ([b"sub", b"m"], 0x500)}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key][0], 0))
            bt.dirinos = (1, names[key][1])
            return bt
        vct.decode_backtrace = dbt
        self.loc.dir_table.update({0x500: self.mp,
                                   0x600: os.path.join(self.mp, "renamed")})
        got, out = self._run()
        self.assertEqual(got, [x, m])
        self.assertTrue(any("all 2 resolved samples were found by inode, as if "
                            "the derived paths had extra leading 'sub' (e.g. "
                            "sub/m -> m)" in m for m in out), out)

    def test_a_directory_moved_under_its_own_name_is_not_said(self):
        # A correct prefix, on the residue pool: one directory was moved
        # under its own name (results -> archive/results), another renamed.
        os.makedirs(os.path.join(self.mp, "archive", "results"))
        m = os.path.join(self.mp, "archive", "results", "m")
        open(m, "w").close()
        os.mkdir(os.path.join(self.mp, "renamed"))
        x = os.path.join(self.mp, "renamed", "x")
        open(x, "w").close()
        self.inos.update({0x10000000001: os.lstat(m).st_ino,
                          0x10000000003: os.lstat(x).st_ino})
        names = {0x10000000001: ([b"results", b"m"], 0x500),
                 0x10000000003: ([b"orig", b"x"], 0x600)}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key][0], 0))
            bt.dirinos = (1, names[key][1])
            return bt
        vct.decode_backtrace = dbt
        self.loc.dir_table.update({0x500: os.path.join(self.mp, "archive", "results"),
                                   0x600: os.path.join(self.mp, "renamed")})
        got, out = self._run()
        self.assertEqual(got, [m, x])
        self.assertFalse(any("check the ancestry prefix" in m for m in out), out)

    def test_prefix_shift(self):
        sh = vct._prefix_shift
        p = str.split
        # one shift explains every sample, renamed ones included
        self.assertEqual(sh([(p("a/b/f", "/"), p("b/f", "/")),
                             (p("a/c/g", "/"), p("d/g", "/"))]), (True, ("a",)))
        self.assertEqual(sh([(p("b/f", "/"), p("a/b/f", "/"))]), (False, ("a",)))
        # renames alone, two different shifts, or a shift that leaves another
        # sample unexplained: not a prefix
        self.assertIsNone(sh([(p("a/f", "/"), p("b/f", "/"))]))
        self.assertIsNone(sh([(p("a/b/f", "/"), p("c/f", "/"))]))
        self.assertIsNone(sh([(p("a/m", "/"), p("m", "/")),
                              (p("b/x", "/"), p("x", "/"))]))
        self.assertIsNone(sh([(p("a/b/m", "/"), p("b/m", "/")),
                              (p("a/b/x", "/"), p("x", "/"))]))
        self.assertIsNone(sh([(p("r/m", "/"), p("archive/r/m", "/")),
                              (p("o/x", "/"), p("n/x", "/"))]))

    def test_a_partly_relocated_sample_is_not_said(self):
        stay = os.path.join(self.mp, "stay")
        os.mkdir(stay)
        x = os.path.join(stay, "x")
        open(x, "w").close()
        self.inos[0x10000000003] = os.lstat(x).st_ino
        names = {0x10000000001: ([b"old", b"m"], 0x500),
                 0x10000000003: ([b"stay", b"x"], os.lstat(stay).st_ino)}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key][0], 0))
            bt.dirinos = (1, names[key][1])
            return bt
        vct.decode_backtrace = dbt
        got, out = self._run()
        self.assertEqual(got, [self.moved, x])
        self.assertFalse(any("check the ancestry prefix" in m for m in out), out)

    def test_a_converging_rerun_passes_the_prefix_guard(self):
        # Both listed inodes were replaced by new ones at the same names, and
        # the parent, though checked as moved, turns out to be where the
        # backtrace says. The paths exist, so the prefix is right; nothing is
        # emitted, and no per-file inode lookup is spent on them.
        os.mkdir(os.path.join(self.mp, "old"))
        for n in ("m", "x"):
            open(os.path.join(self.mp, "old", n), "w").close()
        self.loc.dir_table[0x500] = os.path.join(self.mp, "old")
        got, out = self._run()
        self.assertEqual(got, [])
        self.assertEqual(self.loc.lookups, 1)
        self.assertTrue(any("2 no longer at their name" in m for m in out), out)

    def _count_file_delays(self, go=True):
        calls = []
        saved = vct._reap_pace
        self.addCleanup(setattr, vct, "_reap_pace", saved)

        def pace():
            calls.append(1)
            return go
        vct._reap_pace = pace
        return calls

    def test_an_unrenamed_parent_needs_no_inode_lookup(self):
        # The common case: the parent still is the backtrace's parent inode.
        # The paths are used as they are, with one paced directory check and
        # no per-file check here (the consumer stats each path anyway).
        old = os.path.join(self.mp, "old")
        os.mkdir(old)
        for n in ("m", "x"):
            open(os.path.join(old, n), "w").close()
        real = os.lstat(old).st_ino
        names = {0x10000000001: [b"old", b"m"], 0x10000000003: [b"old", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, real)
            return bt
        vct.decode_backtrace = dbt
        delays = self._count_file_delays()
        got, _ = self._run()
        self.assertEqual(got, [os.path.join(old, "m"), os.path.join(old, "x")])
        self.assertEqual(self.loc.lookups, 0)
        self.assertEqual(len(delays), 1)

    def test_files_under_a_known_gone_parent_cost_no_pace_or_stat(self):
        # A re-run after an rm -rf: once the parent is known gone, the rest of
        # its files are settled from the cache, with no pacing.
        self.loc.dir_table.pop(0x500)              # the parent is gone
        delays = self._count_file_delays()
        got, out = self._run()
        # one parent check, then one paced check for the first file only
        self.assertEqual(len(delays), 2)
        self.assertEqual(self.loc.lookups, 1)
        self.assertEqual(got, [])
        # A fully deleted tree: every sample is gone, which says nothing
        # against the prefix, so the run goes on instead of refusing.
        self.assertTrue(any("all 2 sampled objects are gone" in m for m in out), out)

    def _relist(self, dirino, names=(b"m", b"x")):
        names = {0x10000000001: [b"old", names[0]], 0x10000000003: [b"old", names[1]]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, dirino)
            return bt
        vct.decode_backtrace = dbt

    def test_deleted_files_in_a_live_directory_pass_the_prefix_guard(self):
        # A re-run during purge lag: old/ is still there and unrenamed, its
        # files are deleted. The parent check found old/, which proves the
        # prefix; the paths go on as listed, as before.
        old = os.path.join(self.mp, "old")
        os.mkdir(old)
        self._relist(os.lstat(old).st_ino)
        got, _ = self._run()
        self.assertEqual(got, [os.path.join(old, "m"), os.path.join(old, "x")])
        self.assertEqual(self.loc.lookups, 0)

    def test_a_recreated_directory_passes_the_prefix_guard(self):
        # rm -rf old && mkdir old, re-run during purge lag: a new old/ is at
        # the path, the listed files' parent inode is gone.
        os.mkdir(os.path.join(self.mp, "old"))
        self.loc.dir_table.pop(0x500)
        self._relist(0x500)
        got, out = self._run()
        self.assertEqual(got, [])
        self.assertTrue(any("2 gone" in m for m in out), out)

    def test_live_files_it_cannot_locate_still_fail_the_prefix_guard(self):
        self.inos.update({0x10000000001: 0x111, 0x10000000003: 0x222})
        self.loc.live.update({0x111, 0x222})
        self._relist(0x500, (b"q", b"z"))
        with self.assertRaises(SystemExit):
            self._run()

    def test_a_fully_deleted_tree_past_the_sample_size_goes_on(self):
        keys = [0x10000000100 + i for i in range(70)]

        class Obj:
            def __init__(self, key):
                self.key = key

        class Ioctx:
            def list_objects(self):
                return [Obj(f"{k:x}.00000000") for k in keys]

            def get_xattr(self, key, name):
                return key.split(".")[0].encode()

            def close(self):
                pass
        rados = sys.modules["rados"]
        saved = rados.Rados.open_ioctx
        self.addCleanup(setattr, rados.Rados, "open_ioctx", saved)
        rados.Rados.open_ioctx = lambda self_, pool: Ioctx()

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, [b"old", b"f%d" % key], 0))
            bt.dirinos = (1, 0x500)
            return bt
        vct.decode_backtrace = dbt
        self.inos.update({k: 0x900000 + k for k in keys})
        self.loc.dir_table.pop(0x500)
        got, out = self._run()
        self.assertEqual(got, [])
        self.assertTrue(any("all 64 sampled objects are gone" in m for m in out), out)

    def test_a_parent_check_error_does_not_prove_the_prefix(self):
        if os.geteuid() == 0:
            self.skipTest("root reads through mode 000")
        locked = os.path.join(self.mp, "old")
        os.mkdir(locked)
        os.chmod(locked, 0)
        self.addCleanup(os.chmod, locked, 0o755)
        names = {0x10000000001: [b"old", b"sub", b"m"],
                 0x10000000003: [b"old", b"sub", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x400, 0x500)
            return bt
        vct.decode_backtrace = dbt
        with self.assertRaises(SystemExit):
            self._run()

    def test_a_sample_only_partly_gone_still_fails_the_prefix_guard(self):
        self.inos.update({0x10000000001: 0x111, 0x10000000003: 0x222})
        self.loc.live.add(0x222)                   # 0x111 is gone, 0x222 is not
        self._relist(0x500, (b"q", b"z"))
        with self.assertRaises(SystemExit):
            self._run()

    def test_a_file_moved_into_a_new_directory_at_the_old_path_is_not_settled_gone(self):
        # old/ was replaced by a new old/, and a live file was moved into it
        # under its old name. Its parent inode is gone, but a directory is at
        # the old path, so the file is checked there, not settled from the cache.
        old = os.path.join(self.mp, "old")
        os.mkdir(old)
        m = os.path.join(old, "m")
        open(m, "w").close()
        self.loc.dir_table.pop(0x500)              # the original old/ is gone
        names = {0x10000000001: [b"old", b"x"], 0x10000000003: [b"old", b"m"]}
        self.inos[0x10000000003] = os.lstat(m).st_ino

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x500)
            return bt
        vct.decode_backtrace = dbt
        got, _ = self._run()
        self.assertEqual(got, [m])

    def test_checks_under_a_moved_parent_sleep_the_file_delay_first(self):
        delays = self._count_file_delays()
        self._run()
        # one parent check (cached for the second file), then one per file
        self.assertEqual(len(delays), 3)

    def test_a_stopping_run_ends_the_listing(self):
        self._count_file_delays(go=False)
        got, out = self._run()
        self.assertEqual(got, [])
        self.assertTrue(any("stopping the listing" in m for m in out), out)
        self.assertTrue(any("counts are partial" in m for m in out), out)

    def test_a_backtrace_outside_the_mounted_subtree_is_not_looked_up(self):
        vct._mount_ancestry_prefix = lambda root: (self.mp, "/sub")
        x = os.path.join(self.mp, "x")
        open(x, "w").close()
        self.inos[0x10000000003] = os.lstat(x).st_ino
        names = {0x10000000001: [b"elsewhere", b"m"], 0x10000000003: [b"sub", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x500)
            return bt
        vct.decode_backtrace = dbt
        got, _ = self._run()
        self.assertEqual(got, [os.path.join(self.mp, "elsewhere", "m"), x])
        self.assertEqual(self.loc.lookups, 0)

    def test_a_rerun_during_purge_lag_skips_replaced_files_without_a_lookup(self):
        os.unlink(self.moved)
        open(self.moved, "w").close()          # the transcoded copy, a new inode
        got, out = self._run()
        self.assertEqual(got, [])
        self.assertEqual(self.loc.lookups, 2)  # the directory, then x's inode
        self.assertTrue(any("1 no longer at their name" in m for m in out), out)

    def test_a_fresh_directory_at_the_old_name_is_not_mistaken_for_the_parent(self):
        # da/ renamed to db/, then a new da/ made with the same file
        # name. The listed inode is the one under db/; the new da/m is its own.
        a, b = os.path.join(self.mp, "da"), os.path.join(self.mp, "db")
        os.mkdir(a)
        with open(os.path.join(a, "m"), "w") as fh:
            fh.write("original")
        orig_dir = os.lstat(a).st_ino
        orig = os.lstat(os.path.join(a, "m")).st_ino
        os.rename(a, b)
        os.mkdir(a)
        with open(os.path.join(a, "m"), "w") as fh:
            fh.write("new file, same name")
        self.inos[0x10000000001] = orig
        names = {0x10000000001: [b"da", b"m"], 0x10000000003: [b"da", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, orig_dir)
            return bt
        vct.decode_backtrace = dbt
        self.loc.dir_table[orig_dir] = b
        got, out = self._run()
        self.assertEqual(got, [os.path.join(b, "m")])
        self.assertTrue(any("1 found under a renamed directory" in m for m in out), out)

    def test_the_subtree_roots_own_backtrace_is_not_looked_up(self):
        # On a subtree mount the mount point's own backtrace strips to no
        # name at all; it is emitted as the mount point, not looked up.
        vct._mount_ancestry_prefix = lambda root: (self.mp, "/sub")
        x = os.path.join(self.mp, "x")
        open(x, "w").close()
        self.inos[0x10000000003] = os.lstat(x).st_ino
        names = {0x10000000001: [b"sub"], 0x10000000003: [b"sub", b"x"]}

        def dbt(blob):
            key = int(blob, 16)
            bt = vct.Backtrace((key, names[key], 0))
            bt.dirinos = (1, 0x500)[:len(names[key])]
            return bt
        vct.decode_backtrace = dbt
        got, _ = self._run()
        self.assertEqual(got, [self.mp, x])

    def test_paths_outside_the_roots_are_not_looked_up(self):
        args = Args(paths_from_pool="Q", rados_conffile=None, rados_mon_host=None,
                    rados_keyring=None, rados_name=None, target_pool=None,
                    max_size=None)
        os.mkdir(os.path.join(self.mp, "old"))
        open(os.path.join(self.mp, "old", "x"), "w").close()
        os.mkdir(os.path.join(self.mp, "elsewhere"))   # the run's only root
        with self.assertLogs(level="INFO"):
            got = list(vct._iter_pool_paths(args, [os.path.join(self.mp, "elsewhere")]))
        self.assertEqual(self.loc.lookups, 0)
        self.assertEqual(got, [os.path.join(self.mp, "old", "m"),
                               os.path.join(self.mp, "old", "x")])

    def test_a_lookup_error_falls_back_to_the_backtrace_path(self):
        os.mkdir(os.path.join(self.mp, "old"))
        x = os.path.join(self.mp, "old", "x")
        open(x, "w").close()                     # this one is where it says
        self.inos[0x10000000003] = os.lstat(x).st_ino
        self.loc.errors[0x500] = errno.EIO
        got, out = self._run()
        self.assertEqual(got, [os.path.join(self.mp, "old", "m"), x])
        self.assertTrue(any("inode lookup failed" in m for m in out), out)


class LookupinoRateFlag(unittest.TestCase):
    def test_a_negative_or_non_finite_rate_is_refused(self):
        import subprocess
        for bad in ("-1", "nan", "inf"):
            r = subprocess.run([sys.executable, TARGET, "--lookupino-rate", bad, "/x"],
                               check=False, capture_output=True, text=True)
            self.assertEqual(r.returncode, 2, bad)
            self.assertIn("must be a finite number, 0 or more", r.stderr)

    def test_help_names_the_flag_and_its_default(self):
        import subprocess
        r = subprocess.run([sys.executable, TARGET, "--help"], check=False,
                           capture_output=True, text=True)
        self.assertIn("--lookupino-rate", r.stdout)
        self.assertIn("default 20", " ".join(r.stdout.split()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
