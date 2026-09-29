#!/usr/bin/env python3
"""Unit tests for vcephfs_transcoder.py. Run: python3 test_vcephfs_transcoder.py

Covers the pure, decision-making parts added or changed by the copy_file_range /
replace-lock / subtree-pruning work. These are the places where a silent wrong
answer is expensive: a default that flips back, a lock that stops excluding, a
prune that skips more data than intended, or a warning that stops being true.
"""
import argparse
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
import unittest

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
        so the regulator silently never comes up. Observed on all three
        production jobs 2026-09-06.
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

    start_regulator declines to start in four places. Three already log at
    WARNING. The no-URL path -- the one an omitted --config lands on, and by
    far the likeliest of the four -- logged at INFO, indistinguishable from a
    normal start in a multi-gigabyte log. Three jobs ran unregulated for hours
    on 2026-09-28 for exactly that reason. Reverting the level to INFO must
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
    changes nothing and used to log only an INFO line that read like success.
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
            any("was cleared" in m and "every sample will now fail" in m
                for m in got.output),
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
            any("--no-regulate" in m and "WITHOUT" in m for m in got.output),
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
                and "restart the job for this to take effect" in m
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
            any("embedded credentials" in m and "HTTPBasicAuthHandler" in m
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
        """regulate_period_s used to be read once, before the loop."""
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

    _url_userinfo() used to cut the netloc at the first of "/?#" BEFORE
    looking for "@", agreeing with urllib about where the host ends. That
    failed OPEN: in "http://bob:pa/ss@prom/api/v1/query" the "@" falls past
    the cut, so the URL read as credential-free. Neither refusal layer fired,
    _loggable_url() returned it untouched, and the FULL password reached the
    startup "Config: regulate Prometheus URL None -> ..." INFO line, the
    Starting:/Finished: lines, every _report_state() line and the process
    title. urllib's own _splithost() stops at the same "/", so sample() then
    raised InvalidURL("nonnumeric port: 'pa'") and leaked the password's
    prefix too.

    The boundary is now the LAST "@" anywhere after "://" -- deliberately not
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
        """The authority test used to look at the password too.

        So "bob:p(w@prom:9090/api" read as PromQL: refused by
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

    def test_the_pin_that_motivated_this_clears_on_its_own(self):
        """The observed failure, end to end. A job started --threads 1 pauses
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
        """End to end: the case that was stuck in production."""
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

    It used to cost nothing. Worse, `not paused` counted as `quiet`, so the
    regulator kept easing the file delay DOWN while latency sat past the
    target it exists to defend, and the only remaining protection was the
    pause cliff. For a job pushed above its ceiling by SIGUSR1 -- delay
    already at its floor -- the whole band was a dead zone.
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
        """The invariant that makes the regime above reachable in production.

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

    The startup path used to return None on any sample() exception, which is
    strictly less tolerant than the poll loop it gates -- that loop already
    logs "no usable sample" and holds. Causes are all transient: nan from a
    query window shorter than the volume's request interval, a Prometheus 503
    at the moment the job starts, or a volume with no MDS traffic yet. Measured
    cost of getting this wrong: two volumes pinned at 1 thread / 20ms delay for
    nearly seven hours, 35-80x slower than their siblings.
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
        self.old = time.time() - vct.TMP_ORPHAN_MIN_AGE_S - 3600

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def _aged(self, name):
        p = os.path.join(self.d, name)
        with open(p, "w") as f:
            f.write("x")
        os.utime(p, (self.old, self.old))
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
