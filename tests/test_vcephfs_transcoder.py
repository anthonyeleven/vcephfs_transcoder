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
