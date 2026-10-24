"""S1's client-side pieces that don't need a daemon: the grid model the
scenario reads replies from, and the Claude project slug. The scenario itself
runs against a real daemon in scripts/lab/prove.sh. Run: tests/lab/run_tests.sh"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from labkit import s1  # noqa: E402


def runs(text):
    return [{"text": text[:3]}, {"text": text[3:]}] if text else []


class GridModel(unittest.TestCase):
    def test_snapshot_then_deltas(self):
        g = s1.Grid()
        self.assertTrue(g.apply({"event": "snapshot", "payload": {
            "version": 4, "scrollback": [runs("old line")], "grid": [runs("banner"), runs("> "), runs("")]}}))
        self.assertEqual(g.text(), "old line\nbanner\n>\n")
        self.assertTrue(g.apply({"event": "delta", "payload": {
            "version": 5, "rows": 3, "scrollbackAppended": [runs("banner")],
            "damagedRows": [{"row": 0, "runs": runs("> hi")}, {"row": 1, "runs": runs("reply #1: hi")}]}}))
        self.assertEqual(g.version, 5)
        self.assertEqual(g.text(), "old line\nbanner\n> hi\nreply #1: hi\n")
        self.assertEqual(g.screen(), "> hi\nreply #1: hi")

    def test_other_events_are_not_grid_updates(self):
        g = s1.Grid()
        self.assertFalse(g.apply({"event": "mode", "payload": {"mode": "claimer"}}))
        self.assertEqual(g.version, -1)

    def test_delta_shrinks_and_ignores_rows_out_of_range(self):
        g = s1.Grid()
        g.apply({"event": "snapshot", "payload": {"grid": [runs("a"), runs("b"), runs("c")]}})
        g.apply({"event": "delta", "payload": {"rows": 2, "damagedRows": [{"row": 7, "runs": runs("x")}]}})
        self.assertEqual(g.rows, ["a", "b"])


class Slug(unittest.TestCase):
    def test_matches_claude_project_hash(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "My Repo.v2")
            os.mkdir(p)
            want = "".join(c if c.isalnum() or c == "-" else "-" for c in os.path.realpath(p))
            self.assertEqual(s1.claude_project_slug(p), want)
            self.assertTrue(s1.claude_project_slug(p).endswith("-My-Repo-v2"))


if __name__ == "__main__":
    unittest.main()
