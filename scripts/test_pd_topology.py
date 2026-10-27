import unittest

import pd_topology as t

HOSTS = {
    "a": {"id": 1, "ip": "10.0.0.1"},
    "b": {"id": 2, "ip": "10.0.0.2"},
    "c": {"id": 3, "ip": "10.0.0.3"},
}
ADV = {h: v["ip"] for h, v in HOSTS.items()}
SHARED = ["data_node = 1 @ 10.0.0.1:7001", "data_node = 2 @ 10.0.0.2:7001", "data_node = 3 @ 10.0.0.3:7001",
          "rf = 3", "secret_file = /r/secret"]


def three(**kw):
    return t.pd_confs("three", HOSTS, SHARED, "/r/pd", "/r/pd.sock", metrics="0.0.0.0:9477", **kw)


class PdTopologyTest(unittest.TestCase):
    def test_three_renders_identical_full_voter_lists(self):
        confs = three()
        self.assertEqual(t.check_pd_confs("three", confs, ADV), [])
        lists = {h: [l for l in c.splitlines() if l.startswith("pd_peer")] for h, c in confs.items()}
        self.assertEqual(len({tuple(v) for v in lists.values()}), 1)
        self.assertEqual(lists["a"], ["pd_peer = 1 @ 10.0.0.1:7100", "pd_peer = 2 @ 10.0.0.2:7100",
                                      "pd_peer = 3 @ 10.0.0.3:7100"])
        for c in confs.values():
            self.assertIn("listen = 0.0.0.0:7100", c)
        self.assertEqual([h for h, c in confs.items() if "campaign = true" in c], ["a"])
        self.assertEqual([h for h, c in three(campaigner="b").items() if "campaign = true" in c], ["b"])

    def test_three_needs_three_hosts(self):
        with self.assertRaises(ValueError):
            t.pd_confs("three", {"a": HOSTS["a"]}, SHARED, "/r/pd", "/r/pd.sock")
        with self.assertRaises(ValueError):
            t.pd_confs("bogus", HOSTS, SHARED, "/r/pd", "/r/pd.sock")

    def test_single_perf_is_marked_and_peerless(self):
        confs = t.pd_confs("single-perf", HOSTS, SHARED, "/r/pd", "/r/pd.sock")
        self.assertEqual(list(confs), ["a"])
        self.assertEqual(t.check_pd_confs("single-perf", confs, ADV), [])
        self.assertNotIn("pd_peer", confs["a"])
        unmarked = {"a": confs["a"].replace(t.SINGLE_PERF_MARKER + "\n", "")}
        self.assertTrue(t.check_pd_confs("single-perf", unmarked, ADV))
        self.assertTrue(t.check_pd_confs("single-perf", three(), ADV))

    def assert_rejected(self, confs, needle, adv=ADV):
        errors = t.check_pd_confs("three", confs, adv)
        self.assertTrue(any(needle in e for e in errors), errors)

    def test_rejects_missing_self_entry(self):
        confs = three()
        confs["b"] = confs["b"].replace("pd_peer = 2 @ 10.0.0.2:7100\n", "")
        self.assert_rejected(confs, "no entry for its own pd_id 2")

    def test_rejects_divergent_lists_and_wrong_addresses(self):
        confs = three()
        confs["c"] = confs["c"].replace("pd_peer = 1 @ 10.0.0.1:7100", "pd_peer = 1 @ 10.0.0.9:7100")
        self.assert_rejected(confs, "voter lists differ")
        self.assert_rejected(confs, "advertised address is 10.0.0.1")

    def test_rejects_wildcard_peer_and_listen_as_advertisement(self):
        confs = {h: c.replace("pd_peer = 1 @ 10.0.0.1:7100", "pd_peer = 1 @ 0.0.0.0:7100") for h, c in three().items()}
        self.assert_rejected(confs, "wildcard address")
        confs = three()
        confs["a"] = confs["a"].replace("listen = 0.0.0.0:7100", "listen = 10.0.0.5:7100")
        self.assert_rejected(confs, "binds neither")
        confs = three()
        confs["a"] = confs["a"].replace("listen = 0.0.0.0:7100", "listen = 0.0.0.0:7200")
        self.assert_rejected(confs, "differs from listen port")

    def test_rejects_campaign_join_shared_drift_and_colocation(self):
        confs = three()
        confs["b"] = confs["b"].replace("campaign = false", "campaign = true")
        self.assert_rejected(confs, "exactly one replica")
        confs = three()
        confs["c"] += "join = true\n"
        self.assert_rejected(confs, "`join`")
        confs = three()
        confs["c"] = confs["c"].replace("rf = 3", "rf = 2")
        self.assert_rejected(confs, "shared PD settings")
        confs = three()
        confs["a"] = confs["a"].replace("data_dir = /r/pd\n", "")
        self.assert_rejected(confs, "needs exactly one `data_dir`")
        self.assert_rejected(three(), "share a host", adv={"a": "10.0.0.1", "b": "10.0.0.1", "c": "10.0.0.3"})

    def test_deployed_mode(self):
        hosts = ["a", "b", "c"]
        self.assertIsNone(t.deployed_mode([], hosts, "a"))
        self.assertEqual(t.deployed_mode(["a"], hosts, "a"), "single-perf")
        self.assertEqual(t.deployed_mode(["c", "a", "b"], hosts, "a"), "three")
        with self.assertRaises(ValueError):
            t.deployed_mode(["a", "b"], hosts, "a")

    def test_provision_mode_refuses_to_reuse_state_of_another_or_unknown_mode(self):
        legacy = dict(recorded=None, deployed="single-perf", state_hosts=["a"])
        for target in ("three", None):
            if target is None:
                self.assertEqual(t.provision_mode(target=target, reset=False, **legacy), "single-perf")
                continue
            with self.assertRaises(ValueError):
                t.provision_mode(target=target, reset=False, **legacy)
        self.assertEqual(t.provision_mode(target="three", reset=True, **legacy), "three")
        with self.assertRaises(ValueError):
            t.provision_mode(recorded=None, deployed=None, state_hosts=["a"], target="three", reset=False)
        with self.assertRaises(ValueError):
            t.provision_mode(recorded="three", deployed="single-perf", state_hosts=[], target=None, reset=True)
        self.assertEqual(t.provision_mode(recorded=None, deployed=None, state_hosts=[], target=None, reset=False), "three")
        self.assertEqual(t.provision_mode(recorded="three", deployed="three", state_hosts=["a", "b", "c"],
                                          target="three", reset=False), "three")


if __name__ == "__main__":
    unittest.main()
