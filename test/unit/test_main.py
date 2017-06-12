# -*- coding: utf-8 -*-

import json
import mock
import tempfile
import unittest

import s3_sync.__main__


class TestMain(unittest.TestCase):
    def test_main(self):
        config_file = tempfile.NamedTemporaryFile()
        log_file = tempfile.NamedTemporaryFile()
        json.dump({
            "containers": [{"account": "a",
                            "container": "c",
                            "aws_bucket": "columner-lightsomeness",
                            "aws_identity": "root",
                            "aws_secret": "swordfish"}],
            "devices": "/tmp",
            "items_chunk": 1000,
            "log_file": log_file.name,
            "poll_interval": 1,
            "status_dir": "/tmp",
        }, config_file)
        config_file.flush()

        containers_handled = [0]

        def fake_get_nodes(*a):
            containers_handled[0] += 1
            return (5470049, [])

        fake_argv = ["/usr/bin/thingy", "--once",
                     "--config", config_file.name,
                     "--log-level", "error"]

        with mock.patch("swift.common.ring.Ring.get_nodes", fake_get_nodes), \
                mock.patch("sys.argv", fake_argv):
            s3_sync.__main__.main()

        self.assertGreater(containers_handled[0], 0)

        # Make sure there were no errors in the logs; there's a couple
        # high-level spots that log errors but continue anyway.
        self.assertEqual(log_file.read(), "")
