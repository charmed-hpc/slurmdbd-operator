#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pathlib
import unittest

from utils.confeditor import SlurmdbdConfEditor


class TestConfEditor(unittest.TestCase):
    """Unit tests for slurmdbd configuration file editor."""

    def test_create_config(self) -> None:
        with SlurmdbdConfEditor("test.conf") as editor:
            editor.archive_events = "yes"
            editor.archive_jobs = "yes"
            editor.archive_resvs = True
            editor.archive_steps = False
            editor.archive_txn = "no"
            editor.archive_usage = False
            editor.archive_script = "/usr/sbin/slurm.dbd.archive"
            editor.auth_info = "/var/run/munge/munge.socket.2"
            editor.auth_type = "auth/munge"
            editor.auth_alt_types = "auth/jwt"
            editor.auth_alt_parameters = [["jwt_key", "16549684561684@"]]
            editor.dbd_host = "slurmdbd-0"
            editor.dbd_backup_host = "slurmdbd-1"
            editor.debug_level = "info"
            editor.plugin_dir = "/all/these/cool/plugins"
            editor.purge_event_after = "1month"
            editor.purge_job_after = "12month"
            editor.purge_resv_after = "1month"
            editor.purge_step_after = "1month"
            editor.purge_suspend_after = "1month"
            editor.purge_txn_after = "12month"
            editor.purge_usage_after = "24month"
            editor.log_file = "/var/log/slurmdbd.log"
            editor.pid_file = "/var/run/slurmdbd.pid"
            editor.slurm_user = "slurm"
            editor.storage_pass = "supersecretpasswd"
            editor.storage_type = "accounting_storage/mysql"
            editor.storage_user = "slurm"
            editor.storage_host = "127.0.0.1"
            editor.storage_port = 3306
            editor.storage_loc = "slurm_acct_db"

    def test_read_config(self) -> None:
        with SlurmdbdConfEditor("test.conf") as editor:
            self.assertTrue(editor.archive_events)
            self.assertTrue(editor.archive_jobs)
            self.assertTrue(editor.archive_resvs)
            self.assertFalse(editor.archive_steps)
            self.assertFalse(editor.archive_txn)
            self.assertEqual(editor.archive_script, "/usr/sbin/slurm.dbd.archive")
            self.assertEqual(editor.auth_info, "/var/run/munge/munge.socket.2")
            self.assertEqual(editor.auth_type, "auth/munge")
            self.assertListEqual(editor.auth_alt_types, ["auth/jwt"])
            self.assertListEqual(editor.auth_alt_parameters, [["jwt_key", "16549684561684@"]])
            self.assertEqual(editor.dbd_host, "slurmdbd-0")
            self.assertEqual(editor.dbd_backup_host, "slurmdbd-1")
            self.assertEqual(editor.debug_level, "info")
            self.assertEqual(editor.plugin_dir, ["/all/these/cool/plugins"])
            self.assertEqual(editor.purge_event_after, "1month")
            self.assertEqual(editor.purge_job_after, "12month")
            self.assertEqual(editor.purge_resv_after, "1month")
            self.assertEqual(editor.purge_step_after, "1month")
            self.assertEqual(editor.purge_suspend_after, "1month")
            self.assertEqual(editor.purge_txn_after, "12month")
            self.assertEqual(editor.purge_usage_after, "24month")
            self.assertEqual(editor.log_file, "/var/log/slurmdbd.log")
            self.assertEqual(editor.pid_file, "/var/run/slurmdbd.pid")
            self.assertEqual(editor.slurm_user, "slurm")
            self.assertEqual(editor.storage_pass, "supersecretpasswd")
            self.assertEqual(editor.storage_type, "accounting_storage/mysql")
            self.assertEqual(editor.storage_user, "slurm")
            self.assertEqual(editor.storage_host, "127.0.0.1")
            self.assertEqual(editor.storage_port, 3306)
            self.assertEqual(editor.storage_loc, "slurm_acct_db")

    def test_remove_config(self) -> None:
        with SlurmdbdConfEditor("test.conf") as editor:
            del editor.archive_events
            del editor.archive_jobs
            del editor.archive_resvs
            del editor.archive_steps
            del editor.archive_txn
            del editor.archive_usage
            del editor.archive_script
            del editor.auth_info
            del editor.auth_type
            del editor.auth_alt_types
            del editor.auth_alt_parameters
            del editor.dbd_host
            del editor.dbd_backup_host
            del editor.debug_level
            del editor.plugin_dir
            del editor.purge_event_after
            del editor.purge_job_after
            del editor.purge_resv_after
            del editor.purge_step_after
            del editor.purge_suspend_after
            del editor.purge_txn_after
            del editor.purge_usage_after
            del editor.log_file
            del editor.pid_file
            del editor.slurm_user
            del editor.storage_pass
            del editor.storage_type
            del editor.storage_user
            del editor.storage_host
            del editor.storage_port
            del editor.storage_loc

    @classmethod
    def tearDownClass(cls) -> None:
        pathlib.Path("test.conf").unlink(missing_ok=True)
