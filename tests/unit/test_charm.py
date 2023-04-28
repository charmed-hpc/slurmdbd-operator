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

"""Test default charm events such as upgrade charm, install, etc."""

import unittest
from unittest.mock import PropertyMock, patch

import ops.testing
from charm import SlurmdbdCharm
from ops.model import ActiveStatus, BlockedStatus
from ops.testing import Harness

ops.testing.SIMULATE_CAN_CONNECT = True


class TestCharm(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = Harness(SlurmdbdCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    @unittest.expectedFailure
    def test_install_success(self) -> None:
        """Test that slurmdbd can successfully be installed.

        Notes:
            This method is expected to fail due to the 'version' file missing.
        """
        self.harness.charm.on.install.emit()
        self.assertEqual(
            self.harness.charm.unit.status, ActiveStatus("slurmdbd successfully installed")
        )

    @unittest.expectedFailure
    @patch("slurm_ops_manager.SlurmManager.install")
    def test_install_fail(self, install) -> None:
        """Test that slurmdbd install fail handler works.

        Notes:
            This method is expected to fail due to the 'version' file missing.
        """
        install.side_effect = False
        self.harness.charm.on.install.emit()
        self.assertEqual(
            self.harness.charm.unit.status, BlockedStatus("Error installing slurmdbd")
        )

    @patch("pathlib.Path.read_text", return_value="v1.0.0")
    def test_upgrade_charm(self, _) -> None:
        """Test that charm upgrade procedure works."""
        self.harness.charm.on.upgrade_charm.emit()
        self.assertEqual(self.harness.get_workload_version(), "v1.0.0")

    @patch("ops.model.Unit.is_leader", return_value=True)
    def test_is_leader(self, _) -> None:
        """Test that _is_leader method works.

        Notes:
            The _is_leader method should be removed from charm.py
            since _is_leader is already defined in ops.
        """
        self.assertEqual(self.harness.charm._is_leader(), self.harness.charm.unit.is_leader())

    @patch("ops.model.Unit.is_leader", return_value=False)
    def test_is_not_leader(self, _) -> None:
        """Test opposite case of _is_leader method."""
        self.assertEqual(self.harness.charm._is_leader(), self.harness.charm.unit.is_leader())

    def test_check_status_slurm_not_installed(self) -> None:
        """Test that _check_status method works if slurm is not installed."""
        self.harness.charm._stored.slurm_installed = True
        res = self.harness.charm._check_status()
        self.assertEqual(
            self.harness.charm.unit.status, BlockedStatus("Need relations: MySQL,slurmctld")
        )
        self.assertFalse(
            res, msg="_check_status returned value True instead of expected value False."
        )

    @patch("slurm_ops_manager.SlurmManager.install")
    @patch("utils.manager.SlurmdbdManager.restart")
    @patch("utils.manager.SlurmdbdManager.active", return_value=True)
    @patch("charm.SlurmdbdCharm._check_status")
    def test_check_slurmdbd(self, *_) -> None:
        """Test that _check_slurmdbd method works."""
        self.harness.charm._check_slurmdbd(max_attemps=1)
        self.assertNotEqual(self.harness.charm.unit.status, BlockedStatus("Cannot start slurmdbd"))

    @patch("utils.manager.SlurmdbdManager.restart")
    @patch("utils.manager.SlurmdbdManager.active", new_callable=PropertyMock(return_value=False))
    def test_check_slurmdbd_slurm_not_active(self, *_) -> None:
        """Test that proper block status is thrown if slurm is not active."""
        self.harness.charm._check_slurmdbd(max_attemps=1)
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Cannot start slurmdbd"))

    @unittest.expectedFailure
    def test_on_jwt_available(self) -> None:
        """Test that _on_jwt_available method works.

        Notes:
            This test is expected to fail due to jwt_rsa being in StoredState
            in a separate class.
        """
        self.harness.charm._stored.slurm_installed = True
        self.harness.charm.on.jwt_available.emit()
        self.assertTrue(self.harness.charm._stored.jwt_available)

    @patch("interface_slurmdbd.Slurmdbd.get_munge_key")
    @patch("slurm_ops_manager.SlurmManager.configure_munge_key")
    @patch("slurm_ops_manager.SlurmManager.restart_munged", return_value=True)
    def test_on_munge_available(self, *_) -> None:
        """Test that _on_munge_available method works."""
        self.harness.charm._stored.slurm_installed = True
        self.harness.charm.on.munge_available.emit()
        self.assertTrue(self.harness.charm._stored.munge_available)

    @patch("interface_slurmdbd.Slurmdbd.get_munge_key")
    @patch("slurm_ops_manager.SlurmManager.configure_munge_key")
    @patch("slurm_ops_manager.SlurmManager.restart_munged", return_value=False)
    def test_on_munge_available_fail_restart(self, *_) -> None:
        """Test that _on_munge_available properly handles when munge fails to restart."""
        self.harness.charm._stored.slurm_installed = True
        self.harness.charm.on.munge_available.emit()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Error restarting munge"))

    def test_on_slurmctld_unavailable(self) -> None:
        """Test that _on_slurmctld_unavailable method works."""
        self.harness.charm._slurmdbd.on.slurmctld_unavailable.emit()
        self.assertFalse(self.harness.charm._stored.jwt_available)
        self.assertFalse(self.harness.charm._stored.munge_available)

    @patch("slurm_ops_manager.SlurmManager.port", return_value=12345)
    def test_get_port(self, port) -> None:
        """Test that get_port method works."""
        self.assertEqual(self.harness.charm.get_port(), port)

    @patch("slurm_ops_manager.SlurmManager.hostname", return_value="localhost")
    def test_get_hostname(self, hostname) -> None:
        """Test that get_hostname method works."""
        self.assertEqual(self.harness.charm.get_hostname(), hostname)

    def test_set_db_info(self) -> None:
        """Test that set_db_info method works."""
        db_test_info = {
            "db_username": "test",
            "db_password": "default",
            "db_hostname": "localhost",
            "db_port": "3306",
            "db_name": "slurmdbd_accounting",
        }
        self.harness.charm.set_db_info(db_test_info)
        self.assertEqual(self.harness.charm._stored.db_info, db_test_info)

    def test_cluster_name(self) -> None:
        """Test that the cluster name property works."""
        self.harness.charm.cluster_name = "test-cluster"
        self.assertEqual(self.harness.charm.cluster_name, "test-cluster")
