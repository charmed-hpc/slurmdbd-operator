#!/usr/bin/env python3

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

    def test_install_success(self) -> None:
        """Test that slurmdbd can successfully be installed."""
        self.harness.charm.on.install.emit()
        self.assertEqual(
            self.harness.charm.unit.status, ActiveStatus("slurmdbd successfully installed")
        )

    @patch("slurm_ops_manager.SlurmManager.install")
    def test_install_fail(self, install) -> None:
        """Test that slurmdbd install fail handler works."""
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

    @patch("slurm_ops_manager.SlurmManager.needs_reboot", return_value=True)
    def test_check_status_needs_reboot(self, _) -> None:
        """Test that _check_status method works if unit needs reboot."""
        res = self.harness.charm._check_status()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Machine needs reboot"))
        self.assertFalse(
            res, msg="_check_status returned value True instead of expected value False."
        )

    @patch(
        "charm.SlurmdbdCharm._stored.needs_reboot", new_callable=PropertyMock(return_value=False)
    )
    @patch(
        "charm.SlurmdbdCharm._stored.slurm_installed",
        new_callable=PropertyMock(return_value=False),
    )
    def test_check_status_slurm_not_installed(self, *_) -> None:
        """Test that _check_status method works if slurm is not installed."""
        res = self.harness.charm._check_status()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Error installing slurm"))
        self.assertFalse(
            res, msg="_check_status returned value True instead of expected value False."
        )

    @patch("slurm_ops_manager.SlurmManager.install")
    def test_check_slurmdbd(self, _) -> None:
        """Test that _check_slurmdbd method works.

        Notes:
            This method should eventually be made to install slurm rather than patching.
        """
        self.harness.charm._check_slurmdbd(max_attemps=1)
        self.assertTrue(self.harness.charm._slurm_manager.slurm_is_active())

    @patch("slurm_ops_manager.SlurmManager.slurm_is_active", return_value=False)
    def test_check_slurmdbd_slurm_not_active(self, _) -> None:
        """Test that proper block status is thrown if slurm is not active."""
        self.harness.charm._check_slurmdbd(max_attemps=1)
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Cannot start slurmdbd"))

    def test_on_db_unavailable(self) -> None:
        """Test the that _on_db_available method works."""
        self.harness.charm._db.on.database_unavailable.emit()
        self.assertEqual(self.harness.charm._stored.db_info, {})

    @patch(
        "charm.SlurmdbdCharm._stored.slurm_installed", new_callable=PropertyMock(return_value=True)
    )
    def test_on_jwt_available(self, _) -> None:
        """Test that _on_jwt_available method works."""
        self.harness.charm.on.jwt_available.emit()
        self.assertTrue(self.harness.charm._stored.jwt_available)

    @patch(
        "charm.SlurmdbdCharm._stored.slurm_installed", new_callable=PropertyMock(return_value=True)
    )
    @patch("slurm_ops_manager.SlurmManager.restart_munged", return_value=True)
    def test_on_munge_available(self, *_) -> None:
        """Test that _on_munge_available method works."""
        self.harness.charm.on.munge_available.emit()
        self.assertTrue(self.harness.charm._stored.munge_available)

    @patch(
        "charm.SlurmdbdCharm._stored.slurm_installed", new_callable=PropertyMock(return_value=True)
    )
    @patch("slurm_ops_manager.SlurmManager.restart_munged", return_value=False)
    def test_on_munge_available_fail_restart(self, *_) -> None:
        """Test that _on_munge_available properly handles when munge fails to restart."""
        self.harness.charm.on.munge_available.emit()
        self.assertEqual(self.harness.charm.unit.status, BlockedStatus("Error restarting munge"))

    def test_on_slurmctld_unavailable(self) -> None:
        """Test that _on_slurmctld_unavailable method works."""
        self.harness.charm.on.slurmctld_unavailable.emit()
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
