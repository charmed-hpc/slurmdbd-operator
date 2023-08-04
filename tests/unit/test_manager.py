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

"""Test the slurmdbd manager class."""

import unittest
from unittest.mock import mock_open, patch

from utils.confeditor import SlurmdbdConfEditor
from utils.manager import SlurmdbdManager


class TestCharm(unittest.TestCase):
    def setUp(self) -> None:
        self.slurmdbd_manager = SlurmdbdManager()

    @patch("pathlib.Path.exists", return_value=False)
    @patch("pathlib.Path.touch")
    def test_conf(self, *_):
        """Tests conf that gives a confeditor."""
        conf_editor = self.slurmdbd_manager.conf
        self.assertIsNotNone(conf_editor)
        self.assertTrue(isinstance(conf_editor, SlurmdbdConfEditor))

    @patch("utils.manager.service_running")
    def test_active(self, _service_running):
        """Tests that active returns the service state."""
        _service_running.return_value = False
        self.assertFalse(self.slurmdbd_manager.active)
        _service_running.assert_called_once_with("slurmdbd")

        _service_running.reset_mock()
        _service_running.return_value = True
        self.assertTrue(self.slurmdbd_manager.active)
        _service_running.assert_called_once_with("slurmdbd")

    @patch("utils.manager.service_start")
    def test_start(self, _service_start):
        """Tests that start attempts to start the service."""
        self.slurmdbd_manager.start()
        _service_start.assert_called_once_with("slurmdbd")

    @patch("utils.manager.service_stop")
    def test_stop(self, _service_stop):
        """Tests that start attempts to stop the service."""
        self.slurmdbd_manager.stop()
        _service_stop.assert_called_once_with("slurmdbd")

    @patch("utils.manager.service_restart")
    def test_restart(self, _service_restart):
        """Tests that restart attempts to restart the service."""
        self.slurmdbd_manager.restart()
        _service_restart.assert_called_once_with("slurmdbd")

    def test_set_environment_var_add_variable_to_end(self):
        """Tests that the set_environment_var adds a new environment variable."""
        content = (
            "# Additional options that are passed to the slurmdbd daemon\n"
            '#SLURMDBD_OPTIONS=""\n'
        )
        m = mock_open(read_data=content)
        with patch("builtins.open", m, create=True):
            self.slurmdbd_manager.set_environment_var(mysql_unix_port='"/path/to/file"')

        handle = m()
        handle.writelines.assert_called_once_with(
            [
                "# Additional options that are passed to the slurmdbd daemon\n",
                '#SLURMDBD_OPTIONS=""\n',
                'MYSQL_UNIX_PORT="/path/to/file"\n',
            ]
        )

    def test_set_environment_var_variable_already_defined(self):
        """Tests that the set_environment_var updates an environment variable."""
        content = (
            "# Additional options that are passed to the slurmdbd daemon\n"
            '#SLURMDBD_OPTIONS=""\n'
            "TEST_ENV_VAR=foo\n"
        )
        m = mock_open(read_data=content)
        with patch("builtins.open", m, create=True):
            self.slurmdbd_manager.set_environment_var(test_env_var="bar")

        handle = m()
        handle.writelines.assert_called_once_with(
            [
                "# Additional options that are passed to the slurmdbd daemon\n",
                '#SLURMDBD_OPTIONS=""\n',
                "TEST_ENV_VAR=bar\n",
            ]
        )

    def test_set_environment_var_no_updates_existing(self):
        """Tests that the _update_defaults only changes desired values."""
        content = (
            "# Additional options that are passed to the slurmdbd daemon\n"
            '#SLURMDBD_OPTIONS=""\n'
            "TEST_ENV_VAR=foo\n"
            "TEST_STABLE=don't change me\n"
        )
        m = mock_open(read_data=content)
        with patch("builtins.open", m, create=True):
            self.slurmdbd_manager.set_environment_var(test_env_var="bar")

        handle = m()
        handle.writelines.assert_called_once_with(
            [
                "# Additional options that are passed to the slurmdbd daemon\n",
                '#SLURMDBD_OPTIONS=""\n',
                "TEST_ENV_VAR=bar\n",
                "TEST_STABLE=don't change me\n",
            ]
        )

    def test_set_environment_var_remove_existing(self):
        """Tests that the set_environment_var removes a value."""
        content = (
            "# Additional options that are passed to the slurmdbd daemon\n"
            '#SLURMDBD_OPTIONS=""\n'
            "TEST_ENV_VAR=foo\n"
            "TEST_STABLE=don't change me\n"
        )
        m = mock_open(read_data=content)
        with patch("builtins.open", m, create=True):
            self.slurmdbd_manager.set_environment_var(test_env_var=None)

        handle = m()
        handle.writelines.assert_called_once_with(
            [
                "# Additional options that are passed to the slurmdbd daemon\n",
                '#SLURMDBD_OPTIONS=""\n',
                "TEST_STABLE=don't change me\n",
            ]
        )

    def test_set_environment_var_mixed_scenario(self):
        """Tests that the set_environment_var adds, updates and removes a value."""
        content = (
            "# Additional options that are passed to the slurmdbd daemon\n"
            '#SLURMDBD_OPTIONS=""\n'
            "DONT_TOUCH_ME\n"
            "TEST_UPDATE=foo\n"
            "TEST_REMOVE=remove me\n"
        )
        m = mock_open(read_data=content)
        with patch("builtins.open", m, create=True):
            self.slurmdbd_manager.set_environment_var(
                test_update="bar", test_remove=None, test_new="added"
            )

        handle = m()
        handle.writelines.assert_called_once_with(
            [
                "# Additional options that are passed to the slurmdbd daemon\n",
                '#SLURMDBD_OPTIONS=""\n',
                "DONT_TOUCH_ME\n",
                "TEST_UPDATE=bar\n",
                "TEST_NEW=added\n",
            ]
        )
