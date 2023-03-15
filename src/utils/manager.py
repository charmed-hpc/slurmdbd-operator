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

"""Manager for unit slurmdbd service."""

from charms.operator_libs_linux.v1.systemd import service_restart, service_start, service_stop

from .confeditor import SlurmdbdConfEditor


class SlurmdbdManager:
    """Manage slurmdbd on Juju unit."""

    @property
    def conf(self) -> SlurmdbdConfEditor:
        """Slurmdbd configuration file editor.

        Returns:
            (SlurmdbdConfEditor): Instance of slurmdbd configuration file editor.
        """
        return SlurmdbdConfEditor()

    @staticmethod
    def start() -> None:
        """Start slurmdbd service."""
        service_start("slurmdbd")

    @staticmethod
    def stop() -> None:
        """Stop slurmdbd service."""
        service_stop("slurmdbd")

    @staticmethod
    def restart() -> None:
        """Restart slurmdbd service."""
        service_restart("slurmdbd")
