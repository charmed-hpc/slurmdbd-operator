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

from pathlib import Path
from typing import Optional

from charms.operator_libs_linux.v1.systemd import (
    service_restart,
    service_running,
    service_start,
    service_stop,
)

from .confeditor import SlurmdbdConfEditor

SLURMDBD_DEFAULTS = Path("/etc/default/slurmdbd")


class SlurmdbdManager:
    """Manage slurmdbd on Juju unit."""

    @property
    def conf(self) -> SlurmdbdConfEditor:
        """Slurmdbd configuration file editor.

        Returns:
            SlurmdbdConfEditor: Instance of slurmdbd configuration file editor.
        """
        return SlurmdbdConfEditor()

    @property
    def active(self) -> bool:
        """Get if slurmdbd daemon is active or not."""
        return service_running("slurmdbd")

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

    @classmethod
    def set_environment_var(cls, **kwargs: Optional[str]):
        """Update the environment settings in the /etc/defaults/slurmdbd file.

        Updates the slurmdbd defaults environment file to add the specified
        defaults. For example, to add the MY_VAR environment variable invoke with

            self._update_defaults(my_var="foobar")

        The key will be converted to all uppercase prior to being written to
        the specified file. To remove a setting, specify the value as None, e.g.:

            self._update_defaults(my_var=None)

        Note: variable names will be converted to upper case when being written
        to the file and for comparison of keys.

        Args:
            **kwargs:
                The environment variables that should be set or unset. The key
                will be upper-cased for the environment variable.
        """
        with open(SLURMDBD_DEFAULTS, "w+") as f:
            updated_contents = []

            keys_processed = set()

            for line in f.readlines():
                # Lines beginning with a hash are a comment. Don't process these
                # but do add the line to the output buffer to preserve it.
                if line.startswith("#"):
                    updated_contents.append(line)
                    continue

                # Attempt to get the environment variable and split it on the =.
                # If the line doesn't have an equals, then don't process the line
                # but do add the line to the output buffer to preserve it.
                line_parts = line.split("=", 1)
                if len(line_parts) < 2:
                    updated_contents.append(line)
                    continue

                var_name = line_parts[0].lower()
                if var_name not in kwargs:
                    updated_contents.append(line)
                    continue

                keys_processed.add(var_name)
                # If explicitly None, then remove from the output buffer as
                # the demand is to unset the variable.
                if kwargs[var_name] is None:
                    continue

                updated_contents.append(f"{var_name.upper()}={kwargs[var_name]}\n")

            for key, value in kwargs.items():
                # Skip any keys already processed.
                if key in keys_processed:
                    continue
                updated_contents.append(f"{key.upper()}={value}\n")

            f.seek(0)
            f.truncate()
            f.writelines(updated_contents)
