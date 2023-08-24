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

"""Configuration file editor for slurmdbd.conf."""

import logging
import pathlib
import re
from collections import deque
from datetime import datetime
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class Error(Exception):
    """Raise if error is encountered when editing or parsing slurmdbd.conf."""


class _SlurmdbdToken(Enum):
    """slurmdbd.conf keys. Sourced from `man slurmdbd.conf.5`."""

    ArchiveDir = "ArchiveDir"
    ArchiveEvents = "ArchiveEvents"
    ArchiveJobs = "ArchiveJobs"
    ArchiveResvs = "ArchiveResvs"
    ArchiveScript = "ArchiveScript"
    ArchiveSteps = "ArchiveSteps"
    ArchiveSuspend = "ArchiveSuspend"
    ArchiveTXN = "ArchiveTXN"
    ArchiveUsage = "ArchiveUsage"
    AuthInfo = "AuthInfo"
    AuthAltTypes = "AuthAltTypes"
    AuthAltParameters = "AuthAltParameters"
    AuthType = "AuthType"
    CommitDelay = "CommitDelay"
    CommunicationParameters = "CommunicationParameters"
    DbdBackupHost = "DbdBackupHost"
    DbdAddr = "DbdAddr"
    DbdHost = "DbdHost"
    DbdPort = "DbdPort"
    DebugFlags = "DebugFlags"
    DebugLevel = "DebugLevel"
    DebugLevelSyslog = "DebugLevelSyslog"
    DefaultQOS = "DefaultQOS"
    LogFile = "LogFile"
    LogTimeFormat = "LogTimeFormat"
    MaxQueryTimeRange = "MaxQueryTimeRange"
    MessageTimeout = "MessageTimeout"
    Parameters = "Parameters"
    PidFile = "PidFile"
    PluginDir = "PluginDir"
    PrivateData = "PrivateData"
    PurgeEventAfter = "PurgeEventAfter"
    PurgeJobAfter = "PurgeJobAfter"
    PurgeResvAfter = "PurgeResvAfter"
    PurgeStepAfter = "PurgeStepAfter"
    PurgeSuspendAfter = "PurgeSuspendAfter"
    PurgeTXNAfter = "PurgeTXNAfter"
    PurgeUsageAfter = "PurgeUsageAfter"
    SlurmUser = "SlurmUser"
    StorageHost = "StorageHost"
    StorageBackupHost = "StorageBackupHost"
    StorageLoc = "StorageLoc"
    StorageParameters = "StorageParameters"
    StoragePass = "StoragePass"
    StoragePort = "StoragePort"
    StorageType = "StorageType"
    StorageUser = "StorageUser"
    TCPTimeout = "TCPTimeout"
    TrackSlurmctldDown = "TrackSlurmctldDown"
    TrackWCKey = "TrackWCKey"


# Match lines that do not start with "#". This will drop all comment lines.
_match_no_comment = re.compile(r"^(?!#).*[^\n]$", re.MULTILINE)

# Match if entered auth type is valid.
_match_auth_type = re.compile(r"(?<!.)auth/munge(?!.)")


def _check_auth_type(auth_type: str) -> None:
    if not _match_auth_type.match(auth_type):
        raise Error(f"Not a valid auth type: {auth_type}")


# Match if bool value is either yes or no.
_match_bool = re.compile(r"(?<!.)(yes|no)(?!.)")


def _check_bool(bool_val: Union[str, bool]) -> None:
    if bool_val in [True, False]:
        return
    elif not _match_bool.match(bool_val):
        raise Error(f"Not a valid boolean value: {bool_val}")


# Match if entered debug flags are valid.
_match_debug_flag = re.compile(
    (
        r"(?<!.)(DB_ARCHIVE|DB_ASSOC|DB_EVENT|"
        r"DB_JOB|DB_QOS|DB_QUERY|DB_RESERVATION|"
        r"DB_RESOURCE|DB_STEP|DB_TRES|DB_USAGE|DB_WCKEY|FEDERATION)(?!.)"
    )
)


def _check_debug_flag(debug_flag: str) -> None:
    if not _match_debug_flag.match(debug_flag):
        raise Error(f"Not a valid debug flag: {debug_flag}")


# Match if entered debug level is valid.
_match_debug_level = re.compile(
    r"(?<!.)(quiet|fatal|error|info|verbose|debug|debug2|debug3|debug4|debug5)(?!.)"
)


def _check_debug_level(debug_level: str) -> None:
    if not _match_debug_level.match(debug_level):
        raise Error(f"Not a valid debug level: {debug_level}")


# Match if entered log time format is valid.
_match_log_time_format = re.compile(
    r"(?<!.)(iso8601|iso8601_ms|rfc5424|rfc5424_ms|clock|short)(?!.)"
)


def _check_log_time_format(time_format: str) -> None:
    if not _match_log_time_format.match(time_format):
        raise Error(f"Not a valid log time format: {time_format}")


# Match if password does not contain "#".
_match_password = re.compile(r"#")


def _check_password(password: str) -> None:
    if len(_match_password.findall(password)) > 0:
        raise Error("Password cannot contain '#'")


# Match if entered port number is valid.
_match_port_num = re.compile(r"(?<!\d)\d{1,5}(?!\d)")


def _check_port_num(port_number: int) -> None:
    if not _match_port_num.match(str(port_number)):
        raise Error(f"Not a valid port number: {port_number}")


# Match if entered private data are valid.
_match_private_data = re.compile(r"(?<!.)(accounts|events|jobs|reservations|usage|users)(?!.)")


def _check_private_data(private_data: str) -> None:
    if not _match_private_data.match(private_data):
        raise Error(f"Not a valid private data entry: {private_data}")


# Match if entered storage type is valid.
_match_storage_type = re.compile(r"(?<!.)(accounting_storage/mysql)(?!.)")


def _check_storage_type(storage_type: str) -> None:
    if not _match_storage_type.match(storage_type):
        raise Error(f"Not a valid storage type: {storage_type}")


# Match if entered time meets slurmdbd time format requirements.
_match_time_format = re.compile(r"^\d+(hour|day|month)$")


def _check_time_format(time_format: str) -> None:
    if not _match_time_format.match(time_format):
        raise Error(f"Not a valid time format: {time_format}")


# Match if entered time range is valid.
_match_query_time_format = re.compile(
    r"(?<!.)(\d+-\d+:\d+:\d+|\d+-\d+|\d+:\d+:\d+|\d+:\d+|INFINITE)(?!.)"
)


def _check_query_time_format(time_format: str) -> None:
    if not _match_query_time_format.match(time_format):
        raise Error(f"Not a valid max query time format: {time_format}")


def _parse_token(token: str) -> Dict[str, Any]:
    """Parse slurmdbd configuration tokens into a Pythonic format.

    Args:
        token: Token in "key=value" format.

    Returns:
        Dict[str, Any]: The parsed token.
    """
    # Classify token and return as {token: value}. i.e. {DbdPort: "12345"}
    charset, processed = deque(c for c in token), []
    while charset:
        if (char := charset.popleft()) == "=":
            if hasattr(_SlurmdbdToken, (token_name := "".join(processed))):
                return {getattr(_SlurmdbdToken, token_name): "".join(c for c in charset)}
            else:
                raise Error(f"Unrecognized slurmdbd configuration option: {token_name}")
        else:
            processed.append(char)


class SlurmdbdConfEditor:
    """Edit slurmdbd.conf file.

    Args:
        conf_file_path: Path to the slurmdb configuration file Default: "/etc/slurmdbd.conf".

    Notes:
        See https://manpages.ubuntu.com/manpages/jammy/en/man5/slurmdbd.conf.5.html for more
        information on what each individual slurmdbd configuration parameter controls.
    """

    def __init__(self, conf_file_path: str = "/etc/slurmdbd.conf") -> None:
        self._conf_file = pathlib.Path(conf_file_path)
        self._metadata = {}

        if not self._conf_file.exists():
            logger.debug(f"Creating slurmdbd.conf at {self._conf_file}.")
            self._conf_file.touch()
        else:
            self.load()

    def __enter__(self) -> "SlurmdbdConfEditor":
        """Load slurmdbd.conf file when entering the xcontext manager."""
        return self

    def __exit__(self, *_: Any) -> None:
        """Dump the updated slurmdbd.conf file when exiting the context manager."""
        self.dump()

    @property
    def path(self) -> pathlib.Path:
        """Get the slurmdbd.conf file for pathlib operations."""
        return self._conf_file

    def load(self) -> None:
        """Load slurmdbd.conf file into memory."""
        logger.debug(f"Parsing slurmdbd.conf at {self._conf_file}.")
        target = deque(lex for lex in re.findall(_match_no_comment, self._conf_file.read_text()))
        parsed_data = self._scan(target)
        if parsed_data is None:
            logger.debug(f"Parsed slurmdbd.conf file {self._conf_file} is empty.")
        else:
            self._metadata = parsed_data

    def dump(self) -> None:
        """Dump parsed slurmdbd.conf into a file."""
        if self._metadata == {}:
            logger.warning("Writing empty slurmdbd configuration")

        logger.debug(f"Dumping new slurmdbd.conf to {self._conf_file}.")
        content = [
            "#",
            f"# {self._conf_file} generated at {datetime.now()}",
            "#",
        ]
        content.extend(f"{k.value}={v}" for k, v in self._metadata.items())
        with self._conf_file.open("wt") as fout:
            fout.writelines(f"{i}\n" for i in content)

    def clear(self) -> None:
        """Clear the currently loaded slurmdbd.conf file from metadata."""
        self._metadata = {}

    def _scan(self, metadata: Deque[str]) -> Dict[str, Any]:
        """Recursive scanner for parsing slurmdbd.conf.

        Args:
            metadata: The lines for slurmdbd.conf formatted into a queue.

        Returns:
            (Dict[str, Any]): Parsed slurmdbd.conf file.
        """
        result_store = {}
        while metadata:
            lexeme = metadata.popleft()
            logger.debug(f"Parsing lexeme: {lexeme}.")
            result_store.update(_parse_token(lexeme))

        return result_store

    @property
    def archive_dir(self) -> Optional[str]:
        """Get configuration value for parameter `ArchiveDir`."""
        return self._metadata.get(_SlurmdbdToken.ArchiveDir, None)

    @archive_dir.setter
    def archive_dir(self, value: str) -> None:
        """Set configuration value for parameter `ArchiveDir`."""
        self._metadata[_SlurmdbdToken.ArchiveDir] = value

    @archive_dir.deleter
    def archive_dir(self) -> None:
        """Delete configuration parameter `ArchiveDir`."""
        del self._metadata[_SlurmdbdToken.ArchiveDir]

    @property
    def archive_events(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveEvents`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveEvents, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveEvents, None) == "no"
            else None
        )

    @archive_events.setter
    def archive_events(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveEvents`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveEvents] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_events.deleter
    def archive_events(self) -> None:
        """Delete configuration parameter `ArchiveEvents`."""
        del self._metadata[_SlurmdbdToken.ArchiveEvents]

    @property
    def archive_jobs(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveJobs`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveJobs, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveJobs, None) == "no"
            else None
        )

    @archive_jobs.setter
    def archive_jobs(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveJobs`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveJobs] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_jobs.deleter
    def archive_jobs(self) -> None:
        """Delete configuration parameter `ArchiveJobs`."""
        del self._metadata[_SlurmdbdToken.ArchiveJobs]

    @property
    def archive_resvs(self) -> Optional[str]:
        """Get configuration value for parameter `ArchiveResvs`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveResvs, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveResvs, None) == "no"
            else None
        )

    @archive_resvs.setter
    def archive_resvs(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveResvs`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveResvs] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_resvs.deleter
    def archive_resvs(self) -> None:
        """Delete configuration parameter `ArchiveResvs`."""
        del self._metadata[_SlurmdbdToken.ArchiveResvs]

    @property
    def archive_script(self) -> Optional[str]:
        """Get configuration value for parameter `ArchiveScript`."""
        return self._metadata.get(_SlurmdbdToken.ArchiveScript, None)

    @archive_script.setter
    def archive_script(self, value: str) -> None:
        """Set configuration value for parameter `ArchiveScript`."""
        self._metadata[_SlurmdbdToken.ArchiveScript] = value

    @archive_script.deleter
    def archive_script(self) -> None:
        """Delete configuration parameter `ArchiveScript`."""
        del self._metadata[_SlurmdbdToken.ArchiveScript]

    @property
    def archive_steps(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveSteps`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveSteps, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveSteps, None) == "no"
            else None
        )

    @archive_steps.setter
    def archive_steps(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveSteps`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveSteps] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_steps.deleter
    def archive_steps(self) -> None:
        """Delete configuration parameter `ArchiveSteps`."""
        del self._metadata[_SlurmdbdToken.ArchiveSteps]

    @property
    def archive_suspend(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveSuspend`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveSuspend, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveSuspend, None) == "no"
            else None
        )

    @archive_suspend.setter
    def archive_suspend(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveSuspend`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveSuspend] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_suspend.deleter
    def archive_suspend(self) -> None:
        """Delete configuration parameter `ArchiveSuspend`."""
        del self._metadata[_SlurmdbdToken.ArchiveSuspend]

    @property
    def archive_txn(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveTXN`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveTXN, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveTXN, None) == "no"
            else None
        )

    @archive_txn.setter
    def archive_txn(self, value) -> None:
        """Set configuration value for parameter `ArchiveTXN`."""
        self._metadata[_SlurmdbdToken.ArchiveTXN] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_txn.deleter
    def archive_txn(self) -> None:
        """Delete configuration parameter `ArchiveTXN`."""
        del self._metadata[_SlurmdbdToken.ArchiveTXN]

    @property
    def archive_usage(self) -> Optional[bool]:
        """Get configuration value for parameter `ArchiveUsage`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.ArchiveUsage, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.ArchiveUsage, None) == "no"
            else None
        )

    @archive_usage.setter
    def archive_usage(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `ArchiveUsage`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.ArchiveUsage] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_usage.deleter
    def archive_usage(self) -> None:
        """Delete configuration parameter `ArchiveUsage`."""
        del self._metadata[_SlurmdbdToken.ArchiveUsage]

    @property
    def auth_info(self) -> Optional[str]:
        """Get configuration value for parameter `AuthInfo`."""
        return self._metadata.get(_SlurmdbdToken.AuthInfo, None)

    @auth_info.setter
    def auth_info(self, value: str) -> None:
        """Set configuration value for parameter `AuthInfo`."""
        self._metadata[_SlurmdbdToken.AuthInfo] = value

    @auth_info.deleter
    def auth_info(self) -> None:
        """Delete configuration parameter `AuthInfo`."""
        del self._metadata[_SlurmdbdToken.AuthInfo]

    @property
    def auth_alt_types(self) -> Optional[List[str]]:
        """Get configuration value for parameter `AuthAltTypes`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.AuthAltTypes, None) is None
            else self._metadata.get(_SlurmdbdToken.AuthAltTypes).split(",")
        )

    @auth_alt_types.setter
    def auth_alt_types(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `AuthAltTypes`."""
        value = [value] if isinstance(value, str) else value
        self._metadata[_SlurmdbdToken.AuthAltTypes] = ",".join(value)

    @auth_alt_types.deleter
    def auth_alt_types(self) -> None:
        """Delete configuration parameter `AuthAltTypes`."""
        del self._metadata[_SlurmdbdToken.AuthAltTypes]

    @property
    def auth_alt_parameters(self) -> Optional[List[List[str]]]:
        """Get configuration value for parameter `AuthAltParameters`."""
        if (data := self._metadata.get(_SlurmdbdToken.AuthAltParameters, None)) is None:
            return data
        else:
            return [item.split("=") for item in data.split(",")]

    @auth_alt_parameters.setter
    def auth_alt_parameters(self, value: List[List[str]]) -> None:
        """Set configuration value for parameter `AuthAltParameters`."""
        self._metadata[_SlurmdbdToken.AuthAltParameters] = ",".join(
            f"{i[0]}={i[1]}" for i in value
        )

    @auth_alt_parameters.deleter
    def auth_alt_parameters(self) -> None:
        """Delete configuration parameter `AuthAltParameters`."""
        del self._metadata[_SlurmdbdToken.AuthAltParameters]

    @property
    def auth_type(self) -> Optional[str]:
        """Get configuration value for parameter `AuthType`."""
        return self._metadata.get(_SlurmdbdToken.AuthType, None)

    @auth_type.setter
    def auth_type(self, value: str) -> None:
        """Set configuration value for parameter `AuthType`."""
        _check_auth_type(value)
        self._metadata[_SlurmdbdToken.AuthType] = value

    @auth_type.deleter
    def auth_type(self) -> None:
        """Delete configuration parameter `AuthType`."""
        del self._metadata[_SlurmdbdToken.AuthType]

    @property
    def commit_delay(self) -> Optional[int]:
        """Get configuration value for parameter `CommitDelay`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.CommitDelay, None) is None
            else int(self._metadata.get(_SlurmdbdToken.CommitDelay))
        )

    @commit_delay.setter
    def commit_delay(self, value: int) -> None:
        """Set configuration value for parameter `CommitDelay`."""
        self._metadata[_SlurmdbdToken.CommitDelay] = value

    @commit_delay.deleter
    def commit_delay(self) -> None:
        """Delete configuration parameter `CommitDelay`."""
        del self._metadata[_SlurmdbdToken.CommitDelay]

    @property
    def communication_parameters(self) -> Optional[List[str]]:
        """Get configuration value for parameter `CommunicationParameters`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.CommunicationParameters, None) is None
            else self._metadata.get(_SlurmdbdToken.CommunicationParameters).split(",")
        )

    @communication_parameters.setter
    def communication_parameters(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `CommunicationParameters`."""
        value = [value] if isinstance(value, str) else value
        self._metadata[_SlurmdbdToken.CommunicationParameters] = ",".join(value)

    @communication_parameters.deleter
    def communication_parameters(self) -> None:
        """Delete configuration parameter `CommunicationParameters`."""
        del self._metadata[_SlurmdbdToken.CommunicationParameters]

    @property
    def dbd_backup_host(self) -> Optional[str]:
        """Get configuration value for parameter `DbdBackupHost`."""
        return self._metadata.get(_SlurmdbdToken.DbdBackupHost, None)

    @dbd_backup_host.setter
    def dbd_backup_host(self, value: str) -> None:
        """Set configuration value for parameter `DbdBackupHost`."""
        self._metadata[_SlurmdbdToken.DbdBackupHost] = value

    @dbd_backup_host.deleter
    def dbd_backup_host(self) -> None:
        """Delete configuration parameter `DbdBackupHost`."""
        del self._metadata[_SlurmdbdToken.DbdBackupHost]

    @property
    def dbd_addr(self) -> Optional[str]:
        """Get configuration value for parameter `DbdAddr`."""
        return self._metadata.get(_SlurmdbdToken.DbdAddr, None)

    @dbd_addr.setter
    def dbd_addr(self, value: str) -> None:
        """Set configuration value for parameter `DbdAddr`."""
        self._metadata[_SlurmdbdToken.DbdAddr] = value

    @dbd_addr.deleter
    def dbd_addr(self) -> None:
        """Delete configuration parameter `DbdAddr`."""
        del self._metadata[_SlurmdbdToken.DbdAddr]

    @property
    def dbd_host(self) -> Optional[str]:
        """Get configuration value for parameter `DbdHost`."""
        return self._metadata.get(_SlurmdbdToken.DbdHost, None)

    @dbd_host.setter
    def dbd_host(self, value: str) -> None:
        """Set configuration value for parameter `DbdHost`."""
        self._metadata[_SlurmdbdToken.DbdHost] = value

    @dbd_host.deleter
    def dbd_host(self) -> None:
        """Delete configuration parameter `DbdHost`."""
        del self._metadata[_SlurmdbdToken.DbdHost]

    @property
    def dbd_port(self) -> Optional[int]:
        """Get configuration value for parameter `DbdPort`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.DbdPort, None) is None
            else int(self._metadata.get(_SlurmdbdToken.DbdPort))
        )

    @dbd_port.setter
    def dbd_port(self, value: int) -> None:
        """Set configuration value for parameter `DbdPort`."""
        _check_port_num(value)
        self._metadata[_SlurmdbdToken.DbdPort] = value

    @dbd_port.deleter
    def dbd_port(self) -> None:
        """Delete configuration parameter `DbdPort`."""
        del self._metadata[_SlurmdbdToken.DbdPort]

    @property
    def debug_flags(self) -> Optional[List[str]]:
        """Get configuration value for parameter `DebugFlags`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.DebugFlags, None) is None
            else self._metadata.get(_SlurmdbdToken.DebugFlags).split(",")
        )

    @debug_flags.setter
    def debug_flags(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `DebugFlags`."""
        value = [value] if isinstance(value, str) else value
        for flag in value:
            _check_debug_flag(flag)
        self._metadata[_SlurmdbdToken.DebugFlags] = ",".join(value)

    @debug_flags.deleter
    def debug_flags(self) -> None:
        """Delete configuration parameter `DebugFlags`."""
        del self._metadata[_SlurmdbdToken.DebugFlags]

    @property
    def debug_level(self) -> Optional[str]:
        """Get configuration value for parameter `DebugLevel`."""
        return self._metadata.get(_SlurmdbdToken.DebugLevel, None)

    @debug_level.setter
    def debug_level(self, value: str) -> None:
        """Set configuration value for parameter `DebugLevel`."""
        _check_debug_level(value)
        self._metadata[_SlurmdbdToken.DebugLevel] = value

    @debug_level.deleter
    def debug_level(self) -> None:
        """Delete configuration parameter `DebugLevel`."""
        del self._metadata[_SlurmdbdToken.DebugLevel]

    @property
    def debug_level_syslog(self) -> Optional[str]:
        """Get configuration value for parameter `DebugLevelSyslog`."""
        return self._metadata.get(_SlurmdbdToken.DebugLevelSyslog, None)

    @debug_level_syslog.setter
    def debug_level_syslog(self, value: str) -> None:
        """Set configuration value for parameter `DebugLevelSyslog`."""
        _check_debug_level(value)
        self._metadata[_SlurmdbdToken.DebugLevelSyslog] = value

    @debug_level_syslog.deleter
    def debug_level_syslog(self) -> None:
        """Delete configuration parameter `DebugLevelSyslog`."""
        del self._metadata[_SlurmdbdToken.DebugLevelSyslog]

    @property
    def default_qos(self) -> Optional[str]:
        """Get configuration value for parameter `DefaultQOS`."""
        return self._metadata.get(_SlurmdbdToken.DefaultQOS, None)

    @default_qos.setter
    def default_qos(self, value: str) -> None:
        """Set configuration value for parameter `DefaultQOS`."""
        self._metadata[_SlurmdbdToken.DefaultQOS] = value

    @default_qos.deleter
    def default_qos(self) -> None:
        """Delete configuration parameter `DefaultQOS`."""
        del self._metadata[_SlurmdbdToken.DefaultQOS]

    @property
    def log_file(self) -> Optional[str]:
        """Get configuration value for parameter `LogFile`."""
        return self._metadata.get(_SlurmdbdToken.LogFile, None)

    @log_file.setter
    def log_file(self, value: str) -> None:
        """Set configuration value for parameter `LogFile`."""
        self._metadata[_SlurmdbdToken.LogFile] = value

    @log_file.deleter
    def log_file(self) -> None:
        """Delete configuration parameter `LogFile`."""
        del self._metadata[_SlurmdbdToken.LogFile]

    @property
    def log_time_format(self) -> Optional[str]:
        """Get configuration value for parameter `LogTimeFormat`."""
        return self._metadata.get(_SlurmdbdToken.LogTimeFormat, None)

    @log_time_format.setter
    def log_time_format(self, value: str) -> None:
        """Set configuration value for parameter `LogTimeFormat`."""
        _check_log_time_format(value)
        self._metadata[_SlurmdbdToken.LogTimeFormat] = value

    @log_time_format.deleter
    def log_time_format(self) -> None:
        """Delete configuration parameter `LogTimeFormat`."""
        del self._metadata[_SlurmdbdToken.LogTimeFormat]

    @property
    def max_query_time_range(self) -> Optional[str]:
        """Get configuration value for parameter `MaxQueryTimeRange`."""
        return self._metadata.get(_SlurmdbdToken.MaxQueryTimeRange, None)

    @max_query_time_range.setter
    def max_query_time_range(self, value: str) -> None:
        """Set configuration value for parameter `MaxQueryTimeRange`."""
        _check_query_time_format(value)
        self._metadata[_SlurmdbdToken.MaxQueryTimeRange] = value

    @max_query_time_range.deleter
    def max_query_time_range(self) -> None:
        """Delete configuration parameter `MaxQueryTimeRange`."""
        del self._metadata[_SlurmdbdToken.MaxQueryTimeRange]

    @property
    def message_timeout(self) -> Optional[int]:
        """Get configuration value for parameter `MessageTimeout`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.MessageTimeout, None) is None
            else int(self._metadata.get(_SlurmdbdToken.MessageTimeout))
        )

    @message_timeout.setter
    def message_timeout(self, value: int) -> None:
        """Set configuration value for parameter `MessageTimeout`."""
        self._metadata[_SlurmdbdToken.MessageTimeout] = value

    @message_timeout.deleter
    def message_timeout(self) -> None:
        """Delete configuration parameter `MessageTimeout`."""
        del self._metadata[_SlurmdbdToken.MessageTimeout]

    @property
    def parameters(self) -> Optional[List[str]]:
        """Get configuration value for parameter `Parameters`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.Parameters, None) is None
            else self._metadata.get(_SlurmdbdToken.Parameters).split(",")
        )

    @parameters.setter
    def parameters(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `Parameters`."""
        value = [value] if isinstance(value, str) else value
        self._metadata[_SlurmdbdToken.Parameters] = ",".join(value)

    @parameters.deleter
    def parameters(self) -> None:
        """Delete configuration parameter `Parameters`."""
        del self._metadata[_SlurmdbdToken.Parameters]

    @property
    def pid_file(self) -> Optional[str]:
        """Get configuration value for parameter `PidFile`."""
        return self._metadata.get(_SlurmdbdToken.PidFile, None)

    @pid_file.setter
    def pid_file(self, value: str) -> None:
        """Set configuration value for parameter `PidFile`."""
        self._metadata[_SlurmdbdToken.PidFile] = value

    @pid_file.deleter
    def pid_file(self) -> None:
        """Delete configuration parameter `PidFile`."""
        del self._metadata[_SlurmdbdToken.PidFile]

    @property
    def plugin_dir(self) -> Optional[List[str]]:
        """Get configuration value for parameter `PluginDir`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.PluginDir, None) is None
            else self._metadata.get(_SlurmdbdToken.PluginDir).split(":")
        )

    @plugin_dir.setter
    def plugin_dir(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `PluginDir`."""
        value = [value] if isinstance(value, str) else value
        self._metadata[_SlurmdbdToken.PluginDir] = ":".join(value)

    @plugin_dir.deleter
    def plugin_dir(self) -> None:
        """Delete configuration parameter `PluginDir`."""
        del self._metadata[_SlurmdbdToken.PluginDir]

    @property
    def private_data(self) -> Optional[List[str]]:
        """Get configuration value for parameter `PrivateData`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.PrivateData, None) is None
            else self._metadata.get(_SlurmdbdToken.PrivateData).split(",")
        )

    @private_data.setter
    def private_data(self, value: Union[str, List[str]]) -> None:
        """Set configuration value for parameter `PrivateData`."""
        value = [value] if isinstance(value, str) else value
        for data in value:
            _check_private_data(data)
        self._metadata[_SlurmdbdToken.PrivateData] = ",".join(value)

    @private_data.deleter
    def private_data(self) -> None:
        """Delete configuration parameter `PrivateData`."""
        del self._metadata[_SlurmdbdToken.PrivateData]

    @property
    def purge_event_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeEventAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeEventAfter, None)

    @purge_event_after.setter
    def purge_event_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeEventAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeEventAfter] = value

    @purge_event_after.deleter
    def purge_event_after(self) -> None:
        """Delete configuration parameter `PurgeEventAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeEventAfter]

    @property
    def purge_job_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeJobAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeJobAfter, None)

    @purge_job_after.setter
    def purge_job_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeJobAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeJobAfter] = value

    @purge_job_after.deleter
    def purge_job_after(self) -> None:
        """Delete configuration parameter `PurgeJobAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeJobAfter]

    @property
    def purge_resv_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeResvAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeResvAfter, None)

    @purge_resv_after.setter
    def purge_resv_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeResvAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeResvAfter] = value

    @purge_resv_after.deleter
    def purge_resv_after(self) -> None:
        """Delete configuration parameter `PurgeResvAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeResvAfter]

    @property
    def purge_step_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeStepAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeStepAfter, None)

    @purge_step_after.setter
    def purge_step_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeStepAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeStepAfter] = value

    @purge_step_after.deleter
    def purge_step_after(self) -> None:
        """Delete configuration parameter `PurgeStepAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeStepAfter]

    @property
    def purge_suspend_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeSuspendAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeSuspendAfter, None)

    @purge_suspend_after.setter
    def purge_suspend_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeSuspendAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeSuspendAfter] = value

    @purge_suspend_after.deleter
    def purge_suspend_after(self) -> None:
        """Delete configuration parameter `PurgeSuspendAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeSuspendAfter]

    @property
    def purge_txn_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeTXNAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeTXNAfter, None)

    @purge_txn_after.setter
    def purge_txn_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeTXNAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeTXNAfter] = value

    @purge_txn_after.deleter
    def purge_txn_after(self) -> None:
        """Delete configuration parameter `PurgeTXNAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeTXNAfter]

    @property
    def purge_usage_after(self) -> Optional[str]:
        """Get configuration value for parameter `PurgeUsageAfter`."""
        return self._metadata.get(_SlurmdbdToken.PurgeUsageAfter, None)

    @purge_usage_after.setter
    def purge_usage_after(self, value: str) -> None:
        """Set configuration value for parameter `PurgeUsageAfter`."""
        _check_time_format(value)
        self._metadata[_SlurmdbdToken.PurgeUsageAfter] = value

    @purge_usage_after.deleter
    def purge_usage_after(self) -> None:
        """Delete configuration parameter `PurgeUsageAfter`."""
        del self._metadata[_SlurmdbdToken.PurgeUsageAfter]

    @property
    def slurm_user(self) -> Optional[str]:
        """Get configuration value for parameter `SlurmUser`."""
        return self._metadata.get(_SlurmdbdToken.SlurmUser, None)

    @slurm_user.setter
    def slurm_user(self, value: str) -> None:
        """Set configuration value for parameter `SlurmUser`."""
        self._metadata[_SlurmdbdToken.SlurmUser] = value

    @slurm_user.deleter
    def slurm_user(self) -> None:
        """Delete configuration parameter `SlurmUser`."""
        del self._metadata[_SlurmdbdToken.SlurmUser]

    @property
    def storage_host(self) -> Optional[str]:
        """Get configuration value for parameter `StorageHost`."""
        return self._metadata.get(_SlurmdbdToken.StorageHost, None)

    @storage_host.setter
    def storage_host(self, value: str) -> None:
        """Set configuration value for parameter `StorageHost`."""
        self._metadata[_SlurmdbdToken.StorageHost] = value

    @storage_host.deleter
    def storage_host(self) -> None:
        """Delete configuration parameter `StorageHost`."""
        del self._metadata[_SlurmdbdToken.StorageHost]

    @property
    def storage_backup_host(self) -> Optional[str]:
        """Get configuration value for parameter `StorageBackupHost`."""
        return self._metadata.get(_SlurmdbdToken.StorageBackupHost, None)

    @storage_backup_host.setter
    def storage_backup_host(self, value: str) -> None:
        """Set configuration value for parameter `StorageBackupHost`."""
        self._metadata[_SlurmdbdToken.StorageBackupHost] = value

    @storage_backup_host.deleter
    def storage_backup_host(self) -> None:
        """Delete configuration parameter `StorageBackupHost`."""
        del self._metadata[_SlurmdbdToken.StorageBackupHost]

    @property
    def storage_loc(self) -> Optional[str]:
        """Get configuration value for parameter `StorageLoc`."""
        return self._metadata.get(_SlurmdbdToken.StorageLoc, None)

    @storage_loc.setter
    def storage_loc(self, value: str) -> None:
        """Set configuration value for parameter `StorageLoc`."""
        self._metadata[_SlurmdbdToken.StorageLoc] = value

    @storage_loc.deleter
    def storage_loc(self) -> None:
        """Delete configuration parameter `StorageLoc`."""
        del self._metadata[_SlurmdbdToken.StorageLoc]

    @property
    def storage_parameters(self) -> Optional[List[List[str]]]:
        """Get configuration value for parameter `StorageParameters`."""
        if (data := self._metadata.get(_SlurmdbdToken.StorageParameters), None) is None:
            return data
        else:
            return [item.split("=") for item in data.split(",")]

    @storage_parameters.setter
    def storage_parameters(self, value: List[List[str]]) -> None:
        """Set configuration value for parameter `StorageParameters`."""
        self._metadata[_SlurmdbdToken.StorageParameters] = ",".join(
            f"{i[0]}={i[1]}" for i in value
        )

    @storage_parameters.deleter
    def storage_parameters(self) -> None:
        """Delete configuration parameter `StorageParameters`."""
        del self._metadata[_SlurmdbdToken.StorageParameters]

    @property
    def storage_pass(self) -> Optional[str]:
        """Get configuration value for parameter `StoragePass`."""
        return self._metadata.get(_SlurmdbdToken.StoragePass, None)

    @storage_pass.setter
    def storage_pass(self, value: str) -> None:
        """Set configuration value for parameter `StoragePass`."""
        _check_password(value)
        self._metadata[_SlurmdbdToken.StoragePass] = value

    @storage_pass.deleter
    def storage_pass(self) -> None:
        """Delete configuration parameter `StoragePass`."""
        del self._metadata[_SlurmdbdToken.StoragePass]

    @property
    def storage_port(self) -> Optional[int]:
        """Get configuration value for parameter `StoragePort`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.StoragePort, None) is None
            else int(self._metadata.get(_SlurmdbdToken.StoragePort))
        )

    @storage_port.setter
    def storage_port(self, value: int) -> None:
        """Set configuration value for parameter `StoragePort`."""
        _check_port_num(value)
        self._metadata[_SlurmdbdToken.StoragePort] = value

    @storage_port.deleter
    def storage_port(self) -> None:
        """Delete configuration parameter `StoragePort`."""
        del self._metadata[_SlurmdbdToken.StoragePort]

    @property
    def storage_type(self) -> Optional[str]:
        """Get configuration value for parameter `StorageType`."""
        return self._metadata.get(_SlurmdbdToken.StorageType, None)

    @storage_type.setter
    def storage_type(self, value: str) -> None:
        """Set configuration value for parameter `StorageType`."""
        _check_storage_type(value)
        self._metadata[_SlurmdbdToken.StorageType] = value

    @storage_type.deleter
    def storage_type(self) -> None:
        """Delete configuration parameter `StorageType`."""
        del self._metadata[_SlurmdbdToken.StorageType]

    @property
    def storage_user(self) -> Optional[str]:
        """Get configuration value for parameter `StorageUser`."""
        return self._metadata.get(_SlurmdbdToken.StorageUser, None)

    @storage_user.setter
    def storage_user(self, value: str) -> None:
        """Set configuration value for parameter `StorageUser`."""
        self._metadata[_SlurmdbdToken.StorageUser] = value

    @storage_user.deleter
    def storage_user(self) -> None:
        """Delete configuration parameter `StorageUser`."""
        del self._metadata[_SlurmdbdToken.StorageUser]

    @property
    def tcp_timeout(self) -> Optional[int]:
        """Get configuration value for parameter `TCPTimeout`."""
        return (
            None
            if self._metadata.get(_SlurmdbdToken.TCPTimeout, None) is None
            else int(self._metadata.get(_SlurmdbdToken.TCPTimeout))
        )

    @tcp_timeout.setter
    def tcp_timeout(self, value: int) -> None:
        """Set configuration value for parameter `TCPTimeout`."""
        self._metadata[_SlurmdbdToken.TCPTimeout] = value

    @tcp_timeout.deleter
    def tcp_timeout(self) -> None:
        """Delete configuration parameter `TCPTimeout`."""
        del self._metadata[_SlurmdbdToken.TCPTimeout]

    @property
    def track_slurmctld_down(self) -> Optional[bool]:
        """Get configuration value for parameter `TrackSlurmctldDown`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.TrackSlurmctldDown, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.TrackSlurmctldDown, None) == "no"
            else None
        )

    @track_slurmctld_down.setter
    def track_slurmctld_down(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `TrackSlurmctldDown`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.TrackSlurmctldDown] = (
            "yes" if value is True else "no" if value is False else value
        )

    @track_slurmctld_down.deleter
    def track_slurmctld_down(self) -> None:
        """Delete configuration parameter `TrackSlurmctldDown`."""
        del self._metadata[_SlurmdbdToken.TrackSlurmctldDown]

    @property
    def track_wc_key(self) -> Optional[bool]:
        """Get configuration value for parameter `TrackWCKey`."""
        return (
            True
            if self._metadata.get(_SlurmdbdToken.TrackWCKey, None) == "yes"
            else False
            if self._metadata.get(_SlurmdbdToken.TrackWCKey, None) == "no"
            else None
        )

    @track_wc_key.setter
    def track_wc_key(self, value: Union[str, bool]) -> None:
        """Set configuration value for parameter `TrackWCKey`."""
        _check_bool(value)
        self._metadata[_SlurmdbdToken.TrackWCKey] = (
            "yes" if value is True else "no" if value is False else value
        )

    @track_wc_key.deleter
    def track_wc_key(self) -> None:
        """Delete configuration parameter `TrackWCKey`."""
        del self._metadata[_SlurmdbdToken.TrackWCKey]
