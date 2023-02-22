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

"""Configuration file editor for slurmdbd.conf."""

import logging
import pathlib
import re
from collections import deque
from datetime import datetime
from enum import Enum
from typing import Any, Deque, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)


class SlurmdbdConfError(Exception):
    """Base error for any exceptions raised when operating on slurmdbd.conf."""


class SlurmdbdToken(Enum):
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
match_no_comment = re.compile(r"^(?!#).*[^\n]$", re.MULTILINE)

# Match if entered auth type is valid.
match_auth_type = re.compile(r"(?<!.)auth/munge(?!.)")

# Match if bool value is either yes or no.
match_bool = re.compile(r"(?<!.)(yes|no)(?!.)")

# Match if entered debug flags are valid.
match_debug_flag = re.compile(
    (
        r"(?<!.)(DB_ARCHIVE|DB_ASSOC|DB_EVENT|"
        r"DB_JOB|DB_QOS|DB_QUERY|DB_RESERVATION|"
        r"DB_RESOURCE|DB_STEP|DB_TRES|DB_USAGE|DB_WCKEY|FEDERATION)(?!.)"
    )
)

# Match if entered debug level is valid.
match_debug_level = re.compile(
    r"(?<!.)(quiet|fatal|error|info|verbose|debug|debug2|debug3|debug4|debug5)(?!.)"
)

# Match if entered log time format is valid.
match_log_time_format = re.compile(
    r"(?<!.)(iso8601|iso8601_ms|rfc5424|rfc5424_ms|clock|short)(?!.)"
)

# Match if password does not contain "#".
match_password = re.compile(r"#")

# Match if entered port number is valid.
match_port_num = re.compile(r"(?<!\d)\d{1,5}(?!\d)")

# Match if entered private data are valid.
match_private_data = re.compile(r"(?<!.)(accounts|events|jobs|reservations|usage|users)(?!.)")

# Match if entered storage type is valid.
match_storage_type = re.compile(r"(?<!.)(accounting_storage/mysql)(?!.)")

# Match if entered time meets slurmdbd time format requirements.
match_time_format = re.compile(r"^\d+(hour|day|month)$")

# Match if entered time range is valid.
match_query_time_format = re.compile(
    r"(?<!.)(\d+-\d+:\d+:\d+|\d+-\d+|\d+:\d+:\d+|\d+:\d+|INFINITE)(?!.)"
)


class LintRules:
    """Macros for linting slurmdbd.conf and configuration values."""

    @staticmethod
    def check_auth_type(auth_type: str) -> None:
        if not match_auth_type.match(auth_type):
            raise SlurmdbdConfError(f"Not a valid auth type: {auth_type}")

    @staticmethod
    def check_bool(bool_val: Union[str, bool]) -> None:
        if type(bool_val) == bool:
            return
        elif not match_bool.match(bool_val):
            raise SlurmdbdConfError(f"Not a valid boolean value: {bool_val}")

    @staticmethod
    def check_debug_flag(debug_flag: str) -> None:
        if not match_debug_flag.match(debug_flag):
            raise SlurmdbdConfError(f"Not a valid debug flag: {debug_flag}")

    @staticmethod
    def check_debug_level(debug_level: str) -> None:
        if not match_debug_level.match(debug_level):
            raise SlurmdbdConfError(f"Not a valid debug level: {debug_level}")

    @staticmethod
    def check_log_time_format(time_format: str) -> None:
        if not match_log_time_format.match(time_format):
            raise SlurmdbdConfError(f"Not a valid log time format: {time_format}")

    @staticmethod
    def check_password(password: str) -> None:
        if len(match_password.findall(password)) > 0:
            raise SlurmdbdConfError("Password cannot contain '#'")

    @staticmethod
    def check_port(port_number: int) -> None:
        if not match_port_num.match(str(port_number)):
            raise SlurmdbdConfError(f"Not a valid port number: {port_number}")

    @staticmethod
    def check_private_data(private_data: str) -> None:
        if not match_private_data.match(private_data):
            raise SlurmdbdConfError(f"Not a valid private data entry: {private_data}")

    @staticmethod
    def check_storage_type(storage_type: str) -> None:
        if not match_storage_type.match(storage_type):
            raise SlurmdbdConfError(f"Not a valid storage type: {storage_type}")

    @staticmethod
    def check_time_format(time_format: str) -> None:
        if not match_time_format.match(time_format):
            raise SlurmdbdConfError(f"Not a valid time format: {time_format}")

    @staticmethod
    def check_query_time_format(time_format: str) -> None:
        if not match_query_time_format.match(time_format):
            raise SlurmdbdConfError(f"Not a valid max query time format: {time_format}")


def parse_token(token: str) -> Dict:
    """Parse slurmdbd configuration tokens into Python-understandable format.

    Args:
        token (str): Token in "key=value" format.

    Returns:
        (Dict[str, Any]): The parsed token.
    """
    # Classify token and return as {token: value}. i.e. {DbdPort: "12345"}
    charset, processed = deque(c for c in token), []
    while charset:
        if (c := charset.popleft()) == "=":
            if hasattr(SlurmdbdToken, (token_name := "".join(processed))):
                return {getattr(SlurmdbdToken, token_name): "".join(c for c in charset)}
            else:
                raise SlurmdbdConfError(
                    f"Unrecognized slurmdbd configuration option: {token_name}"
                )
        else:
            processed.append(c)


class SlurmdbdConfEditor:
    """Abstraction of slurmdbd.conf as a Python object.

    Args:
        conf_file_path (str): Path to the slurmdb configuration file
            (Default: "/etc/slurmdbd.conf").
        wait (bool): False - load slurmdbd.conf when SlurmDBConf is initialized.
            True - do not load slurmdbd.conf when SlurmDBConf is initialized. (Default: False)
    """

    def __new__(cls, *args, **kwargs) -> "SlurmdbdConfEditor":
        """Create new SlurmdbdConfEditor instance."""
        if not hasattr(cls, f"_{cls.__name__}__instance"):
            cls.__instance = super(SlurmdbdConfEditor, cls).__new__(cls)
            cls.__instance.__initialized = False
        return cls.__instance

    def __init__(self, conf_file_path: str = "/etc/slurmdbd.conf", wait: bool = False) -> None:
        if self.__initialized:
            return

        self.__conf_file = pathlib.Path(conf_file_path)
        self.__metadata = {}

        if not self.__conf_file.exists():
            logger.debug(f"Creating slurmdbd.conf at {conf_file_path}.")
            self.__conf_file.touch()

        if not wait:
            self.load()

    @property
    def path(self) -> str:
        """Get the file path to the slurmdbd configuration file."""
        return str(self.__conf_file)

    def load(self) -> None:
        """Load slurmdbd.conf file into memory."""
        logger.debug(f"Loading {str(self.__conf_file)} into memory.")
        self.__metadata = self.__scan(
            deque(lex for lex in re.findall(match_no_comment, self.__conf_file.read_text()))
        )

    def dump(self) -> None:
        """Dump parsed slurmdbd.conf into a file."""
        if self.__metadata == {}:
            logger.warning("Writing empty slurmdbd configuration")

        logger.debug(f"Dumping new slurmdbd.conf to {str(self.__conf_file)}.")
        content = [
            "#",
            f"# {str(self.__conf_file)} generated at {str(datetime.now())}",
            "#",
        ]
        content.extend(f"{k.value}={v}" for k, v in self.__metadata.items())
        with self.__conf_file.open("wt") as fout:
            fout.writelines(f"{i}\n" for i in content)

    def clear(self) -> None:
        """Clear the currently loaded slurmdbd.conf file from metadata."""
        self.__metadata = {}

    def __scan(self, metadata: Deque[str], _result_store: Dict[str, Any] = {}) -> Dict[str, Any]:
        """Recursive scanner for parsing slurmdbd.conf.

        Args:
            metadata (Deque[str]): The lines for slurmdbd.conf formatted into a queue.
            _result_store (Dict[str, Any]): Where each parsed line is stored.

        Returns:
            (Dict[str, Any]): Fully parsed slurmdbd.conf file.
        """
        if metadata:
            lexeme = metadata.popleft()
            logger.debug(f"Parsing lexeme: {lexeme}.")
            _result_store.update(parse_token(lexeme))
            self.__scan(metadata, _result_store)

        return _result_store

    @property
    def archive_dir(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.ArchiveDir, None)

    @archive_dir.setter
    def archive_dir(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.ArchiveDir] = value

    @archive_dir.deleter
    def archive_dir(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveDir]

    @property
    def archive_events(self) -> Union[bool, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveEvents, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveEvents, None) == "no"
            else None
        )

    @archive_events.setter
    def archive_events(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveEvents] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_events.deleter
    def archive_events(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveEvents]

    @property
    def archive_jobs(self) -> Union[bool, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveJobs, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveJobs, None) == "no"
            else None
        )

    @archive_jobs.setter
    def archive_jobs(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveJobs] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_jobs.deleter
    def archive_jobs(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveJobs]

    @property
    def archive_resvs(self) -> Union[str, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveResvs, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveResvs, None) == "no"
            else None
        )

    @archive_resvs.setter
    def archive_resvs(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveResvs] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_resvs.deleter
    def archive_resvs(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveResvs]

    @property
    def archive_script(self) -> Union[str, None]:
        return (
            None
            if (holder := self.__metadata.get(SlurmdbdToken.ArchiveScript, None)) is None
            else holder.get("value", None)
        )

    @archive_script.setter
    def archive_script(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.ArchiveScript] = value

    @archive_script.deleter
    def archive_script(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveScript]

    @property
    def archive_steps(self) -> Union[bool, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveSteps, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveSteps, None) == "no"
            else None
        )

    @archive_steps.setter
    def archive_steps(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveSteps] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_steps.deleter
    def archive_steps(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveSteps]

    @property
    def archive_suspend(self) -> Union[bool, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveSuspend, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveSuspend, None) == "no"
            else None
        )

    @archive_suspend.setter
    def archive_suspend(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveSuspend] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_suspend.deleter
    def archive_suspend(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveSuspend]

    @property
    def archive_txn(self) -> Union[bool, None]:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveTXN, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveTXN, None) == "no"
            else None
        )

    @archive_txn.setter
    def archive_txn(self, value) -> None:
        self.__metadata[SlurmdbdToken.ArchiveTXN] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_txn.deleter
    def archive_txn(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveTXN]

    @property
    def archive_usage(self) -> None:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.ArchiveUsage, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.ArchiveUsage, None) == "no"
            else None
        )

    @archive_usage.setter
    def archive_usage(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.ArchiveUsage] = (
            "yes" if value is True else "no" if value is False else value
        )

    @archive_usage.deleter
    def archive_usage(self) -> None:
        del self.__metadata[SlurmdbdToken.ArchiveUsage]

    @property
    def auth_info(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.AuthInfo, None)

    @auth_info.setter
    def auth_info(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.AuthInfo] = value

    @auth_info.deleter
    def auth_info(self) -> None:
        del self.__metadata[SlurmdbdToken.AuthInfo]

    @property
    def auth_alt_types(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.AuthAltTypes, None) is None
            else self.__metadata.get(SlurmdbdToken.AuthAltTypes).split(",")
        )

    @auth_alt_types.setter
    def auth_alt_types(self, value: List[str]) -> None:
        self.__metadata[SlurmdbdToken.AuthAltTypes] = ",".join(value)

    @auth_alt_types.deleter
    def auth_alt_types(self) -> None:
        del self.__metadata[SlurmdbdToken.AuthAltTypes]

    @property
    def auth_alt_parameters(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.AuthAltParameters, None) is None
            else self.__metadata.get(SlurmdbdToken.AuthAltParameters).split(",")
        )

    @auth_alt_parameters.setter
    def auth_alt_parameters(self, value: List[Tuple[str, str]]) -> None:
        self.__metadata[SlurmdbdToken.AuthAltParameters] = ",".join(
            f"{i[0]}={i[1]}" for i in value
        )

    @auth_alt_parameters.deleter
    def auth_alt_parameters(self) -> None:
        del self.__metadata[SlurmdbdToken.AuthAltParameters]

    @property
    def auth_type(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.AuthType, None)

    @auth_type.setter
    def auth_type(self, value: str) -> None:
        LintRules.check_auth_type(value)
        self.__metadata[SlurmdbdToken.AuthType] = value

    @auth_type.deleter
    def auth_type(self) -> None:
        del self.__metadata[SlurmdbdToken.AuthType]

    @property
    def commit_delay(self) -> Union[int, None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.CommitDelay, None) is None
            else int(self.__metadata.get(SlurmdbdToken.CommitDelay))
        )

    @commit_delay.setter
    def commit_delay(self, value: int) -> None:
        self.__metadata[SlurmdbdToken.CommitDelay] = value

    @commit_delay.deleter
    def commit_delay(self) -> None:
        del self.__metadata[SlurmdbdToken.CommitDelay]

    @property
    def communication_parameters(self) -> None:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.CommunicationParameters, None) is None
            else self.__metadata.get(SlurmdbdToken.CommunicationParameters).split(",")
        )

    @communication_parameters.setter
    def communication_parameters(self, value: List[str]) -> None:
        self.__metadata[SlurmdbdToken.CommunicationParameters] = ",".join(value)

    @communication_parameters.deleter
    def communication_parameters(self) -> None:
        del self.__metadata[SlurmdbdToken.CommunicationParameters]

    @property
    def dbd_backup_host(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.DbdBackupHost, None)

    @dbd_backup_host.setter
    def dbd_backup_host(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.DbdBackupHost] = value

    @dbd_backup_host.deleter
    def dbd_backup_host(self) -> None:
        del self.__metadata[SlurmdbdToken.DbdBackupHost]

    @property
    def dbd_addr(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.DbdAddr, None)

    @dbd_addr.setter
    def dbd_addr(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.DbdAddr] = value

    @dbd_addr.deleter
    def dbd_addr(self) -> None:
        del self.__metadata[SlurmdbdToken.DbdAddr]

    @property
    def dbd_host(self) -> None:
        return self.__metadata.get(SlurmdbdToken.DbdHost, None)

    @dbd_host.setter
    def dbd_host(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.DbdHost] = value

    @dbd_host.deleter
    def dbd_host(self) -> None:
        del self.__metadata[SlurmdbdToken.DbdHost]

    @property
    def dbd_port(self) -> Union[int, None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.DbdPort, None) is None
            else int(self.__metadata.get(SlurmdbdToken.DbdPort))
        )

    @dbd_port.setter
    def dbd_port(self, value: int) -> None:
        LintRules.check_port(value)
        self.__metadata[SlurmdbdToken.DbdPort] = value

    @dbd_port.deleter
    def dbd_port(self) -> None:
        del self.__metadata[SlurmdbdToken.DbdPort]

    @property
    def debug_flags(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.DebugFlags, None) is None
            else self.__metadata.get(SlurmdbdToken.DebugFlags).split(",")
        )

    @debug_flags.setter
    def debug_flags(self, value: List[str]) -> None:
        for flag in value:
            LintRules.check_debug_flag(flag)
        self.__metadata[SlurmdbdToken.DebugFlags] = ",".join(value)

    @debug_flags.deleter
    def debug_flags(self) -> None:
        del self.__metadata[SlurmdbdToken.DebugFlags]

    @property
    def debug_level(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.DebugLevel, None)

    @debug_level.setter
    def debug_level(self, value: str) -> None:
        LintRules.check_debug_level(value)
        self.__metadata[SlurmdbdToken.DebugLevel] = value

    @debug_level.deleter
    def debug_level(self) -> None:
        del self.__metadata[SlurmdbdToken.DebugLevel]

    @property
    def debug_level_syslog(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.DebugLevelSyslog, None)

    @debug_level_syslog.setter
    def debug_level_syslog(self, value: str) -> None:
        LintRules.check_debug_level(value)
        self.__metadata[SlurmdbdToken.DebugLevelSyslog] = value

    @debug_level_syslog.deleter
    def debug_level_syslog(self) -> None:
        del self.__metadata[SlurmdbdToken.DebugLevelSyslog]

    @property
    def default_qos(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.DefaultQOS, None)

    @default_qos.setter
    def default_qos(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.DefaultQOS] = value

    @default_qos.deleter
    def default_qos(self) -> None:
        del self.__metadata[SlurmdbdToken.DefaultQOS]

    @property
    def log_file(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.LogFile, None)

    @log_file.setter
    def log_file(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.LogFile] = value

    @log_file.deleter
    def log_file(self) -> None:
        del self.__metadata[SlurmdbdToken.LogFile]

    @property
    def log_time_format(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.LogTimeFormat, None)

    @log_time_format.setter
    def log_time_format(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.LogTimeFormat] = value

    @log_time_format.deleter
    def log_time_format(self) -> None:
        del self.__metadata[SlurmdbdToken.LogTimeFormat]

    @property
    def max_query_time_range(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.MaxQueryTimeRange, None)

    @max_query_time_range.setter
    def max_query_time_range(self, value: str) -> None:
        LintRules.check_query_time_format(value)
        self.__metadata[SlurmdbdToken.MaxQueryTimeRange] = value

    @max_query_time_range.deleter
    def max_query_time_range(self) -> None:
        del self.__metadata[SlurmdbdToken.MaxQueryTimeRange]

    @property
    def message_timeout(self) -> Union[int, None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.MessageTimeout, None) is None
            else int(self.__metadata.get(SlurmdbdToken.MessageTimeout))
        )

    @message_timeout.setter
    def message_timeout(self, value: int) -> None:
        self.__metadata[SlurmdbdToken.MessageTimeout] = value

    @message_timeout.deleter
    def message_timeout(self) -> None:
        del self.__metadata[SlurmdbdToken.MessageTimeout]

    @property
    def parameters(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.Parameters, None) is None
            else self.__metadata.get(SlurmdbdToken.Parameters).split(",")
        )

    @parameters.setter
    def parameters(self, value: List[str]) -> None:
        self.__metadata[SlurmdbdToken.Parameters] = ",".join(value)

    @parameters.deleter
    def parameters(self) -> None:
        del self.__metadata[SlurmdbdToken.Parameters]

    @property
    def pid_file(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PidFile, None)

    @pid_file.setter
    def pid_file(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.PidFile] = value

    @pid_file.deleter
    def pid_file(self) -> None:
        del self.__metadata[SlurmdbdToken.PidFile]

    @property
    def plugin_dir(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.PluginDir, None) is None
            else self.__metadata.get(SlurmdbdToken.PluginDir).split(":")
        )

    @plugin_dir.setter
    def plugin_dir(self, value: List[str]) -> None:
        self.__metadata[SlurmdbdToken.PluginDir] = ":".join(value)

    @plugin_dir.deleter
    def plugin_dir(self) -> None:
        del self.__metadata[SlurmdbdToken.PluginDir]

    @property
    def private_data(self) -> Union[List[str], None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.PrivateData, None) is None
            else self.__metadata.get(SlurmdbdToken.PrivateData).split(",")
        )

    @private_data.setter
    def private_data(self, value: List[str]) -> None:
        for data in value:
            LintRules.check_private_data(data)
        self.__metadata[SlurmdbdToken.PrivateData] = ",".join(value)

    @private_data.deleter
    def private_data(self) -> None:
        del self.__metadata[SlurmdbdToken.PrivateData]

    @property
    def purge_event_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeEventAfter, None)

    @purge_event_after.setter
    def purge_event_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeEventAfter] = value

    @purge_event_after.deleter
    def purge_event_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeEventAfter]

    @property
    def purge_job_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeJobAfter, None)

    @purge_job_after.setter
    def purge_job_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeJobAfter] = value

    @purge_job_after.deleter
    def purge_job_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeJobAfter]

    @property
    def purge_resv_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeResvAfter, None)

    @purge_resv_after.setter
    def purge_resv_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeResvAfter] = value

    @purge_resv_after.deleter
    def purge_resv_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeResvAfter]

    @property
    def purge_step_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeStepAfter, None)

    @purge_step_after.setter
    def purge_step_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeStepAfter] = value

    @purge_step_after.deleter
    def purge_step_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeStepAfter]

    @property
    def purge_suspend_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeSuspendAfter, None)

    @purge_suspend_after.setter
    def purge_suspend_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeSuspendAfter] = value

    @purge_suspend_after.deleter
    def purge_suspend_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeSuspendAfter]

    @property
    def purge_txn_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeTXNAfter, None)

    @purge_txn_after.setter
    def purge_txn_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeTXNAfter] = value

    @purge_txn_after.deleter
    def purge_txn_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeTXNAfter]

    @property
    def purge_usage_after(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.PurgeUsageAfter, None)

    @purge_usage_after.setter
    def purge_usage_after(self, value: str) -> None:
        LintRules.check_time_format(value)
        self.__metadata[SlurmdbdToken.PurgeUsageAfter] = value

    @purge_usage_after.deleter
    def purge_usage_after(self) -> None:
        del self.__metadata[SlurmdbdToken.PurgeUsageAfter]

    @property
    def slurm_user(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.SlurmUser, None)

    @slurm_user.setter
    def slurm_user(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.SlurmUser] = value

    @slurm_user.deleter
    def slurm_user(self) -> None:
        del self.__metadata[SlurmdbdToken.SlurmUser]

    @property
    def storage_host(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StorageHost, None)

    @storage_host.setter
    def storage_host(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.StorageHost] = value

    @storage_host.deleter
    def storage_host(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageHost]

    @property
    def storage_backup_host(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StorageBackupHost, None)

    @storage_backup_host.setter
    def storage_backup_host(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.StorageBackupHost] = value

    @storage_backup_host.deleter
    def storage_backup_host(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageBackupHost]

    @property
    def storage_loc(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StorageLoc, None)

    @storage_loc.setter
    def storage_loc(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.StorageLoc] = value

    @storage_loc.deleter
    def storage_loc(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageLoc]

    @property
    def storage_parameters(self) -> None:
        return self.__metadata[SlurmdbdToken.StorageParameters]

    @storage_parameters.setter
    def storage_parameters(self, value: List[Tuple[str, str]]) -> None:
        self.__metadata[SlurmdbdToken.StorageParameters] = ",".join(
            f"{i[0]}={i[1]}" for i in value
        )

    @storage_parameters.deleter
    def storage_parameters(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageParameters]

    @property
    def storage_pass(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StoragePass, None)

    @storage_pass.setter
    def storage_pass(self, value: str) -> None:
        LintRules.check_password(value)
        self.__metadata[SlurmdbdToken.StoragePass] = value

    @storage_pass.deleter
    def storage_pass(self) -> None:
        del self.__metadata[SlurmdbdToken.StoragePass]

    @property
    def storage_port(self) -> Union[int, None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.StoragePort, None) is None
            else int(self.__metadata.get(SlurmdbdToken.StoragePort))
        )

    @storage_port.setter
    def storage_port(self, value: int) -> None:
        LintRules.check_port(value)
        self.__metadata[SlurmdbdToken.StoragePort] = value

    @storage_port.deleter
    def storage_port(self) -> None:
        del self.__metadata[SlurmdbdToken.StoragePort]

    @property
    def storage_type(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StorageType, None)

    @storage_type.setter
    def storage_type(self, value: str) -> None:
        LintRules.check_storage_type(value)
        self.__metadata[SlurmdbdToken.StorageType] = value

    @storage_type.deleter
    def storage_type(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageType]

    @property
    def storage_user(self) -> Union[str, None]:
        return self.__metadata.get(SlurmdbdToken.StorageUser, None)

    @storage_user.setter
    def storage_user(self, value: str) -> None:
        self.__metadata[SlurmdbdToken.StorageUser] = value

    @storage_user.deleter
    def storage_user(self) -> None:
        del self.__metadata[SlurmdbdToken.StorageUser]

    @property
    def tcp_timeout(self) -> Union[int, None]:
        return (
            None
            if self.__metadata.get(SlurmdbdToken.TCPTimeout, None) is None
            else int(self.__metadata.get(SlurmdbdToken.TCPTimeout))
        )

    @tcp_timeout.setter
    def tcp_timeout(self, value: int) -> None:
        self.__metadata[SlurmdbdToken.TCPTimeout] = value

    @tcp_timeout.deleter
    def tcp_timeout(self) -> None:
        del self.__metadata[SlurmdbdToken.TCPTimeout]

    @property
    def track_slurmctld_down(self) -> None:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.TrackSlurmctldDown, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.TrackSlurmctldDown, None) == "no"
            else None
        )

    @track_slurmctld_down.setter
    def track_slurmctld_down(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.TrackSlurmctldDown] = (
            "yes" if value is True else "no" if value is False else value
        )

    @track_slurmctld_down.deleter
    def track_slurmctld_down(self) -> None:
        del self.__metadata[SlurmdbdToken.TrackSlurmctldDown]

    @property
    def track_wc_key(self) -> None:
        return (
            True
            if self.__metadata.get(SlurmdbdToken.TrackWCKey, None) == "yes"
            else False
            if self.__metadata.get(SlurmdbdToken.TrackWCKey, None) == "no"
            else None
        )

    @track_wc_key.setter
    def track_wc_key(self, value: Union[str, bool]) -> None:
        LintRules.check_bool(value)
        self.__metadata[SlurmdbdToken.TrackWCKey] = (
            "yes" if value is True else "no" if value is False else value
        )

    @track_wc_key.deleter
    def track_wc_key(self) -> None:
        del self.__metadata[SlurmdbdToken.TrackWCKey]
