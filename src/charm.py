#!/usr/bin/env python3
# Copyright 2020 Omnivector Solutions, LLC.
# See LICENSE file for licensing details.

"""Slurmdbd Operator Charm."""

import logging
from pathlib import Path
from time import sleep
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequires,
)
from charms.fluentbit.v0.fluentbit import FluentbitClient
from interface_slurmdbd import Slurmdbd
from interface_slurmdbd_peer import SlurmdbdPeer
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, StoredState
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, ErrorStatus, WaitingStatus
from slurm_ops_manager import SlurmManager
from utils.manager import SlurmdbdManager

logger = logging.getLogger(__name__)


SLURM_ACCT_DB = "slurm_acct_db"
SLURMDBD_DEFAULTS = Path("/etc/default/slurmdbd")


class JwtAvailable(EventBase):
    """Emitted when JWT RSA is available."""


class MungeAvailable(EventBase):
    """Emitted when JWT RSA is available."""


class WriteConfigAndRestartSlurmdbd(EventBase):
    """Emitted when config needs to be written."""


class SlurmdbdCharmEvents(CharmEvents):
    """Slurmdbd emitted events."""

    jwt_available = EventSource(JwtAvailable)
    munge_available = EventSource(MungeAvailable)
    write_config = EventSource(WriteConfigAndRestartSlurmdbd)


class SlurmdbdCharm(CharmBase):
    """Slurmdbd Charm."""

    _stored = StoredState()
    on = SlurmdbdCharmEvents()

    def __init__(self, *args, **kwargs) -> None:
        """Set the default class attributes."""
        super().__init__(*args, **kwargs)

        self._stored.set_default(
            db_info={},
            jwt_available=False,
            munge_available=False,
            slurm_installed=False,
            cluster_name=str(),
        )

        self._slurmdbd_manager = SlurmdbdManager()
        self._db = DatabaseRequires(self, relation_name="database", database_name=SLURM_ACCT_DB)
        self._slurm_manager = SlurmManager(self, "slurmdbd")
        self._slurmdbd = Slurmdbd(self, "slurmdbd")
        self._slurmdbd_peer = SlurmdbdPeer(self, "slurmdbd-peer")
        self._fluentbit = FluentbitClient(self, "fluentbit")

        for event, handler in {
            self.on.install: self._on_install,
            self.on.upgrade_charm: self._on_upgrade,
            self.on.update_status: self._on_update_status,
            self.on.config_changed: self._write_config_and_restart_slurmdbd,
            self.on.jwt_available: self._on_jwt_available,
            self.on.munge_available: self._on_munge_available,
            self.on.write_config: self._write_config_and_restart_slurmdbd,
            self._db.on.database_created: self._on_database_created,
            self._slurmdbd_peer.on.slurmdbd_peer_available: self._write_config_and_restart_slurmdbd,
            self._slurmdbd.on.slurmctld_available: self._on_slurmctld_available,
            self._slurmdbd.on.slurmctld_unavailable: self._on_slurmctld_unavailable,
            # fluentbit
            self.on["fluentbit"].relation_created: self._on_fluentbit_relation_created,
        }.items():
            self.framework.observe(event, handler)

    def _on_install(self, event):
        """Perform installation operations for slurmdbd."""
        self.unit.set_workload_version(Path("version").read_text().strip())

        self.unit.status = WaitingStatus("Installing slurmdbd")

        custom_repo = self.config.get("custom-slurm-repo")
        successful_installation = self._slurm_manager.install(custom_repo)

        if successful_installation:
            self._stored.slurm_installed = True
            self.unit.status = ActiveStatus("slurmdbd successfully installed")
        else:
            self.unit.status = BlockedStatus("Error installing slurmdbd")
            event.defer()
            return

        self._check_status()

    def _on_fluentbit_relation_created(self, event):
        """Set up Fluentbit log forwarding."""
        self._configure_fluentbit()

    def _configure_fluentbit(self):
        logger.debug("## Configuring fluentbit")
        cfg = []
        cfg.extend(self._slurm_manager.fluentbit_config_nhc)
        cfg.extend(self._slurm_manager.fluentbit_config_slurm)
        self._fluentbit.configure(cfg)

    def _on_upgrade(self, event):
        """Perform upgrade operations."""
        self.unit.set_workload_version(Path("version").read_text().strip())

    def _on_update_status(self, event):
        """Handle update status."""
        self._check_status()

    def _on_jwt_available(self, event):
        """Retrieve and configure the jwt_rsa key."""
        # jwt rsa lives in slurm spool dir, it is created when slurm is installed
        if not self._stored.slurm_installed:
            event.defer()
            return

        jwt_rsa = self._slurmdbd.get_jwt_rsa()
        self._slurm_manager.configure_jwt_rsa(jwt_rsa)
        self._stored.jwt_available = True

    def _on_munge_available(self, event):
        """Retrieve munge key and start munged."""
        # munge is installed together with slurm
        if not self._stored.slurm_installed:
            event.defer()
            return

        munge_key = self._slurmdbd.get_munge_key()
        self._slurm_manager.configure_munge_key(munge_key)

        if self._slurm_manager.restart_munged():
            logger.debug("## Munge restarted successfully")
            self._stored.munge_available = True
        else:
            logger.error("## Unable to restart munge")
            self.unit.status = BlockedStatus("Error restarting munge")
            event.defer()

    @classmethod
    def _update_defaults(cls, **kwargs: Optional[str]):
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
        with open("/etc/default/slurmdbd", "w+") as f:
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

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Process the DatabaseCreatedEvent and updates the database parameters.

        Updates the database parameters for the slurmdbd configuration based up on the
        DatabaseCreatedEvent. The type of update depends on the endpoints provided in
        the DatabaseCreatedEvent.

        If the endpoints provided are file paths to unix sockets
        then the /etc/default/slurmdbd file will be updated to tell the MySQL client to
        use the socket.

        If the endpoints provided are Address:Port tuples, then the address and port are
        updated as the database parameters in the slurmdbd.conf configuration file.

        Args:
            event (DatabaseCreatedEvent):
                Information passed by MySQL after the slurm_acct_db database has been created.

        Raises:
            ValueError:
                When the database endpoints are invalid (e.g. empty).
        """
        logger.debug("Configuring new backend database for slurmdbd.")

        socket_endpoints = []
        tcp_endpoints = []
        if not event.endpoints:
            # This is 100% an error condition that the charm doesn't know how to handle
            # and is an unexpected condition. Raise an error here to fail the hook in
            # a bad way. The event isn't deferred as this is a situation that requires
            # a human to look at and resolve the proper next steps. Reprocessing the
            # deferred event will only result in continual errors.
            logger.error(f"No endpoints provided: {event.endpoints}")
            self.unit.status = ErrorStatus("No database endpoints")
            raise ValueError(f"Unexpected endpoint types: {event.endpoints}")

        for endpoint in [ep.strip() for ep in event.endpoints.split(",")]:
            if not endpoint:
                continue

            if endpoint.startswith("file://"):
                socket_endpoints.append(endpoint)
            else:
                tcp_endpoints.append(endpoint)

        db_info = {
            "db_username": event.username,
            "db_password": event.password,
            "db_name": SLURM_ACCT_DB,
        }

        if socket_endpoints:
            # Socket endpoints will be preferred. This is the case when the mysql
            # configuration is using the mysql-router on the local node.
            logger.debug("Updating environment for mysql socket access")
            if len(socket_endpoints) > 1:
                logger.warning(
                    f"{len(socket_endpoints)} socket endpoints are specified, "
                    f"but only first one will be used."
                )
            # Make sure to strip the file:// off the front of the first endpoint
            # otherwise slurmdbd will not be able to connect to the database
            socket = urlparse(socket_endpoints[0]).path
            self._slurmdbd_manager.set_environment_var(mysql_unix_port=f'"{socket}"')
        elif tcp_endpoints:
            # This must be using TCP endpoint and the connection information will
            # be host_address:port. Only one remote mysql service will be configured
            # in this case.
            logger.debug("Using tcp endpoints specified in the relation.")
            if len(tcp_endpoints) > 1:
                logger.warning(
                    f"{len(tcp_endpoints)} tcp endpoints are specified, "
                    f"but only the first one will be used."
                )
            addr, port = tcp_endpoints[0].rsplit(":", 1)
            # Check IPv6 and strip any brackets
            if addr.startswith("[") and addr.endswith("]"):
                addr = addr[1:-1]
            db_info.update(
                {
                    "db_hostname": addr,
                    "db_port": port,
                }
            )
            # Make sure that the MYSQL_UNIX_PORT is removed from the env file.
            self._update_defaults(mysql_unix_port=None)
        else:
            # This is 100% an error condition that the charm doesn't know how to handle
            # and is an unexpected condition. This happens when there are commas but no
            # usable data in the endpoints.
            logger.error(f"No endpoints provided: {event.endpoints}")
            self.unit.status = ErrorStatus("No database endpoints")
            raise ValueError(f"No endpoints provided: {event.endpoints}")

        self.set_db_info(db_info)
        self._write_config_and_restart_slurmdbd(event)

    def _on_slurmctld_available(self, event):
        self.on.jwt_available.emit()
        self.on.munge_available.emit()

        self.on.write_config.emit()
        if self._fluentbit._relation is not None:
            self._configure_fluentbit()

    def _on_slurmctld_unavailable(self, event):
        """Reset state and charm status when slurmctld broken."""
        self._stored.jwt_available = False
        self._stored.munge_available = False
        self._check_status()

    def _is_leader(self):
        return self.model.unit.is_leader()

    def _write_config_and_restart_slurmdbd(self, event):
        """Check for prereqs before writing config/restart of slurmdbd."""
        # Ensure all pre-conditions are met with _check_status(), if not
        # defer the event.
        if not self._check_status():
            event.defer()
            return

        slurmdbd_config = {
            "slurmdbd_debug": self.config.get("slurmdbd-debug"),
            **self._slurmdbd_peer.get_slurmdbd_info(),
            **self._stored.db_info,
        }

        self._slurmdbd_manager.stop()
        self._slurm_manager.render_slurm_configs(slurmdbd_config)

        # At this point, we must guarantee that slurmdbd is correctly
        # initialized. Its startup might take a while, so we have to wait
        # for it.
        self._check_slurmdbd()

        # Only the leader can set relation data on the application.
        # Enforce that no one other than the leader tries to set
        # application relation data.
        if self.model.unit.is_leader():
            self._slurmdbd.set_slurmdbd_info_on_app_relation_data(
                slurmdbd_config,
            )

        self._check_status()

    def _check_slurmdbd(self, max_attemps=5) -> None:
        """Ensure slurmdbd is up and running."""
        logger.debug("## Checking if slurmdbd is active")

        for i in range(max_attemps):
            if self._slurmdbd_manager.active:
                logger.debug("## Slurmdbd running")
                break
            else:
                logger.warning("## Slurmdbd not running, trying to start it")
                self.unit.status = WaitingStatus("Starting slurmdbd")
                self._slurmdbd_manager.restart()
                sleep(3 + i)

        if self._slurmdbd_manager.active:
            self._check_status()
        else:
            self.unit.status = BlockedStatus("Cannot start slurmdbd")

    def _check_status(self) -> bool:  # noqa C901
        """Check that we have the things we need."""
        slurm_installed = self._stored.slurm_installed
        if not slurm_installed:
            self.unit.status = BlockedStatus("Error installing slurm")
            return False

        # we must be sure to initialize the charms correctly. Slurmdbd must
        # first connect to the db to be able to connect to slurmctld correctly
        slurmctld_available = self._stored.jwt_available and self._stored.munge_available
        statuses = {
            "MySQL": {
                "available": self._stored.db_info != {},
                "joined": self._stored.db_info != {},
            },
            "slurmctld": {"available": slurmctld_available, "joined": self._slurmdbd.is_joined},
        }

        relations_needed = []
        waiting_on = []
        for component in statuses.keys():
            if not statuses[component]["joined"]:
                relations_needed.append(component)
            if not statuses[component]["available"]:
                waiting_on.append(component)

        if len(relations_needed):
            msg = f"Need relations: {','.join(relations_needed)}"
            self.unit.status = BlockedStatus(msg)
            return False

        if len(waiting_on):
            msg = f"Waiting on: {','.join(waiting_on)}"
            self.unit.status = WaitingStatus(msg)
            return False

        slurmdbd_info = self._slurmdbd_peer.get_slurmdbd_info()
        if not slurmdbd_info:
            self.unit.status = WaitingStatus("slurmdbd starting")
            return False

        if not self._slurm_manager.check_munged():
            self.unit.status = WaitingStatus("munged starting")
            return False

        self.unit.status = ActiveStatus("slurmdbd available")
        return True

    def get_port(self):
        """Return the port from slurm-ops-manager."""
        return self._slurm_manager.port

    def get_hostname(self):
        """Return the hostname from slurm-ops-manager."""
        return self._slurm_manager.hostname

    def set_db_info(self, new_db_info: Dict[str, Any]) -> None:
        """Set the db_info in the stored state.

        Args:
            new_db_info (Dict[str, Any]):
                New backend database information to set.
        """
        self._stored.db_info.update(new_db_info)

    @property
    def cluster_name(self) -> str:
        """Return the cluster-name."""
        return self._stored.cluster_name

    @cluster_name.setter
    def cluster_name(self, name: str):
        """Set the cluster-name."""
        self._stored.cluster_name = name


if __name__ == "__main__":
    main(SlurmdbdCharm)
