#!/usr/bin/env python3
# Copyright 2020-2024 Omnivector, LLC.
# See LICENSE file for licensing details.

"""Slurmdbd Operator Charm."""

import logging
from time import sleep
from typing import Any, Dict, Union
from urllib.parse import urlparse

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseRequires,
)
from constants import CHARM_MAINTAINED_PARAMETERS, SLURM_ACCT_DB
from interface_slurmctld import Slurmctld, SlurmctldAvailableEvent, SlurmctldUnavailableEvent
from ops import (
    ActiveStatus,
    BlockedStatus,
    CharmBase,
    ConfigChangedEvent,
    InstallEvent,
    StoredState,
    UpdateStatusEvent,
    WaitingStatus,
    main,
)
from slurmdbd_ops import SlurmdbdOpsManager

logger = logging.getLogger(__name__)


class SlurmdbdCharm(CharmBase):
    """Slurmdbd Charm."""

    _stored = StoredState()

    def __init__(self, *args, **kwargs) -> None:
        """Set the default class attributes."""
        super().__init__(*args, **kwargs)

        self._stored.set_default(
            slurm_installed=False,
            db_info={},
        )

        self._db = DatabaseRequires(self, relation_name="database", database_name=SLURM_ACCT_DB)
        self._slurmdbd_ops_manager = SlurmdbdOpsManager()
        self._slurmctld = Slurmctld(self, "slurmctld")

        event_handler_bindings = {
            self.on.install: self._on_install,
            self.on.update_status: self._on_update_status,
            self.on.config_changed: self._write_config_and_restart_slurmdbd,
            self._db.on.database_created: self._on_database_created,
            self._slurmctld.on.slurmctld_available: self._on_slurmctld_available,
            self._slurmctld.on.slurmctld_unavailable: self._on_slurmctld_unavailable,
        }
        for event, handler in event_handler_bindings.items():
            self.framework.observe(event, handler)

    def _on_install(self, event: InstallEvent) -> None:
        """Perform installation operations for slurmdbd."""
        if not self.model.unit.is_leader():
            self.unit.status = BlockedStatus("Only singleton slurmdbd currently supported.")
            event.defer()
            return

        self.unit.status = WaitingStatus("Installing slurmdbd")

        if self._slurmdbd_ops_manager.install() is not False:
            self.unit.set_workload_version(self._slurmdbd_ops_manager.version)
            self._stored.slurm_installed = True

            if self._slurmdbd_ops_manager.start_munge():
                logger.debug("## Munge started successfully")
            else:
                logger.error("## Unable to start munge")
                self.unit.status = BlockedStatus("Error restarting munge")
                event.defer()
                return

            self.unit.status = ActiveStatus("slurmdbd successfully installed")
        self._check_status()

    def _on_update_status(self, event: UpdateStatusEvent) -> None:
        """Handle update status."""
        self._check_status()

    def _on_slurmctld_available(self, event: SlurmctldAvailableEvent) -> None:
        """Retrieve and configure the jwt_rsa and munge_key when slurmctld_available."""
        if self._stored.slurm_installed is not True:
            event.defer()
            return

        if (jwt := event.jwt_rsa) is not None:
            self._slurmdbd_ops_manager.write_jwt_rsa(jwt)

        if (munge_key := event.munge_key) is not None:
            self._slurmdbd_ops_manager.stop_munge()
            self._slurmdbd_ops_manager.write_munge_key(munge_key)
            self._slurmdbd_ops_manager.start_munge()

        # Don't try to write the config before the database has been created.
        # Otherwise, this will trigger a defer on this event, which we don't really need
        # or the munge service will restart too many times, triggering a restart limit on
        # systemctl.
        if self._stored.db_info:
            self._write_config_and_restart_slurmdbd(event)

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
            self.unit.status = BlockedStatus("No database endpoints provided")
            raise ValueError(f"Unexpected endpoint types: {event.endpoints}")

        for endpoint in [ep.strip() for ep in event.endpoints.split(",")]:
            if not endpoint:
                continue

            if endpoint.startswith("file://"):
                socket_endpoints.append(endpoint)
            else:
                tcp_endpoints.append(endpoint)

        db_info = {
            "StorageUser": event.username,
            "StoragePass": event.password,
            "StorageLoc": SLURM_ACCT_DB,
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
            self._slurmdbd_ops_manager.set_environment_var(mysql_unix_port=f'"{socket}"')
        elif tcp_endpoints:
            # This must be using TCP endpoint and the connection information will
            # be host_address:port. Only one remote mysql service will be configured
            # in this case.
            logger.debug("Using tcp endpoints specified in the relation")
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
                    "StorageHost": addr,
                    "StoragePort": port,
                }
            )
            # Make sure that the MYSQL_UNIX_PORT is removed from the env file.
            self._slurmdbd_ops_manager.set_environment_var(mysql_unix_port=None)
        else:
            # This is 100% an error condition that the charm doesn't know how to handle
            # and is an unexpected condition. This happens when there are commas but no
            # usable data in the endpoints.
            logger.error(f"No endpoints provided: {event.endpoints}")
            self.unit.status = BlockedStatus("No database endpoints provided")
            raise ValueError(f"No endpoints provided: {event.endpoints}")

        self._stored.db_info = db_info
        self._write_config_and_restart_slurmdbd(event)

    def _on_slurmctld_unavailable(self, event: SlurmctldUnavailableEvent) -> None:
        """Reset state and charm status when slurmctld broken."""
        self._stored.slurmctld_available = False
        self._check_status()

    def _write_config_and_restart_slurmdbd(
        self,
        event: Union[
            ConfigChangedEvent,
            DatabaseCreatedEvent,
            InstallEvent,
            SlurmctldAvailableEvent,
        ],
    ) -> None:
        """Check that we have what we need before we proceed."""
        # Ensure all pre-conditions are met with _check_status(), if not
        # defer the event.
        if not self._check_status():
            event.defer()
            return

        if (
            charm_config_slurmdbd_conf_params := self.config.get("slurmdbd-conf-parameters")
        ) is not None:
            if (
                charm_config_slurmdbd_conf_params
                != self._stored.user_supplied_slurmdbd_conf_params
            ):
                logger.debug("## User supplied parameters changed.")
                self._stored.user_supplied_slurmdbd_conf_params = charm_config_slurmdbd_conf_params

        if binding := self.model.get_binding("slurmctld"):
            slurmdbd_full_config = {
                **CHARM_MAINTAINED_PARAMETERS,
                **self._stored.db_info,
                **{"DbdHost": self._slurmdbd_ops_manager.hostname},
                **{"DbdAddr": f"{binding.network.ingress_address}"},
                **self._get_user_supplied_parameters(),
            }

            if self._slurmctld.is_joined:
                slurmdbd_full_config["AuthAltTypes"] = "auth/jwt"
                slurmdbd_full_config["AuthAltParameters"] = (
                    '"jwt_key=/var/spool/slurmdbd/jwt_hs256.key"'
                )

            self._slurmdbd_ops_manager.stop_slurmdbd()
            self._slurmdbd_ops_manager.write_slurmdbd_conf(slurmdbd_full_config)

            # At this point, we must guarantee that slurmdbd is correctly
            # initialized. Its startup might take a while, so we have to wait
            # for it.
            self._check_slurmdbd()

            # Only the leader can set relation data on the application.
            # Enforce that no one other than the leader tries to set
            # application relation data.
            if self.model.unit.is_leader():
                self._slurmctld.set_slurmdbd_host_on_app_relation_data(
                    self._slurmdbd_ops_manager.hostname
                )
        else:
            logger.debug("Cannot get network binding. Please Debug.")
            event.defer()
            return

        self._check_status()

    def _get_user_supplied_parameters(self) -> Dict[Any, Any]:
        """Gather, parse, and return the user supplied parameters."""
        user_supplied_parameters = {}
        if custom_config := self.config.get("slurmdbd-conf-parameters"):
            try:
                user_supplied_parameters = {
                    line.split("=")[0]: line.split("=")[1]
                    for line in str(custom_config).split("\n")
                    if not line.startswith("#") and line.strip() != ""
                }
            except IndexError as e:
                logger.error(f"Could not parse user supplied parameters: {e}.")
        return user_supplied_parameters

    def _check_slurmdbd(self, max_attemps: int = 5) -> None:
        """Ensure slurmdbd is up and running."""
        logger.debug("## Checking if slurmdbd is active")

        for i in range(max_attemps):
            if self._slurmdbd_ops_manager.is_slurmdbd_active():
                logger.debug("## Slurmdbd running")
                break
            else:
                logger.warning("## Slurmdbd not running, trying to start it")
                self.unit.status = WaitingStatus("Starting slurmdbd ...")
                self._slurmdbd_ops_manager.restart_slurmdbd()
                sleep(3 + i)

        if self._slurmdbd_ops_manager.is_slurmdbd_active():
            self._check_status()
        else:
            self.unit.status = BlockedStatus("Cannot start slurmdbd.")

    def _check_status(self) -> bool:
        """Check that we have the things we need."""
        if self._stored.slurm_installed is not True:
            self.unit.status = BlockedStatus("Error installing slurmdbd.")
            return False

        if self._stored.db_info == {}:
            self.unit.status = WaitingStatus("Waiting on: MySQL")
            return False

        self.unit.status = ActiveStatus()
        return True


if __name__ == "__main__":
    main.main(SlurmdbdCharm)
