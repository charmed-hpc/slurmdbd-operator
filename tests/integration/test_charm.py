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

"""Test slurmdbd charm against other SLURM charms in the latest/edge channel."""

import asyncio
import logging
import pathlib
from typing import Any, Coroutine

import pytest
import tenacity
from helpers import get_slurmctld_res, get_slurmdbd_res
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

SLURMDBD = "slurmdbd"
SLURMCTLD = "slurmctld"
DATABASE = "mysql"
ROUTER = "mysql-router"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
@pytest.mark.order(1)
async def test_build_and_deploy_against_edge(
    ops_test: OpsTest, slurmdbd_charm: Coroutine[Any, Any, pathlib.Path], charm_base: str
) -> None:
    """Test that the slurmdbd charm can stabilize against slurmctld and MySQL."""
    logger.info(f"Deploying {SLURMDBD} against {SLURMCTLD} and {DATABASE}")
    slurmctld_res = get_slurmctld_res()
    get_slurmdbd_res()
    await asyncio.gather(
        ops_test.model.deploy(
            str(await slurmdbd_charm),
            application_name=SLURMDBD,
            num_units=1,
            base=charm_base,
        ),
        ops_test.model.deploy(
            SLURMCTLD,
            application_name=SLURMCTLD,
            channel="edge",
            num_units=1,
            resources=slurmctld_res,
            base=charm_base,
        ),
        ops_test.model.deploy(
            ROUTER,
            application_name=f"{SLURMDBD}-{ROUTER}",
            channel="dpe/edge",
            num_units=0,
            base=charm_base,
        ),
        ops_test.model.deploy(
            DATABASE,
            application_name=DATABASE,
            channel="8.0/edge",
            num_units=1,
            base="ubuntu@22.04",
        ),
    )
    # Attach resources to charms.
    await ops_test.juju("attach-resource", SLURMCTLD, f"etcd={slurmctld_res['etcd']}")
    # Set relations for charmed applications.
    await ops_test.model.integrate(f"{SLURMDBD}:{SLURMDBD}", f"{SLURMCTLD}:{SLURMDBD}")
    await ops_test.model.integrate(f"{SLURMDBD}-{ROUTER}:backend-database", f"{DATABASE}:database")
    await ops_test.model.integrate(f"{SLURMDBD}:database", f"{SLURMDBD}-{ROUTER}:database")
    # Reduce the update status frequency to accelerate the triggering of deferred events.
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[SLURMDBD], status="active", timeout=1000)
        assert ops_test.model.applications[SLURMDBD].units[0].workload_status == "active"


@pytest.mark.abort_on_fail
@pytest.mark.order(2)
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_slurmdbd_is_active(ops_test: OpsTest) -> None:
    """Test that slurmdbd is active inside Juju unit."""
    logger.info("Checking that slurmdbd daemon is active inside unit")
    slurmdbd_unit = ops_test.model.applications[SLURMDBD].units[0]
    res = (await slurmdbd_unit.ssh("systemctl is-active slurmdbd")).strip("\n")
    assert res == "active"


@pytest.mark.abort_on_fail
@pytest.mark.order(3)
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_slurmdbd_port_listen(ops_test: OpsTest) -> None:
    """Test that slurmdbd is listening on port 6819."""
    logger.info("Checking that slurmdbd is listening on port 6819")
    slurmdbd_unit = ops_test.model.applications[SLURMDBD].units[0]
    res = await slurmdbd_unit.ssh("sudo lsof -t -n -iTCP:6819 -sTCP:LISTEN")
    assert res != ""


@pytest.mark.abort_on_fail
@pytest.mark.order(4)
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_munge_is_active(ops_test: OpsTest) -> None:
    """Test that munge is active inside Juju unit."""
    logger.info("Checking that munge is active inside Juju unit")
    slurmdbd_unit = ops_test.model.applications[SLURMDBD].units[0]
    res = (await slurmdbd_unit.ssh("systemctl is-active munge")).strip("\n")
    assert res == "active"
