#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# The endpoint is never contacted by these tests: only the node count decides
# whether shipping turns on, and nothing is uploaded yet. The short seal
# interval makes the first seal cycle - which repeats the count - run while the
# test is still looking.
PITR_CMDLINE = [
    "--pitr-endpoint", "http://127.0.0.1:9",
    "--pitr-bucket", "pitr-bucket",
    "--pitr-prefix", "chains",
    "--pitr-seal-interval-in-seconds", "1",
    "--logger-log-level", "pitr=debug",
]

# db/pitr/pitr_shipper.cc logs one of these once per boot, on shard 0.
SHIPPING_VERDICT = r"Point-in-time recovery shipping is (on|off)"
# Both the startup refusal and the one a seal cycle makes count the nodes.
SHIPPING_OFF = "Point-in-time recovery shipping is off"


def ignore_shipping_off(manager: ScyllaClusterManager) -> None:
    """A refusal to ship is logged at ERROR, and some of these tests expect one."""
    manager.ignore_log_patterns.append(SHIPPING_OFF)


async def shipping_verdict(manager: ScyllaClusterManager, server_id, from_mark: int | None = None) -> tuple[str, str]:
    """Wait for the node's shipping verdict and return it with the line it came from."""
    log = await manager.server_open_log(server_id)
    _, matches = await log.wait_for(SHIPPING_VERDICT, from_mark=from_mark, timeout=120)
    line, match = matches[0]
    return match.group(1), line


async def test_single_node_ships(manager: ScyllaClusterManager):
    """A lone node with shipping configured turns shipping on."""
    server = await manager.server_add(cmdline=PITR_CMDLINE)

    verdict, line = await shipping_verdict(manager, server.server_id)
    assert verdict == "on", f"a single node refused to ship: {line}"


async def test_node_counts_one_again_after_its_peer_decommissioned(manager: ScyllaClusterManager):
    """
    A node whose only peer has been decommissioned counts as a single node
    again. The count comes from the topology state machine, where a
    decommissioned node moves out of normal_nodes and into left_nodes - which
    count_cluster_nodes() leaves out on purpose. Were the nodes the cluster no
    longer has still counted, a node that once had a peer would refuse to ship
    for good. Issue #7.
    """
    # Joining the peer breaks shipping on the first node, as it should.
    ignore_shipping_off(manager)

    server_a = await manager.server_add(cmdline=PITR_CMDLINE)
    verdict, line = await shipping_verdict(manager, server_a.server_id)
    assert verdict == "on", f"a single node refused to ship before the peer joined: {line}"

    server_b = await manager.server_add()
    await manager.decommission_node(server_b.server_id)

    topology = await manager.get_cql().run_async("SELECT host_id, node_state FROM system.topology")
    logger.info("topology after decommission: %s", [(str(row.host_id), row.node_state) for row in topology])

    # Shipping never turns itself back on within a boot, so the count that
    # matters is the one the restarted node makes. The mark keeps the verdict
    # from the first boot out of the way.
    log = await manager.server_open_log(server_a.server_id)
    mark = await log.mark()
    await manager.server_restart(server_a.server_id)
    await manager.driver_connect(server=server_a)

    verdict, line = await shipping_verdict(manager, server_a.server_id, from_mark=mark)
    assert verdict == "on", f"the decommissioned peer was still counted: {line}"


async def test_second_node_turns_shipping_off(manager: ScyllaClusterManager):
    """
    A node that boots into a cluster of two refuses to ship (off_multi_node),
    so the check above isn't passing for want of a mechanism.
    """
    ignore_shipping_off(manager)

    await manager.server_add(cmdline=PITR_CMDLINE)
    server_b = await manager.server_add(cmdline=PITR_CMDLINE)

    verdict, line = await shipping_verdict(manager, server_b.server_id)
    assert verdict == "off", f"a node in a two-node cluster shipped: {line}"
    assert "counted 2 nodes" in line, f"unexpected reason for refusing to ship: {line}"
