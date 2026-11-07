/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <functional>
#include <optional>

#include "utils/UUID.hh"

namespace db::pitr {

// A chain is one unbroken run of shipped segments from one shard of one boot.
// A restart or an outage ends a chain and the next one gets a fresh id, so an
// id never spans a break in coverage.
using chain_id = utils::tagged_uuid<struct chain_id_tag>;

// One shard's shipped chain as the restore of a backup needs it: the chain's
// key parts without the prefix, which the restore command supplies, plus the
// floor (decision P62). Everything else about the chain is in the chain's own
// objects, which are checksummed, so nothing else belongs here.
//
// Kept in its own header, away from the format and the ledger, because
// replica/database.hh holds a provider of it.
struct shard_chain {
    // Never null: a null id is what a zeroed file holds.
    utils::UUID host_id;
    int64_t boot_epoch_us = 0;
    int64_t chain_start_us = 0;
    chain_id chain;
    // The D27 threshold: the newest held segment id when start_shipping() took
    // effect. Nothing when no segment existed then, which makes the D27 check
    // vacuous; never 0, which is no segment's id and so never stands in for
    // "none" or "unknown".
    std::optional<uint64_t> floor_segment_id;

    bool operator==(const shard_chain&) const = default;
};

// What a snapshot asks each shard for. Installed from the shipper's chain
// (#8, piece 2b); while none is installed a shard reports no chain. It is
// called on the shard it belongs to and must not block.
using chain_provider = std::function<std::optional<shard_chain>()>;

} // namespace db::pitr
