// Copyright 2025 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statspro

// Package statspro provides an event loop that manages table statistics
// management and access.
//
// At any given time there is one thread responsible for pulling work
// from the job queue to execute. The thread has exclusive ownership
// over the job channel.
//
// All stats are persisted within a single database. If there are multiple
// databases, one is selected by random as the storage target. If during
// initialization multiple databases have stats, one will be chosen by
// random as the target. If a database changes between server restarts,
// the storage stats will be useless but not impair regular operations because
// storage is only ever a best-effort content-addressed persistence layer;
// buckets will be regenerated if they are missing. If the database acting
// as a storage target is deleted, we swap the cache to write to a new storage
// target that still exists.
//
// The main data structures:
//  - Table statistics map, that returns a list of table index statistics
//    for a specific branch, database, and table name.
//  - Object caches:
//    - Bucket cache: Chunk addressed hash map. All provider histogram
//      references point to objects in the bucket cache. Backed by a
//      best-effort on-disk prolly.Map to make restarts faster.
//    - Template cache: Table-schema/index addressed stats.Statistics object
//      for a specific index.
//    - Bound cache: Chunk addressed first row for an index histogram.
//
// Work is broken down into:
//  - A basic update cycle of (1) seed database tables, (2) create or pull
//    buckets from disk, (3) commit statistics accessed by the provider.
//  - GC cycle: Mark and sweep the most recent context's active set into
//    new cache/prolly.Map objects.
//  - Branch sync: Update the tracked set of branch-qualified databases.
//
// Regular jobs, GC, and branch-sync are all controlled by tickers at the
// top level that controls that maximum rate of calling each. GC and
// branch-sync are prioritized before jobs, and therefore rate-limited to
// allow the job queue to flush in-between calls.
//
// DDL operations and branch create/delete are concurrent to the event
// loop. We require an extra fixed-sized queue as an intermediary to the
// job queue to protect the main thread's ownership. DDL acquiring the
// provider lock is a deadlock risk -- we cannot do any provider checks
// while holding the db lock. And lastly, the way update jobs are split
// up over time means we need to do special checks when finalizing a set
// of database stats. A race between deleting a database and finalizing
// statistics needs to end with no statistics, which requires a delete check
// for when finalize wins a race.
//
// The stats lifecycle can be controlled with:
//  - dolt_stats_stop: clear queue and disable thread
//  - dolt_stats_restart: clear queue, refresh queue, start thread
//  - dolt_stats_purge: clear queue, refresh queue, clear cache,
//    disable thread
//  - dolt_stats_validate: return report of cache misses for current
//    root value.
//
// `dolt_stats_wait` is additionally useful for blocking on a full
// queue cycle and then validating whether the session head is caught up.
//
// `dolt_stats_sync` can be used to grab the most up-to-date branch set
// for each database. This races with branch ticker and concurrent
// database/branch adds.
//
