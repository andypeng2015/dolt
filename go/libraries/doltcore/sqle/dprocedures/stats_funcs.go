// Copyright 2024 Dolthub, Inc.
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

package dprocedures

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

var statsFuncSchema = []*sql.Column{
	{
		Name:     "message",
		Type:     gmstypes.LongText,
		Nullable: true,
	},
}

const OkResult = "Ok"

func statsFunc(fn func(ctx *sql.Context, args ...string) (interface{}, error)) func(ctx *sql.Context, args ...string) (sql.RowIter, error) {
	return func(ctx *sql.Context, args ...string) (iter sql.RowIter, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("stats function unexpectedly panicked: %s", r)
			}
		}()
		res, err := fn(ctx, args...)
		if err != nil {
			return nil, err
		}
		return rowToIter(res), nil
	}
}

type StatsInfo struct {
	DbCnt             int  `json:"dbCnt"`
	ReadCnt           int  `json:"readCnt"`
	Active            bool `json:"active"`
	DbSeedCnt         int  `json:"dbSeedCnt"`
	StorageBucketCnt  int  `json:"storageBucketCnt"`
	CachedBucketCnt   int  `json:"cachedBucketCnt"`
	CachedBoundCnt    int  `json:"cachedBoundCnt"`
	CachedTemplateCnt int  `json:"cachedTemplateCnt"`
	StatCnt           int  `json:"statCnt"`
	GcCounter         int  `json:"gcCounter"`
	SyncCounter       int  `json:"syncCounter"`
}

func (si StatsInfo) ToJson() string {
	jsonData, err := json.Marshal(si)
	if err != nil {
		return ""
	}
	return string(jsonData)
}

// ToggableStats is a sql.StatsProvider that exposes hooks for
// observing and manipulating background database auto refresh threads.
type ToggableStats interface {
	sql.StatsProvider
	FlushQueue(ctx context.Context) error
	Restart(context.Context) error
	Info(ctx context.Context) (StatsInfo, error)
	Purge(ctx *sql.Context) error
	WaitForDbSync(ctx *sql.Context) error
	Gc(ctx *sql.Context) error
	BranchSync(ctx *sql.Context) error
	ValidateState(ctx context.Context) error
	Init(context.Context, []dsess.SqlDatabase, bool) error
	SetTimers(int64, int64, int64)
}

type BranchStatsProvider interface {
	DropBranchDbStats(ctx *sql.Context, branch, db string, flush bool) error
}

// statsRestart flushes the current job queue and re-inits all
// statistic databases.
func statsRestart(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ToggableStats); ok {
		err := afp.FlushQueue(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to restart collection: %w", err)
		}

		dbs := dSess.Provider().AllDatabases(ctx)
		var sqlDbs []dsess.SqlDatabase
		for _, db := range dbs {
			sqlDb, ok := db.(dsess.SqlDatabase)
			if ok {
				sqlDbs = append(sqlDbs, sqlDb)
			}
		}
		if err := afp.Init(ctx, sqlDbs, true); err != nil {
			return nil, err
		}
		if err := afp.Restart(ctx); err != nil {
			return nil, err
		}

		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsInfo returns the last update for a stats thread
func statsInfo(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		info, err := afp.Info(ctx)
		if err != nil {
			return nil, err
		}
		return info.ToJson(), nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsWait blocks until the job queue executes two full loops
// of instructions, which will (1) pick up and (2) commit new
// sets of index-bucket dependencies.
func statsWait(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.WaitForDbSync(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsGc rewrites the cache to only include objects reachable
// by the current root value.
func statsGc(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.Gc(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsBranchSync update database branch tracking based on the
// most recent session.
func statsBranchSync(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.BranchSync(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsValidate returns inconsistencies if the kv cache is out of date
func statsValidate(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro := dSess.StatsProvider()
	if afp, ok := pro.(ToggableStats); ok {
		if err := afp.ValidateState(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsStop flushes the job queue and leaves the stats provider
// in a paused state.
func statsStop(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if afp, ok := statsPro.(ToggableStats); ok {
		if err := afp.FlushQueue(ctx); err != nil {
			return nil, err
		}
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}

// statsPurge flushes the job queue, deletes the current caches
// and storage targets, re-initializes the tracked database
// states, and returns with stats collection paused.
func statsPurge(ctx *sql.Context, _ ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	pro, ok := dSess.StatsProvider().(ToggableStats)
	if !ok {
		return nil, fmt.Errorf("stats not persisted, cannot purge")
	}

	err := pro.FlushQueue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to flush queue: %w", err)
	}

	dbs := dSess.Provider().AllDatabases(ctx)
	var sqlDbs []dsess.SqlDatabase
	for _, db := range dbs {
		sqlDb, ok := db.(dsess.SqlDatabase)
		if ok {
			sqlDbs = append(sqlDbs, sqlDb)
		}
	}

	// reset state
	if err := pro.Init(ctx, sqlDbs, true); err != nil {
		return "failed to purge stats", err
	}

	//
	if err := pro.Purge(ctx); err != nil {
		return "failed to purge stats", err
	}

	return OkResult, nil
}

// statsTimers updates the stats timers, which go into effect after the next restart.
func statsTimers(ctx *sql.Context, args ...string) (interface{}, error) {
	dSess := dsess.DSessFromSess(ctx.Session)
	statsPro := dSess.StatsProvider()

	if len(args) != 3 {
		return nil, fmt.Errorf("expected timer arguments (ns): (job, gc, sync)")
	}
	job, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("interval timer must be positive intergers")
	}
	gc, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("interval timer must be positive intergers")
	}
	sync, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("interval arguments must be positive intergers")
	}

	if afp, ok := statsPro.(ToggableStats); ok {
		afp.SetTimers(job, gc, sync)
		return OkResult, nil
	}
	return nil, fmt.Errorf("provider does not implement ToggableStats")
}
