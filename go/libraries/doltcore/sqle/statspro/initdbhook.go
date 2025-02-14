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

package statspro

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
)

func NewInitDatabaseHook(sc *StatsCoord) sqle.InitDatabaseHook {
	return func(
		ctx *sql.Context,
		_ *sqle.DoltDatabaseProvider,
		name string,
		denv *env.DoltEnv,
		db dsess.SqlDatabase,
	) error {
		head := denv.RepoState.Head

		sqlDb, ok := db.(sqle.Database)
		if !ok {
			sc.logger.Debugf("stats initialize db failed, expected *sqle.Database, found %T", db)
			return nil
		}

		// call should only fail if backpressure in secondary queue
		sc.AddFs(sqlDb, denv.FS)
		if err != nil {
			sc.logger.Debugf("cannot initialize db stats for %s; queue is closed", sqlDb.AliasedName())
		}
		return nil
	}
}

func NewDropDatabaseHook(sc *StatsCoord) sqle.DropDatabaseHook {
	return func(ctx *sql.Context, name string) {
		if err := sc.DropDbStats(ctx, name, false); err != nil {
			ctx.GetLogger().Debugf("failed to close stats database: %s", err)
		}
	}
}
