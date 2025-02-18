package statspro

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"io"
	"log"
	"strings"
)

// thread that does a full root walk, gets databases/branches/tables

// control work throughput on sender or receiver side?

//

func (sc *StatsCoord) newCycle(ctx context.Context) context.Context {
	sc.cycleMu.Lock()
	defer sc.cycleMu.Unlock()
	if sc.cycleCancel != nil {
		sc.cycleCancel()
	}
	sc.cycleCtx, sc.cycleCancel = context.WithCancel(ctx)
	return sc.cycleCtx
}

func (sc *StatsCoord) cancelSender() {
	sc.cycleMu.Lock()
	defer sc.cycleMu.Unlock()
	if sc.cycleCancel != nil {
		sc.cycleCancel()
		sc.cycleCancel = nil
	}
}

func (sc *StatsCoord) getCycleWaiter() <-chan struct{} {
	sc.cycleMu.Lock()
	defer sc.cycleMu.Unlock()
	return sc.cycleCtx.Done()
}

func (sc *StatsCoord) runIssuer(ctx context.Context) (err error) {
	sc.issuerDone = make(chan struct{})
	defer func() {
		close(sc.issuerDone)
	}()
	var gcKv *memStats
	for {
		gcStart := sc.gcCnt.Load()

		gcKv = nil
		if sc.doGc.Swap(false) {
			gcKv = NewMemStats()
			gcKv.gcGen = gcStart
		}

		cycleCtx := sc.newCycle(ctx)

		sqlCtx, err := sc.ctxGen(cycleCtx)
		if err != nil {
			return err
		}

		newStats, err := sc.newStatsForRoot(sqlCtx, gcKv)
		if err != nil {
			sc.descError("", err)
		}

		if gcKv.isPoisoned() {
			sc.descError(fmt.Sprintf("gc %d was interrupted", gcKv.GcGen()), nil)
			gcKv = nil
		}

		select {
		case <-cycleCtx.Done():
			return context.Cause(cycleCtx)
		default:
		}

		if ok, err := sc.trySwapStats(ctx, gcStart, newStats, gcKv); err != nil || !ok {
			sc.descError("failed to swap stats", err)
		}
	}
}

func (sc *StatsCoord) trySwapStats(ctx context.Context, gcCnt uint64, newStats *rootStats, gcKv *memStats) (bool, error) {
	sc.statsMu.Lock()
	defer sc.statsMu.Unlock()
	var err error
	if gcKv != nil && sc.gcCnt.CompareAndSwap(gcCnt, gcCnt+1) {
		sc.kv = gcKv
		sc.Stats = newStats
		err = sc.sq.DoAsync(func() {
			if err := sc.rotateStorage(ctx); err != nil {
				sc.descError("rotate storage failure", err)
			}
		})
	} else if sc.gcCnt.Load() == gcCnt {
		sc.Stats = newStats
		err = sc.sq.DoAsync(func() {
			if _, err := sc.kv.Flush(ctx); err != nil {
				sc.descError("flush failure", err)
			}
		})
	} else {
		return false, nil
	}
	return true, err
}

func (sc *StatsCoord) newStatsForRoot(ctx *sql.Context, gcKv *memStats) (*rootStats, error) {
	var err error
	dSess := dsess.DSessFromSess(ctx.Session)
	dbs := dSess.Provider().AllDatabases(ctx)
	newStats := newRootStats()
	for _, db := range dbs {
		sqlDb, ok := db.(sqle.Database)
		if !ok {
			continue
		}

		var branches []ref.DoltRef
		if err := sc.sq.DoSync(ctx, func() {
			ddb, ok := dSess.GetDoltDB(ctx, db.Name())
			if !ok {
				sc.descError("dolt database not found "+db.Name(), nil)
			}
			branches, err = ddb.GetBranches(ctx)
			if err != nil {
				sc.descError("getBranches", err)
			}
		}); err != nil {
			return nil, err
		}

		for _, br := range branches {
			sqlDb, err := sqle.RevisionDbForBranch(ctx, db.(dsess.SqlDatabase), br.GetPath(), br.GetPath()+"/"+sqlDb.AliasedName())
			if err != nil {
				sc.descError("revisionForBranch", err)
				continue
			}

			newStats.dbCnt++

			var tableNames []string
			if err := sc.sq.DoSync(ctx, func() {
				tableNames, err = sqlDb.GetTableNames(ctx)
				if err != nil {
					sc.descError("getTableNames", err)
				}
			}); err != nil {
				return nil, err
			}

			for _, tableName := range tableNames {
				tableKey, newTableStats, err := sc.updateTable(ctx, tableName, sqlDb, gcKv)
				if err != nil {
					return nil, err
				}
				newStats.stats[tableKey] = newTableStats
			}
		}
	}

	return newStats, nil
}

func (sc *StatsCoord) finalizeHistogram(template stats.Statistic, buckets []*stats.Bucket, firstBound sql.Row) *stats.Statistic {
	template.LowerBnd = firstBound
	for _, b := range buckets {
		// accumulate counts
		template.RowCnt += b.RowCnt
		template.DistinctCnt += b.DistinctCnt
		template.NullCnt += b.NullCnt
		template.Hist = append(template.Hist, b)
	}
	return &template
}

func (sc *StatsCoord) collectIndexNodes(ctx *sql.Context, prollyMap prolly.Map, idxLen int, nodes []tree.Node) ([]*stats.Bucket, sql.Row, error) {
	updater := newBucketBuilder(sql.StatQualifier{}, idxLen, prollyMap.KeyDesc())
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen))

	firstNodeHash := nodes[0].HashOf()
	lowerBound, ok := sc.kv.GetBound(firstNodeHash, idxLen)
	if !ok {
		sc.sq.DoSync(ctx, func() {
			var err error
			lowerBound, err = firstRowForIndex(ctx, prollyMap, keyBuilder)
			if err != nil {
				sc.descError("get histogram bucket for node", err)
			}
			if sc.Debug {
				log.Printf("put bound:  %s: %v\n", firstNodeHash.String()[:5], lowerBound)
			}

			sc.kv.PutBound(firstNodeHash, lowerBound, idxLen)
		})
	}

	var offset uint64
	for _, n := range nodes {
		if _, ok, err := sc.kv.GetBucket(ctx, n.HashOf(), keyBuilder); err != nil {
			return nil, nil, err
		} else if ok {
			continue
		}

		treeCnt, err := n.TreeCount()
		if err != nil {
			return nil, nil, err
		}

		err = sc.sq.DoSync(ctx, func() {
			updater.newBucket()

			// we read exclusive range [node first key, next node first key)
			start, stop := offset, offset+uint64(treeCnt)
			iter, err := prollyMap.IterOrdinalRange(ctx, start, stop)
			if err != nil {
				sc.descError("get histogram bucket for node", err)
				return
			}
			for {
				// stats key will be a prefix of the index key
				keyBytes, _, err := iter.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					sc.descError("get histogram bucket for node", err)
					return
				}
				// build full key
				for i := range keyBuilder.Desc.Types {
					keyBuilder.PutRaw(i, keyBytes.GetField(i))
				}

				updater.add(keyBuilder.BuildPrefixNoRecycle(prollyMap.Pool(), updater.prefixLen))
				keyBuilder.Recycle()
			}

			// finalize the aggregation
			newBucket, err := updater.finalize(ctx, prollyMap.NodeStore())
			if err != nil {
				sc.descError("get histogram bucket for node", err)
				return
			}
			err = sc.kv.PutBucket(ctx, n.HashOf(), newBucket, keyBuilder)
			if err != nil {
				sc.descError("get histogram bucket for node", err)
				return
			}
		})
		if err != nil {
			return nil, nil, err
		}
		offset += uint64(treeCnt)
	}

	var buckets []*stats.Bucket
	for _, n := range nodes {
		newBucket, ok, err := sc.kv.GetBucket(ctx, n.HashOf(), keyBuilder)
		if err != nil || !ok {
			sc.descError(fmt.Sprintf("missing histogram bucket for node %s", n.HashOf().String()[:5]), err)
			return nil, nil, err
		}
		buckets = append(buckets, newBucket)
	}

	return buckets, lowerBound, nil
}

func (sc *StatsCoord) updateTable(ctx *sql.Context, tableName string, sqlDb dsess.SqlDatabase, gcKv *memStats) (tableIndexesKey, []*stats.Statistic, error) {
	var err error
	var sqlTable *sqle.DoltTable
	var dTab *doltdb.Table
	if err := sc.sq.DoSync(ctx, func() {
		sqlTable, dTab, err = GetLatestTable(ctx, tableName, sqlDb)
		if err != nil {
			sc.descError("GetLatestTable", err)
		}
	}); err != nil {
		return tableIndexesKey{}, nil, err
	}

	tableKey := tableIndexesKey{
		db:     sqlDb.AliasedName(),
		branch: sqlDb.Revision(),
		table:  tableName,
		schema: "",
	}

	var indexes []sql.Index
	if err := sc.sq.DoSync(ctx, func() {
		indexes, err = sqlTable.GetIndexes(ctx)
		if err != nil {
			sc.descError("", err)
		}
	}); err != nil {
		return tableIndexesKey{}, nil, err
	}

	var newTableStats []*stats.Statistic
	for _, sqlIdx := range indexes {
		var idx durable.Index
		var err error
		if strings.EqualFold(sqlIdx.ID(), "PRIMARY") {
			idx, err = dTab.GetRowData(ctx)
		} else {
			idx, err = dTab.GetIndexRowData(ctx, sqlIdx.ID())
		}
		if err != nil {
			sc.descError("GetRowData", err)
			continue
		}

		var template stats.Statistic
		if err := sc.sq.DoSync(ctx, func() {
			_, template, err = sc.getTemplate(ctx, sqlTable, sqlIdx)
			if err != nil {
				sc.descError("", fmt.Errorf("stats collection failed to generate a statistic template: %s.%s.%s:%T; %s", sqlDb.RevisionQualifiedName(), tableName, sqlIdx, sqlIdx, err))
			}
		}); err != nil {
			return tableIndexesKey{}, nil, err
		} else if template.Fds.Empty() {
			return tableIndexesKey{}, nil, fmt.Errorf("failed to creat template for %s/%s/%s/%s", sqlDb.Revision(), sqlDb.AliasedName(), tableName, sqlIdx.ID())
		}

		template.Qual.Database = sqlDb.AliasedName()

		idxLen := len(sqlIdx.Expressions())

		prollyMap := durable.ProllyMapFromIndex(idx)
		var levelNodes []tree.Node
		if err := sc.sq.DoSync(ctx, func() {
			levelNodes, err = tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
			if err != nil {
				sc.descError("", err)
			}
			return
		}); err != nil {
			return tableIndexesKey{}, nil, err
		}
		var buckets []*stats.Bucket
		var firstBound sql.Row
		if len(levelNodes) > 0 {
			buckets, firstBound, err = sc.collectIndexNodes(ctx, prollyMap, idxLen, levelNodes)
			if err != nil {
				sc.descError("", err)
				continue
			}
		}
		newTableStats = append(newTableStats, sc.finalizeHistogram(template, buckets, firstBound))
		if gcKv != nil {
			keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(idxLen))
			gcKv.GcMark(sc.kv, levelNodes, buckets, idxLen, keyBuilder)
		}
	}
	return tableKey, newTableStats, nil
}
