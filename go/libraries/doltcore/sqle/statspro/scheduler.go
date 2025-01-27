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

import (
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type StatsDbController struct {
	ch       chan StatsJob
	destDb   dsess.SqlDatabase
	sourceDb dsess.SqlDatabase
	// qualified db ->
	branches map[string]BranchDb
	dirty    sql.FastIntSet
}

type BranchDb struct {
	db           string
	branch       string
	tableHashes  map[string]hash.Hash
	schemaHashes map[string]hash.Hash
}

type StatsJob interface {
	Finish()
	String() string
}

var _ StatsJob = (*ReadJob)(nil)
var _ StatsJob = (*SeedDbTablesJob)(nil)
var _ StatsJob = (*ControlJob)(nil)
var _ StatsJob = (*FinalizeJob)(nil)

func NewSeedJob(ctx *sql.Context, sqlDb dsess.SqlDatabase) SeedDbTablesJob {
	return SeedDbTablesJob{
		ctx:    ctx,
		sqlDb:  sqlDb,
		tables: nil,
		done:   make(chan struct{}),
	}
}

type tableStatsInfo struct {
	name        string
	schHash     hash.Hash
	idxRoots    []hash.Hash
	bucketCount int
}

type SeedDbTablesJob struct {
	ctx    *sql.Context
	sqlDb  dsess.SqlDatabase
	tables []tableStatsInfo
	done   chan struct{}
}

func (j SeedDbTablesJob) Finish() {
	close(j.done)
}

func (j SeedDbTablesJob) String() string {
	b := strings.Builder{}
	b.WriteString("seed db: ")
	b.WriteString(j.sqlDb.RevisionQualifiedName())
	b.WriteString("[")

	var sep = ""
	for _, ti := range j.tables {
		b.WriteString(sep)
		b.WriteString("(" + ti.name + ": " + ti.schHash.String()[:5] + ")")
	}
	b.WriteString("]")

	return b.String()
}

func NewAnalyzeJob(ctx *sql.Context, sqlDb dsess.SqlDatabase, tables []string, after ControlJob) AnalyzeJob {
	return AnalyzeJob{ctx: ctx, sqlDb: sqlDb, tables: tables, after: after, done: make(chan struct{})}
}

type AnalyzeJob struct {
	ctx    *sql.Context
	sqlDb  dsess.SqlDatabase
	tables []string
	after  ControlJob
	done   chan struct{}
}

func (j AnalyzeJob) String() string {
	return "analyze: [" + strings.Join(j.tables, ", ") + "]"
}

func (j AnalyzeJob) Finish() {
	close(j.done)
	return
}

type ReadJob struct {
	ctx      *sql.Context
	db       dsess.SqlDatabase
	table    string
	m        prolly.Map
	nodes    []tree.Node
	ordinals []updateOrdinal
	colCnt   int
	done     chan struct{}
}

func (j ReadJob) Finish() {
	close(j.done)
}

func (j ReadJob) String() string {
	b := strings.Builder{}
	b.WriteString("read: " + j.db.RevisionQualifiedName() + "/" + j.table + ": ")
	sep := ""
	for _, o := range j.ordinals {
		b.WriteString(fmt.Sprintf("%s[%d-%d]", sep, o.start, o.stop))
		sep = ", "
	}
	return b.String()
}

type finalizeStruct struct {
	buckets []hash.Hash
	tupB    *val.TupleBuilder
}

type FinalizeJob struct {
	tableKey    tableIndexesKey
	keepIndexes map[sql.StatQualifier]bool
	editIndexes map[templateCacheKey]finalizeStruct
	done        chan struct{}
}

func (j FinalizeJob) Finish() {
	close(j.done)
}

func (j FinalizeJob) String() string {
	b := strings.Builder{}
	b.WriteString("finalize " + j.tableKey.String())
	b.WriteString(": ")
	sep := ""
	for idx, fs := range j.editIndexes {
		b.WriteString(fmt.Sprintf("%s(%s: ", sep, idx.idxName))
		sep = ""
		for _, h := range fs.buckets {
			b.WriteString(fmt.Sprintf("%s%s", sep, h.String()[:5]))
			sep = ", "
		}
		b.WriteString(")")
		sep = ", "
	}
	return b.String()
}

func NewControl(desc string, cb func(sc *StatsCoord) error) ControlJob {
	return ControlJob{cb: cb, desc: desc, done: make(chan struct{})}
}

type ControlJob struct {
	cb   func(sc *StatsCoord) error
	desc string
	done chan struct{}
}

func (j ControlJob) Finish() {
	close(j.done)
}

func (j ControlJob) String() string {
	return "ControlJob: " + j.desc
}

func NewStatsCoord(pro *sqle.DoltDatabaseProvider, logger *logrus.Logger, threads *sql.BackgroundThreads, dEnv *env.DoltEnv) *StatsCoord {
	done := make(chan struct{})
	close(done)
	kv := NewMemStats()
	return &StatsCoord{
		dbMu:           &sync.Mutex{},
		statsMu:        &sync.Mutex{},
		logger:         logger,
		Jobs:           make(chan StatsJob, 1024),
		Done:           done,
		Interrupts:     make(chan ControlJob, 1),
		JobInterval:    50 * time.Millisecond,
		gcInterval:     24 * time.Hour,
		branchInterval: 24 * time.Hour,
		bucketCap:      kv.Cap(),
		Stats:          make(map[tableIndexesKey][]*stats.Statistic),
		Branches:       make(map[string][]ref.DoltRef),
		threads:        threads,
		kv:             kv,
		pro:            pro,
		hdp:            dEnv.GetUserHomeDir,
		dialPro:        env.NewGRPCDialProviderFromDoltEnv(dEnv),
	}
}

func (sc *StatsCoord) SetMemOnly(v bool) {
	sc.memOnly = v
}

func (sc *StatsCoord) SetTimers(job, gc, branch int64) {
	sc.JobInterval = time.Duration(job)
	sc.gcInterval = time.Duration(gc)
	sc.branchInterval = time.Duration(branch)
}

type tableIndexesKey struct {
	db     string
	branch string
	table  string
	schema string
}

func (k tableIndexesKey) String() string {
	return k.db + "/" + k.branch + "/" + k.table
}

type StatsCoord struct {
	logger      *logrus.Logger
	JobInterval time.Duration
	threads     *sql.BackgroundThreads
	pro         *sqle.DoltDatabaseProvider
	memOnly     bool

	dbMu           *sync.Mutex
	dbs            []dsess.SqlDatabase
	branchInterval time.Duration

	kv StatsKv

	statsBackingDb string
	cancelSwitch   context.CancelFunc
	dialPro        dbfactory.GRPCDialProvider
	hdp            env.HomeDirProvider

	readCounter atomic.Int32

	activeGc   atomic.Bool
	doGc       atomic.Bool
	disableGc  atomic.Bool
	gcInterval time.Duration
	gcDone     chan struct{}
	gcMu       sync.Mutex
	gcCancel   context.CancelFunc

	doBranchCheck atomic.Bool
	doCapCheck    atomic.Bool
	bucketCnt     atomic.Int64
	bucketCap     int64

	Jobs       chan StatsJob
	Interrupts chan ControlJob
	Done       chan struct{}

	Branches map[string][]ref.DoltRef

	statsMu *sync.Mutex
	// Stats tracks table statistics accessible to sessions.
	Stats map[tableIndexesKey][]*stats.Statistic
}

func (sc *StatsCoord) Stop() {
	select {
	case <-sc.Done:
	default:
		close(sc.Done)
	}
}

func (sc *StatsCoord) Restart(ctx *sql.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sc.Done:
	default:
		sc.Stop()
	}

	sc.Done = make(chan struct{})
	return sc.threads.Add("stats", func(subCtx context.Context) {
		sc.run(ctx.WithContext(subCtx))
	})
}

func (sc *StatsCoord) Close() {
	sc.Stop()
	return
}

func (sc *StatsCoord) Add(ctx *sql.Context, db dsess.SqlDatabase) chan struct{} {
	dSess := dsess.DSessFromSess(ctx.Session)
	dbd, ok := dSess.GetDbData(ctx, db.AliasedName())
	if !ok {
		sc.error(ControlJob{desc: "add db"}, fmt.Errorf("database in branches list does not exist: %s", db.AliasedName()))
		ret := make(chan struct{})
		close(ret)
		return ret
	}
	curBranches, err := dbd.Ddb.GetBranches(ctx)
	if err != nil {
		sc.error(ControlJob{desc: "add db"}, err)
		ret := make(chan struct{})
		close(ret)
		return ret
	}

	ret := sc.Seed(ctx, db)

	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	sc.dbs = append(sc.dbs, db)
	sc.Branches[db.AliasedName()] = curBranches
	if len(sc.dbs) == 1 {
		sc.statsBackingDb = db.AliasedName()
		var mem *memStats
		switch kv := sc.kv.(type) {
		case *memStats:
			mem = kv
		case *prollyStats:
			mem = kv.mem
		default:
			mem = NewMemStats()
			close(ret)
			return ret
		}
		newKv, err := sc.initStorage(ctx, db)
		if err != nil {
			sc.error(ControlJob{desc: "add db"}, err)
			close(ret)
			return ret
		}
		newKv.mem = mem
		sc.kv = newKv
	}
	return ret
}

func (sc *StatsCoord) Drop(dbName string) {
	// deprecated
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	for i, db := range sc.dbs {
		if strings.EqualFold(db.Name(), dbName) {
			sc.dbs = append(sc.dbs[:i], sc.dbs[i+1:]...)
			return
		}
	}
}

type StatsInfo struct {
	DbCnt   int
	ReadCnt int
	Active  bool
	JobCnt  int
}

func (sc *StatsCoord) Info() StatsInfo {
	sc.dbMu.Lock()
	dbCnt := len(sc.dbs)
	defer sc.dbMu.Unlock()

	var active bool
	select {
	case _, ok := <-sc.Interrupts:
		active = ok
	default:
		active = true
	}
	return StatsInfo{
		DbCnt:   dbCnt,
		ReadCnt: int(sc.readCounter.Load()),
		Active:  active,
		JobCnt:  len(sc.Jobs),
	}
}

// event loop must be stopped
func (sc *StatsCoord) flushQueue(ctx context.Context) ([]StatsJob, error) {
	select {
	case <-sc.Done:
	default:
		return nil, fmt.Errorf("cannot read queue while event loop is active")
		// inactive event loop cannot be interrupted, discard
	}
	var ret []StatsJob
	for _ = range len(sc.Jobs) {
		select {
		case <-ctx.Done():
			return nil, nil
		case j, ok := <-sc.Jobs:
			if !ok {
				return nil, nil
			}
			ret = append(ret, j)
		}
	}
	return ret, nil
}

// TODO sendJobs
func (sc *StatsCoord) Seed(ctx *sql.Context, sqlDb dsess.SqlDatabase) chan struct{} {
	j := NewSeedJob(ctx, sqlDb)
	sc.Jobs <- j
	return j.done
}

func (sc *StatsCoord) Control(desc string, cb func(sc *StatsCoord) error) chan struct{} {
	j := NewControl(desc, cb)
	sc.Jobs <- j
	return j.done
}

func (sc *StatsCoord) Interrupt(desc string, cb func(sc *StatsCoord) error) chan struct{} {
	j := NewControl(desc, cb)
	sc.Interrupts <- j
	return j.done
}

func GcSweep(ctx *sql.Context) ControlJob {
	return NewControl("finish GC", func(sc *StatsCoord) error {
		sc.gcMu.Lock()
		defer sc.gcMu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
			sc.bucketCnt.Store(int64(sc.kv.Len()))
			sc.bucketCap = sc.kv.Cap()
			sc.kv.FinishGc()
			sc.activeGc.Store(false)
			close(sc.gcDone)
			sc.gcCancel = nil
			return nil
		}
	})
}

func (sc *StatsCoord) error(j StatsJob, err error) {
	fmt.Println(err.Error())
	sc.logger.Errorf("stats error; job detail: %s; verbose: %s", j.String(), err)
}

// statsRunner operates on stats jobs
func (sc *StatsCoord) run(ctx *sql.Context) error {
	jobTimer := time.NewTimer(0)
	gcTicker := time.NewTicker(sc.gcInterval)
	branchTicker := time.NewTicker(sc.branchInterval)

	for {
		// sequentially test:
		// (1) ctx done/thread canceled
		// (2) GC check
		// (3) branch check
		// (4) cap check
		// (4) job and other tickers
		select {
		case <-sc.Done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if sc.doGc.Swap(false) {
			j := sc.startGcMark(ctx, make(chan struct{}))
			err := sc.sendJobs(ctx, j)
			if err != nil {
				sc.error(j, err)
			}
		}

		if sc.doBranchCheck.Swap(false) {
			j := ControlJob{desc: "branch update"}
			newJobs, err := sc.updateBranches(ctx, j)
			if err != nil {
				sc.error(ControlJob{desc: "branches update"}, err)
			}
			err = sc.sendJobs(ctx, newJobs...)
			if err != nil {
				sc.error(j, err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case j, ok := <-sc.Interrupts:
			if !ok {
				return nil
			}
			if err := j.cb(sc); err != nil {
				sc.error(j, err)
				continue
			}
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-jobTimer.C:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case j, ok := <-sc.Jobs:
				if !ok {
					return nil
				}
				fmt.Println("execute: ", j.String())
				newJobs, err := sc.executeJob(ctx, j)
				if err != nil {
					sc.error(j, err)
				}
				err = sc.sendJobs(ctx, newJobs...)
				if err != nil {
					sc.error(j, err)
				}
				j.Finish()
			default:
			}
		case <-gcTicker.C:
			sc.setGc()
		case <-branchTicker.C:
			sc.doBranchCheck.Store(true)
		}
		jobTimer.Reset(sc.JobInterval)
	}
}

func (sc *StatsCoord) sendJobs(ctx *sql.Context, jobs ...StatsJob) error {
	for i := 0; i < len(jobs); i++ {
		j := jobs[i]
		if j == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sc.Jobs <- j:
			if _, ok := j.(ReadJob); ok {
				sc.readCounter.Add(1)
			}
		default:
			sc.doubleChannelSize(ctx)
			i--
		}
	}
	return nil
}

func (sc *StatsCoord) executeJob(ctx *sql.Context, j StatsJob) ([]StatsJob, error) {
	switch j := j.(type) {
	case SeedDbTablesJob:
		return sc.seedDbTables(ctx, j)
	case ReadJob:
		sc.readCounter.Add(-1)
		return sc.readChunks(ctx, j)
	case FinalizeJob:
		return sc.finalizeUpdate(ctx, j)
	case ControlJob:
		if err := j.cb(sc); err != nil {
			sc.error(j, err)
		}
	case AnalyzeJob:
		return sc.runAnalyze(ctx, j)
	default:
	}
	return nil, nil
}

func (sc *StatsCoord) doubleChannelSize(ctx *sql.Context) {
	var restart bool
	select {
	case <-sc.Done:
	default:
		sc.Stop()
		restart = true
	}
	close(sc.Jobs)
	ch := make(chan StatsJob, cap(sc.Jobs)*2)
	for j := range sc.Jobs {
		ch <- j
	}
	sc.Jobs = ch
	if restart {
		sc.Restart(ctx)
	}
}

func (sc *StatsCoord) runOneInterrupt(ctx *sql.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case j, ok := <-sc.Interrupts:
		if !ok {
			return nil
		}
		if err := j.cb(sc); err != nil {
			return err
		}
	default:
	}
	return nil
}

func (sc *StatsCoord) dropTableJob(sqlDb dsess.SqlDatabase, tableName string) StatsJob {
	return FinalizeJob{
		tableKey: tableIndexesKey{
			db:     sqlDb.AliasedName(),
			branch: sqlDb.Revision(),
			table:  tableName,
		},
		editIndexes: nil,
		done:        make(chan struct{}),
	}
}

func (sc *StatsCoord) dropBranchJob(dbName string, branch string) ControlJob {
	return ControlJob{
		desc: "drop branch",
		cb: func(sc *StatsCoord) error {
			sc.dbMu.Lock()
			defer sc.dbMu.Unlock()
			curRefs := sc.Branches[branch]
			for i, ref := range curRefs {
				if strings.EqualFold(ref.GetPath(), branch) {
					sc.Branches[branch] = append(curRefs[:i], curRefs[:i+1]...)
					break
				}
			}
			for i, db := range sc.dbs {
				if strings.EqualFold(db.Revision(), branch) && strings.EqualFold(db.AliasedName(), dbName) {
					sc.dbs = append(sc.dbs[:i], sc.dbs[1+1:]...)
					break
				}
			}

			// stats lock is more contentious, do last
			sc.statsMu.Lock()
			defer sc.statsMu.Unlock()
			var deleteKeys []tableIndexesKey
			for k, _ := range sc.Stats {
				if strings.EqualFold(dbName, k.db) && strings.EqualFold(branch, k.branch) {
					deleteKeys = append(deleteKeys, k)
				}
			}
			for _, k := range deleteKeys {
				delete(sc.Stats, k)
			}
			return nil
		},
		done: make(chan struct{}),
	}
}

func (sc *StatsCoord) readChunks(ctx context.Context, j ReadJob) ([]StatsJob, error) {
	// check if chunk already in cache
	// if no, see if on disk and we just need to load
	// otherwise perform read to create the bucket, write to disk, update mem ref
	prollyMap := j.m
	updater := newBucketBuilder(sql.StatQualifier{}, j.colCnt, prollyMap.KeyDesc().PrefixDesc(j.colCnt))
	keyBuilder := val.NewTupleBuilder(prollyMap.KeyDesc())

	for i, n := range j.nodes {
		// each node is a bucket
		updater.newBucket()

		// we read exclusive range [node first key, next node first key)
		start, stop := j.ordinals[i].start, j.ordinals[i].stop
		iter, err := j.m.IterOrdinalRange(ctx, start, stop)
		if err != nil {
			return nil, err
		}
		for {
			// stats key will be a prefix of the index key
			keyBytes, _, err := iter.Next(ctx)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			// build full key
			for i := range keyBuilder.Desc.Types {
				keyBuilder.PutRaw(i, keyBytes.GetField(i))
			}

			updater.add(keyBuilder.BuildPrefixNoRecycle(prollyMap.Pool(), updater.prefixLen))
			keyBuilder.Recycle()
		}

		// finalize the aggregation
		bucket, err := updater.finalize(ctx, prollyMap.NodeStore())
		if err != nil {
			return nil, err
		}
		// TODO check for capacity error during GC
		err = sc.kv.PutBucket(ctx, n.HashOf(), bucket, val.NewTupleBuilder(prollyMap.KeyDesc().PrefixDesc(j.colCnt)))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (sc *StatsCoord) runAnalyze(_ context.Context, j AnalyzeJob) ([]StatsJob, error) {
	var ret []StatsJob
	for _, tableName := range j.tables {
		readJobs, _, err := sc.readJobsForTable(j.ctx, j.sqlDb, tableStatsInfo{name: tableName})
		if err != nil {
			return nil, err
		}
		ret = append(ret, readJobs...)
	}
	if j.after.done != nil {
		ret = append(ret, j.after)
	}
	return ret, nil
}

func (sc *StatsCoord) finalizeUpdate(ctx context.Context, j FinalizeJob) ([]StatsJob, error) {
	if len(j.editIndexes) == 0 {
		// delete table
		sc.statsMu.Lock()
		delete(sc.Stats, j.tableKey)
		sc.statsMu.Unlock()
		return nil, nil
	}

	var newStats []*stats.Statistic
	for _, s := range sc.Stats[j.tableKey] {
		if ok := j.keepIndexes[s.Qual]; ok {
			newStats = append(newStats, s)
		}
	}
	for key, fs := range j.editIndexes {
		log.Println("finalize " + key.String())
		template, ok := sc.kv.GetTemplate(key)
		if !ok {
			return nil, fmt.Errorf(" missing template dependency for table: %s", key)
		}
		template.Qual = sql.NewStatQualifier(j.tableKey.db, "", j.tableKey.table, key.idxName)

		for i, bh := range fs.buckets {
			if i == 0 {
				bnd, ok := sc.kv.GetBound(bh)
				if !ok {
					log.Println("chunks: ", fs.buckets)
					for k, v := range sc.kv.(*prollyStats).mem.bounds {
						log.Println("bound: ", k, v)
					}
					return nil, fmt.Errorf("missing read job bound dependency for chunk %s: %s", key, bh)
				}
				template.LowerBnd = bnd[:fs.tupB.Desc.Count()]
			}
			// accumulate counts
			if b, ok, err := sc.kv.GetBucket(ctx, bh, fs.tupB); err != nil {
				return nil, err
			} else if !ok {
				return nil, fmt.Errorf("missing read job bucket dependency for chunk: %s", bh)
			} else {
				template.RowCnt += b.RowCnt
				template.DistinctCnt += b.DistinctCnt
				template.NullCnt += b.NullCnt
				template.Hist = append(template.Hist, b)
			}
		}
		newStats = append(newStats, &template)
	}

	// protected swap
	sc.statsMu.Lock()
	sc.Stats[j.tableKey] = newStats
	log.Println("stat cnt: ", len(sc.Stats), len(newStats))
	sc.statsMu.Unlock()

	return nil, nil
}

func (sc *StatsCoord) updateBranches(ctx *sql.Context, j ControlJob) ([]StatsJob, error) {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	var ret []StatsJob
	newBranches := make(map[string][]ref.DoltRef)
	var newDbs []dsess.SqlDatabase
	for dbName, branches := range sc.Branches {
		var sqlDb dsess.SqlDatabase
		for _, db := range sc.dbs {
			if strings.EqualFold(db.AliasedName(), dbName) {
				sqlDb = db
				break
			}
		}

		if sqlDb.Name() == "" {
			sc.error(j, fmt.Errorf("database in branches list is not tracked: %s", dbName))
			continue
		}

		dSess := dsess.DSessFromSess(ctx.Session)
		dbd, ok := dSess.GetDbData(ctx, dbName)
		if !ok {
			sc.error(j, fmt.Errorf("database in branches list does not exist: %s", dbName))
		}
		curBranches, err := dbd.Ddb.GetBranches(ctx)
		if err != nil {
			sc.error(j, err)
			continue
		}

		newBranches[sqlDb.AliasedName()] = curBranches

		i := 0
		k := 0
		for i < len(branches) && k < len(curBranches) {
			br := curBranches[k]
			switch strings.Compare(branches[i].GetPath(), curBranches[k].GetPath()) {
			case 0:
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}
				newDbs = append(newDbs, sqlDb.(sqle.Database))
				i++
				k++
			case -1:
				//ret = append(ret, sc.dropBranchJob(ctx, dbName, branches[i]))
				i++
			case +1:
				// add
				sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
				if err != nil {
					sc.error(j, err)
					continue
				}

				newDbs = append(newDbs, sqlDb.(sqle.Database))
				ret = append(ret, NewSeedJob(ctx, sqlDb.(sqle.Database)))
				k++
			}
		}
		if k < len(curBranches) {
			br := curBranches[k]
			sqlDb, err := sqle.RevisionDbForBranch(ctx, sqlDb, br.GetPath(), br.GetPath()+"/"+dbName)
			if err != nil {
				sc.error(j, err)
				continue
			}

			newDbs = append(newDbs, sqlDb)
			ret = append(ret, NewSeedJob(ctx, sqlDb))
			k++
		}
	}
	sc.Branches = newBranches
	sc.dbs = newDbs
	return ret, nil
}

func (sc *StatsCoord) countBuckets() int {
	sc.dbMu.Lock()
	defer sc.dbMu.Unlock()
	var cnt int
	for _, ss := range sc.Stats {
		cnt += len(ss)
	}
	return cnt
}

func (sc *StatsCoord) setGc() {
	if !sc.disableGc.Load() {
		sc.doGc.Store(true)
	}
}

func (sc *StatsCoord) startGcMark(ctx *sql.Context, done chan struct{}) StatsJob {
	sc.doGc.Store(false)
	if sc.disableGc.Load() {
		close(done)
		return nil
	}
	sc.gcMu.Lock()
	defer sc.gcMu.Unlock()
	if sc.activeGc.Swap(true) {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-sc.gcDone:
				close(done)
			}
		}()
		return nil
	}

	subCtx, cancel := context.WithCancel(ctx)
	sc.gcCancel = cancel

	sc.kv.StartGc(ctx, int(sc.bucketCap))

	sc.gcDone = make(chan struct{})
	go func(ctx context.Context) {
		defer close(done)
		select {
		case <-ctx.Done():
			close(sc.gcDone)
			return
		case <-sc.gcDone:
		}
	}(subCtx)
	return GcSweep(ctx)
}
