// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type Stores []*core.StoreInfo

func (a Stores) Len() int {
	return len(a)
}

func (a Stores) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a Stores) Less(i, j int) bool {
	return a[i].GetRegionSize() < a[j].GetRegionSize()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	//In short, a suitable store should be up and the down time cannot be
	//longer than MaxStoreDownTime of the cluster, which you can get through cluster.GetMaxStoreDownTime().
	stores := make(Stores, 0)
	cluStores := cluster.GetStores()
	for _, store := range cluStores {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	l := len(stores)
	if l <= 1 {
		return nil
	}
	sort.Sort(stores)
	var region *core.RegionInfo
	var regionCon core.RegionsContainer
	//The scheduler will try to find the region most suitable for moving in the store.
	//First, it will try to select a pending region because pending may mean the disk is overloaded.
	//If there isn’t a pending region, it will try to find a follower region.
	//If it still cannot pick out one region, it will try to pick leader regions.
	//Finally, it will select out the region to move,
	//or the Scheduler will try the next store which has a smaller region size
	//until all stores will have been tried.
	i := l - 1
	for ; i > 0; i-- {

		cluster.GetPendingRegionsWithLock(stores[i].GetID(),
			func(rc core.RegionsContainer) { regionCon = rc })
		region = regionCon.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(),
			func(rc core.RegionsContainer) { regionCon = rc })
		region = regionCon.RandomRegion(nil, nil)
		if region != nil {
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(),
			func(rc core.RegionsContainer) { regionCon = rc })
		region = regionCon.RandomRegion(nil, nil)
		if region != nil {
			break
		}
	}
	if region == nil {
		//all stores will have been tried but there is no suitable store and region
		return nil
	}
	var orgStore, tgtStore *core.StoreInfo
	orgStore = stores[i]
	//GetStoreIds returns a map indicate the region distributed.
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		//?????
		return nil
	}
	for j := 0; j < i; j++ {
		// choose the target store
		//select the store with the smallest region size and.

		if _, ok := storeIds[stores[j].GetID()]; !ok {
			// there's not any part of this region in this store
			tgtStore = stores[j]
			break
		}
	}
	if tgtStore == nil {
		return nil
	}
	if orgStore.GetRegionSize()-tgtStore.GetRegionSize() < 2*region.GetApproximateSize() {
		//we have to make sure that the difference has to be bigger than
		//two times the approximate size of the region
		return nil
	}
	//If the difference is big enough,
	//the Scheduler should allocate a new peer on the target store
	//and create a move peer operator.
	newPeer, err := cluster.AllocPeer(tgtStore.GetID())
	if err != nil {
		return nil
	}
	//!!!
	desc := fmt.Sprintf("move from store %d to store %d", orgStore.GetID(), tgtStore.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, orgStore.GetID(), tgtStore.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}
	return op
}
