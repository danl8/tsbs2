package iot2

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"reflect"
)

// AllInOrder contains info for filling query for all iot2 scenarios in order
type AllInOrder struct {
	core utils.QueryGenerator
}

func NewAllInOrder(core utils.QueryGenerator) utils.QueryFiller {
	return &AllInOrder{
		core: core,
	}
}

var order = 0

// Fill fills in the query.Query with query details.
func (i *AllInOrder) Fill(q query.Query) query.Query {
	defer func() {
		order++
	}()

	switch order % 3 {
	case 0:
		fc, ok := i.core.(DailyAverageLoadFiller)
		checkUnimplementedQuery(ok, i.core, "DailyAverageLoadFiller")
		fc.DailyAverageLoad(q)
		return q
	case 1:
		fc, ok := i.core.(DailyFuelConsumptionRowFiller)
		checkUnimplementedQuery(ok, i.core, "DailyAverageLoadFiller")
		fc.DailyFuelConsumptionRow(q)
		return q
	case 2:
		fc, ok := i.core.(DailyLowFuelCountFiller)
		checkUnimplementedQuery(ok, i.core, "DailyAverageLoadFiller")
		fc.DailyLowFuelCount(q)
		return q
	}

	panic("Code error. Some switch value not implemented")
}

func checkUnimplementedQuery(ok bool, dg utils.QueryGenerator, t string) {
	if !ok {
		panic(fmt.Sprintf("database (%v) does not implement query (%v)", reflect.TypeOf(dg), t))
	}
}
