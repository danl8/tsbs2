package iot2

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// DailyLowFuelCount contains info for filling query for
// selecting daily truck fuel consumption, returns all data (not aggregated number)
type DailyLowFuelCount struct {
	core utils.QueryGenerator
}

func NewDailLowFuelCount(core utils.QueryGenerator) utils.QueryFiller {
	return &DailyLowFuelCount{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DailyLowFuelCount) Fill(q query.Query) query.Query {
	fc, ok := i.core.(DailyLowFuelCountFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DailyLowFuelCount(q)
	return q
}
