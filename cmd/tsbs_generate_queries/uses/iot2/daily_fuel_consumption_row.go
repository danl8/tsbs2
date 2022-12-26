package iot2

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// DailyAverageLoad contains info for filling query for
// selecting daily truck fuel consumption, returns all data (not aggregated number)
type DailyFuelConsumptionRow struct {
	core utils.QueryGenerator
}

func NewDailyFuelConsumptionRow(core utils.QueryGenerator) utils.QueryFiller {
	return &DailyFuelConsumptionRow{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DailyFuelConsumptionRow) Fill(q query.Query) query.Query {
	fc, ok := i.core.(DailyFuelConsumptionRowFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DailyFuelConsumptionRow(q)
	return q
}
