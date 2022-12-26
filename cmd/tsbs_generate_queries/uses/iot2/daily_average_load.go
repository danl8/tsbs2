package iot2

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// DailyAverageLoad contains info for filling query for
// selecting daily truck fuel consumption, returns all data (not aggregated number)
type DailyAverageLoad struct {
	core utils.QueryGenerator
}

func NewDailyAverageLoad(core utils.QueryGenerator) utils.QueryFiller {
	return &DailyAverageLoad{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *DailyAverageLoad) Fill(q query.Query) query.Query {
	fc, ok := i.core.(DailyAverageLoadFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.DailyAverageLoad(q)
	return q
}
