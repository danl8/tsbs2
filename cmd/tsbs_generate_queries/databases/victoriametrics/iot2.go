package victoriametrics

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot2"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"github.com/timescale/tsbs/pkg/query/config"
	"math/rand"
	"time"
)

type IoT2Generator struct {
	*BaseGenerator
	*common.Core
	config *config.QueryGeneratorConfig
}

func (g *IoT2Generator) getConditionForRandomPeriodAndId() (*utils.TimeInterval, int, string) {
	d := time.Duration(int64(g.config.DaysCount) * int64(time.Hour*24))
	interval := g.Interval.MustRandWindow(d)
	truckId := rand.Intn(g.config.TrucksCount)
	steps := fmt.Sprintf("%vd", g.config.DaysCount)
	return interval, truckId, steps
}

func (g *IoT2Generator) DailyFuelConsumptionRow(query query.Query) {
	interval, truckId, step := g.getConditionForRandomPeriodAndId()

	q := fmt.Sprintf(`%s_%s{name="truck_%v"}`,
		iot2.ReadingsTable,
		iot2.FuelConsumptionTag,
		truckId)

	qi := &queryInfo{
		query:    q,
		label:    iot2.GetDailyFuelConsumptionRowLabel(g.config),
		interval: interval,
		step:     step,
	}
	g.fillInQuery(query, qi, false)
}

func (g *IoT2Generator) DailyAverageLoad(query query.Query) {
	interval, truckId, step := g.getConditionForRandomPeriodAndId()

	q := fmt.Sprintf(`avg(avg_over_time(%s_%s{name="truck_%v"}))`,
		iot2.DiagnosticsTable,
		iot2.CurrentLoadTag,
		truckId)
	qi := &queryInfo{
		query:    q,
		label:    iot2.GetDailyAverageLoadLabel(g.config),
		interval: interval,
		step:     step,
	}
	g.fillInQuery(query, qi, true)
}

func (g *IoT2Generator) DailyLowFuelCount(query query.Query) {
	interval, truckId, step := g.getConditionForRandomPeriodAndId()

	q := fmt.Sprintf(`sum(count_le_over_time(%s_%s{name="truck_%v"},0.1),0)`,
		iot2.DiagnosticsTable,
		iot2.FuelStateTag,
		truckId)

	qi := &queryInfo{
		query:    q,
		label:    iot2.GetDailyLowFuelCountLabel(g.config),
		interval: interval,
		step:     step,
	}
	g.fillInQuery(query, qi, true)
}
