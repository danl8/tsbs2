package questdb

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot2"
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

func (g *IoT2Generator) getConditionForRandomPeriodAndId() (string, string, int) {
	d := time.Duration(int64(g.config.DaysCount) * int64(time.Hour*24))
	interval := g.Interval.MustRandWindow(d)
	tagId := rand.Intn(g.config.TrucksCount)

	sql := fmt.Sprintf("(name = 'truck_%v') AND (timestamp >= '%s') AND (timestamp < '%s')",
		tagId,
		interval.StartString(),
		interval.EndString())
	return sql, interval.StartString(), tagId
}

func (g *IoT2Generator) DailyFuelConsumptionRow(query query.Query) {
	periodCondition, startStr, tagId := g.getConditionForRandomPeriodAndId()

	sql := fmt.Sprintf(`SELECT %s, timestamp from '%s' WHERE %s`,
		iot2.FuelConsumptionTag,
		iot2.ReadingsTable,
		periodCondition)

	humanLabel := iot2.GetDailyFuelConsumptionRowLabel(g.config.Format, g.config.DaysCount, g.config.TrucksCount)
	humanDesc := fmt.Sprintf("%s: %s, %v", humanLabel, startStr, tagId)
	g.fillInQuery(query, humanLabel, humanDesc, sql)
}

func (g *IoT2Generator) DailyAverageLoad(query query.Query) {
	periodCondition, startStr, tagId := g.getConditionForRandomPeriodAndId()

	sql := fmt.Sprintf("SELECT AVG(%s) FROM '%s' WHERE %s",
		iot2.CurrentLoadTag,
		iot2.DiagnosticsTable,
		periodCondition)

	humanLabel := iot2.GetDailyAverageLoadLabel(g.config.Format, g.config.DaysCount, g.config.TrucksCount)
	humanDesc := fmt.Sprintf("%s: %s, %v", humanLabel, startStr, tagId)
	g.fillInQuery(query, humanLabel, humanDesc, sql)
}

func (g *IoT2Generator) DailyLowFuelCount(query query.Query) {
	periodCondition, startStr, tagId := g.getConditionForRandomPeriodAndId()

	sql := fmt.Sprintf("SELECT COUNT(*) FROM '%s' WHERE %s <= 0.1 AND %s",
		iot2.DiagnosticsTable,
		iot2.FuelStateTag,
		periodCondition)

	humanLabel := iot2.GetDailyLowFuelCountLabel(g.config.Format, g.config.DaysCount, g.config.TrucksCount)
	humanDesc := fmt.Sprintf("%s: %s, %v", humanLabel, startStr, tagId)
	g.fillInQuery(query, humanLabel, humanDesc, sql)
}
