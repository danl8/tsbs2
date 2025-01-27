package iot2

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/query"
	"github.com/timescale/tsbs/pkg/query/config"
)

const (
	// LabelDailyAverageLoad - daily (maybe n-days) average load of random track
	LabelDailyAverageLoad = "daily-average-load"

	// LabelDailyFuelConsumptionRow - daily (maybe n-days) random truck fuel consumption,
	// 									returns all data (not aggregated number)
	LabelDailyFuelConsumptionRow = "daily-fuel-consumption-row"

	// LabelDailyLowFuelCount - daily (maybe n-days) count of lines when fuel state
	//							for random track was less than 10%
	LabelDailyLowFuelCount = "daily-low-fuel-count"

	// LabelAllInOrder - generates all iot2 queries in constant order
	LabelAllInOrder = "all-in-order"
)

type DailyFuelConsumptionRowFiller interface {
	DailyFuelConsumptionRow(query query.Query)
}

type DailyAverageLoadFiller interface {
	DailyAverageLoad(query query.Query)
}

type DailyLowFuelCountFiller interface {
	DailyLowFuelCount(query query.Query)
}

type AllInOrderFiller interface {
	AllInOrder(query query.Query)
}

const (
	DiagnosticsTable = "diagnostics"
	ReadingsTable    = "readings"

	FuelConsumptionTag = "fuel_consumption"
	FuelStateTag       = "fuel_state"
	CurrentLoadTag     = "current_load"
)

func GetDailyFuelConsumptionRowLabel(config *config.QueryGeneratorConfig) string {
	return fmt.Sprintf("%s fuel consumption all rows, for random %v days, and random truck (trucks count %v)",
		config.Format, config.DaysCount, config.TrucksCount)
}

func GetDailyAverageLoadLabel(config *config.QueryGeneratorConfig) string {
	return fmt.Sprintf("%s average load, for random %v days, and random truck (trucks count %v)",
		config.Format, config.DaysCount, config.TrucksCount)
}

func GetDailyLowFuelCountLabel(config *config.QueryGeneratorConfig) string {
	return fmt.Sprintf("%s count of rows where fuel bellow or equal 10%%, for random %v days, and random truck (trucks count %v)",
		config.Format, config.DaysCount, config.TrucksCount)
}
