package usecases

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/data/usecases/devops"
	"github.com/timescale/tsbs/pkg/data/usecases/iot"
	"math"
)

const errCannotParseTimeFmt = "cannot parse time from string '%s': %v"

func GetSimulatorConfig(dgc *common.DataGeneratorConfig) (common.SimulatorConfig, error) {
	var ret common.SimulatorConfig
	var err error
	tsStart, err := utils.ParseUTCTime(dgc.TimeStart)
	if err != nil {
		return nil, fmt.Errorf(errCannotParseTimeFmt, dgc.TimeStart, err)
	}
	tsEnd, err := utils.ParseUTCTime(dgc.TimeEnd)
	if err != nil {
		return nil, fmt.Errorf(errCannotParseTimeFmt, dgc.TimeEnd, err)
	}

	switch dgc.Use {
	case common.UseCaseDevops:
		assertNoMultithread(dgc.SimWorkersCount, dgc.Use)
		ret = &devops.DevopsSimulatorConfig{
			Start: tsStart,
			End:   tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHost,
		}
	case common.UseCaseIoT:
		ret = &iot.SimulatorConfig{
			Start: tsStart,
			End:   tsEnd,

			InitGeneratorScale:   dgc.InitialScale,
			GeneratorScale:       dgc.Scale,
			GeneratorConstructor: iot.NewTruck,
			SimWorkersCount:      dgc.SimWorkersCount,
		}
	case common.UseCaseCPUOnly:
		assertNoMultithread(dgc.SimWorkersCount, dgc.Use)
		ret = &devops.CPUOnlySimulatorConfig{
			Start: tsStart,
			End:   tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHostCPUOnly,
		}
	case common.UseCaseCPUSingle:
		assertNoMultithread(dgc.SimWorkersCount, dgc.Use)
		ret = &devops.CPUOnlySimulatorConfig{
			Start: tsStart,
			End:   tsEnd,

			InitHostCount:   dgc.InitialScale,
			HostCount:       dgc.Scale,
			HostConstructor: devops.NewHostCPUSingle,
		}
	case common.UseCaseDevopsGeneric:
		assertNoMultithread(dgc.SimWorkersCount, dgc.Use)
		if dgc.InitialScale == dgc.Scale {
			// if no initial scale argument given we will start with 50%. The lower bound is 1
			dgc.InitialScale = uint64(math.Max(float64(1), float64(dgc.Scale/2)))
		}
		ret = &devops.GenericMetricsSimulatorConfig{
			DevopsSimulatorConfig: &devops.DevopsSimulatorConfig{
				Start: tsStart,
				End:   tsEnd,

				InitHostCount:   dgc.InitialScale,
				HostCount:       dgc.Scale,
				HostConstructor: devops.NewHostGenericMetrics,
				MaxMetricCount:  dgc.MaxMetricCountPerHost,
			},
		}
	default:
		err = fmt.Errorf("unknown use case: '%s'", dgc.Use)
	}
	return ret, err
}

func assertNoMultithread(workersCount int, useCase string) {
	if workersCount > 1 {
		panic(fmt.Errorf("multithreading not implemented for use case %v, but %v sim-workers-count spicified in config. Please set "+
			"sim-workers-count = 1 or use another use case", useCase, workersCount))
	}
}
