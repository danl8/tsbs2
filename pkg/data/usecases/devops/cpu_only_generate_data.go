package devops

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"sync"
	"time"
)

// A CPUOnlySimulator generates data similar to telemetry from Telegraf for only CPU metrics.
// It fulfills the Simulator interface.
type CPUOnlySimulator struct {
	*commonDevopsSimulator
}

func (d *CPUOnlySimulator) Mu() *sync.RWMutex {
	return &d.commonDevopsSimulator.mu
}

// Fields returns a map of subsystems to metrics collected
func (d *CPUOnlySimulator) Fields() map[string][]string {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

func (d *CPUOnlySimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}

// Next advances a Point to the next state in the generator.
func (d *CPUOnlySimulator) Next(p *data.Point) bool {
	// Switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval, common.GetGlobalRandomizer())
		}

		d.adjustNumHostsForEpoch()
	}

	return d.populatePoint(p, 0)
}

// CPUOnlySimulatorConfig is used to create a CPUOnlySimulator.
type CPUOnlySimulatorConfig commonDevopsSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *CPUOnlySimulatorConfig) NewSimulator(interval time.Duration, limit uint64, simNumber int) common.Simulator {
	if simNumber != 0 {
		panic(fmt.Sprintf("Multiple simulators not implmented for DevopsSimulatorConfig,but simNumber=%v specified", simNumber))
	}
	hostInfos := make([]Host, c.HostCount)
	for i := 0; i < len(hostInfos); i++ {
		hostInfos[i] = c.HostConstructor(NewHostCtx(i, c.Start))
	}

	epochs := calculateEpochs(commonDevopsSimulatorConfig(*c), interval)
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &CPUOnlySimulator{&commonDevopsSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		hostIndex: 0,
		hosts:     hostInfos,

		epoch:          0,
		epochs:         epochs,
		epochHosts:     c.InitHostCount,
		initHosts:      c.InitHostCount,
		timestampStart: c.Start,
		timestampEnd:   c.End,
		interval:       interval,
	}}

	return sim
}
