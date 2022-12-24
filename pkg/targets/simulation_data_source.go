package targets

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"log"
)

type insertData struct {
	tags   string
	fields string
}

// point is a single row of data keyed by which hypertable it belongs
type point struct {
	hypertable string
	row        *insertData
}

func NewSimulationDataSource(sim common.Simulator, target ImplementedTarget, converter Converter) DataSource {
	return &simulationDataSource{
		simulator:  sim,
		headers:    sim.Headers(),
		serializer: target.Serializer(),
		converter:  converter,
	}
}

type simulationDataSource struct {
	simulator  common.Simulator
	headers    *common.GeneratedDataHeaders
	serializer serialize.PointSerializer
	converter  Converter
}

func (d *simulationDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}

	d.headers = d.simulator.Headers()
	return d.headers
}

func (d *simulationDataSource) NextItem() data.LoadedPoint {
	if d.headers == nil {
		log.Fatal("headers not read before starting to read points")
		return data.LoadedPoint{}
	}
	newSimulatorPoint := data.NewPoint()
	var write bool
	for !d.simulator.Finished() {
		write = d.simulator.Next(newSimulatorPoint)
		if write {
			break
		}
		newSimulatorPoint.Reset()
	}
	if d.simulator.Finished() || !write {
		return data.LoadedPoint{}
	}

	// at this point we have generated new data point in DB independent format *Point
	// we need to transform this value to DB dependent format LoadedPoint
	lp, err := d.converter.PointToLoadedPoint(newSimulatorPoint)
	if err != nil {
		log.Fatal("Can't convert new Datapoint to Internal DataPoint format")
		return data.LoadedPoint{}
	}
	return *lp
}
