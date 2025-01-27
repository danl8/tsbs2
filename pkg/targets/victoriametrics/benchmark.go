package victoriametrics

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
)

type SpecificConfig struct {
	ServerURLs []string `yaml:"urls" mapstructure:"urls"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// loader.Benchmark interface implementation
type benchmark struct {
	serverURLs  []string
	dataSource  targets.DataSource
	dataSources []targets.DataSource
}

func (b *benchmark) GetDataSources() []targets.DataSource {
	return b.dataSources
}

func NewBenchmark(vmSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type == source.FileDataSourceType {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		return &benchmark{
			dataSource: &fileDataSource{
				scanner: bufio.NewScanner(br),
			},
			serverURLs: vmSpecificConfig.ServerURLs,
		}, nil
	} else if dataSourceConfig.Type == source.SimulatorDataSourceType {
		if dataSourceConfig.Simulator.SimWorkersCount <= 1 {
			dataGenerator := &inputs.DataGenerator{}
			simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator, 0)
			if err != nil {
				return nil, err
			}
			target := NewTarget()
			converter := targets.NewSerializerConverter(target.Serializer())
			ds := targets.NewSimulationDataSource(simulator, target, converter)
			return &benchmark{
				dataSource: ds,
				serverURLs: vmSpecificConfig.ServerURLs,
			}, nil
		}

		target := NewTarget()
		converter := targets.NewSerializerConverter(target.Serializer())
		dataSources := make([]targets.DataSource, 0, dataSourceConfig.Simulator.SimWorkersCount)
		for i := 0; i < dataSourceConfig.Simulator.SimWorkersCount; i++ {
			dataGenerator := &inputs.DataGenerator{}
			simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator, i)
			if err != nil {
				return nil, err
			}
			ds := targets.NewSimulationDataSource(simulator, target, converter)
			dataSources = append(dataSources, ds)
		}

		return &benchmark{
			dataSources: dataSources,
			dataSource:  nil,
			serverURLs:  vmSpecificConfig.ServerURLs,
		}, nil

	}

	return nil, errors.New(fmt.Sprintf("Data source type %v is supported for VictoriaMetrics", dataSourceConfig.Type))
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}
	return &factory{bufPool: &bufPool}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{vmURLs: b.serverURLs}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer)}
}
