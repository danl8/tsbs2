package questdb

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
	ILPBindTo string `yaml:"ilp-bind-to" mapstructure:"ilp-bind-to"`
	URL       string `yaml:"url" mapstructure:"url"`
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
	ilpBindTo   string
	url         string
	dataSource  targets.DataSource
	dataSources []targets.DataSource
}

func (b *benchmark) GetDataSources() []targets.DataSource {
	return b.dataSources
}

func NewBenchmark(qdbSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type == source.FileDataSourceType {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		return &benchmark{
			dataSource: &fileDataSource{
				scanner: bufio.NewScanner(br),
			},
			ilpBindTo: qdbSpecificConfig.ILPBindTo,
			url:       qdbSpecificConfig.URL,
		}, nil
	} else if dataSourceConfig.Type == source.SimulatorDataSourceType {
		if dataSourceConfig.Simulator.SimWorkersCount <= 1 {
			dataGenerator := &inputs.DataGenerator{}
			simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator, 0)
			if err != nil {
				return nil, err
			}
			target := NewTarget()
			ds := targets.NewSimulationDataSource(simulator, target, targets.NewSerializerConverter(target.Serializer()))
			return &benchmark{
				dataSource: ds,
				ilpBindTo:  qdbSpecificConfig.ILPBindTo,
				url:        qdbSpecificConfig.URL,
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
			ilpBindTo:   qdbSpecificConfig.ILPBindTo,
			url:         qdbSpecificConfig.URL,
		}, nil
	}

	return nil, errors.New(fmt.Sprintf("Data source type %v is supported for QuestDB", dataSourceConfig.Type))
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
	return &processor{
		ilpBindTo: b.ilpBindTo,
		url:       b.url,
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}
