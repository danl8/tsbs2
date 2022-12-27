package clickhouse

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/pkg/data/source"
	"log"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

const dbType = "clickhouse"

type ClickhouseConfig struct {
	Host     string
	User     string
	Password string

	LogBatches            bool
	InTableTag            bool
	Debug                 int
	DbName                string
	UseOptimizedStructure bool

	dataSourceConf *source.DataSourceConfig
}

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string // hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string // 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

var tableCols map[string][]string

var tagColumnTypes []string

// allows for testing
var fatal = log.Fatalf

// getConnectString() builds connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(conf *ClickhouseConfig, db bool) string {
	// connectString: tcp://127.0.0.1:9000?Debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=User&Password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	if db {
		return fmt.Sprintf("tcp://%s:9000?username=%s&Password=%s&database=%s", conf.Host, conf.User, conf.Password, conf.DbName)
	}

	return fmt.Sprintf("tcp://%s:9000?username=%s&Password=%s", conf.Host, conf.User, conf.Password)
}

// Point is a single row of data keyed by which table it belongs
// Ex.:
// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
type point struct {
	table string
	row   *insertData
}

// scan.Batch interface implementation
type tableArr struct {
	m   map[string][]*insertData
	cnt uint
}

// scan.Batch interface implementation
func (ta *tableArr) Len() uint {
	return ta.cnt
}

// scan.Batch interface implementation
func (ta *tableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	k := that.table
	ta.m[k] = append(ta.m[k], that.row)
	ta.cnt++
}

// scan.BatchFactory interface implementation
type factory struct{}

// scan.BatchFactory interface implementation
func (f *factory) New() targets.Batch {
	return &tableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

const tagsPrefix = "tags"

func NewBenchmark(file string, hashWorkers bool, conf *ClickhouseConfig) (targets.Benchmark, error) {
	dataSourceConfig := conf.dataSourceConf
	if dataSourceConfig.Type == source.FileDataSourceType {
		br := load.GetBufferedReader(dataSourceConfig.File.Location)
		return &benchmark{
			ds: &fileDataSource{
				scanner: bufio.NewScanner(br),
			},
			hashWorkers: hashWorkers,
			conf:        conf,
			dss:         nil,
		}, nil
	} else if dataSourceConfig.Type == source.SimulatorDataSourceType {
		if dataSourceConfig.Simulator.SimWorkersCount <= 1 {
			dataGenerator := &inputs.DataGenerator{}
			simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator, 0)
			if err != nil {
				return nil, err
			}
			ds := targets.NewSimulationDataSource(simulator, NewTarget(), NewConverter())
			return &benchmark{
				ds:          ds,
				dss:         nil,
				hashWorkers: hashWorkers,
				conf:        conf,
			}, nil
		}

		target := NewTarget()
		converter := NewConverter()
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
			ds:          nil,
			dss:         dataSources,
			hashWorkers: hashWorkers,
			conf:        conf,
		}, nil
	}

	return nil, errors.New(fmt.Sprintf("Data source type %v is supported for ClickHouse", dataSourceConfig.Type))
}

// targets.Benchmark interface implementation
type benchmark struct {
	ds          targets.DataSource
	dss         []targets.DataSource
	hashWorkers bool
	conf        *ClickhouseConfig
}

func (b *benchmark) GetDataSources() []targets.DataSource {
	return b.dss
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if b.hashWorkers {
		return &hostnameIndexer{
			partitions: maxPartitions,
		}
	}
	return &targets.ConstantIndexer{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{conf: b.conf}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetDBCreator() targets.DBCreator {
	if b.GetDataSource() != nil {
		return &dbCreator{ds: b.GetDataSource(), config: b.conf}
	}

	if len(b.GetDataSources()) > 0 {
		return &dbCreator{ds: b.GetDataSources()[0], config: b.conf}
	}

	log.Fatal("No single DataSource, not DataSources list specified")
	return nil
}
