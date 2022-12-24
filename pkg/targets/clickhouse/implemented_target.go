package clickhouse

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

type SpecificConfig struct {
	Debug       int    `yaml:"Debug" mapstructure:"Debug"`
	Host        string `yaml:"Host" mapstructure:"Host"`
	LogBatches  bool   `yaml:"log-batches" mapstructure:"log-batches"`
	Password    string `yaml:"Password" mapstructure:"Password"`
	User        string `yaml:"Host" mapstructure:"User"`
	HashWorkers bool   `yaml:"hash-workers" mapstructure:"hash-workers"`
	InTableTag  bool   `yaml:"in-table-tag" mapstructure:"in-table-tag"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

func NewTarget() targets.ImplementedTarget {
	return &clickhouseTarget{}
}

type clickhouseTarget struct{}

func (c clickhouseTarget) Benchmark(name string, dsConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	chSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}
	cc := ClickhouseConfig{
		Host:           chSpecificConfig.Host,
		User:           chSpecificConfig.User,
		Password:       chSpecificConfig.Password,
		LogBatches:     chSpecificConfig.LogBatches,
		InTableTag:     chSpecificConfig.InTableTag,
		Debug:          chSpecificConfig.Debug,
		DbName:         "benchmark",
		dataSourceConf: dsConfig,
	}
	f := ""
	if dsConfig.File != nil {
		f = dsConfig.File.Location
	}

	hw := chSpecificConfig.HashWorkers

	return NewBenchmark(f, hw, &cc)

}

func (c clickhouseTarget) Serializer() serialize.PointSerializer {
	return &timescaledb.Serializer{}
}

func (c clickhouseTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"Host", "localhost", "Hostname of ClickHouse instance")
	flagSet.String(flagPrefix+"User", "default", "User to connect to ClickHouse as")
	flagSet.String(flagPrefix+"Password", "", "Password to connect to ClickHouse")
	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")
	flagSet.Int(flagPrefix+"Debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flagSet.Bool(flagPrefix+"hash-workers", false, "Use hash workers (should be the same as in runner config")
	flagSet.Bool(flagPrefix+"in-table-tag", false, "Whether the partition key (e.g. hostname) should also be in the metrics hypertable")
}

func (c clickhouseTarget) TargetName() string {
	return constants.FormatClickhouse
}
