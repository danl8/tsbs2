package clickhouse

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/targets"
)

type SpecificConverter struct {
}

func (s *SpecificConverter) PointToLoadedPoint(p *data.Point) (*data.LoadedPoint, error) {
	// tags
	buf := make([]byte, 0, 256)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	isFirst := true
	for i, v := range tagValues {
		if !isFirst {
			buf = append(buf, ',')
		}
		isFirst = false
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = serialize.FastFormatAppend(v, buf)
	}
	newPoint := &insertData{}
	newPoint.tags = string(buf)

	// Field row second
	buf = make([]byte, 0, 256)
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixNano()))...)
	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}
	newPoint.fields = string(buf)

	newLoadedPoint := data.NewLoadedPoint(&point{
		table: string(p.MeasurementName()),
		row:   newPoint,
	})
	return &newLoadedPoint, nil
}

func NewConverter() targets.Converter {
	c := SpecificConverter{}
	return &c
}
