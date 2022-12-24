package targets

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// Converter interface to convert data in internal structure in which data generated (data.Point)
// to db specific format in which db part will process (data.LoadedPoint)
type Converter interface {
	PointToLoadedPoint(point *data.Point) (*data.LoadedPoint, error)
}

func NewSerializerConverter(serializer serialize.PointSerializer) Converter {
	return &(SerializerConverter{
		serializer: serializer,
	})
}

type SerializerConverter struct {
	serializer serialize.PointSerializer
}

func (s *SerializerConverter) PointToLoadedPoint(point *data.Point) (*data.LoadedPoint, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err := s.serializer.Serialize(point, buf); err != nil {
		return &data.LoadedPoint{}, nil
	}
	p := data.NewLoadedPoint(buf.Bytes())
	return &p, nil
}
