package questdb

import (
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"net"
)

type processor struct {
	ilpBindTo string
	url       string
	ilpConn   *net.TCPConn
}

func (p *processor) Init(numWorker int, _, _ bool) {
	if len(p.ilpBindTo) == 0 {
		log.Fatal("Only ILP protocol implemented for Quest DB, but ilp-bind-to not specified")
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", p.ilpBindTo)
	if err != nil {
		log.Fatalf("Failed to resolve %s: %s\n", p.ilpBindTo, err.Error())
	}
	p.ilpConn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatalf("Failed connect to %s: %s\n", p.ilpBindTo, err.Error())
	}
}

func (p *processor) Close(_ bool) {
	defer p.ilpConn.Close()
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		_, err = p.ilpConn.Write(batch.buf.Bytes())
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	return metricCnt, uint64(rowCnt)
}
