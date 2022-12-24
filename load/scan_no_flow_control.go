package load

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
)

// scanWithoutFlowControl reads data from the DataSource ds until a limit is reached (if -1, all items are read).
// Data is then placed into appropriate batches, using the supplied PointIndexer,
// which are then dispatched to workers (channel idx chosen by PointIndexer).
// readDs does no flow control, if the capacity of a channel is reached, scanning stops for all
// workers. (should only happen if channel-capacity is low and one worker is unreasonable slower than the rest)
// in that case just set hash-workers to false and use 1 channel for all workers.
func scanWithoutFlowControl(
	ds targets.DataSource,
	dss []targets.DataSource,
	indexer targets.PointIndexer,
	factory targets.BatchFactory,
	channels []chan targets.Batch,
	batchSize uint, limit uint64,
) uint64 {
	if ds == nil && dss == nil {
		log.Fatalf("No single DataSource, nor list of DataSources cpecified")
	}

	if batchSize == 0 {
		panic("batch size can't be 0")
	}
	numChannels := len(channels)
	batches := make([]targets.Batch, numChannels)
	for i := 0; i < numChannels; i++ {
		batches[i] = factory.New()
	}
	var itemsRead uint64

	closeAllWorkers := false
	useWorkers := len(dss) > 0
	workersCount := len(dss)
	workersChan := make(chan *data.LoadedPoint, workersCount*5)
	nilResultsCount := 0
	if useWorkers {
		for i := 0; i < workersCount; i++ {
			go func(workerId int) {
				for closeAllWorkers == false {
					item := dss[workerId].NextItem()
					workersChan <- &item
					if item.Data == nil {
						break
					}
				}
			}(i)
		}
	}

	for {
		if limit > 0 && itemsRead >= limit {
			closeAllWorkers = true
			break
		}

		var item *data.LoadedPoint
		if useWorkers {
			item = <-workersChan
			if item.Data == nil {
				nilResultsCount++

				if nilResultsCount >= workersCount {
					closeAllWorkers = true
					break
				}

				continue
			}
		} else {
			i := ds.NextItem()
			item = &(i)
			if item.Data == nil {
				// Nothing to scan any more - input is empty or failed
				// Time to exit
				closeAllWorkers = true
				break
			}
		}
		itemsRead++

		idx := indexer.GetIndex(*item)
		batches[idx].Append(*item)

		if batches[idx].Len() >= batchSize {
			channels[idx] <- batches[idx]
			batches[idx] = factory.New()
			//$DEBUG: check if channel always filled with enought data
			//log.Infof("Channel %v len: %v, cap: %v", idx, len(channels[idx]), cap(channels[idx]))
			//			if len(channels[idx]) < (cap(channels[idx]) - 2) {
			//				log.Infof("Channel not full len: %v, cap: %v", len(channels[idx]), cap(channels[idx]))
			//			}
		}
	}

	for idx, unfilledBatch := range batches {
		if unfilledBatch.Len() > 0 {
			channels[idx] <- unfilledBatch
		}
	}
	return itemsRead
}
