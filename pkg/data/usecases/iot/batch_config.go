package iot

import (
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

var (
	// Batch chances.
	bMissingChance        = 0.01
	bOutOfOrderChance     = 0.05
	bInsertPreviousChance = 0.5

	// Entry chances.
	eMissingChance        = 0.1
	eOutOfOrderChance     = 0.3
	eInsertPreviousChance = 0.5

	// Zero values.
	zeroTagChance   = 0.01
	zeroFieldChance = 0.1
)

type batchConfig struct {
	// Batch level configs.
	InsertPrevious bool
	Missing        bool
	OutOfOrder     bool

	// Entry level configs.
	ZeroFields          map[int]int
	ZeroTags            map[int]int
	InsertPreviousEntry map[int]bool
	MissingEntries      map[int]bool
	OutOfOrderEntries   map[int]bool
}

func newBatchConfig(outOfOrderBatchCount, outOfOrderEntryCount, fieldCount, tagCount int, randomizer *common.Randomizer) *batchConfig {

	batchMissing := (*randomizer).Float64() < bMissingChance

	if batchMissing {
		return &batchConfig{
			Missing: true,
		}
	}

	batchOutOfOrder := (*randomizer).Float64() < bOutOfOrderChance

	batchInsertPrevious := false
	if outOfOrderBatchCount > 0 {
		batchInsertPrevious = (*randomizer).Float64() < bInsertPreviousChance
	}

	zeroFields := make(map[int]int)
	zeroTags := make(map[int]int)
	insertPreviousEntry := make(map[int]bool)
	missingEntries := make(map[int]bool)
	outOfOrderEntries := make(map[int]bool)

	for i := 0; i < defaultBatchSize; i++ {
		if outOfOrderEntryCount > 0 && (*randomizer).Float64() < eInsertPreviousChance {
			insertPreviousEntry[i] = true
			outOfOrderEntryCount--
		}

		if (*randomizer).Float64() < eMissingChance {
			missingEntries[i] = true
			// Since the entry is missing, no point in setting zero values or making it out-of-order.
			continue
		}

		if fieldCount > 0 && (*randomizer).Float64() < zeroFieldChance {
			zeroFields[i] = (*randomizer).Intn(fieldCount)
		}

		if tagCount > 0 && (*randomizer).Float64() < zeroTagChance {
			zeroTags[i] = (*randomizer).Intn(tagCount)
		}

		if (*randomizer).Float64() < eOutOfOrderChance {
			outOfOrderEntries[i] = true
		}
	}

	return &batchConfig{
		OutOfOrder:     batchOutOfOrder,
		InsertPrevious: batchInsertPrevious,

		ZeroFields:          zeroFields,
		ZeroTags:            zeroTags,
		InsertPreviousEntry: insertPreviousEntry,
		MissingEntries:      missingEntries,
		OutOfOrderEntries:   outOfOrderEntries,
	}
}
