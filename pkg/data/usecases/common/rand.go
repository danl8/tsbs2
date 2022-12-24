package common

import (
	"math/rand"
	"time"
)

type Randomizer interface {
	NormFloat64() float64
	Float64() float64
}

type GlobalRand struct {
}

type ThreadUnsafeRand struct {
	r *rand.Rand
}

func GetUnsafeRandomizer() Randomizer {
	return &ThreadUnsafeRand{
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func GetGlobalRandomizer() Randomizer {
	return &(GlobalRand{})
}

func (t *ThreadUnsafeRand) NormFloat64() float64 {
	return t.r.NormFloat64()
}

func (t *ThreadUnsafeRand) Float64() float64 {
	return t.r.Float64()
}

func (g *GlobalRand) NormFloat64() float64 {
	return rand.NormFloat64()
}

func (g *GlobalRand) Float64() float64 {
	return rand.Float64()
}
