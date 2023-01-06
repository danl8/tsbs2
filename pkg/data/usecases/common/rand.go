package common

import (
	"math/rand"
	"time"
)

type Randomizer interface {
	NormFloat64() float64
	Float64() float64
	Intn(n int) int
}

type GlobalRand struct {
}

type ThreadUnsafeRand struct {
	r *rand.Rand
}

func (t *ThreadUnsafeRand) Intn(n int) int {
	return t.r.Intn(n)
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

func (g *GlobalRand) Intn(n int) int {
	return rand.Intn(n)
}

func (g *GlobalRand) NormFloat64() float64 {
	return rand.NormFloat64()
}

func (g *GlobalRand) Float64() float64 {
	return rand.Float64()
}
