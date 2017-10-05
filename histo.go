package main

import (
	"math"
	"log"
)

// Histogram - log-based histogram class
type Histogram struct {
	min, step float64
	bins []uint32
}

func makeHisto(min, max float64, binCount int) *Histogram {
	if max <= min {
		return nil
	}
	step := math.Log2(max/min) / float64(binCount)
	log.Printf("Histo: min: %.2f, max: %.2f, step: %.2f, size: %d", min, max, step, binCount)
	return &Histogram{min: min, step: step, bins: make([]uint32, binCount)}
}

func (histo *Histogram) update(data []uint32) {
	bcount := len(histo.bins) - 1
	for _, val := range data {
		bin := int(math.Log2(float64(val) / histo.min) / histo.step)
		if bin <= 0 {
			bin = 0
		} else if bin >= bcount {
			bin = bcount
		}

		if histo.bins[bin] < math.MaxUint32 {
			histo.bins[bin]++
		}
	}
}

func (histo *Histogram) clean() {
	histo.bins = make([]uint32, len(histo.bins))
}
