package main

import "fmt"

func hexDump(data []byte) {
	for idx, bt := range data {
		fmt.Printf("%02X ", bt)
		if idx % 16 == 15 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf("\n")
}

