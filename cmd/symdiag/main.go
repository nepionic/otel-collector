// symdiag dumps ADS symbols with their flags to diagnose attribute visibility.
package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
)

func main() {
	settings := ads.ClientSettings{
		TargetNetID: "199.4.42.250.1.1",
		RouterHost:  "127.0.0.1",
		RouterPort:  48898,
	}
	client := ads.NewClient(settings, nil)
	if err := client.Connect(); err != nil {
		log.Fatalf("Connect: %v", err)
	}
	defer client.Disconnect()
	time.Sleep(200 * time.Millisecond)

	const port = 851
	syms, err := client.UploadSymbols(port)
	if err != nil {
		log.Fatalf("UploadSymbols: %v", err)
	}

	fmt.Printf("Total symbols: %d\n\n", len(syms))
	for _, s := range syms {
		if !strings.Contains(strings.ToLower(s.Name), "otel") &&
			!strings.Contains(strings.ToLower(s.Name), "ring") {
			continue
		}
		fmt.Printf("Symbol: %s\n", s.Name)
		fmt.Printf("  Type:    %s\n", s.Type)
		fmt.Printf("  Flags:   0x%08X\n", s.Flags)
		fmt.Printf("  ArrayInfo: %v\n", s.ArrayInfo)
		fmt.Printf("  TypeGUID: %s\n", s.TypeGUID)
		fmt.Printf("  Attributes (%d):\n", len(s.Attributes))
		for _, a := range s.Attributes {
			fmt.Printf("    %s = %q\n", a.Name, a.Value)
		}
		fmt.Println()
	}
}
