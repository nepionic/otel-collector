// symdiag dumps ADS symbols with their flags to diagnose attribute visibility.
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jarmocluyse/ads-go/pkg/ads"
)

func main() {
	targetNetID := flag.String("target-net-id", "", "AMS Net ID of the target TwinCAT system (required)")
	routerHost := flag.String("router-host", "127.0.0.1", "Hostname or IP of the ADS router")
	routerPort := flag.Int("router-port", 48898, "TCP port of the ADS router")
	plcPort := flag.Int("plc-port", 851, "ADS port of the PLC runtime")
	flag.Parse()

	if *targetNetID == "" {
		log.Fatal("missing required -target-net-id flag")
	}

	settings := ads.ClientSettings{
		TargetNetID: *targetNetID,
		RouterHost:  *routerHost,
		RouterPort:  *routerPort,
	}
	client := ads.NewClient(settings, nil)
	if err := client.Connect(); err != nil {
		log.Fatalf("Connect: %v", err)
	}
	defer client.Disconnect()
	time.Sleep(200 * time.Millisecond)

	syms, err := client.UploadSymbols(uint16(*plcPort))
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
