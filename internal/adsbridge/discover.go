package adsbridge

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jarmocluyse/ads-go/pkg/ads"
)

// ErrSymbolNotFound is returned when no matching symbol is found in the
// PLC symbol table.
var ErrSymbolNotFound = errors.New("symbol not found")

// FindSymbolByAttribute scans the full PLC symbol table and returns the name
// of the first symbol whose pragma attributes contain a matching
// attrName=attrValue pair (value comparison is case-insensitive).
//
// Returns ErrSymbolNotFound (wrapped) if no match is found, or a network/ADS
// error if the upload fails.
func FindSymbolByAttribute(client *ads.Client, port uint16, attrName, attrValue string) (string, error) {
	syms, err := client.UploadSymbols(port)
	if err != nil {
		return "", fmt.Errorf("FindSymbolByAttribute: upload failed: %w", err)
	}
	for _, s := range syms {
		for _, a := range s.Attributes {
			if a.Name == attrName && strings.EqualFold(a.Value, attrValue) {
				return s.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no symbol with attribute %s=%q: %w", attrName, attrValue, ErrSymbolNotFound)
}

// FindLogRingSymbol discovers the ADS path of the OTelLogRing variable using
// two strategies tried in order:
//
//  1. Attribute-based: scan the symbol table for a symbol with the pragma
//     attribute {attribute 'otelcol_role' := 'log_ring'}.
//
//  2. Type-based (fallback): scan the symbol table for a symbol whose type
//     name ends with "OTelLogAppender" (case-insensitive) and append ".ring".
//     This handles the common case where TwinCAT does not expose FB-field
//     pragma attributes in the flat ADS symbol table.
//
// Returns ErrSymbolNotFound (wrapped) if neither strategy succeeds.
func FindLogRingSymbol(client *ads.Client, port uint16) (string, error) {
	syms, err := client.UploadSymbols(port)
	if err != nil {
		return "", fmt.Errorf("FindLogRingSymbol: upload failed: %w", err)
	}

	// Strategy 1: explicit pragma attribute on the ring variable itself.
	for _, s := range syms {
		for _, a := range s.Attributes {
			if a.Name == "otelcol_role" && strings.EqualFold(a.Value, "log_ring") {
				return s.Name, nil
			}
		}
	}

	// Strategy 2: find an OTelLogAppender instance and derive the ring path.
	// The ring field is always named "ring" inside OTelLogAppender.
	for _, s := range syms {
		if strings.HasSuffix(strings.ToLower(s.Type), "otellogappender") {
			return s.Name + ".ring", nil
		}
	}

	return "", fmt.Errorf("no OTelLogAppender or otelcol_role=log_ring symbol found: %w", ErrSymbolNotFound)
}

// FindMetricRingSymbol discovers the ADS path of the OTelMetricRing variable
// using two strategies tried in order:
//
//  1. Attribute-based: scan the symbol table for a symbol with the pragma
//     attribute {attribute 'otelcol_role' := 'metric_ring'}.
//
//  2. Type-based (fallback): scan the symbol table for a symbol whose type
//     name ends with "OTelMetricRing" (case-insensitive) and return it
//     directly. Unlike the log ring (a field owned by OTelLogAppender), the
//     metric ring is a standalone variable that the PLC project assigns into
//     OTelMetricCore via its Ring property, so no field suffix is appended.
//
// Returns ErrSymbolNotFound (wrapped) if neither strategy succeeds.
func FindMetricRingSymbol(client *ads.Client, port uint16) (string, error) {
	syms, err := client.UploadSymbols(port)
	if err != nil {
		return "", fmt.Errorf("FindMetricRingSymbol: upload failed: %w", err)
	}

	// Strategy 1: explicit pragma attribute on the ring variable itself.
	for _, s := range syms {
		for _, a := range s.Attributes {
			if a.Name == "otelcol_role" && strings.EqualFold(a.Value, "metric_ring") {
				return s.Name, nil
			}
		}
	}

	// Strategy 2: find a variable declared as (namespace-qualified) OTelMetricRing.
	for _, s := range syms {
		if strings.HasSuffix(strings.ToLower(s.Type), "otelmetricring") {
			return s.Name, nil
		}
	}

	return "", fmt.Errorf("no OTelMetricRing or otelcol_role=metric_ring symbol found: %w", ErrSymbolNotFound)
}
