package main

import "time"

type FlightData struct {
	ICAO24    string  `json:"icao24"`
	Callsign  string  `json:"callsign,omitempty"`
	Alt       int     `json:"alt"`
	Spd       float64 `json:"spd"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Squawk    string  `json:"squawk,omitempty"`
	Timestamp int64   `json:"timestamp"`
	Source    string  `json:"source"`
}

type APIResponse struct {
	Count int          `json:"count"`
	Data  []FlightData `json:"data"`
}

type Config struct {
	ListenAddr       string
	ScrapeInterval   time.Duration
	Sources          []string
	Regions          []Region
	Proxies          []string
	RapidAPIKey      string
	RapidAPIHost     string
	RatePerSecond    float64
	RateBurst        int
	CacheTTL         time.Duration
	RequestTimeout   time.Duration
	ShutdownTimeout  time.Duration
	MaxConcurrentJob int
}

type Region struct {
	Name string
	MinLat float64
	MaxLat float64
	MinLon float64
	MaxLon float64
}

