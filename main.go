package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type ForwardRule struct {
	SourcePort int
	Targets    []Target
	mu         sync.RWMutex
	bestTarget *Target
}

type Target struct {
	IP         string
	Datacenter string
}

func main() {
	rules, err := readRulesFromCSV("ip.csv")
	if err != nil {
		log.Fatalf("Error reading rules: %v", err)
	}

	for datacenter, rule := range rules {
		go startForwarding(datacenter, rule)
	}

	select {}
}

func readRulesFromCSV(filename string) (map[string]*ForwardRule, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	rules := make(map[string]*ForwardRule)
	for _, record := range records[1:] { // Skip header
		ip := record[0]
		datacenter := record[1]

		target := Target{
			IP:         ip,
			Datacenter: datacenter,
		}

		if rule, ok := rules[datacenter]; ok {
			rule.Targets = append(rule.Targets, target)
		} else {
			rules[datacenter] = &ForwardRule{
				SourcePort: 10000 + len(rules),
				Targets:    []Target{target},
			}
		}
	}

	return rules, nil
}

func startForwarding(datacenter string, rule *ForwardRule) {
	go updateBestTarget(rule)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", rule.SourcePort))
	if err != nil {
		log.Printf("Error listening on port %d for %s: %v", rule.SourcePort, datacenter, err)
		return
	}
	defer listener.Close()

	log.Printf("Forwarding from :%d for datacenter %s", rule.SourcePort, datacenter)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection for %s: %v", datacenter, err)
			continue
		}

		go handleConnection(conn, rule)
	}
}

func updateBestTarget(rule *ForwardRule) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		bestTarget := selectTarget(rule.Targets)
		rule.mu.Lock()
		rule.bestTarget = &bestTarget
		rule.mu.Unlock()

		<-ticker.C
	}
}

func handleConnection(source net.Conn, rule *ForwardRule) {
	defer source.Close()

	rule.mu.RLock()
	target := rule.bestTarget
	rule.mu.RUnlock()

	if target == nil {
		log.Printf("No available target")
		return
	}

	destination, err := net.Dial("tcp", fmt.Sprintf("%s:443", target.IP))
	if err != nil {
		log.Printf("Error connecting to target %s:443: %v", target.IP, err)
		return
	}
	defer destination.Close()

	// Use io.Copy for bidirectional data transfer
	go func() {
		_, err := io.Copy(destination, source)
		if err != nil {
			log.Printf("Error copying data from source to target: %v", err)
		}
	}()

	_, err = io.Copy(source, destination)
	if err != nil {
		log.Printf("Error copying data from target to source: %v", err)
	}
}

func selectTarget(targets []Target) Target {
	var bestTarget Target
	lowestLatency := time.Hour

	for _, target := range targets {
		latency := measureLatency(target.IP)
		if latency < lowestLatency {
			lowestLatency = latency
			bestTarget = target
		}
	}

	return bestTarget
}

func measureLatency(ip string) time.Duration {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:80", ip), 5*time.Second)
	if err != nil {
		return time.Hour // Return a high latency if the connection fails
	}
	defer conn.Close()
	return time.Since(start)
}
