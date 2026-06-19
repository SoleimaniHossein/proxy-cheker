package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ProxyType string

const (
	SOCKS4 ProxyType = "socks4"
	SOCKS5 ProxyType = "socks5"
	HTTP   ProxyType = "http"
)

type Proxy struct {
	Type ProxyType
	IP   string
	Port string
	Addr string
}

type TestResult struct {
	Proxy   Proxy
	Working bool
	Latency time.Duration
	Error   string
}

var (
	// Global counters for progress
	testedCount uint32
	totalCount  uint32
)

func main() {
	// Command line flags
	var proxyType string
	var concurrency int
	var timeout int
	var testURL string

	flag.StringVar(&proxyType, "type", "all", "Proxy type to test: http, socks4, socks5, or all")
	flag.IntVar(&concurrency, "concurrency", 100, "Number of concurrent tests (default: 100)")
	flag.IntVar(&timeout, "timeout", 10, "Timeout in seconds for each test (default: 10)")
	flag.StringVar(&testURL, "url", "http://httpbin.org/ip", "Test URL (default: http://httpbin.org/ip)")
	flag.Parse()

	// Determine which files to read
	var files []string
	switch proxyType {
	case "http":
		files = []string{"http.txt"}
	case "socks4":
		files = []string{"socks4.txt"}
	case "socks5":
		files = []string{"socks5.txt"}
	case "all":
		files = []string{"http.txt", "socks4.txt", "socks5.txt"}
	default:
		fmt.Printf("❌ Invalid proxy type: %s. Use: http, socks4, socks5, or all\n", proxyType)
		return
	}

	fmt.Printf("🚀 Proxy Tester - Concurrent Edition\n")
	fmt.Printf("📋 Configuration:\n")
	fmt.Printf("   - Type: %s\n", proxyType)
	fmt.Printf("   - Concurrency: %d\n", concurrency)
	fmt.Printf("   - Timeout: %d seconds\n", timeout)
	fmt.Printf("   - Test URL: %s\n", testURL)
	fmt.Println()

	var allProxies []Proxy

	// Read proxies from files
	for _, filename := range files {
		proxies, err := readProxyFile(filename)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("⚠️  File %s not found, skipping...\n", filename)
			} else {
				fmt.Printf("❌ Error reading %s: %v\n", filename, err)
			}
			continue
		}

		proxyTypeEnum := getProxyType(filename)
		for _, addr := range proxies {
			parts := strings.Split(addr, ":")
			if len(parts) == 2 {
				allProxies = append(allProxies, Proxy{
					Type: proxyTypeEnum,
					IP:   parts[0],
					Port: parts[1],
					Addr: addr,
				})
			}
		}
		fmt.Printf("📁 Loaded %d proxies from %s\n", len(proxies), filename)
	}

	if len(allProxies) == 0 {
		fmt.Println("❌ No proxies found to test!")
		return
	}

	fmt.Printf("\n🔍 Testing %d proxies with %d concurrent workers...\n\n", len(allProxies), concurrency)

	// Create worker pool
	totalCount = uint32(len(allProxies))
	results := make(chan TestResult, len(allProxies))

	// Create work queue
	workQueue := make(chan Proxy, len(allProxies))

	// Fill work queue
	for _, proxy := range allProxies {
		workQueue <- proxy
	}
	close(workQueue)

	// Start workers
	var wg sync.WaitGroup
	timeoutDuration := time.Duration(timeout) * time.Second

	// Progress reporter
	stopProgress := make(chan bool)
	go showProgress(stopProgress)

	// Launch workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(workQueue, results, &wg, timeoutDuration, testURL)
	}

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(results)
		stopProgress <- true
	}()

	// Collect results
	var workingProxies []TestResult
	var failedProxies []TestResult

	// Clear previous logs
	os.Remove("working_proxies.txt")
	os.Remove("working_proxies_sorted.txt")
	os.Remove("working_proxies_detail.csv")
	os.Remove("failed_proxies.txt")

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("%-12s %-25s %-12s %-30s\n", "TYPE", "PROXY", "PING", "STATUS")
	fmt.Println(strings.Repeat("=", 80))

	for result := range results {
		if result.Working {
			workingProxies = append(workingProxies, result)
			printWorkingResult(result)
		} else {
			failedProxies = append(failedProxies, result)
			printFailedResult(result)
		}
	}

	fmt.Println(strings.Repeat("=", 80))

	// Sort working proxies by latency
	sort.Slice(workingProxies, func(i, j int) bool {
		return workingProxies[i].Latency < workingProxies[j].Latency
	})

	// Display sorted results
	displaySortedResults(workingProxies)

	// Display summary
	displaySummary(workingProxies, failedProxies, allProxies)

	// Save results
	saveResults(workingProxies, failedProxies)
}

func worker(workQueue <-chan Proxy, results chan<- TestResult, wg *sync.WaitGroup, timeout time.Duration, testURL string) {
	defer wg.Done()

	for proxy := range workQueue {
		result := testProxyFast(proxy, timeout, testURL)
		results <- result
		atomic.AddUint32(&testedCount, 1)
	}
}

func showProgress(stop <-chan bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tested := atomic.LoadUint32(&testedCount)
			total := atomic.LoadUint32(&totalCount)
			if total > 0 {
				percentage := float64(tested) / float64(total) * 100
				fmt.Printf("\r📊 Progress: %d/%d (%.1f%%) proxies tested", tested, total, percentage)
			}
		case <-stop:
			fmt.Printf("\r📊 Progress: %d/%d (100%%) proxies tested\n",
				atomic.LoadUint32(&testedCount), atomic.LoadUint32(&totalCount))
			return
		}
	}
}

func testProxyFast(proxy Proxy, timeout time.Duration, testURL string) TestResult {
	start := time.Now()

	var transport *http.Transport

	switch proxy.Type {
	case HTTP:
		proxyURL, err := url.Parse("http://" + proxy.Addr)
		if err != nil {
			return TestResult{Proxy: proxy, Working: false, Error: err.Error()}
		}
		transport = &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 0,
			}).DialContext,
			TLSHandshakeTimeout: timeout,
			DisableKeepAlives:   true,
			MaxIdleConns:        0,
			IdleConnTimeout:     0,
		}

	case SOCKS5:
		proxyURL, err := url.Parse("socks5://" + proxy.Addr)
		if err != nil {
			return TestResult{Proxy: proxy, Working: false, Error: err.Error()}
		}
		transport = &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 0,
			}).DialContext,
			TLSHandshakeTimeout: timeout,
			DisableKeepAlives:   true,
			MaxIdleConns:        0,
			IdleConnTimeout:     0,
		}

	case SOCKS4:
		// Fast TCP connection test for SOCKS4
		conn, err := net.DialTimeout("tcp", proxy.Addr, timeout)
		if err != nil {
			return TestResult{
				Proxy:   proxy,
				Working: false,
				Latency: time.Since(start),
				Error:   err.Error(),
			}
		}
		defer conn.Close()

		// Quick SOCKS4 handshake
		handshake := []byte{
			0x04, 0x01, 0x00, 0x50,
			0x00, 0x00, 0x00, 0x01, 0x00,
		}

		conn.SetDeadline(time.Now().Add(timeout))
		_, err = conn.Write(handshake)
		if err != nil {
			return TestResult{
				Proxy:   proxy,
				Working: false,
				Latency: time.Since(start),
				Error:   "handshake failed",
			}
		}

		response := make([]byte, 8)
		_, err = conn.Read(response)
		if err != nil {
			return TestResult{
				Proxy:   proxy,
				Working: false,
				Latency: time.Since(start),
				Error:   "no response",
			}
		}

		if len(response) >= 2 && response[1] == 90 {
			return TestResult{
				Proxy:   proxy,
				Working: true,
				Latency: time.Since(start),
				Error:   "",
			}
		}

		return TestResult{
			Proxy:   proxy,
			Working: false,
			Latency: time.Since(start),
			Error:   fmt.Sprintf("code %d", response[1]),
		}
	}

	if transport == nil {
		return TestResult{Proxy: proxy, Working: false, Error: "Unsupported proxy type"}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(testURL)
	if err != nil {
		return TestResult{
			Proxy:   proxy,
			Working: false,
			Latency: time.Since(start),
			Error:   err.Error(),
		}
	}
	defer resp.Body.Close()

	latency := time.Since(start)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return TestResult{
			Proxy:   proxy,
			Working: true,
			Latency: latency,
			Error:   "",
		}
	}

	return TestResult{
		Proxy:   proxy,
		Working: false,
		Latency: latency,
		Error:   fmt.Sprintf("HTTP %d", resp.StatusCode),
	}
}

func readProxyFile(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var proxies []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Remove protocol prefix if present
		line = strings.TrimPrefix(line, "http://")
		line = strings.TrimPrefix(line, "https://")
		line = strings.TrimPrefix(line, "socks4://")
		line = strings.TrimPrefix(line, "socks5://")
		line = strings.TrimPrefix(line, "socks://")

		if strings.Contains(line, ":") {
			proxies = append(proxies, line)
		}
	}

	return proxies, scanner.Err()
}

func getProxyType(filename string) ProxyType {
	switch filename {
	case "socks4.txt":
		return SOCKS4
	case "socks5.txt":
		return SOCKS5
	case "http.txt":
		return HTTP
	default:
		return HTTP
	}
}

func printWorkingResult(result TestResult) {
	var color, pingColor string

	// Color based on latency
	if result.Latency < 500*time.Millisecond {
		color = "\033[32m" // Green
		pingColor = "\033[32m"
	} else if result.Latency < 2000*time.Millisecond {
		color = "\033[33m" // Yellow
		pingColor = "\033[33m"
	} else {
		color = "\033[31m" // Red
		pingColor = "\033[31m"
	}

	fmt.Printf("%s%-12s %-25s %s%-12s\033[0m \033[32m✅ WORKING\033[0m\n",
		color, result.Proxy.Type,
		result.Proxy.Addr,
		pingColor, fmt.Sprintf("%dms", result.Latency.Milliseconds()))
}

func printFailedResult(result TestResult) {
	fmt.Printf("\033[31m%-12s %-25s %-12s ❌ FAILED: %s\033[0m\n",
		result.Proxy.Type,
		result.Proxy.Addr,
		"-",
		truncateString(result.Error, 40))
}

func displaySortedResults(workingProxies []TestResult) {
	if len(workingProxies) > 0 {
		fmt.Printf("\n\033[32m✅ WORKING PROXIES (Sorted by ping - Fastest first):\033[0m\n")
		fmt.Println(strings.Repeat("-", 80))
		fmt.Printf("%-5s %-12s %-25s %-12s %-20s\n", "RANK", "TYPE", "PROXY", "PING", "SPEED")
		fmt.Println(strings.Repeat("-", 80))

		for i, result := range workingProxies {
			var rankDisplay string
			var speed string

			switch i {
			case 0:
				rankDisplay = "🥇 #1"
				speed = "🚀 FASTEST"
			case 1:
				rankDisplay = "🥈 #2"
				speed = "⚡ FAST"
			case 2:
				rankDisplay = "🥉 #3"
				speed = "👍 GOOD"
			default:
				rankDisplay = fmt.Sprintf("#%d", i+1)
				if result.Latency < 1000*time.Millisecond {
					speed = "✓ FAST"
				} else if result.Latency < 2000*time.Millisecond {
					speed = "○ MEDIUM"
				} else {
					speed = "● SLOW"
				}
			}

			// Color based on latency
			var color string
			if result.Latency < 500*time.Millisecond {
				color = "\033[32m"
			} else if result.Latency < 2000*time.Millisecond {
				color = "\033[33m"
			} else {
				color = "\033[31m"
			}

			fmt.Printf("%s%-5s %-12s %-25s %-12dms %-20s\033[0m\n",
				color, rankDisplay, result.Proxy.Type, result.Proxy.Addr,
				result.Latency.Milliseconds(), speed)
		}
		fmt.Println(strings.Repeat("-", 80))
	}
}

func displaySummary(workingProxies, failedProxies []TestResult, allProxies []Proxy) {
	fmt.Printf("\n📊 SUMMARY:\n")
	fmt.Printf("   ✅ Working:  %d proxies\n", len(workingProxies))
	fmt.Printf("   ❌ Failed:   %d proxies\n", len(failedProxies))
	fmt.Printf("   📊 Total:    %d proxies\n", len(allProxies))

	if len(workingProxies) > 0 {
		// Calculate statistics
		var totalLatency time.Duration
		var fastest, slowest time.Duration
		fastest = workingProxies[0].Latency
		slowest = workingProxies[0].Latency

		for _, result := range workingProxies {
			totalLatency += result.Latency
			if result.Latency < fastest {
				fastest = result.Latency
			}
			if result.Latency > slowest {
				slowest = result.Latency
			}
		}
		avgLatency := totalLatency / time.Duration(len(workingProxies))

		fmt.Printf("   ⚡ Avg ping: %dms\n", avgLatency.Milliseconds())
		fmt.Printf("   🚀 Fastest:   %dms (%s)\n", fastest.Milliseconds(), workingProxies[0].Proxy.Addr)
		fmt.Printf("   🐢 Slowest:   %dms (%s)\n", slowest.Milliseconds(), workingProxies[len(workingProxies)-1].Proxy.Addr)

		// Count by type
		httpCount := 0
		socks4Count := 0
		socks5Count := 0
		for _, result := range workingProxies {
			switch result.Proxy.Type {
			case HTTP:
				httpCount++
			case SOCKS4:
				socks4Count++
			case SOCKS5:
				socks5Count++
			}
		}

		fmt.Printf("\n   📊 Working proxies by type:\n")
		if httpCount > 0 {
			fmt.Printf("      HTTP:   %d\n", httpCount)
		}
		if socks4Count > 0 {
			fmt.Printf("      SOCKS4: %d\n", socks4Count)
		}
		if socks5Count > 0 {
			fmt.Printf("      SOCKS5: %d\n", socks5Count)
		}
	}
}

func saveResults(workingProxies []TestResult, failedProxies []TestResult) {
	if len(workingProxies) > 0 {
		// Save detailed results
		file, _ := os.Create("working_proxies_sorted.txt")
		if file != nil {
			defer file.Close()
			fmt.Fprintf(file, "# Sorted Working Proxies (by ping - fastest first)\n")
			fmt.Fprintf(file, "# Generated: %s\n", time.Now().Format("2006-01-02 15:04:05"))
			fmt.Fprintf(file, "# Total working: %d\n", len(workingProxies))
			fmt.Fprintf(file, "#"+strings.Repeat("-", 70)+"\n\n")

			for i, result := range workingProxies {
				fmt.Fprintf(file, "#%-3d | %-8s | %-21s | %dms\n",
					i+1, result.Proxy.Type, result.Proxy.Addr, result.Latency.Milliseconds())
				fmt.Fprintf(file, "%s://%s\n", result.Proxy.Type, result.Proxy.Addr)
			}
		}

		// Save CSV
		csvFile, _ := os.Create("working_proxies_detail.csv")
		if csvFile != nil {
			defer csvFile.Close()
			fmt.Fprintf(csvFile, "Rank,Type,Proxy,Latency(ms),Latency\n")
			for i, result := range workingProxies {
				fmt.Fprintf(csvFile, "%d,%s,%s,%d,%s\n",
					i+1, result.Proxy.Type, result.Proxy.Addr,
					result.Latency.Milliseconds(), result.Latency)
			}
		}

		// Save with timestamps
		timestampFile, _ := os.Create("working_proxies.txt")
		if timestampFile != nil {
			defer timestampFile.Close()
			timestamp := time.Now().Format("2006-01-02 15:04:05")
			for i, result := range workingProxies {
				fmt.Fprintf(timestampFile, "[%s] #%d %s://%s | Latency: %dms\n",
					timestamp, i+1, result.Proxy.Type, result.Proxy.Addr, result.Latency.Milliseconds())
			}
		}

		fmt.Printf("\n✅ Results saved:\n")
		fmt.Printf("   📄 working_proxies.txt (with timestamps)\n")
		fmt.Printf("   📊 working_proxies_sorted.txt (sorted by ping)\n")
		fmt.Printf("   📈 working_proxies_detail.csv (for Excel)\n")
	}

	if len(failedProxies) > 0 {
		file, _ := os.Create("failed_proxies.txt")
		if file != nil {
			defer file.Close()
			fmt.Fprintf(file, "# Failed Proxies\n")
			fmt.Fprintf(file, "# Generated: %s\n", time.Now().Format("2006-01-02 15:04:05"))
			fmt.Fprintf(file, "# Total failed: %d\n", len(failedProxies))
			fmt.Fprintf(file, "#"+strings.Repeat("-", 70)+"\n\n")

			for _, result := range failedProxies {
				fmt.Fprintf(file, "%s://%s | Error: %s | Latency: %dms\n",
					result.Proxy.Type, result.Proxy.Addr, result.Error, result.Latency.Milliseconds())
			}
			fmt.Printf("   📝 failed_proxies.txt\n")
		}
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
