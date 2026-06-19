# type = http, socks4, socks5, all
go run proxy_tester.go -type=http -concurrency=500 --timeout=10

# Test with custom URL
go run proxy_tester.go -type=http -url="http://api.ipify.org"
