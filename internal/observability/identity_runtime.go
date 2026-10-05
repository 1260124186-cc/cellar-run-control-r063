package observability

import "os"

func processID() int {
	return os.Getpid()
}

func hostName() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return host
}
