package mate

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// mqReconnectLogInterval limits logs on reconnect / dial failure storms (disk safety).
const mqReconnectLogInterval = 60 * time.Second

type logThrottle struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func (t *logThrottle) allow(key string, minInterval time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last == nil {
		t.last = make(map[string]time.Time)
	}
	now := time.Now()
	if prev, ok := t.last[key]; ok && now.Sub(prev) < minInterval {
		return false
	}
	t.last[key] = now
	return true
}

type Logger interface {
	Info(...interface{})
	Error(...interface{})
}

type ConsoleOutput struct {
}

func (co *ConsoleOutput) Info(contents ...interface{}) {
	co.output("INFO", fmt.Sprintln(contents...))
}

func (co *ConsoleOutput) Error(contents ...interface{}) {
	co.output("ERROR", fmt.Sprintln(contents...))
}

func (co *ConsoleOutput) output(level string, content string) {
	var body strings.Builder
	body.WriteString(fmt.Sprintf("[%s][%s][QueueMate] ", time.Now().Format("2006-01-02 15:04:05"), level))
	body.WriteString(content)
	fmt.Println(body.String())
}
