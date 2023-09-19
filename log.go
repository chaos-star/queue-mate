package mate

import (
	"fmt"
	"strings"
	"time"
)

type Logger interface {
	Info(string)
	Error(string)
}

type ConsoleOutput struct {
}

func (co *ConsoleOutput) Info(content string) {
	co.output("INFO", content)
}

func (co *ConsoleOutput) Error(content string) {
	co.output("ERROR", content)
}

func (co *ConsoleOutput) output(level string, content string) {
	var body strings.Builder
	body.WriteString(fmt.Sprintf("[%s][%s][QueueMate] ", time.Now().Format("2006-01-02 15:04:05"), level))
	body.WriteString(content)
	fmt.Println(body.String())
}
