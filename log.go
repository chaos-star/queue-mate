package mate

import (
	"fmt"
	"strings"
	"time"
)

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
