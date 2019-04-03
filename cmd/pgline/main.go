package main

import (
	"fmt"
	"github.com/jsteenb2/pgkons/internal/postgres"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jsteenb2/pgkons/internal/runner"

	"github.com/chzyer/readline"
)

func usage(completer *readline.PrefixCompleter, w io.Writer) {
	io.WriteString(w, "commands:\n")
	io.WriteString(w, completer.Tree("  "))
}

func filterInput(r rune) (rune, bool) {
	switch r {
	// block CtrlZ feature
	case readline.CharCtrlZ:
		return r, false
	}
	return r, true
}

type te
rmRunner struct {
	*postgres.Client
}

func main() {
	cfgs, err := runner.LoadConfigs()
	if err != nil {
		panic(err)
	}
	cfgNames := func(string) []string {
		var cc []string
		for _, c := range cfgs {
			cc = append(cc, c.Name)
		}
		return cc
	}

	var completer = readline.NewPrefixCompleter(
		readline.PcItem("stats",
			readline.PcItem("schemas"),
			readline.PcItem("tables"),
			readline.PcItem("views"),
			readline.PcItem("summary"),
		),
		readline.PcItem("profile",
			readline.PcItem("new"),
			readline.PcItem("use",
				readline.PcItemDynamic(cfgNames),
			),
			//readline.PcItem("remove"),
		),
	)

	l, err := readline.NewEx(&readline.Config{
		Prompt:              "\033[32mpgkons\033[0m \033[31m»\033[0m ",
		HistoryFile:         "/tmp/readline.tmp",
		AutoComplete:        completer,
		InterruptPrompt:     "^C",
		EOFPrompt:           "exit",
		HistorySearchFold:   true,
		FuncFilterInputRune: filterInput,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	setPasswordCfg := l.GenPasswordConfig()
	setPasswordCfg.SetListener(func(line []rune, pos int, key rune) (newLine []rune, newPos int, ok bool) {
		l.SetPrompt(fmt.Sprintf("Enter password(%v): ", len(line)))
		l.Refresh()
		return nil, 0, false
	})

	log.SetOutput(l.Stderr())
	for {
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue
			}
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "mode "):
			switch line[5:] {
			case "vi":
				l.SetVimMode(true)
			case "emacs":
				l.SetVimMode(false)
			default:
				println("invalid mode:", line[5:])
			}
		case line == "mode":
			if l.IsVimMode() {
				println("current mode: vim")
			} else {
				println("current mode: emacs")
			}
		case line == "login":
			pswd, err := l.ReadPassword("please enter your password: ")
			if err != nil {
				break
			}
			println("you enter:", strconv.Quote(string(pswd)))
		case line == "help":
			usage(completer, l.Stderr())
		case line == "setpassword":
			pswd, err := l.ReadPasswordWithConfig(setPasswordCfg)
			if err == nil {
				println("you set:", strconv.Quote(string(pswd)))
			}
		case strings.HasPrefix(line, "setprompt"):
			if len(line) <= 10 {
				log.Println("setprompt <prompt>")
				break
			}
			l.SetPrompt(line[10:])
		case strings.HasPrefix(line, "say"):
			line := strings.TrimSpace(line[3:])
			if len(line) == 0 {
				log.Println("say what?")
				break
			}
			go func() {
				for range time.Tick(time.Second) {
					log.Println(line)
				}
			}()
		case line == "bye":
			goto exit
		case line == "sleep":
			log.Println("sleep 4 second")
			time.Sleep(4 * time.Second)
		case line == "":
		default:
			log.Println("you said:", strconv.Quote(line))
		}
	}
exit:
}

func
