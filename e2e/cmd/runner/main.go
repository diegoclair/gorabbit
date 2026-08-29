// Command runner executes the validation scripts step by step: every step says
// what it asserted and the number it measured, and what the harness only
// measures is printed as an observation instead of a verdict.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/diegoclair/gorabbit/e2e/internal/mgmt"
	"github.com/diegoclair/gorabbit/e2e/internal/rediscache"
	"github.com/diegoclair/gorabbit/e2e/internal/scenario"
)

func main() {
	amqpHost := flag.String("amqp-host", "127.0.0.1:5673", "host:port of the broker's amqp listener")
	mgmtURL := flag.String("mgmt", "http://127.0.0.1:15673", "base url of the broker's management api")
	redisAddr := flag.String("redis", "127.0.0.1:6380", "host:port of redis")
	user := flag.String("user", "guest", "broker user")
	password := flag.String("password", "guest", "broker password")
	vhost := flag.String("vhost", "e2e", "vhost the harness owns and resets between scripts")
	binDir := flag.String("bin", "bin", "directory holding the built application binaries")
	logDir := flag.String("logs", ".logs", "directory the per-process logs are written to")
	only := flag.String("scenario", "all", "comma separated script names, or all")
	keep := flag.Bool("keep", false, "leave the processes of the last script running for manual poking")
	list := flag.Bool("list", false, "print the script names and exit")
	flag.Parse()

	if *list {
		for _, s := range scenarios() {
			fmt.Printf("%-14s %s\n", s.Name, s.Title)
		}
		return
	}

	selected, err := selectScenarios(*only)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	amqpURL := fmt.Sprintf("amqp://%s:%s@%s/%s", url.QueryEscape(*user), url.QueryEscape(*password), *amqpHost, url.PathEscape(*vhost))

	env := &scenario.Env{
		AMQPURL:   amqpURL,
		RedisAddr: *redisAddr,
		BinDir:    *binDir,
		LogDir:    *logDir,
		Mgmt:      mgmt.New(*mgmtURL, *user, *password, *vhost),
		Cache:     rediscache.New(*redisAddr),
		Keep:      *keep,
	}

	if err := waitForInfrastructure(env); err != nil {
		fmt.Fprintf(os.Stderr, "the harness could not reach its infrastructure, so no script ran: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("broker %s (management %s), redis %s, binaries in %s, process logs in %s\n",
		*amqpHost, *mgmtURL, *redisAddr, *binDir, *logDir)

	os.Exit(runAll(env, selected))
}

func waitForInfrastructure(env *scenario.Env) error {
	ctx := context.Background()

	if _, err := scenario.WaitFor("the management api to answer", 60*time.Second, func() (bool, string, error) {
		if err := env.Mgmt.Ready(ctx); err != nil {
			return false, "not answering yet", err
		}
		return true, "answering", nil
	}); err != nil {
		return err
	}

	_, err := scenario.WaitFor("redis to answer", 30*time.Second, func() (bool, string, error) {
		if err := env.Cache.Ping(ctx); err != nil {
			return false, "not answering yet", err
		}
		return true, "answering", nil
	})

	return err
}

func runAll(env *scenario.Env, selected []scenario.Scenario) int {
	results := make([]scenario.Result, 0, len(selected))

	for i, s := range selected {
		run := scenario.NewRun(env, os.Stdout, s, i+1, len(selected))

		if err := run.Reset(); err != nil {
			run.Step("the broker and the cache start this script empty", func() (string, error) {
				return "", err
			})
		} else {
			s.Body(run)
		}

		if env.Keep && i == len(selected)-1 {
			run.Cleanup()
		} else {
			keep := env.Keep
			env.Keep = false
			run.Cleanup()
			env.Keep = keep
		}

		results = append(results, run.Result())
	}

	return summarize(results)
}

func summarize(results []scenario.Result) int {
	fmt.Printf("\n%s\nSUMMARY\n%s\n", divider, divider)

	exitCode := 0
	for _, r := range results {
		fmt.Printf("  %-9s %-14s %d steps, %d observations\n", r.Outcome, r.Name, r.Steps, r.Observations)

		for _, failure := range r.Failures {
			fmt.Printf("            failed: %s\n", failure)
			exitCode = 1
		}
		for _, missing := range r.Unmeasured {
			fmt.Printf("            did not run: %s\n", missing)
			exitCode = 1
		}
	}

	if exitCode == 0 {
		fmt.Printf("\nevery assertion held and every observation was measured\n")
	} else {
		fmt.Printf("\nsomething did not hold or could not be measured — the lines above say which\n")
	}

	return exitCode
}

const divider = "────────────────────────────────────────────────────────────────────────────"

func selectScenarios(only string) ([]scenario.Scenario, error) {
	all := scenarios()
	if only == "all" {
		return all, nil
	}

	byName := map[string]scenario.Scenario{}
	for _, s := range all {
		byName[s.Name] = s
	}

	var selected []scenario.Scenario
	for _, name := range strings.Split(only, ",") {
		name = strings.TrimSpace(name)
		s, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("unknown script %q; -list prints the names", name)
		}
		selected = append(selected, s)
	}

	return selected, nil
}
