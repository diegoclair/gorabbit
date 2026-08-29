// Package scenario is the machinery of the validation log: it starts real
// processes, drives them over HTTP and prints one line per claim with the number
// it measured, so a run can be read step by step instead of at the end.
package scenario

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/diegoclair/gorabbit/e2e/internal/mgmt"
	"github.com/diegoclair/gorabbit/e2e/internal/rediscache"
)

type Env struct {
	AMQPURL   string
	RedisAddr string
	BinDir    string
	LogDir    string
	Mgmt      *mgmt.Client
	Cache     *rediscache.Client
	Keep      bool
}

type Scenario struct {
	Name  string
	Title string
	Body  func(*Run)
}

type Outcome string

const (
	OutcomePassed   Outcome = "PASS"
	OutcomeFailed   Outcome = "FAIL"
	OutcomeObserved Outcome = "OBSERVED"
)

type Result struct {
	Name         string
	Outcome      Outcome
	Steps        int
	Observations int
	Failures     []string
	Unmeasured   []string
}

type Run struct {
	Env *Env

	name   string
	out    io.Writer
	number int
	result Result
	procs  []*Proc
}

func NewRun(env *Env, out io.Writer, s Scenario, index, total int) *Run {
	fmt.Fprintf(out, "\n%s\n", divider)
	fmt.Fprintf(out, "SCENARIO %d/%d  %s — %s\n", index, total, s.Name, s.Title)
	fmt.Fprintf(out, "%s\n", divider)

	return &Run{
		Env:    env,
		name:   s.Name,
		out:    out,
		result: Result{Name: s.Name, Outcome: OutcomePassed},
	}
}

const divider = "────────────────────────────────────────────────────────────────────────────"

// Once a claim has failed, the claims after it would only measure the wreckage.
func (r *Run) Step(assertion string, probe func() (string, error)) {
	r.number++
	r.result.Steps++

	if r.result.Outcome == OutcomeFailed {
		r.print("SKIP", assertion, "not run: an earlier step of this scenario failed")
		r.result.Unmeasured = append(r.result.Unmeasured, assertion)
		return
	}

	observed, err := probe()
	if err != nil {
		r.result.Outcome = OutcomeFailed
		r.result.Failures = append(r.result.Failures, fmt.Sprintf("%s: %v", assertion, err))
		r.print("FAIL", assertion, err.Error())
		return
	}

	r.print("PASS", assertion, observed)
}

// Observe answers a question the harness states no verdict about: what it
// measures is information, not a guarantee the library owes.
func (r *Run) Observe(question string, probe func() (string, error)) {
	r.number++
	r.result.Observations++

	if r.result.Outcome == OutcomeFailed {
		r.print("SKIP", question, "not measured: an earlier step of this scenario failed")
		r.result.Unmeasured = append(r.result.Unmeasured, question)
		return
	}

	measured, err := probe()
	if err != nil {
		r.result.Unmeasured = append(r.result.Unmeasured, fmt.Sprintf("%s: %v", question, err))
		r.print("OBS?", question, "could not measure: "+err.Error())
		return
	}

	r.print("OBS", question, measured)
}

func (r *Run) Note(text string) {
	fmt.Fprintf(r.out, "      NOTE  %s\n", text)
}

func (r *Run) print(verdict, claim, observed string) {
	fmt.Fprintf(r.out, "  [%02d] %-5s %s\n", r.number, verdict, claim)
	fmt.Fprintf(r.out, "            %s\n", observed)
}

// A script that asserts anything has a verdict; OBSERVED belongs to one that
// only measures.
func (r *Run) Result() Result {
	switch {
	case len(r.result.Failures) > 0 || len(r.result.Unmeasured) > 0:
		r.result.Outcome = OutcomeFailed
	case r.result.Steps > 0:
		r.result.Outcome = OutcomePassed
	default:
		r.result.Outcome = OutcomeObserved
	}

	return r.result
}

// Reset gives the scenario a broker and a cache with nothing in them, which is
// what lets one scenario run alone and mean the same as in a full pass.
func (r *Run) Reset() error {
	if err := r.Env.Mgmt.ResetVhost(ctx()); err != nil {
		return err
	}

	return r.Env.Cache.FlushAll(ctx())
}

func (r *Run) Publisher(name, appName string, extra ...string) (*Proc, error) {
	args := []string{
		"-amqp", r.Env.AMQPURL,
		"-redis", r.Env.RedisAddr,
		"-app", appName,
	}

	return r.start(name, "publisher", append(args, extra...))
}

func (r *Run) Consumer(name, appName, queue string, extra ...string) (*Proc, error) {
	args := []string{
		"-amqp", r.Env.AMQPURL,
		"-redis", r.Env.RedisAddr,
		"-app", appName,
		"-queue", queue,
	}

	return r.start(name, "consumer", append(args, extra...))
}

func (r *Run) start(name, binary string, args []string) (*Proc, error) {
	p, err := startProc(name, filepath.Join(r.Env.LogDir, r.name), filepath.Join(r.Env.BinDir, binary), args)
	if err != nil {
		return nil, err
	}

	r.procs = append(r.procs, p)

	return p, nil
}

// Cleanup leaves the processes alive in keep mode, which is the point of it:
// the operator gets an environment to poke at with the addresses printed.
func (r *Run) Cleanup() {
	if r.Env.Keep {
		fmt.Fprintf(r.out, "\n  kept alive for manual poking:\n")
		for _, p := range r.procs {
			fmt.Fprintf(r.out, "    %-12s %s   (log: %s)\n", p.Name, p.BaseURL, p.LogPath)
		}
		return
	}

	for _, p := range r.procs {
		if err := p.Stop(); err != nil {
			fmt.Fprintf(r.out, "      NOTE  could not stop %s: %v\n", p.Name, err)
		}
	}
}

// Nothing here synchronises by sleeping: a wait that runs out has to say what
// it wanted and what it last saw.
func WaitFor(what string, timeout time.Duration, probe func() (bool, string, error)) (string, error) {
	deadline := time.Now().Add(timeout)
	observed := "nothing yet"
	var lastErr error

	for {
		ok, current, err := probe()
		if err != nil {
			lastErr = err
		} else {
			observed, lastErr = current, nil
			if ok {
				return observed, nil
			}
		}

		if time.Now().After(deadline) {
			if lastErr != nil {
				return observed, fmt.Errorf("timed out after %s waiting for %s; last error: %w", timeout, what, lastErr)
			}
			return observed, fmt.Errorf("timed out after %s waiting for %s; last observed: %s", timeout, what, observed)
		}

		time.Sleep(150 * time.Millisecond)
	}
}

// WaitStable waits for a measurement to stop moving, which is how a claim about
// something not happening gets a defined moment to be made at.
func WaitStable(what string, quiet, timeout time.Duration, sample func() (string, error)) (string, error) {
	deadline := time.Now().Add(timeout)
	previous := ""
	since := time.Now()

	for {
		current, err := sample()
		if err != nil {
			return previous, fmt.Errorf("sampling %s: %w", what, err)
		}

		if current != previous {
			previous, since = current, time.Now()
		} else if time.Since(since) >= quiet {
			return current, nil
		}

		if time.Now().After(deadline) {
			return previous, fmt.Errorf("%s never settled within %s; last observed: %s", what, timeout, previous)
		}

		time.Sleep(200 * time.Millisecond)
	}
}
