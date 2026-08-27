package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("cpa-session-archive-backup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	source := flags.String("source", "", "active SQLite archive database")
	destination := flags.String("destination", "", "new backup database")
	timeout := flags.Duration("timeout", 2*time.Minute, "online backup timeout")
	retryInterval := flags.Duration("retry-interval", 25*time.Millisecond, "busy retry interval")
	pagesPerStep := flags.Int("pages-per-step", 1024, "SQLite pages copied between deadline checks (maximum 4096)")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		fmt.Fprintln(stderr, publicFailure(errInvalidArguments))
		return 2
	}
	result, err := createOnlineBackup(context.Background(), backupOptions{
		Source:        *source,
		Destination:   *destination,
		Timeout:       *timeout,
		RetryInterval: *retryInterval,
		PagesPerStep:  *pagesPerStep,
	})
	if err != nil {
		fmt.Fprintln(stderr, publicFailure(err))
		return 1
	}
	if err = encodeResult(stdout, result); err != nil {
		fmt.Fprintln(stderr, publicFailure(errBackupVerification))
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
