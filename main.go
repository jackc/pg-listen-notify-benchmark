package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"os"
	"time"
)

func main() {
	connConfig := extractConfig()

	notifier, err := pgx.Connect(connConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgx.Connect failed:", err)
		os.Exit(1)
	}

	listener, err := pgx.Connect(connConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgx.Connect failed:", err)
		os.Exit(1)
	}

	err = listener.Listen("bench")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Listen failed:", err)
		os.Exit(1)
	}

	_, err = notifier.Prepare("notify", "select pg_notify($1, $2)")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Prepare failed:", err)
		os.Exit(1)
	}

	_, err = notifier.Exec("notify", "bench", "hello")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Listen failed:", err)
		os.Exit(1)
	}

	n, err := listener.WaitForNotification(time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Did not receive notification", err)
		os.Exit(1)
	}

	fmt.Println(n)
}

func extractConfig() pgx.ConnConfig {
	var config pgx.ConnConfig

	config.Host = os.Getenv("PG_HOST")
	if config.Host == "" {
		config.Host = "localhost"
	}

	config.User = os.Getenv("PG_USER")
	if config.User == "" {
		config.User = os.Getenv("USER")
	}

	config.Password = os.Getenv("PG_PASSWORD")

	config.Database = os.Getenv("PG_DATABASE")
	if config.Database == "" {
		config.Database = config.User
	}

	return config
}
