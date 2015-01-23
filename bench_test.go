package main

import (
	"github.com/jackc/pgx"
	"strconv"
	"testing"
	"time"
)

func mustConnect(t testing.TB, connConfig pgx.ConnConfig) *pgx.Conn {
	conn, err := pgx.Connect(connConfig)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func mustPrepare(t testing.TB, conn *pgx.Conn, name, sql string) *pgx.PreparedStatement {
	ps, err := conn.Prepare(name, sql)
	if err != nil {
		t.Fatal(err)
	}

	return ps
}

func mustExec(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (commandTag pgx.CommandTag) {
	var err error
	if commandTag, err = conn.Exec(sql, arguments...); err != nil {
		t.Fatalf("Exec unexpectedly failed with %v: %v", sql, err)
	}

	return commandTag
}

func BenchmarkManualSequentialListenNotify(b *testing.B) {
	connConfig := extractConfig()
	notifier := mustConnect(b, connConfig)
	defer notifier.Close()
	listener := mustConnect(b, connConfig)
	defer listener.Close()

	err := listener.Listen("bench")
	if err != nil {
		b.Fatal(err)
	}

	mustPrepare(b, notifier, "notify", "select pg_notify($1, $2)")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mustExec(b, notifier, "notify", "bench", strconv.FormatInt(int64(i), 10))

		n, err := listener.WaitForNotification(time.Second)
		if err != nil {
			b.Fatal(err)
		}

		j, err := strconv.ParseInt(n.Payload, 10, 64)
		if err != nil {
			b.Fatal(err)
		}

		if n.Channel != "bench" || i != int(j) {
			b.Fatal("Did not receive expected notification")
		}
	}
}

func BenchmarkManualParallelListenNotify(b *testing.B) {
	connConfig := extractConfig()
	notifier := mustConnect(b, connConfig)
	defer notifier.Close()
	listener := mustConnect(b, connConfig)
	defer listener.Close()

	err := listener.Listen("bench")
	if err != nil {
		b.Fatal(err)
	}

	mustPrepare(b, notifier, "notify", "select pg_notify($1, $2)")

	doneChan := make(chan bool)

	b.ResetTimer()

	go func() {
		defer func() { doneChan <- true }()
		for i := 0; i < b.N; i++ {
			n, err := listener.WaitForNotification(time.Second)
			if err != nil {
				b.Fatal(err)
			}

			j, err := strconv.ParseInt(n.Payload, 10, 64)
			if err != nil {
				b.Fatal(err)
			}

			if n.Channel != "bench" || i != int(j) {
				b.Fatal("Did not receive expected notification")
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		mustExec(b, notifier, "notify", "bench", strconv.FormatInt(int64(i), 10))
	}

	<-doneChan
}

func BenchmarkSingleInsertWithoutNotify(b *testing.B) {
	connConfig := extractConfig()
	conn := mustConnect(b, connConfig)
	defer conn.Close()

	mustExec(b, conn, "drop table if exists notify_bench")
	mustExec(b, conn, "create table notify_bench(id serial primary key)")

	mustPrepare(b, conn, "insertBench", "insert into notify_bench(id) values($1)")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mustExec(b, conn, "insertBench", int32(i))
	}
}

func BenchmarkSingleInsertWithTriggeredNotify(b *testing.B) {
	connConfig := extractConfig()
	inserter := mustConnect(b, connConfig)
	defer inserter.Close()
	mustExec(b, inserter, "drop table if exists notify_bench")
	mustExec(b, inserter, "create table notify_bench(id serial primary key, n integer)")
	mustExec(b, inserter, `
CREATE OR REPLACE FUNCTION insert_notifier() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('bench', new.n::text);
    return new;
  end;
$$;
  `)
	mustExec(b, inserter, "CREATE TRIGGER insert_notifier AFTER INSERT ON notify_bench FOR EACH ROW EXECUTE PROCEDURE insert_notifier()")

	mustPrepare(b, inserter, "insertNotifyBench", "insert into notify_bench(n) values($1)")

	listener := mustConnect(b, connConfig)
	err := listener.Listen("bench")
	if err != nil {
		b.Fatal(err)
	}

	doneChan := make(chan bool)

	b.ResetTimer()

	go func() {
		defer func() { doneChan <- true }()
		for i := 0; i < b.N; i++ {
			n, err := listener.WaitForNotification(time.Second)
			if err != nil {
				b.Fatal(err)
			}

			_, err = strconv.ParseInt(n.Payload, 10, 64)
			if err != nil {
				b.Fatal(err)
			}

			if n.Channel != "bench" {
				b.Fatal("Did not receive expected notification")
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		mustExec(b, inserter, "insertNotifyBench", int32(i))
	}

	<-doneChan
}

func BenchmarkMultipleInsertWithNotify(b *testing.B) {
	connConfig := extractConfig()
	inserter := mustConnect(b, connConfig)
	defer inserter.Close()
	mustExec(b, inserter, "drop table if exists notify_bench")
	mustExec(b, inserter, "create table notify_bench(id serial primary key, n integer)")
	mustExec(b, inserter, `
CREATE OR REPLACE FUNCTION insert_notifier() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  begin
    perform pg_notify('bench', new.n::text);
    return new;
  end;
$$;
  `)
	mustExec(b, inserter, "CREATE TRIGGER insert_notifier AFTER INSERT ON notify_bench FOR EACH ROW EXECUTE PROCEDURE insert_notifier()")

	mustPrepare(b, inserter, "insertMultipleNotifyBench", "insert into notify_bench(n) select generate_series(1,$1)")

	listener := mustConnect(b, connConfig)
	err := listener.Listen("bench")
	if err != nil {
		b.Fatal(err)
	}

	doneChan := make(chan bool)

	b.ResetTimer()

	go func() {
		defer func() { doneChan <- true }()

		expectedCount := 10 * b.N
		for i := 0; i < expectedCount; i++ {
			n, err := listener.WaitForNotification(time.Second)
			if err != nil {
				b.Fatal(err)
			}

			_, err = strconv.ParseInt(n.Payload, 10, 64)
			if err != nil {
				b.Fatal(err)
			}

			if n.Channel != "bench" {
				b.Fatal("Did not receive expected notification")
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		mustExec(b, inserter, "insertMultipleNotifyBench", 10)
	}

	<-doneChan
}
