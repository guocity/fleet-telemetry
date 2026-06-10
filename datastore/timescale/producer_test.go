package timescale

// Integration test against a real TimescaleDB. Skipped unless
// TIMESCALE_TEST_DSN is set, e.g.
//
//	TIMESCALE_TEST_DSN=postgresql://postgres:postgres@192.168.18.2:5432 \
//	  go test ./datastore/timescale/
//
// The test creates (and drops) its own database fleet_telemetry_test and
// applies new_implementation/schema.sql, so production data is untouched.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/protos"
	"github.com/teslamotors/fleet-telemetry/telemetry"
)

const testDB = "fleet_telemetry_test"
const testVin = "5YJ3E1EA0TEST0001"

func setupTestDB(t *testing.T, serverDSN string) string {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, strings.TrimRight(serverDSN, "/")+"/template1")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)
	if _, err = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", testDB)); err != nil {
		t.Fatalf("drop test db: %v", err)
	}
	if _, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatalf("create test db: %v", err)
	}

	dsn := strings.TrimRight(serverDSN, "/") + "/" + testDB
	schema, err := os.ReadFile(filepath.Join("..", "..", "new_implementation", "schema.sql"))
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect test db: %v", err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return dsn
}

func vPayload(createdAt time.Time, speed float64, gear protos.ShiftState) *protos.Payload {
	return &protos.Payload{
		Vin:       testVin,
		CreatedAt: timestamppb.New(createdAt),
		Data: []*protos.Datum{
			{Key: protos.Field_VehicleSpeed,
				Value: &protos.Value{Value: &protos.Value_DoubleValue{DoubleValue: speed}}},
			{Key: protos.Field_Gear,
				Value: &protos.Value{Value: &protos.Value_ShiftStateValue{ShiftStateValue: gear}}},
			{Key: protos.Field_Location,
				Value: &protos.Value{Value: &protos.Value_LocationValue{
					LocationValue: &protos.LocationValue{Latitude: 40.85, Longitude: -73.97}}}},
			{Key: protos.Field_ChargeAmps,
				Value: &protos.Value{Value: &protos.Value_Invalid{Invalid: true}}},
		},
	}
}

func count(t *testing.T, p *Producer, q string) int {
	var n int
	if err := p.pool.QueryRow(context.Background(), q).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", q, err)
	}
	return n
}

func TestProducerIntegration(t *testing.T) {
	serverDSN := os.Getenv("TIMESCALE_TEST_DSN")
	if serverDSN == "" {
		t.Skip("TIMESCALE_TEST_DSN not set")
	}
	dsn := setupTestDB(t, serverDSN)
	ctx := context.Background()
	logger, _ := logrus.NoOpLogger()

	prod, err := NewProducer(ctx, &Config{DSN: dsn, BatchSize: 1000, FlushIntervalMS: 60000}, logger)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	p := prod.(*Producer)
	entry := &telemetry.Record{Vin: testVin, TxType: "V", DeviceClientVersion: "1.2.0"}
	t0 := time.Now().UTC().Truncate(time.Second)

	// ── change-only signal storage ────────────────────────────────
	if err := p.produceSignals(ctx, entry, vPayload(t0, 0, protos.ShiftState_ShiftStateP)); err != nil {
		t.Fatalf("produceSignals 1: %v", err)
	}
	// identical resend, later ts → all deduped
	if err := p.produceSignals(ctx, entry, vPayload(t0.Add(time.Second), 0, protos.ShiftState_ShiftStateP)); err != nil {
		t.Fatalf("produceSignals 2: %v", err)
	}
	// speed + gear change, location/invalid unchanged → 2 new rows
	if err := p.produceSignals(ctx, entry, vPayload(t0.Add(2*time.Second), 25.5, protos.ShiftState_ShiftStateD)); err != nil {
		t.Fatalf("produceSignals 3: %v", err)
	}
	p.mu.Lock()
	err = p.flushLocked(ctx)
	p.mu.Unlock()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := count(t, p, "SELECT count(*) FROM signal_changes"); got != 6 {
		t.Errorf("signal_changes rows = %d, want 6 (4 initial + 2 changes)", got)
	}
	if got := count(t, p, "SELECT count(*) FROM signal_latest"); got != 4 {
		t.Errorf("signal_latest rows = %d, want 4", got)
	}
	var speed float64
	if err := p.pool.QueryRow(ctx,
		`SELECT l.v_num FROM signal_latest l JOIN signals s USING (signal_id)
		 WHERE s.name = 'VehicleSpeed'`).Scan(&speed); err != nil || speed != 25.5 {
		t.Errorf("latest VehicleSpeed = %v (err %v), want 25.5", speed, err)
	}

	// restart: state must reload from DB and still dedup correctly
	if err := prod.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	prod, err = NewProducer(ctx, &Config{DSN: dsn, BatchSize: 1000, FlushIntervalMS: 60000}, logger)
	if err != nil {
		t.Fatalf("NewProducer restart: %v", err)
	}
	p = prod.(*Producer)
	if err := p.produceSignals(ctx, entry, vPayload(t0.Add(3*time.Second), 25.5, protos.ShiftState_ShiftStateD)); err != nil {
		t.Fatalf("produceSignals 4: %v", err)
	}
	p.mu.Lock()
	err = p.flushLocked(ctx)
	p.mu.Unlock()
	if err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	if got := count(t, p, "SELECT count(*) FROM signal_changes"); got != 6 {
		t.Errorf("after restart replay: signal_changes = %d, want still 6", got)
	}

	// ── alert episodes ────────────────────────────────────────────
	started := timestamppb.New(t0)
	alertsOpen := &protos.VehicleAlerts{Vin: testVin, Alerts: []*protos.VehicleAlert{
		{Name: "DI_a183_holdReleaseRqrd", StartedAt: started,
			Audiences: []protos.Audience{protos.Audience_Customer, protos.Audience_Service}},
	}}
	alertsClosed := &protos.VehicleAlerts{Vin: testVin, Alerts: []*protos.VehicleAlert{
		{Name: "DI_a183_holdReleaseRqrd", StartedAt: started, EndedAt: timestamppb.New(t0.Add(5 * time.Second)),
			Audiences: []protos.Audience{protos.Audience_Service, protos.Audience_Customer}},
	}}
	for i, a := range []*protos.VehicleAlerts{alertsOpen, alertsClosed, alertsClosed} {
		if err := p.produceAlerts(ctx, entry, a); err != nil {
			t.Fatalf("produceAlerts %d: %v", i, err)
		}
	}
	if got := count(t, p, "SELECT count(*) FROM alert_episodes"); got != 1 {
		t.Errorf("alert_episodes = %d, want 1", got)
	}
	if got := count(t, p, "SELECT count(*) FROM alert_episodes WHERE ended_at IS NOT NULL"); got != 1 {
		t.Errorf("closed episodes = %d, want 1 (ended_at must be filled by re-send)", got)
	}

	// ── connectivity & errors: natural-key dedup ──────────────────
	conn := &protos.VehicleConnectivity{Vin: testVin, ConnectionId: "11111111-2222-3333-4444-555555555555",
		Status: protos.ConnectivityEvent_CONNECTED, NetworkInterface: "wifi", CreatedAt: timestamppb.New(t0)}
	for i := 0; i < 2; i++ {
		if err := p.produceConnectivity(ctx, entry, conn); err != nil {
			t.Fatalf("produceConnectivity: %v", err)
		}
	}
	if got := count(t, p, "SELECT count(*) FROM connectivity_events"); got != 1 {
		t.Errorf("connectivity_events = %d, want 1", got)
	}

	verr := &protos.VehicleErrors{Vin: testVin, Errors: []*protos.VehicleError{
		{Name: "unsupported_field", CreatedAt: timestamppb.New(t0),
			Tags: map[string]string{"field_name": "ScheduledDepartureTime"}},
	}}
	for i := 0; i < 2; i++ {
		if err := p.produceErrors(ctx, entry, verr); err != nil {
			t.Fatalf("produceErrors: %v", err)
		}
	}
	if got := count(t, p, "SELECT count(*) FROM error_events"); got != 1 {
		t.Errorf("error_events = %d, want 1", got)
	}

	// device_client_version captured from the record
	var ver string
	if err := p.pool.QueryRow(ctx,
		"SELECT device_client_version FROM vehicles WHERE vin = $1", testVin).Scan(&ver); err != nil || ver != "1.2.0" {
		t.Errorf("device_client_version = %q (err %v), want 1.2.0", ver, err)
	}

	if err := prod.Close(); err != nil {
		t.Fatalf("final close: %v", err)
	}
}
