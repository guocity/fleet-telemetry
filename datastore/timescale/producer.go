// Package timescale implements a telemetry.Producer that writes vehicle
// telemetry directly to TimescaleDB, replacing the temporary JSONL
// filewriter. Schema: new_implementation/schema.sql.
//
// Storage is change-only: a signal row is inserted into signal_changes only
// when the value differs from the previous value for that (vehicle, signal),
// and signal_latest always holds the current state. Alerts are upserted as
// episodes keyed by (vehicle, alert, started_at); connectivity and errors
// insert on natural keys with ON CONFLICT DO NOTHING, so retransmissions
// never create duplicate rows.
package timescale

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/teslamotors/fleet-telemetry/datastore/simple/transformers"
	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/protos"
	"github.com/teslamotors/fleet-telemetry/telemetry"
)

// Config for the TimescaleDB producer
type Config struct {
	// DSN of the fleet_telemetry database,
	// e.g. postgresql://postgres:postgres@192.168.18.2:5432/fleet_telemetry
	DSN string `json:"dsn"`
	// BatchSize flushes buffered signal rows when reached (default 500)
	BatchSize int `json:"batch_size"`
	// FlushIntervalMS flushes buffered signal rows at least this often (default 2000)
	FlushIntervalMS int `json:"flush_interval_ms"`
}

type sigKey struct {
	vehicleID int16
	signalID  int16
}

// signalRow mirrors the signal_changes columns; valueKey is the canonical
// comparison form used for change detection.
type signalRow struct {
	ts        time.Time
	vehicleID int16
	signalID  int16
	vNum      *float64
	vLong     *int64
	vBool     *bool
	vText     *string
	vLoc      pgtype.Point
	vJSON     *string
	invalid   bool
	valueKey  string
}

// Producer writes telemetry records to TimescaleDB
type Producer struct {
	config *Config
	pool   *pgxpool.Pool
	logger *logrus.Logger

	mu         sync.Mutex
	vehicleIDs map[string]int16
	clientVers map[string]string
	signalIDs  map[string]int16
	alertIDs   map[string]int16
	latestKey  map[sigKey]string
	latestTS   map[sigKey]time.Time
	buf        []signalRow
	pendingLat map[sigKey]signalRow

	done chan struct{}
	wg   sync.WaitGroup
}

// NewProducer connects to TimescaleDB and loads the change-detection state
func NewProducer(ctx context.Context, config *Config, logger *logrus.Logger) (telemetry.Producer, error) {
	if config.DSN == "" {
		return nil, fmt.Errorf("timescale dsn is required")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 500
	}
	if config.FlushIntervalMS <= 0 {
		config.FlushIntervalMS = 2000
	}

	pool, err := pgxpool.New(ctx, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("timescale connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("timescale ping: %w", err)
	}

	p := &Producer{
		config:     config,
		pool:       pool,
		logger:     logger,
		vehicleIDs: make(map[string]int16),
		clientVers: make(map[string]string),
		signalIDs:  make(map[string]int16),
		alertIDs:   make(map[string]int16),
		latestKey:  make(map[sigKey]string),
		latestTS:   make(map[sigKey]time.Time),
		pendingLat: make(map[sigKey]signalRow),
		done:       make(chan struct{}),
	}
	if err := p.loadState(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("timescale load state: %w", err)
	}

	p.wg.Add(1)
	go p.flushLoop()

	logger.ActivityLog("timescale_producer_initialized", logrus.LogInfo{
		"signals": len(p.signalIDs), "vehicles": len(p.vehicleIDs),
	})
	return p, nil
}

// Produce dispatches one telemetry record to the database
func (p *Producer) Produce(entry *telemetry.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	switch payload := entry.GetProtoMessage().(type) {
	case *protos.Payload:
		err = p.produceSignals(ctx, entry, payload)
	case *protos.VehicleAlerts:
		err = p.produceAlerts(ctx, entry, payload)
	case *protos.VehicleErrors:
		err = p.produceErrors(ctx, entry, payload)
	case *protos.VehicleConnectivity:
		err = p.produceConnectivity(ctx, entry, payload)
	default:
		err = fmt.Errorf("unknown txType: %s", entry.TxType)
	}
	if err != nil {
		p.ReportError("timescale_produce_error", err, logrus.LogInfo{"vin": entry.Vin, "txtype": entry.TxType})
	}
}

// Close flushes buffered rows and closes the pool
func (p *Producer) Close() error {
	close(p.done)
	p.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p.mu.Lock()
	err := p.flushLocked(ctx)
	p.mu.Unlock()
	p.pool.Close()
	return err
}

// ProcessReliableAck noop
func (p *Producer) ProcessReliableAck(_ *telemetry.Record) {}

// ReportError logs an error
func (p *Producer) ReportError(message string, err error, logInfo logrus.LogInfo) {
	p.logger.ErrorLog(message, err, logInfo)
}

// ─────────────────────────────────────────────────────────────────
//  Vehicle data (txtype V) — change-only signal storage
// ─────────────────────────────────────────────────────────────────

func (p *Producer) produceSignals(ctx context.Context, entry *telemetry.Record, payload *protos.Payload) error {
	vid, err := p.vehicleID(ctx, entry.Vin, entry.DeviceClientVersion)
	if err != nil {
		return err
	}
	ts := payload.GetCreatedAt().AsTime()
	data := transformers.PayloadToMap(payload, true, entry.Vin, p.logger)

	p.mu.Lock()
	defer p.mu.Unlock()
	for name, wrapper := range data {
		if name == "Vin" || name == "CreatedAt" || name == "IsResend" {
			continue
		}
		row, ok := classify(wrapper)
		if !ok {
			continue
		}
		sid, err := p.signalIDLocked(ctx, name)
		if err != nil {
			return err
		}
		key := sigKey{vid, sid}
		row.ts, row.vehicleID, row.signalID = ts, vid, sid

		if prev, seen := p.latestKey[key]; seen {
			if prev == row.valueKey {
				if ts.After(p.latestTS[key]) {
					p.latestTS[key] = ts
				}
				continue // unchanged — the whole point of this schema
			}
			if !ts.After(p.latestTS[key]) {
				continue // out-of-order resend/replay of an older value
			}
		}
		p.latestKey[key] = row.valueKey
		p.latestTS[key] = ts
		p.buf = append(p.buf, row)
		p.pendingLat[key] = row
	}

	if len(p.buf) >= p.config.BatchSize {
		return p.flushLocked(ctx)
	}
	return nil
}

func (p *Producer) flushLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(p.config.FlushIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			p.mu.Lock()
			err := p.flushLocked(ctx)
			p.mu.Unlock()
			cancel()
			if err != nil {
				p.ReportError("timescale_flush_error", err, nil)
			}
		}
	}
}

// flushLocked writes buffered signal_changes rows (COPY) and the pending
// signal_latest upserts. Caller holds p.mu.
func (p *Producer) flushLocked(ctx context.Context) error {
	if len(p.buf) == 0 {
		return nil
	}
	rows := p.buf
	pending := p.pendingLat
	p.buf = nil
	p.pendingLat = make(map[sigKey]signalRow)

	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"signal_changes"},
		[]string{"ts", "vehicle_id", "signal_id", "v_num", "v_long", "v_bool", "v_text", "v_loc", "v_json", "invalid"},
		pgx.CopyFromSlice(len(rows), func(i int) ([]interface{}, error) {
			r := rows[i]
			return []interface{}{r.ts, r.vehicleID, r.signalID, r.vNum, r.vLong, r.vBool, r.vText, r.vLoc, r.vJSON, r.invalid}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy signal_changes: %w", err)
	}

	batch := &pgx.Batch{}
	for _, r := range pending {
		batch.Queue(
			`INSERT INTO signal_latest
			   (vehicle_id, signal_id, ts, v_num, v_long, v_bool, v_text, v_loc, v_json, invalid)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT (vehicle_id, signal_id) DO UPDATE SET
			   ts=EXCLUDED.ts, v_num=EXCLUDED.v_num, v_long=EXCLUDED.v_long,
			   v_bool=EXCLUDED.v_bool, v_text=EXCLUDED.v_text, v_loc=EXCLUDED.v_loc,
			   v_json=EXCLUDED.v_json, invalid=EXCLUDED.invalid
			 WHERE EXCLUDED.ts >= signal_latest.ts`,
			r.vehicleID, r.signalID, r.ts, r.vNum, r.vLong, r.vBool, r.vText, r.vLoc, r.vJSON, r.invalid)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("upsert signal_latest: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────
//  Alerts — episode upserts
// ─────────────────────────────────────────────────────────────────

func (p *Producer) produceAlerts(ctx context.Context, entry *telemetry.Record, payload *protos.VehicleAlerts) error {
	vid, err := p.vehicleID(ctx, entry.Vin, entry.DeviceClientVersion)
	if err != nil {
		return err
	}
	now := time.Now().UTC()

	type episode struct {
		alertID   int16
		startedAt time.Time
		endedAt   *time.Time
		audiences []string
	}
	episodes := make(map[string]episode)
	for _, alert := range payload.GetAlerts() {
		if alert.GetName() == "" || alert.GetStartedAt() == nil {
			continue
		}
		aid, err := p.alertID(ctx, alert.GetName())
		if err != nil {
			return err
		}
		ep := episode{alertID: aid, startedAt: alert.GetStartedAt().AsTime()}
		if alert.GetEndedAt() != nil {
			t := alert.GetEndedAt().AsTime()
			ep.endedAt = &t
		}
		if len(alert.GetAudiences()) > 0 {
			for _, a := range alert.GetAudiences() {
				ep.audiences = append(ep.audiences, a.String())
			}
			sort.Strings(ep.audiences)
		}
		k := fmt.Sprintf("%d|%d", aid, ep.startedAt.UnixNano())
		if cur, ok := episodes[k]; !ok || (cur.endedAt == nil && ep.endedAt != nil) {
			episodes[k] = ep
		}
	}

	batch := &pgx.Batch{}
	for _, ep := range episodes {
		batch.Queue(
			`INSERT INTO alert_episodes
			   (vehicle_id, alert_id, started_at, ended_at, audiences, last_reported)
			 VALUES ($1,$2,$3,$4,$5,$6)
			 ON CONFLICT (vehicle_id, alert_id, started_at) DO UPDATE SET
			   ended_at      = COALESCE(alert_episodes.ended_at, EXCLUDED.ended_at),
			   audiences     = COALESCE(EXCLUDED.audiences, alert_episodes.audiences),
			   last_reported = GREATEST(alert_episodes.last_reported, EXCLUDED.last_reported)`,
			vid, ep.alertID, ep.startedAt, ep.endedAt, ep.audiences, now)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("upsert alert_episodes: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────
//  Errors & connectivity — natural-key inserts
// ─────────────────────────────────────────────────────────────────

func (p *Producer) produceErrors(ctx context.Context, entry *telemetry.Record, payload *protos.VehicleErrors) error {
	vid, err := p.vehicleID(ctx, entry.Vin, entry.DeviceClientVersion)
	if err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, ve := range payload.GetErrors() {
		ts := time.Now().UTC()
		if ve.GetCreatedAt() != nil {
			ts = ve.GetCreatedAt().AsTime()
		}
		var tags *string
		if len(ve.GetTags()) > 0 {
			b, _ := json.Marshal(ve.GetTags())
			s := string(b)
			tags = &s
		}
		var body *string
		if ve.GetBody() != "" {
			b := ve.GetBody()
			body = &b
		}
		batch.Queue(
			`INSERT INTO error_events (vehicle_id, ts, name, body, tags)
			 VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
			vid, ts, ve.GetName(), body, tags)
	}
	if err := p.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("insert error_events: %w", err)
	}
	return nil
}

func (p *Producer) produceConnectivity(ctx context.Context, entry *telemetry.Record, payload *protos.VehicleConnectivity) error {
	vid, err := p.vehicleID(ctx, entry.Vin, entry.DeviceClientVersion)
	if err != nil {
		return err
	}
	ts := time.Now().UTC()
	if payload.GetCreatedAt() != nil {
		ts = payload.GetCreatedAt().AsTime()
	}
	var iface *string
	if payload.GetNetworkInterface() != "" {
		s := payload.GetNetworkInterface()
		iface = &s
	}
	_, err = p.pool.Exec(ctx,
		`INSERT INTO connectivity_events (vehicle_id, connection_id, status, network_interface, ts)
		 VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
		vid, payload.GetConnectionId(), payload.GetStatus().String(), iface, ts)
	if err != nil {
		return fmt.Errorf("insert connectivity_events: %w", err)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────
//  Dimension caches
// ─────────────────────────────────────────────────────────────────

func (p *Producer) vehicleID(ctx context.Context, vin, clientVersion string) (int16, error) {
	p.mu.Lock()
	vid, ok := p.vehicleIDs[vin]
	known := p.clientVers[vin]
	p.mu.Unlock()

	if !ok {
		err := p.pool.QueryRow(ctx,
			`INSERT INTO vehicles (vin) VALUES ($1)
			 ON CONFLICT (vin) DO UPDATE SET vin = EXCLUDED.vin
			 RETURNING vehicle_id`, vin).Scan(&vid)
		if err != nil {
			return 0, fmt.Errorf("upsert vehicle %s: %w", vin, err)
		}
		p.mu.Lock()
		p.vehicleIDs[vin] = vid
		p.mu.Unlock()
	}
	if clientVersion != "" && clientVersion != known {
		if _, err := p.pool.Exec(ctx,
			`UPDATE vehicles SET device_client_version = $1 WHERE vehicle_id = $2`,
			clientVersion, vid); err == nil {
			p.mu.Lock()
			p.clientVers[vin] = clientVersion
			p.mu.Unlock()
		}
	}
	return vid, nil
}

// signalIDLocked requires p.mu held
func (p *Producer) signalIDLocked(ctx context.Context, name string) (int16, error) {
	if sid, ok := p.signalIDs[name]; ok {
		return sid, nil
	}
	var sid int16
	err := p.pool.QueryRow(ctx,
		`INSERT INTO signals (signal_id, name)
		 SELECT GREATEST(1000, max(signal_id) + 1), $1 FROM signals
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING signal_id`, name).Scan(&sid)
	if err != nil {
		return 0, fmt.Errorf("register signal %s: %w", name, err)
	}
	p.signalIDs[name] = sid
	return sid, nil
}

func (p *Producer) alertID(ctx context.Context, name string) (int16, error) {
	p.mu.Lock()
	aid, ok := p.alertIDs[name]
	p.mu.Unlock()
	if ok {
		return aid, nil
	}
	err := p.pool.QueryRow(ctx,
		`INSERT INTO alert_types (name) VALUES ($1)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING alert_id`, name).Scan(&aid)
	if err != nil {
		return 0, fmt.Errorf("register alert type %s: %w", name, err)
	}
	p.mu.Lock()
	p.alertIDs[name] = aid
	p.mu.Unlock()
	return aid, nil
}

// loadState fills the dimension and change-detection caches from the database
func (p *Producer) loadState(ctx context.Context) error {
	rows, err := p.pool.Query(ctx, `SELECT name, signal_id FROM signals`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var id int16
		if err := rows.Scan(&name, &id); err != nil {
			return err
		}
		p.signalIDs[name] = id
	}

	rows, err = p.pool.Query(ctx, `SELECT vin, vehicle_id, COALESCE(device_client_version,'') FROM vehicles`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var vin, ver string
		var id int16
		if err := rows.Scan(&vin, &id, &ver); err != nil {
			return err
		}
		p.vehicleIDs[vin] = id
		p.clientVers[vin] = ver
	}

	rows, err = p.pool.Query(ctx, `SELECT name, alert_id FROM alert_types`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var id int16
		if err := rows.Scan(&name, &id); err != nil {
			return err
		}
		p.alertIDs[name] = id
	}

	rows, err = p.pool.Query(ctx,
		`SELECT vehicle_id, signal_id, ts, v_num, v_long, v_bool, v_text, v_loc, v_json::text, invalid
		 FROM signal_latest`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var vid, sid int16
		var ts time.Time
		var vNum *float64
		var vLong *int64
		var vBool *bool
		var vText, vJSON *string
		var vLoc pgtype.Point
		var invalid bool
		if err := rows.Scan(&vid, &sid, &ts, &vNum, &vLong, &vBool, &vText, &vLoc, &vJSON, &invalid); err != nil {
			return err
		}
		key := sigKey{vid, sid}
		p.latestTS[key] = ts
		p.latestKey[key] = canonicalKey(vNum, vLong, vBool, vText, vLoc, normJSON(vJSON), invalid)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────
//  Value classification & canonical comparison keys
// ─────────────────────────────────────────────────────────────────

func fmtFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// classify maps a typed wrapper produced by transformers.PayloadToMap
// (includeTypes=true) onto schema columns. Returns ok=false for shapes that
// carry no storable value.
func classify(wrapper interface{}) (signalRow, bool) {
	var r signalRow
	m, ok := wrapper.(map[string]interface{})
	if !ok || len(m) != 1 {
		return r, false
	}
	var typeKey string
	var val interface{}
	for k, v := range m {
		typeKey, val = k, v
	}

	switch typeKey {
	case "invalid":
		r.invalid = true
	case "doubleValue", "floatValue":
		var f float64
		switch n := val.(type) {
		case float64:
			f = n
		case float32:
			f = float64(n)
		default:
			return r, false
		}
		r.vNum = &f
	case "intValue", "longValue":
		var i int64
		switch n := val.(type) {
		case int32:
			i = int64(n)
		case int64:
			i = n
		case int:
			i = int64(n)
		default:
			return r, false
		}
		r.vLong = &i
	case "booleanValue":
		b, ok := val.(bool)
		if !ok {
			return r, false
		}
		r.vBool = &b
	case "locationValue":
		loc, ok := val.(map[string]float64)
		if !ok {
			return r, false
		}
		r.vLoc = pgtype.Point{P: pgtype.Vec2{X: loc["longitude"], Y: loc["latitude"]}, Valid: true}
	case "doorValue", "tireLocation":
		b, err := json.Marshal(val) // Go sorts map keys — canonical
		if err != nil {
			return r, false
		}
		s := string(b)
		r.vJSON = &s
	default:
		// stringValue, time, and every proto-enum-as-string case
		s, ok := val.(string)
		if !ok {
			b, err := json.Marshal(val)
			if err != nil {
				return r, false
			}
			s = string(b)
			r.vJSON = &s
			break
		}
		r.vText = &s
	}

	r.valueKey = canonicalKey(r.vNum, r.vLong, r.vBool, r.vText, r.vLoc, r.vJSON, r.invalid)
	return r, true
}

func canonicalKey(vNum *float64, vLong *int64, vBool *bool, vText *string, vLoc pgtype.Point, vJSON *string, invalid bool) string {
	switch {
	case invalid:
		return "i"
	case vNum != nil:
		return "n:" + fmtFloat(*vNum)
	case vLong != nil:
		return "l:" + strconv.FormatInt(*vLong, 10)
	case vBool != nil:
		if *vBool {
			return "b:t"
		}
		return "b:f"
	case vText != nil:
		return "s:" + *vText
	case vLoc.Valid:
		return "g:(" + fmtFloat(vLoc.P.X) + "," + fmtFloat(vLoc.P.Y) + ")"
	case vJSON != nil:
		return "j:" + *vJSON
	}
	return ""
}

// normJSON re-canonicalizes jsonb text output through Go's json.Marshal
// (sorted keys, no extra whitespace).
func normJSON(s *string) *string {
	if s == nil {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal([]byte(*s), &v); err != nil {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return s
	}
	out := string(b)
	return &out
}
