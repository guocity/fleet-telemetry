package simple

import (
	"reflect"
	"sync"
	"time"
)

// DeltaTracker tracks the last known state for each VIN to enable delta encoding
type DeltaTracker struct {
	mu         sync.RWMutex
	lastState  map[string]map[string]interface{} // VIN -> field -> value
	lastUpdate map[string]time.Time              // VIN -> last update time
	ttl        time.Duration                     // How long to keep state
}

// NewDeltaTracker creates a new delta tracker
func NewDeltaTracker(ttl time.Duration) *DeltaTracker {
	if ttl == 0 {
		ttl = 24 * time.Hour // Default: keep state for 24 hours
	}
	
	dt := &DeltaTracker{
		lastState:  make(map[string]map[string]interface{}),
		lastUpdate: make(map[string]time.Time),
		ttl:        ttl,
	}
	
	// Start cleanup goroutine
	go dt.cleanupLoop()
	
	return dt
}

// GetChanges returns only the fields that have changed since last update
// Returns: (changedFields, isFullSnapshot)
func (dt *DeltaTracker) GetChanges(vin string, currentData map[string]interface{}) (map[string]interface{}, bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	
	lastState, exists := dt.lastState[vin]
	
	// If no previous state exists, or state is too old, return full snapshot
	if !exists {
		dt.lastState[vin] = deepCopy(currentData).(map[string]interface{})
		dt.lastUpdate[vin] = time.Now()
		return currentData, true
	}

	// Check if state is stale
	if time.Since(dt.lastUpdate[vin]) > dt.ttl {
		dt.lastState[vin] = deepCopy(currentData).(map[string]interface{})
		dt.lastUpdate[vin] = time.Now()
		return currentData, true
	}
	
	// Calculate delta
	changes := make(map[string]interface{})
	
	for key, newValue := range currentData {
		oldValue, hadKey := lastState[key]
		
		// Include if: new field, or value changed
		if !hadKey || !valuesEqual(oldValue, newValue) {
			changes[key] = newValue
			lastState[key] = deepCopy(newValue)
		}
	}
	
	// Check for removed fields (fields in old but not in new)
	for key := range lastState {
		if _, exists := currentData[key]; !exists {
			changes[key] = nil // Explicitly mark as removed
			delete(lastState, key)
		}
	}
	
	dt.lastUpdate[vin] = time.Now()
	
	// If everything changed, it's effectively a full snapshot
	isFullSnapshot := len(changes) == len(currentData)
	
	return changes, isFullSnapshot
}

// ForceSnapshot forces the next record for this VIN to be a full snapshot
func (dt *DeltaTracker) ForceSnapshot(vin string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	
	delete(dt.lastState, vin)
	delete(dt.lastUpdate, vin)
}

// cleanupLoop periodically removes stale state
func (dt *DeltaTracker) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	
	for range ticker.C {
		dt.cleanup()
	}
}

// cleanup removes stale entries
func (dt *DeltaTracker) cleanup() {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	
	now := time.Now()
	for vin, lastTime := range dt.lastUpdate {
		if now.Sub(lastTime) > dt.ttl {
			delete(dt.lastState, vin)
			delete(dt.lastUpdate, vin)
		}
	}
}

// valuesEqual compares two values for equality
func valuesEqual(a, b interface{}) bool {
	// Use reflect.DeepEqual for complex types
	return reflect.DeepEqual(a, b)
}

// deepCopy creates a deep copy of a value
func deepCopy(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			result[k] = deepCopy(v)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, v := range val {
			result[i] = deepCopy(v)
		}
		return result
	case map[interface{}]interface{}:
		result := make(map[interface{}]interface{}, len(val))
		for k, v := range val {
			result[k] = deepCopy(v)
		}
		return result
	default:
		// For primitive types, return as-is (they're immutable)
		return v
	}
}
