#!/usr/bin/env python3
"""
Compare storage sizes between full and delta-encoded telemetry files.
Run this on your existing data to estimate savings.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List

def analyze_file(file_path: Path) -> dict:
    """Analyze a JSONL file and simulate delta encoding."""
    
    full_size = 0
    delta_size = 0
    record_count = 0
    snapshot_count = 0
    delta_count = 0
    
    # Track state per VIN
    state: Dict[str, Dict[str, Any]] = {}
    
    with open(file_path, 'r') as f:
        for line in f:
            if not line.strip():
                continue
                
            record_count += 1
            full_record = json.loads(line)
            full_size += len(line)
            
            vin = full_record.get('vin', 'unknown')
            current_data = full_record.get('data', {})
            
            # Initialize state for this VIN
            if vin not in state:
                state[vin] = {}
                # First record is always a snapshot
                delta_size += len(line)
                snapshot_count += 1
                state[vin] = current_data.copy()
                continue
            
            # Calculate changes
            changes = {}
            for key, value in current_data.items():
                if key not in state[vin] or state[vin][key] != value:
                    changes[key] = value
            
            # Check for removed fields
            for key in state[vin]:
                if key not in current_data:
                    changes[key] = None
            
            if changes:
                # Create delta record
                delta_record = {
                    'vin': vin,
                    'time': full_record['time'],
                    'txid': full_record.get('txid', ''),
                    'type': 'delta',
                    'data': changes
                }
                delta_json = json.dumps(delta_record) + '\n'
                delta_size += len(delta_json)
                delta_count += 1
                
                # Update state
                state[vin].update(changes)
                # Remove None values
                state[vin] = {k: v for k, v in state[vin].items() if v is not None}
            # else: no changes, record not written (size += 0)
    
    savings = full_size - delta_size
    savings_pct = (savings / full_size * 100) if full_size > 0 else 0
    
    return {
        'file': file_path.name,
        'full_size': full_size,
        'delta_size': delta_size,
        'savings': savings,
        'savings_pct': savings_pct,
        'record_count': record_count,
        'snapshot_count': snapshot_count,
        'delta_count': delta_count,
        'skipped_count': record_count - snapshot_count - delta_count
    }

def format_size(bytes_size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} TB"

def main():
    if len(sys.argv) < 2:
        print("Usage: python compare_sizes.py <telemetry_file.jsonl> [<file2.jsonl> ...]")
        print("\nExample:")
        print("  python compare_sizes.py /data/telemetry/5YJ3E1EA2JF013888/2026/01/V.jsonl")
        sys.exit(1)
    
    results = []
    total_full = 0
    total_delta = 0
    
    for file_path in sys.argv[1:]:
        path = Path(file_path)
        if not path.exists():
            print(f"Warning: {file_path} not found, skipping")
            continue
        
        print(f"Analyzing {path.name}...", end=' ', flush=True)
        result = analyze_file(path)
        results.append(result)
        total_full += result['full_size']
        total_delta += result['delta_size']
        print(f"✓ ({result['savings_pct']:.1f}% savings)")
    
    if not results:
        print("\nNo valid files analyzed.")
        sys.exit(1)
    
    # Print detailed results
    print("\n" + "="*80)
    print("DETAILED RESULTS")
    print("="*80)
    
    for r in results:
        print(f"\nFile: {r['file']}")
        print(f"  Records: {r['record_count']:,} total")
        print(f"    • {r['snapshot_count']:,} snapshots")
        print(f"    • {r['delta_count']:,} deltas")
        print(f"    • {r['skipped_count']:,} unchanged (skipped)")
        print(f"  Size:")
        print(f"    • Full format:  {format_size(r['full_size'])}")
        print(f"    • Delta format: {format_size(r['delta_size'])}")
        print(f"    • Savings:      {format_size(r['savings'])} ({r['savings_pct']:.1f}%)")
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    total_savings = total_full - total_delta
    total_savings_pct = (total_savings / total_full * 100) if total_full > 0 else 0
    
    print(f"Total size (full format):  {format_size(total_full)}")
    print(f"Total size (delta format): {format_size(total_delta)}")
    print(f"Total savings:             {format_size(total_savings)} ({total_savings_pct:.1f}%)")
    
    # Extrapolate to yearly savings
    if total_full > 0:
        print("\n" + "="*80)
        print("YEARLY PROJECTION (per vehicle)")
        print("="*80)
        
        # Assume the analyzed period and extrapolate
        daily_full = total_full  # Assume input is ~1 day
        daily_delta = total_delta
        
        print(f"Daily:   {format_size(daily_full)} → {format_size(daily_delta)}")
        print(f"Monthly: {format_size(daily_full * 30)} → {format_size(daily_delta * 30)}")
        print(f"Yearly:  {format_size(daily_full * 365)} → {format_size(daily_delta * 365)}")
        print(f"Yearly savings: {format_size((daily_full - daily_delta) * 365)}")

if __name__ == '__main__':
    main()
