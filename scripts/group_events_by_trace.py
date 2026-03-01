#!/usr/bin/env python3
"""
Group JSONL events by trace_id and output as grouped JSON.

Usage: python group_events_by_trace.py <events.jsonl> [output.json]

Input:  events.jsonl - JSONL file with one event per line
Output: Grouped JSON with trace_id -> events[] structure
"""

import json
import sys
from collections import defaultdict
from pathlib import Path


def group_events_by_trace(input_path: str, output_path: str | None = None) -> dict:
    """
    Read JSONL file and group events by trace_id.
    
    Returns dict of { trace_id: [events...] }
    """
    grouped = defaultdict(list)
    
    with open(input_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                event = json.loads(line)
                trace_id = event.get('trace_id', 'unknown')
                grouped[trace_id].append(event)
            except json.JSONDecodeError as e:
                print(f"Warning: Skipping malformed JSON on line {line_num}: {e}", file=sys.stderr)
    
    # Sort events within each trace by sequence and timestamp
    for trace_id in grouped:
        grouped[trace_id].sort(key=lambda e: (e.get('sequence', 0), e.get('timestamp_ns', 0)))
    
    # Convert to regular dict (sorted by trace_id for consistent output)
    result = dict(sorted(grouped.items()))
    
    # Output
    output_data = {
        "summary": {
            "total_events": sum(len(events) for events in result.values()),
            "unique_traces": len(result),
            "traces": {
                trace_id: {
                    "event_count": len(events),
                    "event_types": list(set(
                        list(e.get('event', {}).keys())[0] if e.get('event') else 'unknown'
                        for e in events
                    ))
                }
                for trace_id, events in result.items()
            }
        },
        "traces": result
    }
    
    if output_path:
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"✅ Grouped {output_data['summary']['total_events']} events into {output_data['summary']['unique_traces']} traces")
        print(f"   Output written to: {output_path}")
    else:
        # Print to stdout
        print(json.dumps(output_data, indent=2))
    
    return result


def print_summary(grouped: dict):
    """Print a human-readable summary of grouped events."""
    print("\n" + "=" * 60)
    print("TRACE SUMMARY")
    print("=" * 60)
    
    for trace_id, events in sorted(grouped.items()):
        event_types = defaultdict(int)
        for e in events:
            if 'event' in e and e['event']:
                event_type = list(e['event'].keys())[0]
                event_types[event_type] += 1
        
        print(f"\n📍 {trace_id}")
        print(f"   Total events: {len(events)}")
        for event_type, count in sorted(event_types.items()):
            print(f"   - {event_type}: {count}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not Path(input_file).exists():
        print(f"Error: File not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    
    grouped = group_events_by_trace(input_file, output_file)
    
    # Also print summary if outputting to file
    if output_file:
        print_summary(grouped)
