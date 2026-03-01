#!/usr/bin/env python3
"""
Live Terminal Dashboard for Concurrent Recording Sessions.

Monitors test_recordings_concurrent_validation/sessions/*/events.jsonl in real-time,
showing:
- Total events, orphan/non-orphan breakdown
- Per-protocol counts (Postgres, Redis, HTTP, gRPC)
- Non-deterministic event counts
- Top traces by event volume
- Event rate (events/sec)
- Session info and last update timestamp

Usage:
  Terminal 1: Run concurrent test
    cargo test -p test-server --test concurrent_validation -- --nocapture

  Terminal 2: Start dashboard (auto-detects latest session)
    python3 scripts/live_concurrent_dashboard.py

  Or monitor specific session:
    python3 scripts/live_concurrent_dashboard.py test-server/test_recordings_concurrent_validation/sessions/20260218_103132/events.jsonl

Controls:
  - Press Ctrl+C to exit cleanly
  - Resizes automatically to terminal width
  - Handles session rollover (detects new sessions)
"""

import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional


class RecordingDashboard:
    """Live dashboard for monitoring concurrent recording sessions."""

    def __init__(self, events_file: Optional[str] = None, refresh_rate: float = 1.0):
        self.events_file = events_file
        self.refresh_rate = refresh_rate
        self.running = False
        self.file_handle = None
        self.inode = None
        self.file_position = 0

        # Stats
        self.total_events = 0
        self.orphan_events = 0
        self.non_orphan_events = 0
        self.protocol_counts = defaultdict(int)
        self.nd_events = 0
        self.traces = defaultdict(int)  # trace_id -> event count
        self.scope_sequences = defaultdict(list)  # scope_id -> [sequences]

        # Timing
        self.start_time = time.time()
        self.last_update = None
        self.first_event_time = None
        self.last_event_time = None

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.running = False

    def _get_terminal_size(self) -> tuple:
        """Get terminal dimensions."""
        try:
            import shutil
            return shutil.get_terminal_size()
        except:
            return (80, 24)

    def _clear_screen(self):
        """Clear terminal screen."""
        os.system('clear' if os.name != 'nt' else 'cls')

    def _find_latest_session(self) -> Optional[Path]:
        """Find the most recent concurrent validation session."""
        base_dir = Path("test-server/test_recordings_concurrent_validation/sessions")
        if not base_dir.exists():
            # Try from scripts directory
            base_dir = Path("../test-server/test_recordings_concurrent_validation/sessions")
            if not base_dir.exists():
                return None

        sessions = sorted(base_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
        for session in sessions:
            if session.is_dir():
                events_file = session / "events.jsonl"
                if events_file.exists():
                    return events_file
        return None

    def _open_file(self) -> bool:
        """Open the events file and seek to current position."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

        if not self.events_file:
            latest = self._find_latest_session()
            if not latest:
                return False
            self.events_file = str(latest)

        path = Path(self.events_file)
        if not path.exists():
            return False

        try:
            self.file_handle = open(path, 'r')
            self.inode = path.stat().st_ino

            if self.file_position > 0:
                self.file_handle.seek(self.file_position)
            else:
                for line in self.file_handle.readlines():
                    self._process_line(line)
                self.file_position = self.file_handle.tell()

            return True
        except Exception as e:
            print(f"Error opening file: {e}", file=sys.stderr)
            return False

    def _check_file_rollover(self) -> bool:
        """Check if file has been replaced (new session started)."""
        if not self.events_file:
            return False

        path = Path(self.events_file)
        if not path.exists():
            return True  # File gone, probably new session

        try:
            current_inode = path.stat().st_ino
            if current_inode != self.inode:
                return True
        except:
            return True

        return False

    def _detect_protocol(self, event: dict) -> str:
        """Detect protocol from event structure."""
        event_data = event.get('event', {})

        if 'PgMessage' in event_data:
            return 'Postgres'
        elif 'RedisResponse' in event_data or 'RedisCommand' in event_data:
            return 'Redis'
        elif 'HttpRequest' in event_data or 'HttpResponse' in event_data:
            return 'HTTP'
        elif 'GrpcRequest' in event_data or 'GrpcResponse' in event_data:
            return 'gRPC'
        elif 'TcpData' in event_data:
            return 'TCP'
        else:
            # Try to infer from scope_id
            scope_id = event.get('scope_id', '')
            if 'pg' in scope_id.lower() or 'postgres' in scope_id.lower():
                return 'Postgres'
            elif 'redis' in scope_id.lower():
                return 'Redis'
            elif 'http' in scope_id.lower():
                return 'HTTP'
            elif 'grpc' in scope_id.lower():
                return 'gRPC'
            return 'Unknown'

    def _process_line(self, line: str):
        """Process a single JSONL line."""
        line = line.strip()
        if not line:
            return

        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return

        self.total_events += 1

        # Trace tracking
        trace_id = event.get('trace_id', 'unknown')
        self.traces[trace_id] += 1

        # Orphan vs non-orphan
        if trace_id == 'orphan':
            self.orphan_events += 1
        else:
            self.non_orphan_events += 1

        # Protocol detection
        protocol = self._detect_protocol(event)
        self.protocol_counts[protocol] += 1

        # Non-deterministic detection
        metadata = event.get('metadata', {})
        if metadata.get('nd_kind') or metadata.get('nd_seq'):
            self.nd_events += 1

        # Scope sequence tracking
        scope_id = event.get('scope_id', 'unknown')
        scope_seq = event.get('scope_sequence')
        if scope_seq is not None:
            self.scope_sequences[scope_id].append(scope_seq)

        # Timing
        timestamp_ns = event.get('timestamp_ns')
        if timestamp_ns:
            event_time = timestamp_ns / 1e9
            if self.first_event_time is None:
                self.first_event_time = event_time
            self.last_event_time = event_time

        self.last_update = datetime.now()

    def _read_new_lines(self):
        """Read any new lines from the file."""
        if not self.file_handle:
            if not self._open_file():
                return

        # Check for rollover
        if self._check_file_rollover():
            print("\n[New session detected, switching...]", file=sys.stderr)
            self._reset_stats()
            self.events_file = None  # Will auto-detect new session
            if not self._open_file():
                return

        # Read new lines
        lines = self.file_handle.readlines()
        self.file_position = self.file_handle.tell()

        for line in lines:
            self._process_line(line)

    def _reset_stats(self):
        """Reset all statistics (for session rollover)."""
        self.total_events = 0
        self.orphan_events = 0
        self.non_orphan_events = 0
        self.protocol_counts.clear()
        self.nd_events = 0
        self.traces.clear()
        self.scope_sequences.clear()
        self.first_event_time = None
        self.last_event_time = None
        self.file_position = 0

    def _format_duration(self, seconds: float) -> str:
        """Format duration as HH:MM:SS."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _render(self):
        """Render the dashboard."""
        self._clear_screen()
        cols, rows = self._get_terminal_size()

        # Header
        print("╔" + "═" * (cols - 2) + "╗")
        title = "🔴 LIVE CONCURRENT RECORDINGS DASHBOARD"
        subtitle = f"Session: {Path(self.events_file).parent.name if self.events_file else 'Waiting...'}"
        print(f"║ {title:<{cols-3}}║")
        print(f"║ {subtitle:<{cols-3}}║")
        print("╠" + "═" * (cols - 2) + "╣")

        # Time and rate
        elapsed = time.time() - self.start_time
        event_rate = self.total_events / elapsed if elapsed > 0 else 0

        time_str = f"⏱️  Runtime: {self._format_duration(elapsed)}"
        rate_str = f"📊 Rate: {event_rate:.1f} events/sec"
        update_str = f"🔄 Last Update: {self.last_update.strftime('%H:%M:%S') if self.last_update else 'N/A'}"

        print(f"║ {time_str:<25} {rate_str:<30} {update_str:<{cols-60}}║")
        print("╠" + "═" * (cols - 2) + "╣")

        # Event counts
        print(f"║ {'EVENT COUNTS':^{cols-2}}║")
        print("╠" + "═" * (cols - 2) + "╣")
        total_str = f"📈 Total Events: {self.total_events:,}"
        orphan_str = f"👻 Orphan: {self.orphan_events:,} ({100*self.orphan_events/self.total_events:.1f}%)" if self.total_events > 0 else "👻 Orphan: 0 (0.0%)"
        non_orphan_str = f"✅ Attributed: {self.non_orphan_events:,} ({100*self.non_orphan_events/self.total_events:.1f}%)" if self.total_events > 0 else "✅ Attributed: 0 (0.0%)"
        nd_str = f"🎲 Non-Deterministic: {self.nd_events:,}"

        print(f"║ {total_str:<25} {orphan_str:<35} {non_orphan_str:<{cols-65}}║")
        print(f"║ {nd_str:<{cols-2}}║")
        print("╠" + "═" * (cols - 2) + "╣")

        # Protocol breakdown
        print(f"║ {'PROTOCOL BREAKDOWN':^{cols-2}}║")
        print("╠" + "═" * (cols - 2) + "╣")

        protocols = ['Postgres', 'Redis', 'HTTP', 'gRPC', 'TCP', 'Unknown']
        proto_lines = []
        for proto in protocols:
            count = self.protocol_counts.get(proto, 0)
            if count > 0 or proto in ['Postgres', 'Redis']:
                pct = f"({100*count/self.total_events:.1f}%)" if self.total_events > 0 else "(0.0%)"
                bar_len = min(int(count / max(self.total_events * 0.01, 1)), 20)
                bar = "█" * bar_len + "░" * (20 - bar_len)
                proto_lines.append(f"{proto:10} {bar} {count:>6,} {pct:>7}")

        for line in proto_lines[:5]:
            print(f"║ {line:<{cols-2}}║")

        print("╠" + "═" * (cols - 2) + "╣")

        # Top traces
        print(f"║ {'TOP TRACES BY EVENT COUNT':^{cols-2}}║")
        print("╠" + "═" * (cols - 2) + "╣")

        sorted_traces = sorted(self.traces.items(), key=lambda x: x[1], reverse=True)
        top_n = min(10, (rows - 25) // 2)

        for i, (trace_id, count) in enumerate(sorted_traces[:top_n], 1):
            pct = f"({100*count/self.total_events:.1f}%)" if self.total_events > 0 else "(0.0%)"
            trace_display = trace_id[:40] + "..." if len(trace_id) > 43 else trace_id
            line = f"{i:2}. {trace_display:<45} {count:>6,} {pct:>7}"
            print(f"║ {line:<{cols-2}}║")

        # Fill remaining space
        current_line = 25 + len(proto_lines) + len(sorted_traces[:top_n])
        for _ in range(rows - current_line - 2):
            print(f"║ {' ' * (cols-2)}║")

        # Footer
        print("╚" + "═" * (cols - 2) + "╝")
        print("Press Ctrl+C to exit | Auto-refreshes every second")

    def run(self):
        """Main dashboard loop."""
        self.running = True
        self.start_time = time.time()

        print("Starting dashboard... Looking for concurrent validation recordings...")

        while self.running:
            try:
                self._read_new_lines()
                self._render()
                time.sleep(self.refresh_rate)
            except Exception as e:
                print(f"\nError: {e}", file=sys.stderr)
                time.sleep(1)

        # Cleanup
        if self.file_handle:
            self.file_handle.close()
        print("\nDashboard stopped.")


def main():
    events_file = sys.argv[1] if len(sys.argv) > 1 else None
    refresh_rate = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    dashboard = RecordingDashboard(events_file, refresh_rate)
    dashboard.run()


if __name__ == "__main__":
    main()
