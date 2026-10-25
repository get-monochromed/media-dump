#!/usr/bin/env python3
"""
photorec_pipeline.py — photo-recovery pipeline with live streaming sorting and
a btop-style TUI (stdlib curses, zero dependencies).

Threads:
  * photorec carves media-only formats (white-listed tokens only) from a
    device/disk image into a staging dir. A full carve can take hours.
  * a scanner thread polls the staging dir and hands every file to worker
    threads THE MOMENT it is fully written — sorting runs concurrently.
  * a curses renderer draws btop-like boxes: header with live progress/ETA,
    photorec feed pane, sorter flow pane, keys footer.

Usage:
  sudo python3 photorec_pipeline.py [--yes] [options] <device-or-image>

    /dev/sdb1          whole partition (sudo)
    ~/photorec/phone.dd --ext     disk image (no sudo needed)

  python3 photorec_pipeline.py --sort-only [staging_dir]   # no carving

TUI keys:  q / Ctrl-C = graceful stop (sorter drains; staging kept for
           --sort-only) | arrows / PgUp / PgDn scroll sorter pane | f follow |
           s pause photorec feed intake | r force redraw

Useful flags:
  --skip png,gif   exclude format tokens from carving (family tokens:
                   riff -> wav/avi/webp, mov -> mov/mp4/3gp/heic, ogg -> ogg/ogm/ogv)
  --freespace      carve only unallocated space (recommended)
  --icon-size N    square images <= N px quarantined as icons (default 256)
  --no-tui         plain text mode (also automatic when output is piped)

Runs die with the session — wrap in tmux if you disconnect:
  sudo tmux new -s rec 'sudo python3 photorec_pipeline.py /dev/sdX1 --yes'
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    import curses
except ImportError:                      # no curses -> always plain mode
    curses = None

DEFAULT_OUTPUT = "~/photorec-sorted"
DEFAULT_STAGING = "~/photorec"
STABLE_SECS = 10                         # file unchanged this long = fully written
POLL_INTERVAL = 2                        # scanner poll seconds
PHOTOREC_BINARY = shutil.which("photorec")
IDENTIFY_BINARY = shutil.which("identify")

# Photorec whitelist is by FORMAT FAMILY (see __doc__).
PHOTOREC_FORMATS = [
    "jpg", "png", "gif", "bmp", "riff", "tif",                     # images
    "mov", "mpg", "m2ts", "ts", "mkv", "flv", "asf", "dv",         # video
    "mp3", "aif", "ape", "flac", "ogg", "amr",                     # audio
    "au", "gsm", "dss", "ra",
]
ICON_SCAN_EXTS = {"jpg", "jpeg", "png", "webp", "bmp", "gif", "tif", "tiff", "heic"}
SVG_EXTS = {"svg"}

PREFIX = {
    "sort":     "[sort    ]",
    "icon":     "[icon-?  ]",
    "svg":      "[svg-del]",
    "error":    "[error   ]",
    "photo":    "[photorec]",
    "pipeline": "[pipeline]",
}

colorsort = {"sort": 1, "icon": 2, "svg": 3, "error": 4}   # curses color pairs


def sudo_aware_default(sub: str) -> str:
    """Default folders live in the *invoking* user's home even under sudo."""
    home = None
    su = os.environ.get("SUDO_USER")
    if su and su != "root":
        try:
            import pwd
            home = pwd.getpwnam(su).pw_dir
        except (ImportError, KeyError):
            pass
    if not home:
        home = os.path.expanduser("~")
    return os.path.join(home, sub)


# ================================================================ args
def parse_args(argv=None):
    default_output = sudo_aware_default("photorec-sorted")
    default_staging = sudo_aware_default("photorec")
    p = argparse.ArgumentParser(
        description="Carve media from a device/disk image with photorec and sort it live (btop-style TUI).",
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", nargs="?", default=None,
                   help="Device, partition or disk image. Omitted only with --sort-only.")
    p.add_argument("--output", default=default_output,
                   help=f"Sorted output folder (default: {default_output})")
    p.add_argument("--staging", default=default_staging,
                   help=f"Parent folder for carved session dirs (default: {default_staging})")
    p.add_argument("--icon-size", type=int, default=256,
                   help="Max side (px) of square images treated as icons (default 256)")
    p.add_argument("--workers", type=int, default=4,
                   help="Number of sorting worker threads (default 4)")
    p.add_argument("--stable-secs", type=int, default=STABLE_SECS,
                   help="Seconds a file must stay unchanged to count as fully "
                        "written by photorec (default 10)")
    p.add_argument("--ext", action="store_true",
                   help="Force ext2/3/4 filesystem carving mode (use for ext4 images)")
    p.add_argument("--freespace", action="store_true",
                   help="Scan only unallocated space (default scans whole partition)")
    p.add_argument("--dry-run", action="store_true",
                   help="Sorting stages only: print actions, move/delete nothing. "
                        "Carving itself is never simulated.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    p.add_argument("--no-tui", action="store_true",
                   help="Plain text mode even on a TTY (auto-fallback when piped)")
    p.add_argument("--skip", default="", metavar="FORMATS",
                   help="Comma-separated photorec tokens to exclude, e.g. --skip png,gif")
    p.add_argument("--status-secs", type=int, default=15,
                   help="Seconds between plain-mode [status] lines (default 15)")
    p.add_argument("--sort-only", nargs="?", const="__default__", default=None,
                   metavar="SRC", help="Skip photorec; sort an existing staging dir")
    return p.parse_args(argv)


# ================================================================ shared state
class ProgressState:
    def __init__(self):
        self._lock = threading.Lock()
        self._d = {"pos": 0, "size": None, "rate": 0.0, "eta": None,
                   "elapsed": 0.0}

    def update(self, **kw):
        with self._lock:
            self._d.update(kw)

    def snapshot(self):
        with self._lock:
            return dict(self._d)


class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self._ext = {}
        self.quarantined = 0
        self.svg_removed = 0
        self.renamed = 0
        self.errors = 0
        self.dispatched = 0

    def add_ext(self, name, count=1):
        with self._lock:
            self._ext[name] = self._ext.get(name, 0) + count

    def bump(self, attr, count=1):
        with self._lock:
            setattr(self, attr, getattr(self, attr) + count)

    @property
    def moved(self):
        with self._lock:
            return sum(self._ext.values()), dict(self._ext)


class Bus:
    """All worker output routes through here. TUI buffers, plain prints."""

    def __init__(self, tui: bool):
        self.tui = tui
        self.paused = False
        self.dirty = threading.Event()
        self.photo = deque(maxlen=900)
        self.sort = deque(maxlen=5000)
        self._lock = threading.Lock()
        self._last_photo = 0.0

    def emit_pipeline(self, text):
        if not self.tui:
            print(f"{PREFIX['pipeline']} {text}", flush=True)
        with self._lock:
            self.photo.append((time.strftime("%H:%M:%S"), text))
        self.dirty.set()

    def emit_photo(self, text):
        if self.tui and self.paused:
            return
        if not self.tui:
            print(f"{PREFIX['photo']} {text}", flush=True)
        with self._lock:
            self.photo.append((time.strftime("%H:%M:%S"), text))
        self.dirty.set()

    def emit_photo_throttled(self, text, min_interval=1.0):
        now = time.time()
        with self._lock:
            if now - self._last_photo < min_interval:
                return
            self._last_photo = now
        self.emit_photo(text)

    def emit_sort(self, kind, text):
        if not self.tui:
            print(f"{PREFIX[kind]} {text}", flush=True)
        with self._lock:
            self.sort.append((kind, time.strftime("%H:%M:%S"), text))
        self.dirty.set()


# ================================================================ helpers
def unique_destination(dest_dir: Path, name: str) -> Path:
    candidate = dest_dir / name
    if not candidate.exists():
        return candidate
    stem, suffix = Path(name).stem, Path(name).suffix
    counter = 2
    while True:
        candidate = dest_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def human_bytes(n: float) -> str:
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if abs(n) < 1000 or unit == "TB":
            return f"{n:,.1f} {unit}"
        n /= 1000.0
    return f"{n:,.1f} TB"


def fmt_eta(secs) -> str:
    if secs is None:
        return "--"
    if 0 <= secs < 1:
        return "now"
    secs = max(0, round(secs))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m {s:02d}s"


def get_source_size(source: Path):
    try:
        if not os.path.isfile(source):
            name = os.path.basename(str(source).rstrip("/"))
            with open(f"/sys/class/block/{name}/size") as f:
                return int(f.read().strip()) * 512
        return os.stat(source).st_size
    except OSError:
        return None


def pid_io_stats(pid: int) -> dict | None:
    try:
        vals = {}
        with open(f"/proc/{pid}/io") as f:
            for line in f:
                k, _, v = line.partition(":")
                try:
                    vals[k] = int(v)
                except ValueError:
                    pass
        return vals
    except OSError:
        return None


CSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
CHARSET_RE = re.compile(r"\x1b[()][@A-B0-9]")


def scrub(line: str) -> str:
    line = CHARSET_RE.sub("", line)
    line = CSI_RE.sub("", line)
    line = line.replace("\x1b", "")
    return "".join(c for c in line if c.isprintable()).strip()


def log_carved_count(log_file: Path) -> int:
    try:
        names = set()
        for line in log_file.read_text(errors="ignore").splitlines():
            for m in re.finditer(r"/(f[0-9]+\.[a-zA-Z0-9]+)\s", line):
                names.add(m.group(1))
        return len(names)
    except OSError:
        return 0


# ================================================================ ownership
INVOKE_UG = None


def invoking_ug():
    global INVOKE_UG
    if INVOKE_UG is None:
        su = os.environ.get("SUDO_USER")
        if not su or su == "root":
            INVOKE_UG = (-1, -1)
        else:
            import pwd
            try:
                INVOKE_UG = (pwd.getpwnam(su).pw_uid, pwd.getpwnam(su).pw_gid)
            except KeyError:
                INVOKE_UG = (-1, -1)
    return INVOKE_UG


def chown_one(path: Path) -> None:
    uid, gid = invoking_ug()
    if uid < 0:
        return
    try:
        os.chown(path, uid, gid)
    except OSError:
        pass


def chown_to_invoking_user(*paths: Path) -> None:
    uid, gid = invoking_ug()
    if uid < 0:
        return
    for p in paths:
        for q in [p, *p.rglob("*")]:
            chown_one(q)


# ================================================================ sort worker
def process_file(path: Path, output_dir: Path, args, stats: Stats, dry: bool,
                 bus: Bus) -> None:
    try:
        ext = path.suffix.lstrip(".").lower()

        if ext in SVG_EXTS:                      # svg junk: delete outright
            if not dry:
                path.unlink()
            stats.bump("svg_removed")
            bus.emit_sort("svg", f"deleted  {path}")
            return

        if ext in ICON_SCAN_EXTS and IDENTIFY_BINARY:
            dim = subprocess.run(
                [IDENTIFY_BINARY, "-ping", "-format", "%w %h", str(path)],
                capture_output=True, text=True)
            try:
                w, h = (int(x) for x in dim.stdout.split())
            except ValueError:
                w = h = None
            if w is not None and w == h and max(w, h) <= args.icon_size:
                dest_dir = output_dir / "suspect-icons"
                if dry:
                    bus.emit_sort("icon", f"{w}x{h} {path} -> suspect-icons")
                    stats.add_ext("suspect-icons")
                    return
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest = unique_destination(dest_dir, path.name)
                if dest.name != path.name:
                    stats.bump("renamed")
                shutil.move(str(path), str(dest))
                chown_one(dest_dir)
                chown_one(dest)
                stats.add_ext("suspect-icons")
                stats.bump("quarantined")
                bus.emit_sort("icon", f"{w}x{h} {dest}  <-  {path}")
                return

        name_ext = ext or "no_extension"
        if dry:
            bus.emit_sort("sort", f"{name_ext}/{path.name}  <-  {path}")
            stats.add_ext(name_ext)
            return
        dest_dir = output_dir / name_ext
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = unique_destination(dest_dir, path.name)
        if dest.name != path.name:
            stats.bump("renamed")
        shutil.move(str(path), str(dest))
        chown_one(dest_dir)
        chown_one(dest)
        stats.add_ext(name_ext)
        bus.emit_sort("sort", f"{name_ext}/{dest.name}  <-  {path}")
    except Exception as exc:                     # never kill the worker
        stats.bump("errors")
        bus.emit_sort("error", f"{path}: {exc}")


# ================================================================ scanner
def scan_and_sort(staging_dirs, output_dir: Path, args, stats: Stats,
                  carve_done, quit_event, pool: ThreadPoolExecutor, bus: Bus):
    """Scanner thread: dispatches a file once photorec stops writing it."""
    stable_secs = args.stable_secs
    dry = args.dry_run
    pending = {}                                  # path -> (size, mtime_ns, seen)

    while True:
        stopping = quit_event.is_set()
        found = []
        for base in staging_dirs:
            try:
                for p in base.rglob("*"):
                    if p.is_file() and p.name not in ("photorec.log", "report.xml",
                                                      "report.txt"):
                        found.append(p)
            except OSError:
                pass
        now = time.time()
        for p in found:
            if p not in pending:
                try:
                    st = p.stat()
                except OSError:
                    continue
                pending[p] = (st.st_size, st.st_mtime_ns, now)
        still = []
        for p, (size, mtime, seen) in list(pending.items()):
            try:
                st = p.stat()
            except OSError:
                del pending[p]
                continue
            if st.st_size == size and st.st_mtime_ns == mtime:
                stable = 0 if (stopping or carve_done.is_set()) else stable_secs
                if now - seen >= stable:
                    del pending[p]
                    still.append(p)
            else:
                pending[p] = (st.st_size, st.st_mtime_ns, now)
        for p in still:
            stats.bump("dispatched")
            pool.submit(process_file, p, output_dir, args, stats, dry, bus)
        if stopping and not pending:
            return
        if not stopping and carve_done.is_set() and not pending:
            return
        time.sleep(POLL_INTERVAL)


# ================================================================ monitor
def monitor_progress(pid, source: Path, stats: Stats, carve_done, quit_event,
                     status_secs: int, prog: ProgressState, bus: Bus):
    """Samples /proc/<pid>/io of photorec: TUI gets snapshots, plain lines."""
    WAIT = min(5, max(1, status_secs))
    size = get_source_size(source)
    samples = []
    start = time.time()
    last_print = start
    base = None
    while not carve_done.is_set() and not quit_event.is_set():
        time.sleep(WAIT)
        io = pid_io_stats(pid)
        now = time.time()
        if io is None:                           # process gone
            return
        rb = io.get("rchar", io.get("read_bytes", 0))
        if base is None:
            base = rb
        samples.append((now, rb))
        if len(samples) == 1:
            continue
        pos = rb - base
        t0, b0 = samples[0]
        rate = (samples[-1][1] - b0) / max(now - t0, 0.001)
        elapsed = now - start
        eta = (size - pos) / rate if (size and rate > 0) else None
        prog.update(pos=pos, size=size, rate=rate, eta=eta, elapsed=elapsed)
        if bus.tui:
            continue
        if now - last_print < status_secs:
            continue
        last_print = now
        if size:
            print(f"[status] read {human_bytes(pos)} / {human_bytes(size)} "
                  f"({min(pos / size, 1.0) * 100:5.1f}%)  {human_bytes(rate)}/s  "
                  f"ETA {fmt_eta(eta)}   carved: {stats.dispatched}   "
                  f"[{fmt_eta(elapsed)} elapsed]", flush=True)
        else:
            print(f"[status] read {human_bytes(pos)} (size unknown)  "
                  f"{human_bytes(rate)}/s   carved: {stats.dispatched}   "
                  f"[{fmt_eta(elapsed)} elapsed]", flush=True)


# ================================================================ photorec
def enable_tokens(skip: str):
    skip_set = {s.strip().lower() for s in skip.split(",") if s.strip()}
    unknown = skip_set - set(PHOTOREC_FORMATS)
    if unknown:
        print(f"note: --skip token(s) not in whitelist (already not carved): "
              f"{', '.join(sorted(unknown))}", file=sys.stderr)
    formats = [f for f in PHOTOREC_FORMATS if f not in skip_set]
    if not formats:
        return None, "nothing left to carve after --skip"
    return ["fileopt", "everything,disable"] + [f"{f},enable" for f in formats] + \
        ["search"], None


def run_photorec(source: Path, session_dir: Path, args, stats: Stats,
                 carve_done, quit_event, prog: ProgressState, bus: Bus):
    tokens, err = enable_tokens(args.skip)
    if err or tokens is None:
        bus.emit_pipeline(err or "could not build whitelist")
        carve_done.set()
        return
    log_file = session_dir / "photorec.log"
    cmd = [PHOTOREC_BINARY, "/log", "/logname", str(log_file),
           "/d", str(session_dir / "carved"), "/cmd", str(source),
           ",".join(tokens)]
    wl = ", ".join(t[:-7] for t in tokens[2:-1]) + ", search"
    bus.emit_pipeline(f"carving {source} -> {session_dir}")
    bus.emit_pipeline(f"carving whitelist: {wl}")
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
    except Exception as exc:
        bus.emit_pipeline(f"failed to start photorec: {exc}")
        carve_done.set()
        return
    threading.Thread(
        target=monitor_progress,
        args=(proc.pid, source, stats, carve_done, quit_event,
              args.status_secs, prog, bus),
        daemon=True).start()

    interesting = re.compile(r"(error|warning|syntax|abort)", re.I)
    reader_stop = threading.Event()

    def pump_stdout():                       # reader thread -> bus
        try:
            for raw in proc.stdout:
                s = scrub(raw)
                if not s:
                    continue
                if interesting.search(s):
                    bus.emit_photo(s)        # errors always shown
                else:
                    bus.emit_photo_throttled(s, 2.0)
        except (ValueError, OSError):
            pass
        reader_stop.set()

    threading.Thread(target=pump_stdout, daemon=True).start()

    # wait until photorec exits on its own OR the user asked to stop
    while proc.poll() is None and not reader_stop.is_set():
        if quit_event.is_set():
            proc.terminate()
            break
        try:
            proc.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            pass
    if quit_event.is_set() and proc.poll() is None:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    elif proc.poll() is None:
        proc.wait()
    reader_stop.wait(timeout=3)
    carved_total = log_carved_count(log_file)
    bus.emit_pipeline(f"photorec finished. {carved_total} file(s) carved "
                      f"({'stopped by user' if quit_event.is_set() else 'complete'})")
    carve_done.set()


# ================================================================ TUI
class TUI:
    """Std-curses UI. Renderer thread is the main thread; workers feed Bus."""

    def __init__(self, bus: Bus, stats: Stats, prog: ProgressState,
                 quit_event: threading.Event, left_label: str):
        self.bus = bus
        self.stats = stats
        self.prog = prog
        self.quit_event = quit_event
        self.left_label = left_label
        self.offset = 0                          # 0 == follow
        self.stdscr = None
        self.color_ok = False
        loc = os.environ.get("LC_ALL") or os.environ.get("LANG") or ""
        self.uni = "utf" in loc.lower()
        self.b = "┌┐└┘─│" if self.uni else "+-+|- "
        self.b = self.b[:6]

    # -- lifecycle
    def __enter__(self):
        import locale
        try:
            locale.setlocale(locale.LC_ALL, "")
        except locale.Error:
            pass
        self.stdscr = curses.initscr()
        try:
            curses.start_color()
            curses.use_default_colors()
            self.color_ok = True
            for i, fn in ((1, curses.COLOR_GREEN), (2, curses.COLOR_YELLOW),
                          (3, curses.COLOR_MAGENTA), (4, curses.COLOR_RED),
                          (5, curses.COLOR_CYAN), (6, curses.COLOR_BLUE)):
                curses.init_pair(i, fn, -1)
        except curses.error:
            self.color_ok = False
        curses.curs_set(0)
        self.stdscr.nodelay(True)
        self.stdscr.keypad(True)
        curses.cbreak()
        curses.noecho()
        return self

    def __exit__(self, *exc):
        try:
            curses.curs_set(1)
        except curses.error:
            pass
        curses.endwin()
        return False

    def attr(self, pair, bold=False):
        a = 0
        if self.color_ok:
            a |= curses.color_pair(pair)
        if bold:
            a |= curses.A_BOLD
        return a

    def put(self, y, x, w, text, attr=0):
        if y < 0 or y >= curses.LINES or x >= curses.COLS:
            return
        try:
            self.stdscr.addnstr(y, x, str(text), max(0, w))
        except curses.error:
            pass

    def hline(self, y, x, n, ch, attr=0):
        self.put(y, x, n, ch * max(0, n), attr)

    def vline(self, y, x, n, ch, attr=0):
        for i in range(max(0, n)):
            self.put(y + i, x, 1, ch, attr)

    def box(self, y, x, h, w, title="", pair=0):
        if h < 2 or w < 4:
            return
        a = self.attr(pair)
        tl, tr, bl, br, hor, vert = (self.b[0], self.b[1], self.b[2],
                                     self.b[3], self.b[4], self.b[5])
        a_bold = a | curses.A_BOLD
        self.put(y, x, 1, tl, a_bold)
        self.put(y, x + w - 1, 1, tr, a_bold)
        self.put(y + h - 1, x, 1, bl, a_bold)
        self.put(y + h - 1, x + w - 1, 1, br, a_bold)
        self.hline(y, x + 1, w - 2, hor, pair)
        self.hline(y + h - 1, x + 1, w - 2, hor, pair)
        self.vline(y + 1, x, h - 2, vert, pair)
        self.vline(y + 1, x + w - 1, h - 2, vert, pair)
        if title:
            self.put(y, x + 2, w - 4, f" {title} ", a_bold)

    # -- keys (return True to exit)
    def key(self, ch, content_h) -> bool:
        if ch == -1:
            return False
        if ch == curses.KEY_RESIZE:
            curses.update_lines_cols()
            return False
        if ch in (ord("q"), ord("Q")):
            self.quit_event.set()
            self.bus.dirty.set()
            return True
        if ch == curses.KEY_UP:
            self.offset += 1
        elif ch == curses.KEY_DOWN:
            self.offset = max(0, self.offset - 1)
        elif ch == curses.KEY_PPAGE:
            self.offset += max(1, content_h)
        elif ch == curses.KEY_NPAGE:
            self.offset = max(0, self.offset - max(1, content_h))
        elif ch in (ord("f"), ord("F")):
            self.offset = 0
        elif ch in (ord("s"), ord("S")):
            self.bus.paused = not self.bus.paused
            self.bus.dirty.set()
        elif ch in (ord("r"), ord("R")):
            self.bus.dirty.set()
        return False

    # -- frame
    def frame(self):
        scr, cols, lines = self.stdscr, curses.COLS, curses.LINES
        scr.erase()
        HEADER_H, FOOTER_H = 4, 1
        if lines < 12 or cols < 46:
            self.put(0, 0, cols - 1,
                     "Terminal too small — need at least 46 columns x 12 lines",
                     curses.A_BOLD)
            scr.refresh()
            return
        pane_y = HEADER_H
        pane_h = lines - pane_y - FOOTER_H
        content_h = pane_h - 2
        left_w = max(30, cols // 2)
        right_w = cols - left_w

        # header: identity + progress + counters
        self.box(0, 0, 4, cols, "photorec-pipeline", 5)
        ident = (f" src {self.left_label} ", self.attr(0))
        self.put(0, 2, cols - 4, ident[0])
        p = self.prog.snapshot()
        self.put(1, 2, cols - 4, self.progress_text(p), self.attr(5, bold=True))
        moved, _ = self.stats.moved
        chips = (f"carved {self.stats.dispatched}   sorted {moved}   "
                 f"icon? {self.stats.quarantined}   svg ✖ {self.stats.svg_removed}   "
                 f"renamed {self.stats.renamed}   err {self.stats.errors}"
                 + ("   [ photorec feed PAUSED ]" if self.bus.paused else ""))
        self.put(2, 2, cols - 4, chips, self.attr(0, bold=True))

        # left: photorec feed
        self.box(pane_y, 0, pane_h, left_w, f"photorec feed  ({len(self.bus.photo)} lines, "
                                            "s pauses)", 6)
        with self.bus._lock:
            photo = list(self.bus.photo)
        plen = len(photo)
        pbase = max(0, plen - content_h)
        for i in range(content_h):
            idx = pbase + i
            if idx < plen:
                ts, text = photo[idx]
                pair = 4 if re.search(r"(?i)error|syntax|abort", text) else 0
                self.put(pane_y + 1 + i, 2, left_w - 4, f"{ts} {text}",
                         self.attr(pair, bold=bool(pair)))

        # right: sorter flow (scrollable, follows by default)
        with self.bus._lock:
            sitems = list(self.bus.sort)
        total = len(sitems)
        follow = self.offset == 0
        start = max(0, total - content_h) if follow else min(self.offset - 1,
                                                             max(0, total - content_h))
        title = f"sorter flow  ({total} events, {'following' if follow else 'scrolled'})"
        self.box(pane_y, left_w, pane_h, right_w, title, 1 if follow else 2)
        for i in range(content_h):
            idx = start + i
            if 0 <= idx < total:
                kind, ts, text = sitems[idx]
                pair = colorsort.get(kind, 0)
                self.put(pane_y + 1 + i, left_w + 2, right_w - 4, f"{ts} {text}",
                         self.attr(pair, bold=(kind == "error")))

        # footer
        keys = (" q quit | OCRPTY GCC-C: widen? " if False else
                " q quit | arrows scroll | f follow | s pause feed | r redraw ")
        self.put(lines - 1, 0, cols - len(keys), "", 0)
        self.put(lines - 1, cols - len(keys), len(keys) - 0, keys,
                 self.attr(0) | curses.A_REVERSE)
        scr.refresh()

    def progress_text(self, p) -> str:
        size, pos, rate = p["size"], p["pos"], p["rate"]
        if size:
            return (f"read {human_bytes(pos)} / {human_bytes(size)} "
                    f"({min(pos / size, 1.0) * 100:5.1f}%)  {human_bytes(rate)}/s  "
                    f"ETA {fmt_eta(p['eta'])}  [{fmt_eta(p['elapsed'])} elapsed]")
        return (f"read {human_bytes(pos)} (size unknown)  {human_bytes(rate)}/s  "
                f"[{fmt_eta(p['elapsed'])} elapsed]")

    def run(self) -> int:
        with self:
            while True:
                ch = self.stdscr.getch()
                content_h = max(1, curses.LINES - 7)
                quit_hit = self.key(ch, content_h)
                if self.quit_event.is_set():
                    return 0
                self.frame()
                # consume any remaining buffered keys quickly
                for _ in range(30):
                    ch = self.stdscr.getch()
                    if ch == -1:
                        break
                    if self.key(ch, content_h):
                        return 0
                self.bus.dirty.wait(0.12)
                self.bus.dirty.clear()


# ================================================================ confirm
def confirm(args, do_carve, formats_shown):
    if args.yes or args.dry_run:
        return
    if do_carve:
        print(f"Source   : {args.source}  (photorec is read-only on the source)")
        print(f"Staging  : {args.staging}")
        print(f"Output   : {args.output}")
        print(f"Whitelist: {formats_shown}")
        if args.skip:
            print(f"Excluded : {args.skip}")
        print("svg recovered files will be deleted; square low-res images quarantined.")
    print("Proceed? [y/N] ", end="", flush=True)
    if input().strip().lower() not in ("y", "yes"):
        print("Aborted. Nothing was changed.")
        sys.exit(1)


# ================================================================ main
def main(argv=None):
    args = parse_args(argv)
    output_dir = Path(args.output).expanduser().resolve()
    staging_dir = Path(args.staging).expanduser().resolve()

    toks, err = enable_tokens(args.skip)
    if err or toks is None:
        print(f"error: {err}", file=sys.stderr)
        return 1
    formats_shown = ", ".join(t[:-7] for t in toks[2:])

    if args.sort_only:
        src = args.sort_only if args.sort_only != "__default__" else None
        if src is None:
            sessions = [d for d in staging_dir.glob("carved-*") if d.is_dir()]
            if not sessions:
                print(f"error: no carved-* session dirs under {staging_dir}",
                      file=sys.stderr)
                return 1
            src = max(sessions, key=lambda c: c.stat().st_mtime)
            print(f"[sort-only] using newest session: {src}", flush=True)
        src = Path(src).expanduser().resolve()
        if not src.is_dir():
            print(f"error: staging dir not found: {src}", file=sys.stderr)
            return 1
        staging_dirs, do_carve = [src], False
        source_label = f"(sort-only) {src}"
    else:
        if not args.source:
            print("error: a source device/image is required (or use --sort-only)",
                  file=sys.stderr)
            return 1
        if not PHOTOREC_BINARY:
            print("error: photorec not found in PATH", file=sys.stderr)
            return 1
        if not os.path.exists(str(args.source)):
            print(f"error: source device/disk image not found: {args.source}",
                  file=sys.stderr)
            return 1
        if os.geteuid() != 0 and str(args.source).startswith("/dev/"):
            print("error: carving a device requires root:\n"
                  "  sudo python3 photorec_pipeline.py ...", file=sys.stderr)
            return 2
        session = f"carved-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        session_dir = staging_dir / session
        staging_dirs, do_carve = [session_dir], True
        source_label = str(args.source)
        if not args.dry_run:
            session_dir.mkdir(parents=True, exist_ok=True)

    confirm(args, do_carve, formats_shown)
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    tui = (curses is not None and not args.no_tui
           and sys.stdout.isatty() and sys.stdin.isatty())
    bus = Bus(tui=tui)
    stats = Stats()
    prog = ProgressState()
    pool = ThreadPoolExecutor(max_workers=args.workers)
    carve_done = threading.Event()
    if not do_carve:
        carve_done.set()          # sort-only: staging is already complete
    quit_event = threading.Event()

    threads = []
    if do_carve:
        threads.append(threading.Thread(
            target=run_photorec,
            args=(Path(args.source).resolve(), staging_dirs[0], args, stats,
                  carve_done, quit_event, prog, bus), daemon=True))
    threads.append(threading.Thread(
        target=scan_and_sort,
        args=(staging_dirs, output_dir, args, stats, carve_done, quit_event,
              pool, bus), daemon=True))
    for t in threads:
        t.start()

    def drain():
        for t in threads:
            t.join(timeout=86400)
        pool.shutdown(wait=True)
        if not args.dry_run:
            chown_to_invoking_user(output_dir, *staging_dirs)

    try:
        if tui:
            label = ("carving-session " + staging_dirs[0].name
                     if do_carve else "sort-only " + staging_dirs[0].name)
            ui = TUI(bus, stats, prog, quit_event, f"{source_label}  |  {label}")
            ui.run()                     # blocks until q or a key-quit
            drain()
        else:
            try:
                while any(t.is_alive() for t in threads):
                    time.sleep(0.5)
            except KeyboardInterrupt:
                quit_event.set()
                print("\n[interrupt] draining sorter, stopping photorec...",
                      file=sys.stderr, flush=True)
                drain()
    finally:
        pass

    moved, exts = stats.moved
    print("\n--- summary ---")
    if do_carve:
        print(f"total carved by photorec: {stats.dispatched}")
    else:
        print(f"total files processed  : {stats.dispatched}")
    for name, count in sorted(exts.items(), key=lambda x: (-x[1], x[0])):
        print(f"{name:<16} {count:>6} file(s)  ->  {output_dir / name}")
    print(f"total sorted           : {moved}")
    print(f"suspect icons quarantined : {stats.quarantined}")
    print(f"svg deleted            : {stats.svg_removed}")
    print(f"duplicates renamed     : {stats.renamed}")
    print(f"errors                 : {stats.errors}")
    if quit_event.is_set():
        print("(stopped by user; staged leftovers ready for --sort-only)")
    if moved == 0 and not exts:
        print("nothing was sorted — check staging folder content")
    return 0


if __name__ == "__main__":
    sys.exit(main())
