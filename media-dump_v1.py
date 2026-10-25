#!/usr/bin/env python3
"""
media-dump-v1 — photo-recovery pipeline with live streaming sorting.

Threads:
  * photorec carves media-only formats (white-listed formats only) from a
    device/disk image into a staging dir. A full carve can take hours.
  * a scanner thread polls the staging dir in the background and hands every
    file to worker threads THE MOMENT it is fully written — sorting happens
    concurrently with carving, so your sorted folders fill up in real time.

Sorting rules (applied to each file as soon as it lands in staging):
  - svg    -> deleted outright (UI glyphs / iconpack fragments; Photorec
              "svg" carves are almost entirely false-positive text junk)
  - probable icon: square image with max side <= --icon-size (default 256)
           -> moved to <output>/suspect-icons/ for review
  - else   -> moved to <output>/<extension>/ (collisions auto-renamed)

Usage:
  media-dump-v1 [--yes] [options] <device-or-image>
    (device sources re-exec themselves through sudo automatically — the
     password is asked once, right after launch; disk images need no sudo)

  media-dump-v1 /dev/sdb1 --yes
  media-dump-v1 ~/photorec/phone.dd --yes --ext
  # sorting-only pass over previously carved files (never asks sudo):
  media-dump-v1 --sort-only [staging_dir_or_session_dir]
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DEFAULT_OUTPUT = "~/photorec-sorted"
DEFAULT_STAGING = "~/photorec"
STABLE_SECS = 10      # file must stay unchanged this long to count as fully written
POLL_INTERVAL = 2     # scanner poll seconds
PHOTOREC_BINARY = shutil.which("photorec")
IDENTIFY_BINARY = shutil.which("identify")

# Photorec fileopt tokens (by photorec's own fileopt menu names).
# NB: photorec whitelists by FORMAT FAMILY, not extension:
#   "riff" -> wav / avi / webp / cda / qcp   (file_riff.c family)
#   "mov"  -> mov / mp4 / 3gp / heic / m4p   (file_mov.c family)
#   "ogg"  -> ogg / ogm / ogv                (file_ogg.c family)
PHOTOREC_FORMATS = [
    # images
    "jpg", "png", "gif", "bmp", "riff", "tif",
    # video: mov token covers mov/mp4/3gp/heic/m4p
    "mov", "mpg", "m2ts", "ts", "mkv", "flv", "asf", "dv",
    # audio: ogg token covers ogg/ogm/ogv
    "mp3", "aif", "ape", "flac", "ogg", "amr",
    "au", "gsm", "dss", "ra",
]
# image extensions that get the square/size icon heuristic applied
ICON_SCAN_EXTS = {"jpg", "jpeg", "png", "webp", "bmp", "gif", "tif", "tiff", "heic"}
SVG_EXTS = {"svg"}


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Carve media from a device/disk image with photorec and sort it live.",
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", nargs="?", default=None,
                   help="Device, partition or disk image. Omitted only with --sort-only.")
    p.add_argument("--output", default=DEFAULT_OUTPUT,
                   help=f"Sorted output folder (default: {DEFAULT_OUTPUT})")
    p.add_argument("--staging", default=DEFAULT_STAGING,
                   help=f"Parent folder for carved session dirs (default: {DEFAULT_STAGING})")
    p.add_argument("--icon-size", type=int, default=256,
                   help="Max side (px) of square images treated as icons (default 256)")
    p.add_argument("--workers", type=int, default=4,
                   help="Number of sorting worker threads (default 4)")
    p.add_argument("--stable-secs", type=int, default=STABLE_SECS,
                   help="Seconds a file must stay unchanged to be considered fully "
                        "written by photorec (default 10)")
    p.add_argument("--ext", action="store_true",
                   help="Force ext2/3/4 filesystem carving mode (use for ext4 disk images)")
    p.add_argument("--freespace", action="store_true",
                   help="Scan only unallocated space (default scans whole partition)")
    p.add_argument("--dry-run", action="store_true",
                   help="Sorting stages only: print actions, move/delete nothing. "
                        "Carving itself is never simulated.")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    p.add_argument("--skip", default="", metavar="FORMATS",
                   help="Comma-separated photorec tokens to EXCLUDE from carving, "
                        "e.g. --skip png,gif (iconpack/UI glyph junk)")
    p.add_argument("--status-secs", type=int, default=15,
                   help="Seconds between [status] progress lines (default 15)")
    p.add_argument("--sort-only", nargs="?", const="__default__", default=None,
                   metavar="SRC",
                   help="Skip photorec. Sort an existing staging iterable — give the "
                        "session dir, or leave empty to use the newest carved-* dir "
                        f"under --staging (default {DEFAULT_STAGING})")
    return p.parse_args(argv)


def unique_destination(dest_dir: Path, name: str) -> Path:
    """report.jpg -> report_2.jpg -> report_3.jpg (never overwrites)."""
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


class Stats:
    """Thread-safe counters."""

    def __init__(self):
        self._lock = threading.Lock()
        self._ext = {}
        self.quarantined = 0
        self.svg_removed = 0
        self.renamed = 0
        self.errors = 0
        self.carved = 0

    def add_ext(self, name, count=1):
        with self._lock:
            self._ext[name] = self._ext.get(name, 0) + count

    def bump(self, attr):
        with self._lock:
            setattr(self, attr, getattr(self, attr) + 1)

    @property
    def moved(self):
        with self._lock:
            return sum(self._ext.values()), dict(self._ext)


def process_file(path: Path, output_dir: Path, args, stats: Stats, dry: bool) -> None:
    try:
        ext = path.suffix.lstrip(".").lower()

        # 1) svg: delete outright, user-approved policy
        if ext in SVG_EXTS:
            if not dry:
                path.unlink()
            stats.bump("svg_removed")
            print(f"[svg-del] deleted  {path}", flush=True)
            return

        # 2) icon heuristic: square and small
        if ext in ICON_SCAN_EXTS and IDENTIFY_BINARY:
            dim = subprocess.run(
                [IDENTIFY_BINARY, "-ping", "-format", "%w %h", str(path)],
                capture_output=True, text=True)
            try:
                w, h = (int(x) for x in dim.stdout.split())
            except ValueError:
                w = h = None  # corrupted/mangled carve -> treat as normal file
            if w is not None and w == h and max(w, h) <= args.icon_size:
                dest_dir = output_dir / "suspect-icons"
                if dry:
                    print(f"[icon-?  ] {w}x{h} {path} -> suspect-icons", flush=True)
                    stats.add_ext("suspect-icons")
                    return
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest = unique_destination(dest_dir, path.name)
                if dest.name != path.name:
                    stats.bump("renamed")
                shutil.move(str(path), str(dest))
                stats.add_ext("suspect-icons")
                stats.bump("quarantined")
                print(f"[icon-?  ] {w}x{h} {dest}  <-  {path}", flush=True)
                return

        # 3) normal case: sort into per-extension folder
        name_ext = ext or "no_extension"
        if dry:
            print(f"[sort    ] {name_ext}/{path.name}  <-  {path}", flush=True)
            stats.add_ext(name_ext)
            return
        dest_dir = output_dir / name_ext
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = unique_destination(dest_dir, path.name)
        if dest.name != path.name:
            stats.bump("renamed")
        shutil.move(str(path), str(dest))
        stats.add_ext(name_ext)
        print(f"[sort    ] {name_ext}/{dest.name}  <-  {path}", flush=True)
    except Exception as exc:  # never let one bad file kill the worker thread
        stats.bump("errors")
        print(f"[error   ] {path}: {exc}", file=sys.stderr, flush=True)


def scan_and_sort(staging_dirs, output_dir: Path, args, stats: Stats,
                  carve_done: threading.Event, pool: ThreadPoolExecutor) -> None:
    """Scanner thread. Dispatches a file to the pool once photorec stops writing it.

    A file counts as complete when its (size, mtime_ns) stays unchanged for
    --stable-secs seconds. Once photorec has exited, leftovers are dispatched
    immediately.
    """
    stable_secs = args.stable_secs
    dry = args.dry_run
    pending: dict = {}   # path -> (size, mtime_ns, first_stable_seen)

    while True:
        found = []
        for base in staging_dirs:
            try:
                for p in base.rglob("*"):
                    # skip photorec's own bookkeeping files
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
        for p, (size, mtime, seen_at) in list(pending.items()):
            try:
                st = p.stat()
            except OSError:
                del pending[p]   # vanished
                continue
            if st.st_size == size and st.st_mtime_ns == mtime:
                stable = (now - seen_at) >= (0 if carve_done.is_set() else stable_secs)
                if stable:
                    del pending[p]
                    still.append(p)
            else:
                pending[p] = (st.st_size, st.st_mtime_ns, now)  # still being written
        for p in still:
            stats.bump("carved")
            pool.submit(process_file, p, output_dir, args, stats, dry)
        if carve_done.is_set() and not pending:
            return
        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------- formats
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


def get_source_size(source: Path) -> int | None:
    """Source size in bytes: file -> stat size; block device -> sysfs size."""
    try:
        if not os.path.isfile(source):
            name = os.path.basename(str(source).rstrip("/"))
            with open(f"/sys/class/block/{name}/size") as f:
                return int(f.read().strip()) * 512
        return os.stat(source).st_size
    except OSError:
        return None


def pid_io_stats(pid: int) -> dict | None:
    """Read /proc/<pid>/io as a dict. rchar counts every read() byte (device,
    image file, cache) — the honest progress metric for carving."""
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


CSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")        # ncurses CSI sequences
CHARSET_RE = re.compile(r"\x1b[()][@A-B0-9]")            # charset switches


def scrub(line: str) -> str:
    """Remove ncurses escape sequences that photorec prints even in script mode."""
    line = CHARSET_RE.sub("", line)
    line = CSI_RE.sub("", line)
    line = line.replace("\x1b", "")
    line = "".join(c for c in line if c.isprintable())
    return line.strip()


def log_carved_count(log_file: Path) -> int:
    """Count individual carved files from photorec's own log (lines look like
    .../recup_dir.1/f0006778.jpg\t 6778-6785)."""
    try:
        names = set()
        for line in log_file.read_text(errors="ignore").splitlines():
            for m in re.finditer(r"/(f[0-9]+\.[a-zA-Z0-9]+)\s", line):
                names.add(m.group(1))
        return len(names)
    except OSError:
        return 0


def monitor_progress(pid, source: Path, args, stats: Stats,
                     carve_done: threading.Event):
    """Prints [status] progress lines: bytes read, rate, ETA, carved count."""
    WAIT = min(5, max(1, args.status_secs))
    size = get_source_size(source)
    samples = []                 # (time, rchar_bytes)
    start = time.time()
    last_print = start
    base = None                  # io bytes at monitor start
    while not carve_done.is_set():
        time.sleep(WAIT)
        io = pid_io_stats(pid)
        now = time.time()
        if io is None:           # photorec process gone / io unreadable
            return
        rb = io.get("rchar", io.get("read_bytes", 0))
        if base is None:
            base = rb
        samples.append((now, rb))
        if len(samples) < 2:
            continue
        pos = rb - base
        t0, b0 = samples[0]
        rate = (samples[-1][1] - b0) / max(now - t0, 0.001)
        elapsed = now - start
        eta = (size - pos) / rate if (size and rate > 0) else None
        if now - last_print < args.status_secs:
            continue
        last_print = now
        if size:
            print(f"[status] read {human_bytes(pos)} / {human_bytes(size)} "
                  f"({min(pos / size, 1.0) * 100:5.1f}%)  {human_bytes(rate)}/s  "
                  f"ETA {fmt_eta(eta)}   carved: {stats.carved}   "
                  f"[{fmt_eta(elapsed)} elapsed]", flush=True)
        else:
            print(f"[status] read {human_bytes(pos)} (size unknown)  "
                  f"{human_bytes(rate)}/s   carved: {stats.carved}   "
                  f"[{fmt_eta(elapsed)} elapsed]", flush=True)


# ---------------------------------------------------------------- photorec
def sudo_aware_default(sub: str) -> str:
    """Default folders live in the *invoking* user's home even when run via sudo."""
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


def enable_tokens(skip: str):
    """fileopt whitelist tokens after removing --skip entries."""
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
                 carve_done: threading.Event):
    """Thread: run photorec scripted, stream its stdout; set carve_done on exit."""
    tokens, err = enable_tokens(args.skip)
    if err or tokens is None:
        print(f"[pipeline] {err}", file=sys.stderr, flush=True)
        carve_done.set()
        return
    log_file = session_dir / "photorec.log"
    cmd = [PHOTOREC_BINARY, "/log", "/logname", str(log_file),
           "/d", str(session_dir / "carved"), "/cmd", str(source),
           ",".join(tokens)]
    wl = ", ".join(t[:-7] for t in tokens[2:-1]) + ", search"
    print(f"[pipeline] carving {source} -> {session_dir}", flush=True)
    print(f"[pipeline] carving whitelist: {wl}", flush=True)
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
    except Exception as exc:
        print(f"[pipeline] failed to start photorec: {exc}", file=sys.stderr,
              flush=True)
        carve_done.set()
        return
    threading.Thread(target=monitor_progress,
                     args=(proc.pid, source, args, stats, carve_done),
                     daemon=True).start()

    interesting = re.compile(r"(error|warning|syntax|abort)", re.I)
    for raw in proc.stdout:
        s = scrub(raw)
        if not s:
            continue
        if interesting.search(s):
            print(f"[photorec] {s}", flush=True)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    proc.wait()
    carved_total = log_carved_count(log_file)
    print(f"[pipeline] photorec finished. {carved_total} file(s) carved.",
          flush=True)
    carve_done.set()


# ---------------------------------------------------------------- chown fix
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
    """Hand files created as root back to the invoking user at run end."""
    uid, gid = invoking_ug()
    if uid < 0:
        return
    for p in paths:
        for q in [p, *p.rglob("*")]:
            chown_one(q)


# ---------------------------------------------------------------- confirm
def confirm(args, media_count=0, other_count=0):
    if args.yes or args.dry_run:
        return
    if hasattr(args, "source") and args.source:
        print(f"Source   : {args.source}  (photorec is read-only on the source)")
        print(f"Staging  : {args.staging}")
        print(f"Output   : {args.output}")
        print(f"Whitelist: {', '.join(PHOTOREC_FORMATS)}")
        if getattr(args, "skip", None):
            print(f"Excluded : {args.skip}")
        print("svg recovered files will be deleted; square low-res images quarantined.")
    print("Proceed? [y/N] ", end="", flush=True)
    if input().strip().lower() not in ("y", "yes"):
        print("Aborted. Nothing was changed.")
        sys.exit(1)


# ---------------------------------------------------------------- main
def reexec_via_sudo() -> None:
    """Re-run this script as root with the same args (device sources only).

    Lets you type `media-dump-v1 /dev/sda ...` without a sudo
    prefix: sudo asks for the password once right after launch, then the
    whole run continues as root. SUDO_USER is still set, so all results
    are chowned back to the invoking user at the end.
    """
    argv0 = sys.argv[0]
    if os.sep not in argv0:                    # invoked via $PATH
        argv0 = shutil.which(argv0) or argv0
    script = os.path.realpath(argv0)
    sudo = shutil.which("sudo")
    if not sudo:
        print("error: device source needs root but `sudo` is not installed — "
              "run this script as root another way.", file=sys.stderr)
        sys.exit(2)
    print("[pipeline] device source needs root — re-executing via sudo "
          "(password is asked once)...", file=sys.stderr, flush=True)
    cmd = [sudo, "--", (sys.executable or "python3"), script, *sys.argv[1:]]
    os.execvp(sudo, cmd)                       # replaces this process


def main(argv=None):
    args = parse_args(argv)
    # A device source needs root: replace this process with sudo <us>.
    # Disk images are carved fine as the user; --sort-only never escalates.
    if (not args.sort_only and args.source
            and str(args.source).startswith("/dev/") and os.geteuid() != 0):
        reexec_via_sudo()          # replaces the process; never returns
    output_dir = Path(args.output).expanduser().resolve()
    staging_dir = Path(args.staging).expanduser().resolve()

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
        staging_dirs = [src]
        do_carve = False
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
        # root guaranteed for device sources by reexec_via_sudo() above
        session = f"carved-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        session_dir = staging_dir / session
        staging_dirs = [session_dir]
        do_carve = True
        if not args.dry_run:
            session_dir.mkdir(parents=True, exist_ok=True)

    confirm(args)
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    stats = Stats()
    pool = ThreadPoolExecutor(max_workers=args.workers)
    carve_done = threading.Event()
    if not do_carve:
        carve_done.set()          # sort-only: staging is already complete

    threads = []
    if do_carve:
        threads.append(threading.Thread(
            target=run_photorec,
            args=(Path(args.source).resolve(), staging_dirs[0], args, stats,
                  carve_done), daemon=True))
    threads.append(threading.Thread(
        target=scan_and_sort,
        args=(staging_dirs, output_dir, args, stats, carve_done, pool),
        daemon=True))
    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[interrupt] draining sorter; photorec was terminated...",
              file=sys.stderr, flush=True)
        # let scanner wrap up quickly
        carve_done.set()
        for t in threads:
            t.join(timeout=30)
    finally:
        pool.shutdown(wait=True)
        if not args.dry_run:
            chown_to_invoking_user(output_dir, *staging_dirs)

    moved, exts = stats.moved
    print("\n--- summary ---")
    if stats.carved:
        print(f"total carved by photorec: {stats.carved}")
    for name, count in sorted(exts.items(), key=lambda x: (-x[1], x[0])):
        print(f"{name:<16} {count:>6} file(s)  ->  {output_dir / name}")
    print(f"total sorted           : {moved}")
    print(f"suspect icons in output: {stats.quarantined}")
    print(f"svg deleted            : {stats.svg_removed}")
    print(f"duplicates renamed     : {stats.renamed}")
    print(f"errors                 : {stats.errors}")
    if moved == 0 and stats.svg_removed == 0 and stats.quarantined == 0:
        print("nothing was sorted — check staging folder content")
    return 0


if __name__ == "__main__":
    sys.exit(main())
