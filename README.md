# media-dump

Carve media (photos, video, audio) with [PhotoRec](https://www.cgsecurity.org/wiki/PhotoRec) and sort it into clean folders **live — while the carve is still running**.

* **`main` (v2)** — everything below, in a btop-style curses TUI → `media-dump.py`
* **branch `v1`** — the original plain-text version → `media-dump_v1.py` (identical behaviour, status lines instead of a TUI)

## What it does

1. Carves **media only** (images + video + audio; audio deliberately kept) from a device, partition or disk image using photorec's scripting mode — formats are whitelisted through photorec's `fileopt` menu, so non-media files are never carved and never survive.
2. A background scanner watches the staging folder and hands every file to sorting workers **the moment photorec finishes writing it**. Your per-extension folders fill up in real time, hours before a big carve ends.
3. Per-file policy:
   * `svg` → deleted outright (photorec "svg" hits are almost all text/UI-glyph false positives)
   * square image with max side ≤ `--icon-size` (default 256 px) → quarantined into `suspect-icons/` for review (iconpack / system-glyph junk)
   * everything else → `<output>/<extension>/` (collisions auto-renamed, never overwritten)
4. On exit, every file and folder is chowned back to the **invoking user** (sudo-aware) — never left as root.
5. Progress/ETA comes from photorec's own `rchar` counters; the source device/image is only ever **read**.

## Requirements

* python3 — stdlib only, no pip packages (v2's TUI uses `curses`)
* `photorec` (from the `testdisk` package)
* optional: ImageMagick's `identify` for the square-icon heuristic

## Install (optional)

```bash
mkdir -p ~/.local/bin
cp media-dump.py ~/.local/bin/media-dump
# and for v1: cp media-dump_v1.py ~/.local/bin/media-dump-v1
```

(`~/.local/bin` is on `$PATH` by default on Fedora; make sure the copies are `chmod +x`.)

## Usage (v2)

```bash
media-dump /dev/sdb1 --yes             # carve a partition (auto-sudo, password once)
media-dump ~/disk.img --yes --ext      # carve an ext2/3/4 image (no sudo needed)
media-dump /dev/sda --skip png,gif -y  # iconpack-heavy drives
media-dump /dev/sda -y --no-tui        # plain status lines instead
media-dump --sort-only                 # finish sorting a leftover session
```

Device sources re-exec the script through `sudo` **once**, right after launch — the whole run (TUI, photorec, sorting) then executes as root, with all results chowned back to the invoking user. Disk images and `--sort-only` runs never escalate. The classic `sudo python3 media-dump.py ...` style still works unchanged.

TUI keys: `q` = quit (terminates the carve cleanly, drains staged files, prints a summary) · `s` = pause/unpause the live feed.

| flag | meaning |
|---|---|
| `--output DIR` | sorted output folder (default `~/photorec-sorted`) |
| `--staging DIR` | raw carve sessions live here (default `~/photorec`) |
| `--skip a,b` | exclude photorec format tokens from carving, e.g. `--skip png,gif` |
| `--icon-size N` | max square side treated as "suspect icon" (default 256) |
| `--ext` / `--freespace` | ext filesystem mode / scan unallocated space only |
| `--sort-only [DIR]` | sort an existing staging/session dir (or the newest one) |
| `--dry-run` | show what sorting would do, move/delete nothing |
| `--no-tui` | plain-text mode even on a terminal |

Default folders live in the invoking user's home (`SUDO_USER`-aware). Photorec's whitelist is by *format family*: the `riff` token carves wav/avi/webp, `mov` carves mov/mp4/3gp/heic, `ogg` carves ogg/ogm/ogv, etc.
