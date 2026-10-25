# media-dump — v1 (plain)

The original no-frills **media-dump**: carve media-only formats with photorec and sort every file into per-extension folders **live, while photorec is still carving**.

Same policy as v2: images + video + audio kept · `svg` deleted · small square images quarantined to `suspect-icons/` · files chowned back to the invoking user · auto-sudo for device sources.

```bash
media-dump-v1 /dev/sdb1 --yes             # device carve: auto-sudo, asked once
media-dump-v1 ~/disk.img --yes --ext      # images need no sudo
media-dump-v1 --sort-only                 # never asks sudo
```

The newer version with a btop-style curses TUI lives on the `main` branch.
