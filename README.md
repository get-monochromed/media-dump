# photorec-pipeline — v1 (plain)

The original no-frills version: carve media-only formats with photorec and sort every file into per-extension folders **live, while photorec is still carving**.

Same policy as v2: images + video + audio kept · `svg` deleted · small square images quarantined to `suspect-icons/` · files chowned back to the invoking user.

```bash
sudo python3 photorec_pipeline_v1.py /dev/sdb1 --yes
sudo python3 photorec_pipeline_v1.py ~/disk.img --yes --ext
sudo python3 photorec_pipeline_v1.py /dev/sda --skip png,gif -y
python3 photorec_pipeline_v1.py --sort-only
```

The newer version with a btop-style curses TUI lives on the `main` branch.
