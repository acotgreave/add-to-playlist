"""
One-time backfill script: fix release_year and populate original_year.

Strategy:
  1. Tracks with mb_work_id → query MusicBrainz Work for begin-date → original_year
  2. Tracks with mb_recording_id but no mb_work_id → query Recording with work-rels
       → if work found, store mb_work_id + original_year
       → also find earliest release date → update release_year if earlier
  3. Save checkpoint CSV every CHECKPOINT_EVERY tracks (crash-safe)

Runtime: ~45 minutes for 1,333 tracks (MusicBrainz rate limit: 1 req/sec)

Usage:
  python fix_years.py           # full run
  python fix_years.py --dry-run # print what would change, don't save
"""

import argparse
import csv
import sys

# Force UTF-8 output so arrow characters don't crash on Windows cp1252 terminals
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import time
from pathlib import Path

ROOT     = Path(__file__).parent
CSV_PATH = ROOT / 'docs' / 'data' / 'tracks.csv'
CHECKPOINT_EVERY = 50

CSV_FIELDNAMES = [
    'episode_number', 'episode_pid', 'episode_title', 'episode_date',
    'track_type', 'title', 'artist',
    'release_year', 'original_year',
    'is_cover', 'original_artist',
    'genre1', 'genre2', 'genre3', 'other_genres',
    'spotify_id', 'album_art_url',
    'mb_recording_id', 'mb_work_id',
    'source', 'needs_review', 'notes',
]


def init_mb():
    import musicbrainzngs
    musicbrainzngs.set_useragent(
        'AddToPlaylistYearFixer', '1.0',
        'https://github.com/acotgreave/add-to-playlist'
    )
    return musicbrainzngs


def parse_year(s):
    import re
    if not s:
        return None
    m = re.search(r'\b(1[5-9]\d{2}|20[012]\d)\b', str(s))
    return int(m.group(1)) if m else None


def load_csv():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def save_csv(rows, path=None):
    out = path or CSV_PATH
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def get_work_date(mb, work_id):
    """Return (composition_year, composer) from a MusicBrainz Work.

    Composition date lives on the composer→work artist-relation as
    'begin' (start of composition) / 'end' (completion). We prefer
    'end' (finished year) falling back to 'begin'.
    """
    try:
        time.sleep(1.1)
        result = mb.get_work_by_id(work_id, includes=['artist-rels', 'work-rels'])
        work = result.get('work', {})

        year = None
        composer = ''

        for rel in work.get('artist-relation-list', []):
            if rel.get('type') in ('composer', 'lyricist', 'writer'):
                if not composer:
                    composer = rel.get('artist', {}).get('name', '')
                # 'end' = completion year; fall back to 'begin'
                rel_year = parse_year(rel.get('end', '')) or parse_year(rel.get('begin', ''))
                if rel_year and not year:
                    year = rel_year

        return year, composer
    except Exception as e:
        print(f"    [MB] Work {work_id} failed: {e}")
        return None, ''


def get_recording_info(mb, rec_id):
    """
    Return (work_id, earliest_release_year) from a MusicBrainz Recording.
    """
    try:
        time.sleep(1.1)
        result = mb.get_recording_by_id(rec_id, includes=['work-rels', 'releases'])
        rec = result.get('recording', {})

        # Work ID from performance relationship
        work_id = ''
        for wr in rec.get('work-relation-list', []):
            if wr.get('type') == 'performance':
                work_id = wr.get('work', {}).get('id', '')
                break

        # Earliest release year
        releases = rec.get('release-list', [])
        years = [parse_year(r.get('date', '')) for r in releases]
        years = [y for y in years if y]
        earliest = min(years) if years else None

        return work_id, earliest
    except Exception as e:
        print(f"    [MB] Recording {rec_id} failed: {e}")
        return '', None


def run(dry_run=False):
    print(f"[fix_years] Loading {CSV_PATH}")
    rows = load_csv()
    total = len(rows)
    print(f"[fix_years] {total} tracks loaded")

    mb = init_mb()

    changed = 0
    checked = 0

    # De-duplicate: process each unique mb_work_id only once
    work_cache = {}   # work_id → (year, composer)

    for i, row in enumerate(rows):
        work_id  = row.get('mb_work_id', '').strip()
        rec_id   = row.get('mb_recording_id', '').strip()
        orig_yr  = row.get('original_year', '').strip()
        rel_yr   = row.get('release_year', '').strip()
        title    = row.get('title', '')
        artist   = row.get('artist', '')

        updated = False

        # ── Phase A: have work_id → get composition date ──────────────────
        if work_id and not orig_yr:
            if work_id not in work_cache:
                print(f"  [{i+1}/{total}] Work lookup: {title} — {artist}")
                work_cache[work_id] = get_work_date(mb, work_id)
                checked += 1

            year, composer = work_cache[work_id]
            if year:
                if not dry_run:
                    row['original_year'] = year
                    if composer and not row.get('original_artist'):
                        row['original_artist'] = composer
                print(f"    → original_year = {year}")
                changed += 1
                updated = True

        # ── Phase B: no work_id → check recording for work + earliest date ─
        elif not work_id and rec_id:
            print(f"  [{i+1}/{total}] Recording lookup: {title} — {artist}")
            found_work_id, earliest = get_recording_info(mb, rec_id)
            checked += 1

            if found_work_id:
                if not dry_run:
                    row['mb_work_id'] = found_work_id
                print(f"    → mb_work_id = {found_work_id}")

                # Now get the work date
                if found_work_id not in work_cache:
                    work_cache[found_work_id] = get_work_date(mb, found_work_id)
                    checked += 1

                year, composer = work_cache[found_work_id]
                if year and not orig_yr:
                    if not dry_run:
                        row['original_year'] = year
                        if composer and not row.get('original_artist'):
                            row['original_artist'] = composer
                    print(f"    → original_year = {year}")
                    changed += 1
                    updated = True

            # Update release_year if MB earliest is earlier than current
            if earliest and rel_yr:
                current = parse_year(rel_yr)
                if current and earliest < current:
                    if not dry_run:
                        row['release_year'] = earliest
                    print(f"    → release_year {current} → {earliest}")
                    updated = True

        # ── Checkpoint save ────────────────────────────────────────────────
        if not dry_run and (i + 1) % CHECKPOINT_EVERY == 0:
            save_csv(rows)
            print(f"[fix_years] Checkpoint saved ({i+1}/{total}, {changed} changes so far)")

    # Final save
    if not dry_run:
        save_csv(rows)

    print(f"\n[fix_years] Done. Checked {checked} MB entries, made {changed} year updates.")
    if dry_run:
        print("[fix_years] DRY RUN — no files were modified.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n[fix_years] Interrupted — partial results saved to checkpoint.")
        sys.exit(1)
