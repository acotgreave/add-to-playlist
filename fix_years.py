"""
Backfill / migrate year fields in tracks.csv.

New field structure
-------------------
  year                  best-guess "when first made" = min of the others
  track_release_year    year of the specific Spotify recording (was release_year)
  original_year         composition date — classical works only, validated
                        against composer's birth year so bad MB data is rejected
  earliest_release_year first ever commercial release found in MusicBrainz

Strategy
--------
  Migration : rename release_year → track_release_year in existing rows.
  Phase A   : tracks with mb_recording_id → query Recording for
              earliest_release_year and (if missing) mb_work_id.
  Phase B   : tracks with mb_work_id → query Work for original_year,
              using composer birth year to reject implausible dates.
  Compute   : year = min(original_year, earliest_release_year, track_release_year)
  Also clears any pre-existing original_year that fails the birth-year check.

Runtime: ~60 min for 1,333 tracks (MusicBrainz 1 req/sec rate limit).

Usage:
  python fix_years.py           # full run
  python fix_years.py --dry-run # print changes, don't save
"""

import argparse
import csv
import sys
import time
from pathlib import Path

# Force UTF-8 so arrow/emoji characters don't crash on Windows cp1252 terminals
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

ROOT     = Path(__file__).parent
CSV_PATH = ROOT / 'docs' / 'data' / 'tracks.csv'
CHECKPOINT_EVERY = 50

CSV_FIELDNAMES = [
    'episode_number', 'episode_pid', 'episode_title', 'episode_date',
    'track_type', 'title', 'artist',
    'year', 'track_release_year', 'original_year', 'earliest_release_year',
    'is_cover', 'original_artist',
    'genre1', 'genre2', 'genre3', 'other_genres',
    'spotify_id', 'album_art_url',
    'mb_recording_id', 'mb_work_id',
    'source', 'needs_review', 'notes',
]


# ---------------------------------------------------------------------------
# MusicBrainz setup
# ---------------------------------------------------------------------------

def init_mb():
    import musicbrainzngs
    musicbrainzngs.set_useragent(
        'AddToPlaylistYearFixer', '2.0',
        'https://github.com/acotgreave/add-to-playlist'
    )
    return musicbrainzngs


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def parse_year(s):
    import re
    if not s:
        return None
    m = re.search(r'\b(1[5-9]\d{2}|20[012]\d)\b', str(s))
    return int(m.group(1)) if m else None


def compute_year(original_year, earliest_release_year, track_release_year):
    """Best-guess year = earliest of the three non-null values."""
    candidates = [v for v in [original_year, earliest_release_year, track_release_year]
                  if v and int(v) > 0]
    return min(candidates) if candidates else ''


def load_csv():
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def save_csv(rows, path=None):
    out = path or CSV_PATH
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def migrate_row(row):
    """Rename release_year → track_release_year; initialise new fields."""
    # Migrate old field name (idempotent — safe to call on already-migrated rows)
    if 'release_year' in row and 'track_release_year' not in row:
        row['track_release_year'] = row.pop('release_year')
    row.setdefault('year', '')
    row.setdefault('track_release_year', '')
    row.setdefault('original_year', '')
    row.setdefault('earliest_release_year', '')
    return row


# ---------------------------------------------------------------------------
# MusicBrainz helpers
# ---------------------------------------------------------------------------

_composer_birth_cache = {}   # artist_id → birth_year (int or None)
_work_cache           = {}   # work_id   → (composition_year, composer_name)


def get_composer_birth_year(mb, artist_id):
    """Return composer's birth year, or None. Cached per artist_id."""
    if not artist_id:
        return None
    if artist_id in _composer_birth_cache:
        return _composer_birth_cache[artist_id]
    try:
        time.sleep(1.1)
        result = mb.get_artist_by_id(artist_id)
        artist = result.get('artist', {})
        born = parse_year(artist.get('life-span', {}).get('begin', ''))
        _composer_birth_cache[artist_id] = born
        return born
    except Exception as e:
        print(f"    [MB] Artist {artist_id} failed: {e}")
        _composer_birth_cache[artist_id] = None
        return None


def get_work_date(mb, work_id):
    """Return (composition_year, composer_name) for a MusicBrainz Work.

    Reads the begin/end dates from the composer→work relationship.
    Validates against the composer's birth year — if the composition year
    precedes the composer's birth, it's bad MB data and is discarded.
    """
    if work_id in _work_cache:
        return _work_cache[work_id]

    try:
        time.sleep(1.1)
        result = mb.get_work_by_id(work_id, includes=['artist-rels', 'work-rels'])
        work = result.get('work', {})

        composition_year = None
        composer = ''
        composer_id = ''

        for rel in work.get('artist-relation-list', []):
            if rel.get('type') in ('composer', 'lyricist', 'writer'):
                if not composer:
                    composer = rel.get('artist', {}).get('name', '')
                    composer_id = rel.get('artist', {}).get('id', '')
                # Prefer 'end' (completion year) over 'begin' (start)
                rel_year = (parse_year(rel.get('end', ''))
                            or parse_year(rel.get('begin', '')))
                if rel_year and not composition_year:
                    composition_year = rel_year

        # Sanity check: composition can't predate the composer's birth
        if composition_year and composer_id:
            born = get_composer_birth_year(mb, composer_id)
            if born and composition_year < born:
                print(f"    [sanity] Rejecting {composition_year} for '{composer}' "
                      f"(born {born}) — bad MB data")
                composition_year = None

        _work_cache[work_id] = (composition_year, composer)
        return composition_year, composer

    except Exception as e:
        print(f"    [MB] Work {work_id} failed: {e}")
        _work_cache[work_id] = (None, '')
        return None, ''


def get_recording_info(mb, rec_id):
    """Return (work_id, earliest_release_year) from a MusicBrainz Recording."""
    try:
        time.sleep(1.1)
        result = mb.get_recording_by_id(rec_id, includes=['work-rels', 'releases'])
        rec = result.get('recording', {})

        work_id = ''
        for wr in rec.get('work-relation-list', []):
            if wr.get('type') == 'performance':
                work_id = wr.get('work', {}).get('id', '')
                break

        releases = rec.get('release-list', [])
        years = [parse_year(r.get('date', '')) for r in releases]
        years = [y for y in years if y]
        earliest = min(years) if years else None

        return work_id, earliest

    except Exception as e:
        print(f"    [MB] Recording {rec_id} failed: {e}")
        return '', None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run=False):
    print(f"[fix_years] Loading {CSV_PATH}")
    rows = load_csv()
    total = len(rows)
    print(f"[fix_years] {total} tracks loaded")

    # Step 0: migrate field names
    rows = [migrate_row(r) for r in rows]
    print("[fix_years] Field names migrated (release_year -> track_release_year)")

    mb = init_mb()

    changed_orig  = 0   # original_year updates
    changed_earl  = 0   # earliest_release_year updates
    changed_year  = 0   # year (computed) updates
    cleared_bad   = 0   # bad original_year values cleared
    checked       = 0

    for i, row in enumerate(rows):
        work_id  = row.get('mb_work_id', '').strip()
        rec_id   = row.get('mb_recording_id', '').strip()
        title    = row.get('title', '')
        artist   = row.get('artist', '')

        # ── Clear existing original_year values that fail the sanity check ──
        existing_orig = parse_year(row.get('original_year', ''))
        if existing_orig and work_id:
            # Re-validate against composer birth year using work_cache if populated
            # (will be checked properly when we do the full work lookup below)
            pass  # handled in Phase B below

        # ── Phase A: fetch earliest_release_year from Recording ─────────────
        if rec_id:
            earl_str = row.get('earliest_release_year', '').strip()
            if not earl_str:
                print(f"  [{i+1}/{total}] Recording lookup: {title} — {artist}")
                found_work_id, earliest = get_recording_info(mb, rec_id)
                checked += 1

                if found_work_id and not work_id:
                    if not dry_run:
                        row['mb_work_id'] = found_work_id
                    work_id = found_work_id
                    print(f"    -> mb_work_id = {found_work_id}")

                if earliest:
                    if not dry_run:
                        row['earliest_release_year'] = earliest
                    print(f"    -> earliest_release_year = {earliest}")
                    changed_earl += 1

        # ── Phase B: fetch composition date from Work ────────────────────────
        if work_id:
            orig_str = row.get('original_year', '').strip()

            # Always re-check: even if original_year is set we validate it
            needs_check = not orig_str or existing_orig
            if needs_check:
                if work_id not in _work_cache:
                    if not orig_str:
                        print(f"  [{i+1}/{total}] Work lookup: {title} — {artist}")
                    year, composer = get_work_date(mb, work_id)
                    checked += 1
                else:
                    year, composer = _work_cache[work_id]

                # If existing original_year fails validation, clear it
                if existing_orig and (not year or year != existing_orig):
                    if not dry_run:
                        row['original_year'] = year or ''
                    if existing_orig and not year:
                        print(f"    -> cleared bad original_year {existing_orig}")
                        cleared_bad += 1
                    elif year and year != existing_orig:
                        print(f"    -> original_year {existing_orig} -> {year}")
                elif not orig_str and year:
                    if not dry_run:
                        row['original_year'] = year
                        if composer and not row.get('original_artist'):
                            row['original_artist'] = composer
                    print(f"    -> original_year = {year}")
                    changed_orig += 1

        # ── Compute year ─────────────────────────────────────────────────────
        new_year = compute_year(
            parse_year(row.get('original_year', '')),
            parse_year(row.get('earliest_release_year', '')),
            parse_year(row.get('track_release_year', '')),
        )
        old_year = row.get('year', '')
        if str(new_year) != str(old_year):
            if not dry_run:
                row['year'] = new_year
            changed_year += 1

        # ── Checkpoint ────────────────────────────────────────────────────────
        if not dry_run and (i + 1) % CHECKPOINT_EVERY == 0:
            save_csv(rows)
            print(f"[fix_years] Checkpoint {i+1}/{total} "
                  f"(orig:{changed_orig} earl:{changed_earl} "
                  f"year:{changed_year} cleared:{cleared_bad})")

    # Final save
    if not dry_run:
        save_csv(rows)

    print(f"\n[fix_years] Done. Checked {checked} MB entries.")
    print(f"  original_year updates  : {changed_orig}")
    print(f"  earliest_release updates: {changed_earl}")
    print(f"  year (computed) updates: {changed_year}")
    print(f"  bad original_year cleared: {cleared_bad}")
    if dry_run:
        print("[fix_years] DRY RUN — no files were modified.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n[fix_years] Interrupted — partial results already checkpointed.")
        sys.exit(1)
