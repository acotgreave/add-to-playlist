"""
One-time fix: populate missing artist names for tracks where the scraper
left the artist field blank but a Spotify ID was found.

For each such track, queries the Spotify API using the stored spotify_id
to get the canonical artist name(s).

Usage:
  python fix_artists.py           # fix and save
  python fix_artists.py --dry-run # show what would change, don't save
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ROOT     = Path(__file__).parent
CSV_PATH = ROOT / 'docs' / 'data' / 'tracks.csv'

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


def get_spotify():
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    cid    = os.getenv('SPOTIFY_CLIENT_ID', '')
    secret = os.getenv('SPOTIFY_CLIENT_SECRET', '')
    if not cid or not secret:
        print("[fix_artists] ERROR: SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set")
        sys.exit(1)
    auth = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    return spotipy.Spotify(auth_manager=auth)


def run(dry_run=False):
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    missing = [(i, r) for i, r in enumerate(rows)
               if not r.get('artist', '').strip() and r.get('spotify_id', '').strip()]
    also_missing_spotify = [r for r in rows
                            if not r.get('artist', '').strip()
                            and not r.get('spotify_id', '').strip()]

    print(f"[fix_artists] {len(rows)} total tracks")
    print(f"[fix_artists] {len(missing)} tracks with empty artist but have spotify_id")
    print(f"[fix_artists] {len(also_missing_spotify)} tracks with empty artist AND no spotify_id (skipped)")

    if not missing:
        print("[fix_artists] Nothing to do.")
        return

    sp = get_spotify()
    fixed = 0

    # Batch Spotify track lookups (up to 50 at a time)
    batch_size = 50
    for batch_start in range(0, len(missing), batch_size):
        batch = missing[batch_start:batch_start + batch_size]
        ids   = [r.get('spotify_id', '') for _, r in batch]

        try:
            results = sp.tracks(ids, market='GB')
            tracks  = results.get('tracks', [])
        except Exception as e:
            print(f"[fix_artists] Spotify batch failed: {e}")
            continue

        for (row_idx, row), sp_track in zip(batch, tracks):
            if not sp_track:
                print(f"  SKIP (not found): {row['title']}")
                continue

            artist_names = ', '.join(a['name'] for a in sp_track.get('artists', []))
            if not artist_names:
                print(f"  SKIP (no artist in Spotify): {row['title']}")
                continue

            print(f"  {'WOULD FIX' if dry_run else 'FIXED'}: "
                  f"'{row['title']}' -> artist='{artist_names}'")

            if not dry_run:
                rows[row_idx]['artist'] = artist_names
                fixed += 1

        time.sleep(0.1)  # gentle rate limiting

    if not dry_run:
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction='ignore')
            w.writeheader()
            w.writerows(rows)
        print(f"\n[fix_artists] Done. Fixed {fixed} tracks. Saved to {CSV_PATH}")
    else:
        print(f"\n[fix_artists] DRY RUN — {len(missing)} would be fixed.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    run(dry_run=args.dry_run)
