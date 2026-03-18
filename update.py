"""
Headless updater for the Add to Playlist static site.

Run weekly by GitHub Actions (or manually) to:
  1. Fetch the latest episode list from BBC Radio 4
  2. Scrape tracks from any new episodes
  3. Fetch metadata (Spotify / MusicBrainz / Last.fm) for new tracks
  4. Write updated site/data/tracks.csv

Usage:
  python update.py              # check for new episodes only
  python update.py --all        # re-scrape all episodes from scratch

Environment variables (set as GitHub Secrets):
  SPOTIFY_CLIENT_ID
  SPOTIFY_CLIENT_SECRET
  LASTFM_API_KEY
  LASTFM_API_SECRET
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

import scraper
import metadata as meta

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT      = Path(__file__).parent
SITE_CSV  = ROOT / 'docs' / 'data' / 'tracks.csv'
DATA_DIR  = ROOT / 'data'

CSV_FIELDNAMES = [
    'episode_number', 'episode_pid', 'episode_title', 'episode_date',
    'track_type',
    'title', 'artist',
    'year', 'track_release_year', 'original_year', 'earliest_release_year',
    'is_cover', 'original_artist',
    'genre1', 'genre2', 'genre3', 'other_genres',
    'spotify_id', 'album_art_url',
    'mb_recording_id', 'mb_work_id',
    'source', 'needs_review', 'notes',
]


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_tracks():
    if not SITE_CSV.exists():
        return []
    with open(SITE_CSV, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def save_tracks(rows):
    SITE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(SITE_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    print(f"[update] Saved {len(rows)} rows to {SITE_CSV}")


def split_genres(genre_str):
    parts = [g.strip() for g in genre_str.split(',') if g.strip()] if genre_str else []
    return {
        'genre1':       parts[0] if len(parts) > 0 else '',
        'genre2':       parts[1] if len(parts) > 1 else '',
        'genre3':       parts[2] if len(parts) > 2 else '',
        'other_genres': ', '.join(parts[3:]) if len(parts) > 3 else '',
    }


def recompute_episode_numbers(rows):
    """Assign sequential episode_number (1 = oldest broadcast) to all rows."""
    ep_dates = {}
    for r in rows:
        pid  = r.get('episode_pid', '')
        date = r.get('episode_date', '')
        if pid and pid not in ep_dates:
            ep_dates[pid] = date

    dated   = sorted([(d, p) for p, d in ep_dates.items() if d])
    undated = [('' , p) for p, d in ep_dates.items() if not d]
    ordered = dated + undated

    ep_num = {pid: i + 1 for i, (_, pid) in enumerate(ordered)}
    for r in rows:
        r['episode_number'] = ep_num.get(r.get('episode_pid', ''), '')
    return rows


def make_row(episode, track, meta_data=None):
    row = {
        'episode_number':        '',
        'episode_pid':           episode['pid'],
        'episode_title':         episode.get('title', ''),
        'episode_date':          episode.get('date', ''),
        'track_type':            track.get('track_type', 'featured'),
        'title':                 track.get('title', ''),
        'artist':                track.get('artist', ''),
        'year':                  '',
        'track_release_year':    '',
        'original_year':         '',
        'earliest_release_year': '',
        'is_cover':              '',
        'original_artist':       '',
        'genre1': '', 'genre2': '', 'genre3': '', 'other_genres': '',
        'spotify_id':            '',
        'album_art_url':         '',
        'mb_recording_id':       '',
        'mb_work_id':            '',
        'source':                '',
        'needs_review':          '',
        'notes':                 '',
    }
    if meta_data:
        for field in ['year', 'track_release_year', 'original_year',
                      'earliest_release_year', 'is_cover', 'original_artist',
                      'spotify_id', 'album_art_url',
                      'mb_recording_id', 'mb_work_id',
                      'source', 'needs_review', 'notes']:
            val = meta_data.get(field, '')
            row[field] = '' if val is None else val
        row.update(split_genres(meta_data.get('genre', '') or ''))
    return row


# ---------------------------------------------------------------------------
# Main update logic
# ---------------------------------------------------------------------------

def run_update(scrape_all=False):
    print("[update] Loading existing tracks...")
    existing_rows = load_tracks()
    existing_pids = {r['episode_pid'] for r in existing_rows}
    print(f"[update] {len(existing_rows)} tracks across {len(existing_pids)} episodes")

    session = scraper.make_session()

    # --- Step 1: get episode list from BBC ---
    print("[update] Fetching episode list from BBC...")
    all_episodes = scraper.get_all_episode_pids(session=session)
    print(f"[update] BBC has {len(all_episodes)} episodes total")

    if scrape_all:
        new_episodes = all_episodes
    else:
        new_episodes = [e for e in all_episodes if e['pid'] not in existing_pids]

    if not new_episodes:
        print("[update] No new episodes found. CSV is up to date.")
        return

    print(f"[update] {len(new_episodes)} new episode(s) to scrape")

    # --- Step 2: scrape each new episode ---
    new_rows = []
    for i, episode in enumerate(new_episodes, 1):
        pid = episode['pid']
        print(f"[update] ({i}/{len(new_episodes)}) Scraping {pid}: {episode.get('title','')}")

        tracks, date, _ = scraper.scrape_episode(pid, session=session)

        if date and not episode.get('date'):
            episode['date'] = date

        if not tracks:
            print(f"[update]   No tracks found for {pid}, skipping.")
            continue

        print(f"[update]   Found {len(tracks)} track(s). Fetching metadata...")

        for j, track in enumerate(tracks, 1):
            title  = track.get('title', '')
            artist = track.get('artist', '')
            print(f"[update]     [{j}/{len(tracks)}] {title} — {artist}")

            meta_data = meta.get_track_metadata(title, artist)
            row = make_row(episode, track, meta_data)
            new_rows.append(row)

        time.sleep(1)   # polite gap between episodes

    if not new_rows:
        print("[update] No new tracks to add.")
        return

    print(f"[update] Adding {len(new_rows)} new track(s)...")

    if scrape_all:
        all_rows = new_rows
    else:
        all_rows = existing_rows + new_rows

    all_rows = recompute_episode_numbers(all_rows)
    save_tracks(all_rows)
    print(f"[update] Done. Total: {len(all_rows)} tracks.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update Add to Playlist CSV')
    parser.add_argument('--all', action='store_true',
                        help='Re-scrape all episodes from scratch')
    args = parser.parse_args()

    try:
        run_update(scrape_all=args.all)
    except KeyboardInterrupt:
        print("\n[update] Interrupted.")
        sys.exit(1)
