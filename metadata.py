"""
Music metadata lookup: release year, genre, cover detection.

Priority chain per track:
  1. Spotify  — release date, artist genres
  2. MusicBrainz — covers/originals, classical works, composition dates
  3. Last.fm  — genre tags as supplement

For covers, we report:
  release_year        = the year of the specific version heard (e.g. Jimi's 1968)
  original_year       = original composition/release year (e.g. Dylan's 1967)
  is_cover            = True
  original_artist     = the original artist/composer

For classical works:
  release_year        = composition year (from MusicBrainz work)
  is_cover            = False (unless it's a modern arrangement)
"""

import os
import re
import time
import json
import unicodedata
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent / 'data'

# ---------------------------------------------------------------------------
# Lazy-loaded API clients
# ---------------------------------------------------------------------------

_spotify = None
_lastfm = None


def _get_spotify():
    global _spotify
    if _spotify is not None:
        return _spotify
    try:
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials
        cid = os.getenv('SPOTIFY_CLIENT_ID', '')
        secret = os.getenv('SPOTIFY_CLIENT_SECRET', '')
        if not cid or not secret:
            return None
        auth = SpotifyClientCredentials(client_id=cid, client_secret=secret)
        _spotify = spotipy.Spotify(auth_manager=auth)
        return _spotify
    except Exception as e:
        print(f"[metadata] Spotify init failed: {e}")
        return None


def _get_lastfm():
    global _lastfm
    if _lastfm is not None:
        return _lastfm
    try:
        import pylast
        key = os.getenv('LASTFM_API_KEY', '')
        secret = os.getenv('LASTFM_API_SECRET', '')
        if not key or not secret:
            return None
        _lastfm = pylast.LastFMNetwork(api_key=key, api_secret=secret)
        return _lastfm
    except Exception as e:
        print(f"[metadata] Last.fm init failed: {e}")
        return None


def _init_musicbrainz():
    try:
        import musicbrainzngs
        musicbrainzngs.set_useragent(
            'AddToPlaylistScraper', '1.0',
            'https://github.com/local/add-to-playlist'
        )
        return musicbrainzngs
    except Exception as e:
        print(f"[metadata] MusicBrainz init failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Metadata cache (avoid re-querying the same track)
# ---------------------------------------------------------------------------

_meta_cache = None


def _load_cache():
    global _meta_cache
    if _meta_cache is not None:
        return _meta_cache
    path = DATA_DIR / 'metadata_cache.json'
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            _meta_cache = json.load(f)
    else:
        _meta_cache = {}
    return _meta_cache


def _save_cache():
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / 'metadata_cache.json', 'w', encoding='utf-8') as f:
        json.dump(_meta_cache, f, indent=2, ensure_ascii=False)


def _cache_key(title, artist):
    norm = unicodedata.normalize('NFKD', f"{title}|||{artist}").lower()
    return re.sub(r'\s+', ' ', norm).strip()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_track_metadata(title, artist, progress_cb=None):
    """
    Returns a metadata dict:
    {
        release_year:     int or None,
        original_year:    int or None,   # if cover/classical
        is_cover:         bool,
        original_artist:  str,           # composer or original performer
        genre:            str,           # comma-separated
        spotify_id:       str,
        mb_recording_id:  str,
        mb_work_id:       str,
        source:           str,           # 'spotify', 'musicbrainz', 'lastfm', 'manual'
        needs_review:     bool,
        notes:            str,
    }
    """
    cache = _load_cache()
    key = _cache_key(title, artist)

    if key in cache:
        return cache[key]

    result = _empty_meta()

    # --- Try Spotify ---
    sp_data = _lookup_spotify(title, artist)
    if sp_data:
        result.update(sp_data)
        result['source'] = 'spotify'

    # --- Always try MusicBrainz for cover/original/classical info ---
    mb_data = _lookup_musicbrainz(title, artist, progress_cb)
    if mb_data:
        # MusicBrainz wins for original_year, is_cover, original_artist
        if mb_data.get('is_cover') is not None:
            result['is_cover'] = mb_data['is_cover']
        if mb_data.get('original_year'):
            result['original_year'] = mb_data['original_year']
        if mb_data.get('original_artist'):
            result['original_artist'] = mb_data['original_artist']
        if mb_data.get('mb_recording_id'):
            result['mb_recording_id'] = mb_data['mb_recording_id']
        if mb_data.get('mb_work_id'):
            result['mb_work_id'] = mb_data['mb_work_id']
        if mb_data.get('release_year') and not result.get('release_year'):
            result['release_year'] = mb_data['release_year']
        if mb_data.get('genre'):
            result['genre'] = mb_data['genre']
        if not result.get('source') or result['source'] == 'spotify':
            if mb_data.get('mb_recording_id'):
                result['source'] = ('spotify+musicbrainz'
                                    if result['source'] == 'spotify'
                                    else 'musicbrainz')

    # --- Supplement genre from Last.fm ---
    if not result.get('genre'):
        lfm_genre = _lookup_lastfm_genre(title, artist)
        if lfm_genre:
            result['genre'] = lfm_genre
            if result['source'] == 'unknown':
                result['source'] = 'lastfm'

    # Flag if we're not confident
    if not result.get('release_year'):
        result['needs_review'] = True

    cache[key] = result
    _save_cache()

    time.sleep(0.3)  # gentle rate limiting
    return result


def _empty_meta():
    return {
        'release_year': None,
        'original_year': None,
        'is_cover': False,
        'original_artist': '',
        'genre': '',
        'spotify_id': '',
        'album_art_url': '',
        'mb_recording_id': '',
        'mb_work_id': '',
        'source': 'unknown',
        'needs_review': False,
        'notes': '',
    }


# ---------------------------------------------------------------------------
# Spotify lookup
# ---------------------------------------------------------------------------

def _lookup_spotify(title, artist):
    sp = _get_spotify()
    if not sp:
        return None

    try:
        # Build query: prefer "track:X artist:Y" form
        query = f'track:"{_clean(title)}"'
        if artist:
            query += f' artist:"{_clean(artist)}"'

        results = sp.search(q=query, type='track', limit=5, market='GB')
        tracks = results.get('tracks', {}).get('items', [])

        if not tracks:
            # Fallback: simple keyword search
            query = f"{title} {artist}".strip()
            results = sp.search(q=query, type='track', limit=5, market='GB')
            tracks = results.get('tracks', {}).get('items', [])

        if not tracks:
            return None

        # Pick best match (first result that matches title reasonably well)
        track = _best_spotify_match(tracks, title, artist)
        if not track:
            return None

        # Release year from album
        release_date = track.get('album', {}).get('release_date', '')
        year = _parse_year(release_date)

        # Genre from artist (Spotify stores genres at artist level)
        genres = _get_spotify_artist_genres(sp, track)

        # Album art — images list is largest→smallest; pick smallest (64px) for thumbnails
        images = track.get('album', {}).get('images', [])
        album_art = images[-1].get('url', '') if images else ''

        return {
            'release_year': year,
            'genre': ', '.join(genres[:8]) if genres else '',
            'spotify_id': track.get('id', ''),
            'album_art_url': album_art,
        }

    except Exception as e:
        print(f"[metadata] Spotify error for '{title}' / '{artist}': {e}")
        return None


def _best_spotify_match(tracks, title, artist):
    title_l = title.lower()
    artist_l = artist.lower()

    for t in tracks:
        t_title = t.get('name', '').lower()
        t_artists = [a['name'].lower() for a in t.get('artists', [])]

        title_match = _fuzzy_match(title_l, t_title)
        artist_match = not artist_l or any(_fuzzy_match(artist_l, a) for a in t_artists)

        if title_match and artist_match:
            return t

    return tracks[0] if tracks else None


def _fuzzy_match(a, b, threshold=0.7):
    """Very lightweight fuzzy match (no external library needed)."""
    a, b = _clean(a), _clean(b)
    if a == b:
        return True
    if a in b or b in a:
        return True
    # Simple character overlap ratio
    set_a, set_b = set(a.split()), set(b.split())
    if not set_a or not set_b:
        return False
    overlap = len(set_a & set_b) / max(len(set_a), len(set_b))
    return overlap >= threshold


def _get_spotify_artist_genres(sp, track):
    try:
        artist_id = track['artists'][0]['id']
        artist_data = sp.artist(artist_id)
        return artist_data.get('genres', [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# MusicBrainz lookup
# ---------------------------------------------------------------------------

def _lookup_musicbrainz(title, artist, progress_cb=None):
    mb = _init_musicbrainz()
    if not mb:
        return None

    try:
        # Search for recordings
        kwargs = {'recording': title, 'limit': 5}
        if artist:
            kwargs['artist'] = artist

        result = mb.search_recordings(**kwargs)
        recordings = result.get('recording-list', [])

        if not recordings:
            return None

        # Find best match
        rec = _best_mb_recording(recordings, title, artist)
        if not rec:
            return None

        data = {
            'mb_recording_id': rec.get('id', ''),
            'release_year': None,
            'is_cover': False,
            'original_year': None,
            'original_artist': '',
            'genre': '',
            'mb_work_id': '',
        }

        # Release year from earliest release
        releases = rec.get('release-list', [])
        if releases:
            years = []
            for rel in releases:
                y = _parse_year(rel.get('date', ''))
                if y:
                    years.append(y)
            if years:
                data['release_year'] = min(years)

        # Genre/tags from recording
        tags = rec.get('tag-list', [])
        if tags:
            top_tags = sorted(tags, key=lambda t: int(t.get('count', 0)), reverse=True)
            data['genre'] = ', '.join(t['name'] for t in top_tags[:8])

        # Fetch full recording detail to check work relationships
        time.sleep(0.5)  # MusicBrainz rate limit: 1 req/sec
        try:
            detail = mb.get_recording_by_id(
                rec['id'],
                includes=['work-rels', 'artist-rels', 'tags', 'releases']
            )
            rec_detail = detail.get('recording', {})

            # Check work relationships (covers show up here)
            work_rels = rec_detail.get('work-relation-list', [])
            for wr in work_rels:
                if wr.get('type') == 'performance':
                    work = wr.get('work', {})
                    data['mb_work_id'] = work.get('id', '')

                    # Get work details (composer, date)
                    work_data = _get_mb_work(mb, work.get('id', ''))
                    if work_data:
                        data.update(work_data)

                    # Check if this is a cover (attributes list)
                    attrs = wr.get('attribute-list', [])
                    if 'cover' in [a.lower() for a in attrs]:
                        data['is_cover'] = True

                    break

        except Exception as e:
            print(f"[metadata] MB detail fetch failed: {e}")

        return data

    except Exception as e:
        print(f"[metadata] MusicBrainz error for '{title}' / '{artist}': {e}")
        return None


def _best_mb_recording(recordings, title, artist):
    title_l = title.lower()
    artist_l = artist.lower()

    for rec in recordings:
        r_title = rec.get('title', '').lower()
        r_artists = [
            ac.get('artist', {}).get('name', '').lower()
            for ac in rec.get('artist-credit', [])
            if isinstance(ac, dict)
        ]

        if _fuzzy_match(title_l, r_title):
            if not artist_l or any(_fuzzy_match(artist_l, a) for a in r_artists):
                return rec

    return recordings[0] if recordings else None


def _get_mb_work(mb, work_id):
    """Fetch a MusicBrainz Work to get composer and composition date."""
    if not work_id:
        return None
    try:
        time.sleep(0.5)
        result = mb.get_work_by_id(work_id, includes=['artist-rels', 'tags'])
        work = result.get('work', {})

        composer = ''
        composition_year = None

        # Composer from artist relations
        for rel in work.get('artist-relation-list', []):
            if rel.get('type') in ('composer', 'lyricist', 'writer'):
                composer = rel.get('artist', {}).get('name', '')
                break

        # Composition date from work attributes or disambiguation
        # MusicBrainz stores it in 'work.attributes' or sometimes in disambiguation
        for attr in work.get('attribute-list', []):
            val = attr.get('value', '')
            y = _parse_year(val)
            if y:
                composition_year = y
                break

        # Genre/tags from work
        tags = work.get('tag-list', [])
        top_tags = sorted(tags, key=lambda t: int(t.get('count', 0)), reverse=True)
        genre = ', '.join(t['name'] for t in top_tags[:8])

        return {
            'original_artist': composer,
            'original_year': composition_year,
            'genre': genre or None,
        }

    except Exception as e:
        print(f"[metadata] MB work fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Last.fm genre lookup
# ---------------------------------------------------------------------------

def _lookup_lastfm_genre(title, artist):
    lfm = _get_lastfm()
    if not lfm:
        return ''
    try:
        track = lfm.get_track(artist, title)
        tags = track.get_top_tags(limit=5)
        return ', '.join(t.item.name for t in tags[:8] if t.item)
    except Exception:
        return ''


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _clean(s):
    """Lowercase, remove punctuation noise, normalise whitespace."""
    s = unicodedata.normalize('NFKD', s).lower()
    s = re.sub(r"['\u2019\u2018`]", '', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _parse_year(date_str):
    """Extract a 4-digit year from a date string like '1968', '1968-07-01'."""
    if not date_str:
        return None
    m = re.search(r'\b(1[5-9]\d{2}|20[012]\d)\b', str(date_str))
    return int(m.group(1)) if m else None
