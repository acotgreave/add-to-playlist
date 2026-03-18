"""
Music metadata lookup: release year, genre, cover detection.

Priority chain per track:
  1. Spotify  — track_release_year (Spotify album date), artist genres
  2. MusicBrainz — covers/originals, classical works, composition dates,
                   earliest_release_year (first ever commercial release)
  3. Last.fm  — genre tags as supplement

Fields returned:
  year                = best-guess "when first made" = min(original_year,
                        earliest_release_year, track_release_year)
  track_release_year  = year of the specific Spotify recording found
  original_year       = composition date (classical works only, validated
                        against composer birth year)
  earliest_release_year = first MB commercial release across all recordings
  is_cover            = True if this is a cover version
  original_artist     = composer or original performer
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
    # v2 cache uses new field names
    path = DATA_DIR / 'metadata_cache_v2.json'
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            _meta_cache = json.load(f)
    else:
        _meta_cache = {}
    return _meta_cache


def _save_cache():
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / 'metadata_cache_v2.json', 'w', encoding='utf-8') as f:
        json.dump(_meta_cache, f, indent=2, ensure_ascii=False)


def _cache_key(title, artist):
    norm = unicodedata.normalize('NFKD', f"{title}|||{artist}").lower()
    return re.sub(r'\s+', ' ', norm).strip()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_track_metadata(title, artist, progress_cb=None):
    """
    Returns a metadata dict with fields:
      year, track_release_year, original_year, earliest_release_year,
      is_cover, original_artist, genre,
      spotify_id, album_art_url,
      mb_recording_id, mb_work_id,
      source, needs_review, notes
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
        if mb_data.get('earliest_release_year'):
            result['earliest_release_year'] = mb_data['earliest_release_year']
        # MB earliest release fills track_release_year if Spotify didn't provide one
        if mb_data.get('earliest_release_year') and not result.get('track_release_year'):
            result['track_release_year'] = mb_data['earliest_release_year']
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

    # --- Compute best-guess year ---
    candidates = [v for v in [
        result.get('original_year'),
        result.get('earliest_release_year'),
        result.get('track_release_year'),
    ] if v]
    result['year'] = min(candidates) if candidates else None

    # Flag if we're not confident
    if not result.get('year'):
        result['needs_review'] = True

    cache[key] = result
    _save_cache()

    time.sleep(0.3)  # gentle rate limiting
    return result


def _empty_meta():
    return {
        'year':                  None,
        'track_release_year':    None,
        'original_year':         None,
        'earliest_release_year': None,
        'is_cover':              False,
        'original_artist':       '',
        'artist':                '',   # from Spotify — backfills blank scraper artist
        'genre':                 '',
        'spotify_id':            '',
        'album_art_url':         '',
        'mb_recording_id':       '',
        'mb_work_id':            '',
        'source':                'unknown',
        'needs_review':          False,
        'notes':                 '',
    }


# ---------------------------------------------------------------------------
# Spotify lookup
# ---------------------------------------------------------------------------

def _lookup_spotify(title, artist):
    sp = _get_spotify()
    if not sp:
        return None

    try:
        query = f'track:"{_clean(title)}"'
        if artist:
            query += f' artist:"{_clean(artist)}"'

        results = sp.search(q=query, type='track', limit=5, market='GB')
        tracks = results.get('tracks', {}).get('items', [])

        if not tracks:
            query = f"{title} {artist}".strip()
            results = sp.search(q=query, type='track', limit=5, market='GB')
            tracks = results.get('tracks', {}).get('items', [])

        if not tracks:
            return None

        track = _best_spotify_match(tracks, title, artist)
        if not track:
            return None

        release_date = track.get('album', {}).get('release_date', '')
        year = _parse_year(release_date)

        genres = _get_spotify_artist_genres(sp, track)

        images = track.get('album', {}).get('images', [])
        album_art = images[-1].get('url', '') if images else ''

        spotify_artist = ', '.join(a['name'] for a in track.get('artists', []))

        return {
            'track_release_year': year,   # year of this specific Spotify recording
            'genre': ', '.join(genres[:8]) if genres else '',
            'spotify_id': track.get('id', ''),
            'album_art_url': album_art,
            'artist': spotify_artist,     # backfill if scraper left artist blank
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
    a, b = _clean(a), _clean(b)
    if a == b:
        return True
    if a in b or b in a:
        return True
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
        kwargs = {'recording': title, 'limit': 5}
        if artist:
            kwargs['artist'] = artist

        result = mb.search_recordings(**kwargs)
        recordings = result.get('recording-list', [])

        if not recordings:
            return None

        rec = _best_mb_recording(recordings, title, artist)
        if not rec:
            return None

        data = {
            'mb_recording_id':      rec.get('id', ''),
            'earliest_release_year': None,
            'is_cover':             False,
            'original_year':        None,
            'original_artist':      '',
            'genre':                '',
            'mb_work_id':           '',
        }

        # Earliest release year from this recording's releases
        releases = rec.get('release-list', [])
        if releases:
            years = [_parse_year(r.get('date', '')) for r in releases]
            years = [y for y in years if y]
            if years:
                data['earliest_release_year'] = min(years)

        # Genre/tags
        tags = rec.get('tag-list', [])
        if tags:
            top_tags = sorted(tags, key=lambda t: int(t.get('count', 0)), reverse=True)
            data['genre'] = ', '.join(t['name'] for t in top_tags[:8])

        # Fetch full recording detail for work relationships
        time.sleep(0.5)
        try:
            detail = mb.get_recording_by_id(
                rec['id'],
                includes=['work-rels', 'artist-rels', 'tags', 'releases']
            )
            rec_detail = detail.get('recording', {})

            # Update earliest release from full detail
            full_releases = rec_detail.get('release-list', [])
            if full_releases:
                years = [_parse_year(r.get('date', '')) for r in full_releases]
                years = [y for y in years if y]
                if years:
                    data['earliest_release_year'] = min(years)

            work_rels = rec_detail.get('work-relation-list', [])
            for wr in work_rels:
                if wr.get('type') == 'performance':
                    work = wr.get('work', {})
                    data['mb_work_id'] = work.get('id', '')

                    work_data = _get_mb_work(mb, work.get('id', ''))
                    if work_data:
                        data.update(work_data)

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


# Cache composer birth years to avoid redundant MB lookups
_composer_birth_cache = {}


def _get_composer_birth_year(mb, artist_id):
    """Fetch a composer's birth year from MusicBrainz (cached)."""
    if not artist_id:
        return None
    if artist_id in _composer_birth_cache:
        return _composer_birth_cache[artist_id]
    try:
        time.sleep(0.5)
        result = mb.get_artist_by_id(artist_id)
        artist = result.get('artist', {})
        life_span = artist.get('life-span', {})
        born = _parse_year(life_span.get('begin', ''))
        _composer_birth_cache[artist_id] = born
        return born
    except Exception:
        _composer_birth_cache[artist_id] = None
        return None


def _get_mb_work(mb, work_id):
    """Fetch a MusicBrainz Work to get composer and composition date.

    Composition date is read from the composer→work relationship's
    begin/end dates. Validated against the composer's birth year to
    reject bad MB data (e.g. a 2015 pop song incorrectly tagged 1829).
    """
    if not work_id:
        return None
    try:
        time.sleep(0.5)
        result = mb.get_work_by_id(work_id, includes=['artist-rels', 'tags'])
        work = result.get('work', {})

        composer = ''
        composer_id = ''
        composition_year = None

        for rel in work.get('artist-relation-list', []):
            if rel.get('type') in ('composer', 'lyricist', 'writer'):
                if not composer:
                    composer = rel.get('artist', {}).get('name', '')
                    composer_id = rel.get('artist', {}).get('id', '')
                # Prefer 'end' (completion) over 'begin' (start of composition)
                rel_year = (_parse_year(rel.get('end', ''))
                            or _parse_year(rel.get('begin', '')))
                if rel_year and not composition_year:
                    composition_year = rel_year

        # Sanity check: a composition can't predate the composer's birth
        if composition_year and composer_id:
            born = _get_composer_birth_year(mb, composer_id)
            if born and composition_year < born:
                composition_year = None  # bad MB data — discard

        tags = work.get('tag-list', [])
        top_tags = sorted(tags, key=lambda t: int(t.get('count', 0)), reverse=True)
        genre = ', '.join(t['name'] for t in top_tags[:8])

        return {
            'original_artist': composer,
            'original_year':   composition_year,
            'genre':           genre or None,
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
    s = unicodedata.normalize('NFKD', s).lower()
    s = re.sub(r"['\u2019\u2018`]", '', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def _parse_year(date_str):
    if not date_str:
        return None
    m = re.search(r'\b(1[5-9]\d{2}|20[012]\d)\b', str(date_str))
    return int(m.group(1)) if m else None
