"""
BBC Radio 4 'Add to Playlist' scraper.

Fetches all episodes and their track listings. Two track types:
  'featured'   — the five tracks chosen by guests
  'background' — other music heard in the episode

BBC HTML structure (confirmed from live page, 2025):
  Tracks are NOT in structured segment elements. They live entirely in the
  long-form synopsis text (.synopsis-toggle__long) as plain <p> paragraphs:

    <div class="synopsis-toggle__long">
      <p>Episode description...</p>
      <p>Producer: ... <br/>Presented by ...</p>
      <p>The five tracks in this week's playlist:</p>          ← featured header
      <p>Wolf Totem by The HU<br/>Daybreak by Ravel<br/>...</p> ← featured tracks
      <p>Other music in this episode:</p>                       ← background header
      <p>Creep by Radiohead<br/>Running Up That Hill by Kate Bush<br/>...</p>
    </div>

  Track line format:  "Title by Artist"  (split on LAST ' by ')
  Exceptions:         "Title from Show"  (e.g. "Ex-Wives from SIX: The Musical")
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
from pathlib import Path

SHOW_PID = 'm00106lb'
BASE_URL = 'https://www.bbc.co.uk'
DATA_DIR = Path(__file__).parent / 'data'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

JSON_HEADERS = {
    **HEADERS,
    'Accept': 'application/json, */*;q=0.8',
}


def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# ---------------------------------------------------------------------------
# Episode list
# ---------------------------------------------------------------------------

def get_all_episode_pids(session=None, progress_cb=None):
    """
    Returns list of dicts: {pid, title, date}
    Iterates through all pages of the episodes listing.
    """
    if session is None:
        session = make_session()

    episodes = []
    page = 1

    while True:
        url = f"{BASE_URL}/programmes/{SHOW_PID}/episodes/player?page={page}"

        if progress_cb:
            progress_cb({'step': 'list', 'page': page,
                         'message': f'Fetching episode list page {page}...'})

        try:
            resp = session.get(url, timeout=20)
        except requests.RequestException as e:
            if progress_cb:
                progress_cb({'step': 'error', 'message': str(e)})
            break

        if resp.status_code == 404:
            break
        if resp.status_code != 200:
            if progress_cb:
                progress_cb({'step': 'error',
                             'message': f'HTTP {resp.status_code} on page {page}'})
            break

        resp.encoding = 'utf-8'  # BBC headers often declare iso-8859-1 for UTF-8 content
        soup = BeautifulSoup(resp.text, 'lxml')
        found = _parse_episode_list_page(soup)

        if not found:
            break

        episodes.extend(found)

        # BBC paginates with ?page=N; stop when no "next" link
        next_link = (
            soup.select_one('a[rel="next"]') or
            soup.select_one('.pagination__next a') or
            soup.select_one('a.pagination__next') or
            soup.find('a', string=re.compile(r'Next', re.I))
        )
        if not next_link:
            break

        page += 1
        time.sleep(1)

    if progress_cb:
        progress_cb({'step': 'list_done', 'count': len(episodes),
                     'message': f'Found {len(episodes)} episodes.'})
    return episodes


def _parse_episode_list_page(soup):
    """Extract episode {pid, title, date} dicts from a listing page."""
    episodes = []

    # Primary: items with data-pid attribute
    items = soup.select('[data-pid]')

    if not items:
        # Fallback: links matching /programmes/XXXXXXXX (8-char pid)
        pid_re = re.compile(r'/programmes/([a-z0-9]{8})$')
        seen = set()
        for a in soup.find_all('a', href=pid_re):
            m = pid_re.search(a['href'])
            if not m:
                continue
            pid = m.group(1)
            if pid in seen or pid == SHOW_PID:
                continue
            seen.add(pid)
            episodes.append({'pid': pid, 'title': a.get_text(strip=True), 'date': ''})
        return episodes

    for item in items:
        pid = item.get('data-pid', '').strip()
        if not pid or pid == SHOW_PID or len(pid) != 8:
            continue

        # Title
        title_el = (
            item.select_one('.programme__title span:first-child') or
            item.select_one('.programme__title') or
            item.select_one('h2') or item.select_one('h3')
        )
        subtitle_el = item.select_one('.programme__subtitle')
        title = title_el.get_text(strip=True) if title_el else pid
        if subtitle_el:
            title = f"{title}: {subtitle_el.get_text(strip=True)}"

        # Date
        date_el = item.select_one('time') or item.select_one('.broadcast-event__time')
        date = ''
        if date_el:
            date = (date_el.get('datetime') or
                    date_el.get('title') or
                    date_el.get_text(strip=True))

        episodes.append({'pid': pid, 'title': title.strip(), 'date': date})

    return episodes


# ---------------------------------------------------------------------------
# Track listing for a single episode
# ---------------------------------------------------------------------------

def scrape_episode(pid, session=None, progress_cb=None):
    """
    Returns (tracks, date_str, description) where:
      tracks      = list of {title, artist, track_type}
      date_str    = ISO date from JSON-LD, e.g. '2025-12-12' ('' if not found)
      description = episode description text extracted from synopsis ('' if not found)
    """
    if session is None:
        session = make_session()

    if progress_cb:
        progress_cb({'step': 'episode', 'pid': pid,
                     'message': f'Scraping episode {pid}...'})

    # Primary: parse synopsis text (confirmed working structure); also extracts date
    tracks, date, description = _parse_episode_html(pid, session)
    if tracks:
        return tracks, date, description

    # Fallback: segments.json API (no description available from this source)
    return _try_segments_json(pid, session) or [], date, description


def _try_segments_json(pid, session):
    """Attempt to retrieve tracks via /programmes/{pid}/segments.json."""
    url = f"{BASE_URL}/programmes/{pid}/segments.json"
    try:
        resp = session.get(url, headers=JSON_HEADERS, timeout=15)
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except ValueError:
        return None

    tracks = []
    segment_events = data.get('segment_events') or data.get('segments') or []

    for event in segment_events:
        seg = event.get('segment', event)

        if seg.get('type') not in ('music', 'Music', None):
            continue

        title = (seg.get('title') or '').strip()
        if not title:
            continue

        artist = _extract_artist_from_contributions(
            seg.get('contributions') or event.get('contributions') or []
        )
        if not artist:
            artist = (seg.get('artist_name') or
                      seg.get('primary_contributor', {}).get('name') or '')

        tracks.append({
            'title': title,
            'artist': artist.strip(),
            'track_type': 'featured',
        })

    return tracks if tracks else None


def _extract_artist_from_contributions(contributions):
    """Pick the most relevant contributor name from a contributions list."""
    priority = [
        'Performer', 'Artist', 'Vocalist', 'Singer',
        'Band', 'Group', 'Orchestra', 'Ensemble', 'Choir',
        'Composer',
    ]
    by_role = {}
    for c in contributions:
        role = c.get('role', '')
        name = (c.get('name') or
                c.get('contributor', {}).get('name') or '')
        if role and name and role not in by_role:
            by_role[role] = name

    for role in priority:
        if role in by_role:
            return by_role[role]

    # Return any contributor
    return next(iter(by_role.values()), '')


def _parse_episode_html(pid, session):
    """
    Parse an episode page. Returns (tracks, date_str, description).

    Tracks live in .synopsis-toggle__long as plain <p> elements.
    Date is extracted from the JSON-LD RadioEpisode block (reliable source).
    Description is the first non-credits paragraph before the track-list headers.
    """
    url = f"{BASE_URL}/programmes/{pid}"
    try:
        resp = session.get(url, timeout=20)
    except requests.RequestException as e:
        print(f"[scraper] Error fetching {pid}: {e}")
        return [], '', ''

    if resp.status_code != 200:
        print(f"[scraper] HTTP {resp.status_code} for episode {pid}")
        return [], '', ''

    resp.encoding = 'utf-8'  # BBC headers often declare iso-8859-1 for UTF-8 content
    soup = BeautifulSoup(resp.text, 'lxml')

    # Save HTML for debugging if debug dir exists
    debug_dir = DATA_DIR / 'debug_html'
    if debug_dir.exists():
        (debug_dir / f'{pid}.html').write_text(resp.text, encoding='utf-8', errors='replace')

    # --- Extract date from JSON-LD (most reliable source) ---
    date = ''
    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            ld = json.loads(tag.string or '')
            if ld.get('@type') == 'RadioEpisode':
                date = ld.get('datePublished', '')
                break
        except Exception:
            pass

    # --- Tracks + description are in the long synopsis div ---
    synopsis = soup.select_one('.synopsis-toggle__long')
    if not synopsis:
        synopsis = soup

    tracks, description = _parse_synopsis_paragraphs(synopsis.find_all('p'))
    return tracks, date, description


def _parse_synopsis_paragraphs(paragraphs):
    """
    Walk through <p> elements looking for section headers then track lists.
    Also collects pre-header paragraphs as the episode description.

    Returns (tracks, description_str).

    Header keywords → set current_type.
    Next non-empty paragraph after a header → parse as track lines.
    Repeat until no more paragraphs.
    """
    tracks = []
    description_parts = []
    current_type = None  # None = haven't hit a header yet

    for p in paragraphs:
        text = p.get_text(strip=True)
        if not text:
            continue

        text_lower = text.lower()

        # Check for section header
        if _is_featured_header(text_lower):
            current_type = 'featured'
            continue
        if _is_background_header(text_lower):
            current_type = 'background'
            continue

        # Pre-header paragraphs: collect as episode description
        if current_type is None:
            # Skip credits lines (Producer / Presented by)
            if text_lower.startswith('producer') or text_lower.startswith('presented'):
                continue
            # Limit to first 2 descriptive paragraphs
            if len(description_parts) < 2:
                description_parts.append(text)
            continue

        # This paragraph should contain the track list for the current section
        lines = _br_split(p)
        for line in lines:
            t = _parse_track_line(line)
            if t:
                t['track_type'] = current_type
                tracks.append(t)

        # Reset: next paragraph will be skipped until another header is found.
        # This prevents credits / description paragraphs from being parsed as tracks.
        current_type = None

    description = ' '.join(description_parts)
    return tracks, description


# Section header keyword matching.
# Headers always end with ':' — the episode description also says "five tracks"
# so we must require the colon to avoid false positives.
_FEATURED_KW  = ['five tracks', 'tracks in this week', 'playlist:',
                  'songs chosen', 'tracks chosen', 'the tracks:', 'chosen by']
_BACKGROUND_KW = ['other music', 'also featured', 'also in this episode',
                  'more music', 'additional music']


def _is_featured_header(text):
    # Must end with ':' to avoid matching episode description ("they add five tracks...")
    if not text.rstrip().endswith(':'):
        return False
    return any(k in text for k in _FEATURED_KW)


def _is_background_header(text):
    if not text.rstrip().endswith(':'):
        return False
    return any(k in text for k in _BACKGROUND_KW)


def _br_split(p_el):
    """
    Extract text lines from a <p> by splitting on <br> tags.
    Handles both <br/> and the malformed BBC variant.
    """
    lines = []
    current = []
    for child in p_el.children:
        if getattr(child, 'name', None) == 'br':
            line = ''.join(current).strip()
            if line:
                lines.append(line)
            current = []
        elif hasattr(child, 'get_text'):
            current.append(child.get_text())
        else:
            current.append(str(child))
    tail = ''.join(current).strip()
    if tail:
        lines.append(tail)
    return lines


def _parse_track_line(line):
    """
    Parse a track line into {title, artist}.

    Formats seen on Add to Playlist:
      "Wolf Totem by The HU"
      "Daybreak (Lever du jour) by Maurice Ravel"
      "Running Up That Hill (A Deal With God) by Kate Bush"
      "Ex-Wives from SIX: The Musical"

    Strategy: split on the LAST occurrence of ' by ' to handle 'by' in titles.
    Fallback: split on ' from ' (for musical theatre tracks).
    """
    line = line.strip()
    if not line or len(line) < 3:
        return None

    if ' by ' in line:
        idx = line.rfind(' by ')
        title  = line[:idx].strip()
        artist = line[idx + 4:].strip()
        if title:
            return {'title': title, 'artist': artist}

    if ' from ' in line:
        idx = line.rfind(' from ')
        title  = line[:idx].strip()
        artist = line[idx + 6:].strip()
        if title:
            return {'title': title, 'artist': artist}

    # No separator — return as title-only (will be flagged needs_review)
    return {'title': line, 'artist': ''}


# ---------------------------------------------------------------------------
# Episode cache (tracks which episodes have been scraped)
# ---------------------------------------------------------------------------

def load_episode_cache():
    path = DATA_DIR / 'episodes_cache.json'
    if path.exists():
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_episode_cache(cache):
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / 'episodes_cache.json', 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)


# ---------------------------------------------------------------------------
# Debug helpers
# ---------------------------------------------------------------------------

def save_debug_html(pid):
    """Fetch and save raw HTML for an episode for inspection."""
    debug_dir = DATA_DIR / 'debug_html'
    debug_dir.mkdir(parents=True, exist_ok=True)
    session = make_session()
    resp = session.get(f"{BASE_URL}/programmes/{pid}", timeout=20)
    resp.encoding = 'utf-8'
    path = debug_dir / f'{pid}.html'
    path.write_text(resp.text, encoding='utf-8', errors='replace')
    return str(path)
