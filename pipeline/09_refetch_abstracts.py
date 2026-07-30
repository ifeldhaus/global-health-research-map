"""
pipeline/09_refetch_abstracts.py

Refetch fuller abstracts for works whose stored (OpenAlex) abstract is thin.

Most works already carry the full publisher abstract from OpenAlex (median ~262
words). A minority are thin — corrections, letters, or records where OpenAlex has
only a fragment. For those, a fuller abstract often exists in Europe PMC, PubMed,
or Crossref. This script targets only the thin ones (word count < THRESHOLD),
looks up their DOI/PMID via the OpenAlex API, and pulls the best available
abstract, keeping whichever source is longest. It never shortens an abstract.

Non-destructive & resumable:
  - `abstract_orig`   one-time snapshot of the original OpenAlex abstract
  - `abstract_source` provenance ('openalex' | 'europepmc' | 'pubmed' | 'crossref');
                      NULL = not yet processed, so reruns resume automatically.

Scope:
  - default: all classified works with abstract word count < THRESHOLD
  - --ids W123,W456 : restrict to specific works (used to prep the validation set)

Usage:
    uv run python pipeline/09_refetch_abstracts.py [--threshold 150] [--ids W..,W..]
"""

import argparse
import html
import json
import re
import sys
import time
import urllib.parse
import urllib.request
import os

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import DB  # noqa: E402

MAILTO = 'ifeld03@gmail.com'
UA = f'ghrm-abstract-refetch/1.0 (mailto:{MAILTO})'
# Candidacy ceiling (words). The classifier truncates to 300w, so anything at or
# above this already fills its window; below it, a fuller source can add signal.
# Candidacy is only a cost filter -- the *upgrade* decision is comparison-based.
CAP = 300


def wc(text):
    return len(text.split()) if text else 0


# Section labels that mark a structured abstract (BACKGROUND: ... RESULTS: ...).
SECTION_RE = re.compile(
    r'\b(BACKGROUND|INTRODUCTION|OBJECTIVES?|AIMS?|PURPOSE|METHODS?|'
    r'DESIGN|SETTING|PARTICIPANTS|RESULTS|FINDINGS|CONCLUSIONS?|'
    r'INTERPRETATION|DISCUSSION|MEASUREMENTS?)\b\s*[:\-]', re.I)
# Openers that suggest a stored abstract is only the conclusion/final section.
CONCLUSION_OPENER_RE = re.compile(
    r'^\s*(in conclusion|we conclude|these findings|our findings|'
    r'overall|taken together|in summary|this study (shows|suggests|found)|'
    r'the results (show|suggest)|conclusions?\s*[:\-])', re.I)


def n_sections(text):
    return len(set(m.group(1).upper() for m in SECTION_RE.finditer(text or '')))


def looks_conclusion_only(text):
    """Stored abstract that reads like just the final section: a conclusion-type
    opener with at most one section label present."""
    if not text:
        return False
    return bool(CONCLUSION_OPENER_RE.match(text)) and n_sections(text) <= 1


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def _get_raw(url, timeout=25):
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8', 'replace')


def strip_tags(s):
    if not s:
        return s
    s = re.sub(r'<[^>]+>', ' ', s)          # JATS / HTML tags
    s = html.unescape(s)                     # &#xae; -> ®, &amp; -> & , etc.
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _looks_non_english(s):
    """Fraction of non-Latin (CJK/Cyrillic/etc.) letters is high."""
    if not s:
        return False
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return False
    non_latin = sum(1 for c in letters if ord(c) > 0x024F)
    return non_latin / len(letters) > 0.20


def openalex_ids(short_ids):
    """Batch-fetch DOI/PMID for up to 50 OpenAlex short ids. -> {wid: (doi, pmid)}"""
    out = {}
    for k in range(0, len(short_ids), 50):
        chunk = short_ids[k:k + 50]
        f = 'openalex_id:' + '|'.join(chunk)
        url = ('https://api.openalex.org/works?'
               + urllib.parse.urlencode({'filter': f, 'select': 'id,doi,ids',
                                         'per-page': 50, 'mailto': MAILTO}))
        try:
            d = _get(url)
        except Exception as e:
            print(f'  openalex batch err: {type(e).__name__} {str(e)[:60]}')
            time.sleep(2)
            continue
        for w in d.get('results', []):
            wid = w['id'].split('/')[-1]
            doi = (w.get('doi') or '').replace('https://doi.org/', '') or None
            pmid = (w.get('ids', {}).get('pmid') or '').split('/')[-1] or None
            out[wid] = (doi, pmid)
        time.sleep(0.15)
    return out


def from_europepmc(doi, pmid):
    q = f'DOI:"{doi}"' if doi else (f'EXT_ID:{pmid} AND SRC:MED' if pmid else None)
    if not q:
        return None
    url = ('https://www.ebi.ac.uk/europepmc/webservices/rest/search?'
           + urllib.parse.urlencode({'query': q, 'format': 'json',
                                     'resultType': 'core', 'pageSize': 1}))
    try:
        d = _get(url)
        res = d.get('resultList', {}).get('result', [])
        return strip_tags(res[0].get('abstractText')) if res else None
    except Exception:
        return None


def _abstract_from_block(block):
    """Assemble AbstractText segments (with structured labels) from one XML block."""
    segs = re.findall(r'<AbstractText([^>]*)>(.*?)</AbstractText>', block, re.S)
    if not segs:
        return None
    out = []
    for attrs, txt in segs:
        m = re.search(r'Label="([^"]+)"', attrs)
        txt = strip_tags(txt)
        out.append(f'{m.group(1)}: {txt}' if m else txt)
    return strip_tags(' '.join(out))


def from_pubmed(pmid):
    if not pmid:
        return None
    url = ('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
           + urllib.parse.urlencode({'db': 'pubmed', 'id': pmid,
                                     'rettype': 'abstract', 'retmode': 'xml',
                                     'email': MAILTO}))
    try:
        xml = _get_raw(url)
    except Exception:
        return None
    # Primary <Abstract> (drop <OtherAbstract> translation blocks entirely).
    m = re.search(r'<Abstract>(.*?)</Abstract>', xml, re.S)
    primary = _abstract_from_block(m.group(1)) if m else None
    # English translations, if the primary is non-English or missing.
    eng_other = None
    for attrs, body in re.findall(r'<OtherAbstract([^>]*)>(.*?)</OtherAbstract>', xml, re.S):
        if re.search(r'Language="eng"', attrs):
            eng_other = _abstract_from_block(body)
            break
    if primary and not _looks_non_english(primary):
        return primary
    if eng_other:
        return eng_other
    return primary  # non-English primary, no English alt: last resort


def from_crossref(doi):
    if not doi:
        return None
    url = f'https://api.crossref.org/works/{urllib.parse.quote(doi)}?mailto={MAILTO}'
    try:
        d = _get(url)
        return strip_tags(d.get('message', {}).get('abstract'))
    except Exception:
        return None


def best_abstract(current, doi, pmid):
    """Longest external abstract found. Returns (text, source, external_wc)."""
    best, src = current or '', 'openalex'
    for fn, name in ((lambda: from_europepmc(doi, pmid), 'europepmc'),
                     (lambda: from_pubmed(pmid), 'pubmed'),
                     (lambda: from_crossref(doi), 'crossref')):
        cand = fn()
        time.sleep(0.12)
        if cand and not _looks_non_english(cand) and wc(cand) > wc(best):
            best, src = cand, name
    return best, src


def upgrade_reason(cur, best):
    """Decide whether `best` is a more complete abstract than `cur`.
    Comparison-based, not a raw length threshold: catches conclusion-only stored
    abstracts even when they are moderately long. Returns a reason str or ''."""
    if not best or best == cur:
        return ''
    gain = wc(best) - wc(cur)
    if gain <= 0:
        return ''
    cur_sec, best_sec = n_sections(cur), n_sections(best)
    if looks_conclusion_only(cur) and gain >= 15:
        return f'stored looked conclusion-only (+{gain}w)'
    if best_sec >= 3 and cur_sec <= 1 and gain >= 15:
        return f'stored missing sections ({cur_sec}->{best_sec}, +{gain}w)'
    if gain >= 30:
        return f'materially longer (+{gain}w)'
    if gain >= 15 and wc(best) >= wc(cur) * 1.4:
        return f'longer (+{gain}w, {wc(cur)}->{wc(best)})'
    return ''


def ensure_columns(con):
    cols = [r[1] for r in con.execute("PRAGMA table_info('works')").fetchall()]
    if 'abstract_orig' not in cols:
        con.execute("ALTER TABLE works ADD COLUMN abstract_orig VARCHAR")
        con.execute("UPDATE works SET abstract_orig = abstract")
    if 'abstract_source' not in cols:
        con.execute("ALTER TABLE works ADD COLUMN abstract_source VARCHAR")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cap', type=int, default=CAP,
                    help='candidacy word ceiling (compare only abstracts shorter than this)')
    ap.add_argument('--ids', type=str, default='')
    ap.add_argument('--force', action='store_true',
                    help='re-evaluate even already-processed rows, comparing against abstract_orig')
    ap.add_argument('--report', type=str, default='validation/abstract_refetch_report.csv')
    args = ap.parse_args()

    con = duckdb.connect(DB)
    ensure_columns(con)
    # compare against the pristine original so reruns/--force are idempotent
    src_expr = 'COALESCE(abstract_orig, abstract)'
    processed = '' if args.force else 'AND abstract_source IS NULL'

    if args.ids:
        wanted = [x.strip().split('/')[-1] for x in args.ids.split(',') if x.strip()]
        rows = con.execute(
            f"""SELECT openalex_id, {src_expr} FROM works
                WHERE regexp_replace(openalex_id,'.*/','') IN ? {processed}""",
            [wanted]).fetchall()
    else:
        rows = con.execute(
            f"""SELECT openalex_id, {src_expr} FROM works
                WHERE classified_topic {processed}
                  AND array_length(string_split({src_expr},' '),1) < ?""",
            [args.cap]).fetchall()

    print(f'candidates to compare: {len(rows):,}')
    if not rows:
        con.close()
        return

    short = {r[0].split('/')[-1]: r for r in rows}
    id_list = list(short.keys())

    report = [['openalex_id', 'stored_wc', 'stored_sections', 'stored_conclusion_only',
               'best_source', 'best_wc', 'best_sections', 'upgraded', 'reason']]
    upgraded = same = 0
    for k in range(0, len(id_list), 50):
        batch = id_list[k:k + 50]
        idmap = openalex_ids(batch)
        for wid in batch:
            full_id, cur = short[wid]
            doi, pmid = idmap.get(wid, (None, None))
            text, src = best_abstract(cur, doi, pmid)
            reason = upgrade_reason(cur, text) if src != 'openalex' else ''
            if reason:
                con.execute("UPDATE works SET abstract=?, abstract_source=? WHERE openalex_id=?",
                            [text, src, full_id])
                upgraded += 1
            else:
                # keep original text; mark processed (restore orig in case --force reran)
                con.execute("UPDATE works SET abstract=?, abstract_source='openalex' WHERE openalex_id=?",
                            [cur, full_id])
                same += 1
            report.append([wid, wc(cur), n_sections(cur), int(looks_conclusion_only(cur)),
                           src if reason else '', wc(text) if src != 'openalex' else '',
                           n_sections(text) if src != 'openalex' else '',
                           int(bool(reason)), reason])
        done = k + len(batch)
        print(f'  {done:,}/{len(id_list):,}  upgraded={upgraded} unchanged={same}', flush=True)

    con.close()

    import csv as _csv
    rp = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.report)
    with open(rp, 'w', newline='') as f:
        _csv.writer(f).writerows(report)

    # flag stored-partial-with-no-better-source: reads conclusion-only but nothing fuller found
    stranded = [r for r in report[1:] if r[3] and not r[7]]
    print(f'\ndone. upgraded {upgraded:,}, unchanged {same:,}. report -> {args.report}')
    if stranded:
        print(f'  ⚠ {len(stranded)} look conclusion-only but no fuller source exists '
              f'(stored abstract is the only version available).')


if __name__ == '__main__':
    main()
