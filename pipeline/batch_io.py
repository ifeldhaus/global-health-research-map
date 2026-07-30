"""
pipeline/batch_io.py

Batch export/import for subscription-based classification.

The API scripts (02/03/06) call the Anthropic API directly and need an API
key. This module supports an alternative route: export unclassified works as
batch files, classify them with Claude Code (subscription-billed agents),
and import the returned labels. Both routes share the SAME system prompts
(build_system_prompt / SYSTEM_PROMPT) and the SAME label parsing and DB
writes (parse_label / write_results), so results are method-identical
regardless of route.

Usage:
    uv run python pipeline/batch_io.py status
    uv run python pipeline/batch_io.py export --task topic   --batch-size 50 --out-dir batches
    uv run python pipeline/batch_io.py export --task methods --batch-size 50 --out-dir batches
    uv run python pipeline/batch_io.py export --task country --batch-size 50 --out-dir batches
    uv run python pipeline/batch_io.py import --task topic results1.txt [results2.txt ...]

Export writes, per task:
    <out-dir>/<task>_system.txt      the exact classification system prompt
    <out-dir>/<task>_NNN.jsonl       one paper per line:
                                     {"openalex_id", "title", "abstract"}

Result files for import: one line per paper, `<openalex_id>|<raw_label>`,
where <raw_label> is the classifier output in the task's usual format
(e.g. `A|A04|high`, `M05|med`, `KE,TZ|high`). Lines are validated through
the task's parse_label, so malformed labels coerce exactly as they would
on the API route.
"""

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.utils import truncate_abstract  # noqa: E402

DB = 'data/global_health.duckdb'


def _load_module(filename: str):
    path = Path(__file__).parent / filename
    spec = importlib.util.spec_from_file_location(filename.removesuffix('.py'), path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _registry() -> dict:
    """Task name → (module filename, flag column, system prompt getter)."""
    return {
        'topic':   ('02_topic_classify.py',   'classified_topic',
                    lambda m: m.build_system_prompt()),
        'methods': ('03_methods_classify.py', 'classified_method',
                    lambda m: m.build_system_prompt()),
        'country': ('06_study_country.py',    'classified_country',
                    lambda m: m.SYSTEM_PROMPT),
    }


def get_task(name: str):
    filename, flag_col, get_system = _registry()[name]
    mod = _load_module(filename)
    return mod, flag_col, get_system(mod)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_status():
    con = duckdb.connect(DB, read_only=True)
    total = con.execute('SELECT COUNT(*) FROM works').fetchone()[0]
    print(f'works: {total:,}')
    for task, (_, flag_col, _g) in _registry().items():
        done = con.execute(
            f'SELECT COUNT(*) FROM works WHERE {flag_col} = TRUE'
        ).fetchone()[0]
        print(f'  {task:<8} classified: {done:,}')
    con.close()


def cmd_export(task: str, batch_size: int, out_dir: str):
    mod, _flag, system = get_task(task)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DB, read_only=True)
    rows = mod.load_unclassified(con)
    con.close()

    (out / f'{task}_system.txt').write_text(system)

    n_batches = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        lines = [
            json.dumps({
                'openalex_id': oid,
                'title': title,
                'abstract': truncate_abstract(abstract),
            }, ensure_ascii=False)
            for oid, title, abstract in chunk
        ]
        (out / f'{task}_{n_batches:04d}.jsonl').write_text('\n'.join(lines))
        n_batches += 1

    print(f'{task}: exported {len(rows):,} works in {n_batches} batches '
          f'of ≤{batch_size} to {out}/')
    print(f'  system prompt: {out}/{task}_system.txt')


def cmd_import(task: str, result_files: list[str]):
    mod, _flag, _system = get_task(task)

    results: list[tuple[str, str]] = []
    skipped = 0
    for rf in result_files:
        for line in Path(rf).read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            if '|' not in line or not line.startswith(('https://openalex.org/', 'W')):
                skipped += 1
                continue
            oid, raw = line.split('|', 1)
            # Bare IDs → full OpenAlex URLs as stored in the DB
            if oid.startswith('W'):
                oid = f'https://openalex.org/{oid}'
            results.append((oid, raw))

    if skipped:
        print(f'  WARNING: skipped {skipped} malformed lines')
    if not results:
        print('No valid result lines found; nothing imported.')
        return

    con = duckdb.connect(DB)
    # write_results matches on openalex_id; stray IDs update nothing and
    # re-imports simply overwrite the same rows (idempotent).
    mod.write_results(con, results)
    con.close()

    print(f'{task}: imported {len(results):,} labels from '
          f'{len(result_files)} file(s)')


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='cmd', required=True)

    sub.add_parser('status')

    p_exp = sub.add_parser('export')
    p_exp.add_argument('--task', choices=['topic', 'methods', 'country'],
                       required=True)
    p_exp.add_argument('--batch-size', type=int, default=50)
    p_exp.add_argument('--out-dir', default='batches')

    p_imp = sub.add_parser('import')
    p_imp.add_argument('--task', choices=['topic', 'methods', 'country'],
                       required=True)
    p_imp.add_argument('results', nargs='+')

    args = parser.parse_args()
    if args.cmd == 'status':
        cmd_status()
    elif args.cmd == 'export':
        cmd_export(args.task, args.batch_size, args.out_dir)
    elif args.cmd == 'import':
        cmd_import(args.task, args.results)


if __name__ == '__main__':
    main()
