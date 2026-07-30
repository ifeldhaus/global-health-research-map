import subprocess
import platform
import duckdb

DB = 'data/global_health.duckdb'


def notify(title: str, message: str):
    """Desktop notification --- works on Mac and Linux."""
    if platform.system() == 'Darwin':
        subprocess.run(['osascript', '-e',
                        f'display notification "{message}" with title "{title}"'])
    elif platform.system() == 'Linux':
        subprocess.run(['notify-send', title, message])


def pipeline_complete(script_name: str, db_path: str = DB):
    """Call at end of every pipeline script."""
    con = duckdb.connect(db_path, read_only=True)
    n = con.execute('SELECT COUNT(*) FROM works').fetchone()[0]
    con.close()
    notify(
        title=f'Pipeline: {script_name} complete',
        message=f'{n:,} works in database. Ready for next stage.'
    )
    print(f'\n✓ {script_name} complete. {n:,} works in database.')


def truncate_abstract(text: str, max_words: int = 300) -> str:
    """Truncate abstract to max_words for LLM classification."""
    words = text.split()
    if len(words) <= max_words:
        return text
    return ' '.join(words[:max_words]) + '...'


# Evidence-type grouping of the method taxonomy. Used to separate primary
# research from opinion/discourse in the lens analyses. Commentary/editorial
# (M15) alone is ~28% of the corpus and would distort "where research happens"
# analyses if left in, so lenses A/B/D restrict to `empirical`.
#   empirical      — primary studies (trials, observational, modelling, econ, etc.)
#   synthesis      — rigorous secondary research (systematic & scoping reviews)
#   non_empirical  — narrative reviews, commentary/editorial/perspective, other/unclear
EVIDENCE_TYPE_MAP = {
    'M01': 'empirical', 'M02': 'empirical', 'M03': 'empirical',
    'M04': 'empirical', 'M06': 'empirical', 'M07': 'empirical',
    'M08': 'empirical', 'M09': 'empirical', 'M10': 'empirical',
    'M11': 'empirical', 'M12': 'empirical', 'M16': 'empirical',
    'M17': 'empirical',
    'M05': 'synthesis', 'M13': 'synthesis',
    'M14': 'non_empirical', 'M15': 'non_empirical', 'M18': 'non_empirical',
}


def evidence_type(method_type):
    """Map a method_type code to its evidence-type group (or None)."""
    if method_type is None:
        return None
    return EVIDENCE_TYPE_MAP.get(method_type, 'non_empirical')
