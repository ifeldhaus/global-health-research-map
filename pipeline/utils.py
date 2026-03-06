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
