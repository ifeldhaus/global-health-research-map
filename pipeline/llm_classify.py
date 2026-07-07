"""
pipeline/llm_classify.py

Shared harness for the three LLM classification scripts:
    02_topic_classify.py, 03_methods_classify.py, 06_study_country.py

Provides the Anthropic client, retry policy, billing-error abort, batch
runner, and the chunked main loop. Each script supplies only its prompt,
mock classifier, label parser, and DB read/write functions.

The model defaults to claude-haiku-4-5 and can be overridden per run:
    CLASSIFIER_MODEL=claude-sonnet-5 uv run python pipeline/02_topic_classify.py
"""

import asyncio
import os

import anthropic
import duckdb
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from pipeline.utils import truncate_abstract

DEFAULT_MODEL = 'claude-haiku-4-5'

# Papers below this abstract length are skipped by all classifiers and
# tagged by tag_unclassifiable.py — keep the two in sync.
MIN_ABSTRACT_LENGTH = 50

# Boilerplate patterns that appear in the abstract field but aren't real
# abstracts (e.g. journal descriptions stored by OpenAlex). Shared with
# tag_unclassifiable.py.
JUNK_ABSTRACT_PATTERNS = [
    'Annals of Global Health is a peer-reviewed%',
    'Welcome to Annals of Global Health%',
]


def get_model() -> str:
    return os.getenv('CLASSIFIER_MODEL', DEFAULT_MODEL)


class BillingError(Exception):
    """Raised when the API returns a billing/credits error."""
    pass


def _is_retryable(exc: BaseException) -> bool:
    """Only retry on transient errors (rate-limit, server errors), not 400s."""
    if isinstance(exc, anthropic.RateLimitError):
        return True
    if isinstance(exc, anthropic.APIStatusError) and exc.status_code >= 500:
        return True
    return False


_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic()
    return _client


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception(_is_retryable),
)
async def _classify_one(
    openalex_id: str, title: str, abstract: str, system: str, max_tokens: int,
) -> tuple[str, str]:
    """Returns (openalex_id, raw_label_string)."""
    user_content = f'Title: {title}\n\nAbstract: {truncate_abstract(abstract)}'
    msg = await _get_client().messages.create(
        model=get_model(),
        max_tokens=max_tokens,
        # cache_control caches the (large, static) system prompt across
        # requests — prompts below the model's cacheable minimum are
        # simply not cached, at no cost.
        system=[{
            'type': 'text',
            'text': system,
            'cache_control': {'type': 'ephemeral'},
        }],
        messages=[{'role': 'user', 'content': user_content}],
    )
    return openalex_id, msg.content[0].text.strip()


async def _classify_batch(
    batch: list[tuple[str, str, str]],
    system: str,
    max_tokens: int,
    mock_fn=None,
) -> list[tuple[str, str]]:
    """Classify one chunk. mock_fn(title, abstract) -> raw label, if set."""
    if mock_fn is not None:
        return [(oid, mock_fn(title, abstract)) for oid, title, abstract in batch]

    tasks = [
        _classify_one(oid, title, abstract, system, max_tokens)
        for oid, title, abstract in batch
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for billing errors first — abort the entire batch without writing.
    for r in results:
        if isinstance(r, Exception):
            err_msg = str(r).lower()
            if 'credit balance' in err_msg or 'billing' in err_msg:
                raise BillingError(
                    'API credit balance too low. The script will stop now.\n'
                    'Top up credits at https://console.anthropic.com/settings/billing\n'
                    'then re-run this script — it will resume where it left off.'
                )

    # Only include successful results; failures remain unclassified
    # and will be retried on the next run.
    out = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            print(f'  WARNING: skipping {batch[i][0]} (will retry next run): {r}')
        else:
            out.append(r)
    return out


def run_classification(
    con: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str, str]],
    system: str,
    write_fn,
    *,
    max_tokens: int,
    chunk_size: int = 10,
    mock_fn=None,
) -> bool:
    """Chunked classification loop with progress output.

    rows:     list of (openalex_id, title, abstract)
    write_fn: write_fn(con, results) persists a chunk's results
    Returns True if the run completed, False if it aborted on a billing error
    (progress up to that point is already written).
    """
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        try:
            results = asyncio.run(
                _classify_batch(chunk, system, max_tokens, mock_fn=mock_fn)
            )
        except BillingError as e:
            print(f'\n✗ {e}')
            print(f'  Progress saved: {total:,}/{len(rows):,} classified so far.')
            return False
        if results:
            write_fn(con, results)
        total += len(results)
        pct = total / len(rows) * 100
        print(f'  {total:,}/{len(rows):,} ({pct:.1f}%) classified')
    return True


def mode_label(test: bool, mock: bool) -> str:
    parts = []
    if test:
        parts.append('TEST')
    if mock:
        parts.append('MOCK')
    return f' [{" + ".join(parts)}]' if parts else ''
