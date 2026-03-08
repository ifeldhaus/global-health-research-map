"""
validation/02_kappa.py

Computes Cohen's kappa between human labels and LLM classifications,
generates confusion matrices, and writes a validation report.

Prerequisites:
    validation/validation_sample_labeled.csv must exist with columns:
        human_topic_category, human_subtopic, human_method
    (added by hand-labeling the sample from 01_sample.py)

Usage:
    uv run python validation/02_kappa.py            # compute kappa
    uv run python validation/02_kappa.py --test      # mock human labels for testing
"""

import argparse
import csv
import os
import random
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import cohen_kappa_score, confusion_matrix

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import notify  # noqa: E402

SAMPLE_PATH = Path('validation/validation_sample.csv')
LABELED_PATH = Path('validation/validation_sample_labeled.csv')
CONFUSION_DIR = Path('validation/confusion_matrices')
REPORT_PATH = Path('validation/VALIDATION_REPORT.md')
TAXONOMY_DIR = Path('data/taxonomy')


# ---------------------------------------------------------------------------
# Taxonomy label loaders
# ---------------------------------------------------------------------------

def load_topic_labels() -> dict[str, str]:
    path = TAXONOMY_DIR / 'topic_taxonomy.csv'
    labels = {}
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['category_letter']] = row['category_name']
    return labels


def load_method_labels() -> dict[str, str]:
    path = TAXONOMY_DIR / 'methods_taxonomy.csv'
    labels = {}
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['method_id']] = row['method_name']
    return labels


# ---------------------------------------------------------------------------
# Kappa interpretation
# ---------------------------------------------------------------------------

def interpret_kappa(kappa: float) -> tuple[str, str]:
    """Return (rating, recommendation) for a kappa score."""
    if kappa >= 0.75:
        return 'Excellent', 'Proceed to analysis.'
    elif kappa >= 0.65:
        return 'Good', 'Proceed, but document limitation in report.'
    elif kappa >= 0.50:
        return ('Moderate', 'Review confusion matrix, revise prompt, '
                're-run classification, re-validate on fresh sample.')
    else:
        return ('Poor', 'Taxonomy or prompt needs significant revision. '
                'Do not proceed to analysis.')


def overall_decision(kappas: dict[str, float]) -> str:
    """Return PASS / REVISE / FAIL based on all kappa scores."""
    cat_kappa = kappas.get('topic_category', 0)
    mth_kappa = kappas.get('methods', 0)

    if cat_kappa >= 0.75 and mth_kappa >= 0.70:
        return 'PASS'
    elif cat_kappa >= 0.50 and mth_kappa >= 0.50:
        return 'REVISE'
    else:
        return 'FAIL'


# ---------------------------------------------------------------------------
# Mock human labels for --test mode
# ---------------------------------------------------------------------------

def generate_mock_labels(df: pd.DataFrame, disagreement_rate: float = 0.20) -> pd.DataFrame:
    """Copy LLM labels and introduce random disagreement for testing."""
    df = df.copy()

    # Get valid categories and methods from the data
    topic_cats = sorted(df['topic_category'].dropna().unique())
    subtopics = sorted(df['topic_subtopic'].dropna().unique())
    methods = sorted(df['method_type'].dropna().unique())

    random.seed(42)

    human_topic_cats = []
    human_subtopics = []
    human_methods = []
    agree_topics = []
    agree_methods = []

    for _, row in df.iterrows():
        # Topic category
        if random.random() < disagreement_rate:
            # Disagree: pick a different category
            other_cats = [c for c in topic_cats if c != row['topic_category']]
            human_cat = random.choice(other_cats) if other_cats else row['topic_category']
            agree_topic = 0
        else:
            human_cat = row['topic_category']
            agree_topic = 1

        # Subtopic (correlated with category)
        if agree_topic == 0:
            # If category disagrees, subtopic must also disagree
            other_subs = [s for s in subtopics if not s.startswith(row['topic_category'])]
            human_sub = random.choice(other_subs) if other_subs else row['topic_subtopic']
        elif random.random() < disagreement_rate * 0.5:
            # Sometimes disagree on subtopic even if category matches
            same_cat_subs = [s for s in subtopics
                             if s.startswith(human_cat) and s != row['topic_subtopic']]
            human_sub = random.choice(same_cat_subs) if same_cat_subs else row['topic_subtopic']
            agree_topic = 0
        else:
            human_sub = row['topic_subtopic']

        # Method
        if random.random() < disagreement_rate:
            other_methods = [m for m in methods if m != row['method_type']]
            human_method = random.choice(other_methods) if other_methods else row['method_type']
            agree_method = 0
        else:
            human_method = row['method_type']
            agree_method = 1

        human_topic_cats.append(human_cat)
        human_subtopics.append(human_sub)
        human_methods.append(human_method)
        agree_topics.append(agree_topic)
        agree_methods.append(agree_method)

    df['human_topic_category'] = human_topic_cats
    df['human_subtopic'] = human_subtopics
    df['human_method'] = human_methods
    df['agree_topic'] = agree_topics
    df['agree_method'] = agree_methods

    return df


# ---------------------------------------------------------------------------
# Confusion matrix visualization
# ---------------------------------------------------------------------------

def plot_confusion_matrix(
    y_true: list, y_pred: list, labels: list,
    title: str, filename: str, label_names: dict | None = None,
):
    """Generate and save a confusion matrix heatmap."""
    CONFUSION_DIR.mkdir(parents=True, exist_ok=True)

    # Filter to labels that actually appear
    present = sorted(set(y_true) | set(y_pred))
    present = [l for l in labels if l in present]
    if not present:
        return

    cm = confusion_matrix(y_true, y_pred, labels=present)

    # Display labels
    if label_names:
        display_labels = [f"{l} {label_names.get(l, '')[:15]}" for l in present]
    else:
        display_labels = present

    fig, ax = plt.subplots(figsize=(max(8, len(present) * 0.6),
                                     max(6, len(present) * 0.5)))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax,
                xticklabels=display_labels, yticklabels=display_labels,
                linewidths=0.5, linecolor='white')
    ax.set_xlabel('LLM Classification')
    ax.set_ylabel('Human Label')
    ax.set_title(title)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)

    path = CONFUSION_DIR / filename
    fig.savefig(path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f'    ✓ {filename}')
    return path


# ---------------------------------------------------------------------------
# Compute kappa and confusion analysis
# ---------------------------------------------------------------------------

def compute_kappa_analysis(df: pd.DataFrame) -> dict:
    """Compute all kappa scores and confusion analysis."""
    topic_labels = load_topic_labels()
    method_labels = load_method_labels()

    results = {}

    # --- Topic category kappa ---
    valid_topic = df.dropna(subset=['human_topic_category', 'topic_category'])
    if len(valid_topic) > 0:
        human_cats = valid_topic['human_topic_category'].tolist()
        llm_cats = valid_topic['topic_category'].tolist()

        kappa_cat = cohen_kappa_score(human_cats, llm_cats)
        accuracy_cat = sum(h == l for h, l in zip(human_cats, llm_cats)) / len(human_cats)

        # Per-category accuracy
        per_cat_accuracy = {}
        for cat in sorted(set(human_cats)):
            mask = [h == cat for h in human_cats]
            cat_human = [h for h, m in zip(human_cats, mask) if m]
            cat_llm = [l for l, m in zip(llm_cats, mask) if m]
            correct = sum(h == l for h, l in zip(cat_human, cat_llm))
            per_cat_accuracy[cat] = {
                'n': len(cat_human),
                'correct': correct,
                'accuracy': correct / len(cat_human) if cat_human else 0,
            }

        # Confusion pairs
        error_pairs = [(h, l) for h, l in zip(human_cats, llm_cats) if h != l]
        confusion_pairs = Counter(error_pairs).most_common(10)

        results['topic_category'] = {
            'kappa': kappa_cat,
            'accuracy': accuracy_cat,
            'n': len(valid_topic),
            'per_category': per_cat_accuracy,
            'confusion_pairs': confusion_pairs,
        }

        # Plot confusion matrix
        all_cats = sorted(set(human_cats) | set(llm_cats))
        plot_confusion_matrix(
            human_cats, llm_cats, all_cats,
            'Topic Category: Human vs LLM',
            'topic_category_confusion.png',
            topic_labels,
        )
    else:
        results['topic_category'] = {'kappa': 0, 'accuracy': 0, 'n': 0}

    # --- Topic subtopic kappa ---
    valid_sub = df.dropna(subset=['human_subtopic', 'topic_subtopic'])
    if len(valid_sub) > 0:
        human_subs = valid_sub['human_subtopic'].tolist()
        llm_subs = valid_sub['topic_subtopic'].tolist()
        kappa_sub = cohen_kappa_score(human_subs, llm_subs)
        accuracy_sub = sum(h == l for h, l in zip(human_subs, llm_subs)) / len(human_subs)

        results['topic_subtopic'] = {
            'kappa': kappa_sub,
            'accuracy': accuracy_sub,
            'n': len(valid_sub),
        }
    else:
        results['topic_subtopic'] = {'kappa': 0, 'accuracy': 0, 'n': 0}

    # --- Methods kappa ---
    valid_method = df.dropna(subset=['human_method', 'method_type'])
    if len(valid_method) > 0:
        human_methods = valid_method['human_method'].tolist()
        llm_methods = valid_method['method_type'].tolist()
        kappa_mth = cohen_kappa_score(human_methods, llm_methods)
        accuracy_mth = sum(h == l for h, l in zip(human_methods, llm_methods)) / len(human_methods)

        # Confusion pairs
        error_pairs = [(h, l) for h, l in zip(human_methods, llm_methods) if h != l]
        method_confusion = Counter(error_pairs).most_common(10)

        results['methods'] = {
            'kappa': kappa_mth,
            'accuracy': accuracy_mth,
            'n': len(valid_method),
            'confusion_pairs': method_confusion,
        }

        # Plot confusion matrix
        all_methods = sorted(set(human_methods) | set(llm_methods))
        plot_confusion_matrix(
            human_methods, llm_methods, all_methods,
            'Methods: Human vs LLM',
            'methods_confusion.png',
            method_labels,
        )
    else:
        results['methods'] = {'kappa': 0, 'accuracy': 0, 'n': 0}

    # --- Confidence calibration ---
    if 'topic_confidence' in df.columns and len(valid_topic) > 0:
        calibration = {}
        for conf_level in ['high', 'med', 'low']:
            mask = valid_topic['topic_confidence'] == conf_level
            subset = valid_topic[mask]
            if len(subset) > 0:
                correct = sum(
                    h == l for h, l in
                    zip(subset['human_topic_category'], subset['topic_category'])
                )
                calibration[conf_level] = {
                    'n': len(subset),
                    'accuracy': correct / len(subset),
                }
            else:
                calibration[conf_level] = {'n': 0, 'accuracy': 0}
        results['confidence_calibration'] = calibration

    return results


# ---------------------------------------------------------------------------
# Generate validation report
# ---------------------------------------------------------------------------

def generate_report(results: dict, n_papers: int) -> str:
    """Generate a markdown validation report."""
    topic_labels = load_topic_labels()
    method_labels = load_method_labels()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

    kappas = {
        'topic_category': results.get('topic_category', {}).get('kappa', 0),
        'topic_subtopic': results.get('topic_subtopic', {}).get('kappa', 0),
        'methods': results.get('methods', {}).get('kappa', 0),
    }
    decision = overall_decision(kappas)

    lines = []
    lines.append('# Validation Report')
    lines.append(f'\nGenerated: {timestamp}')
    lines.append(f'Sample size: {n_papers} papers')
    lines.append('')

    # Decision banner
    if decision == 'PASS':
        lines.append('## Decision: ✅ PASS')
        lines.append('\nAll kappa scores meet or exceed target thresholds. '
                      'Proceed to analysis.')
    elif decision == 'REVISE':
        lines.append('## Decision: ⚠️ REVISE')
        lines.append('\nKappa scores are moderate. Review confusion pairs, revise '
                      'classification prompts, re-run overnight, and re-validate.')
    else:
        lines.append('## Decision: ❌ FAIL')
        lines.append('\nKappa scores are below acceptable thresholds. Taxonomy or '
                      'prompts need significant revision before proceeding.')

    # Kappa summary
    lines.append('\n## Kappa Scores\n')
    lines.append('| Classifier | Kappa | Accuracy | N | Target | Rating |')
    lines.append('|---|---|---|---|---|---|')

    for name, key, target in [
        ('Topic category', 'topic_category', 0.75),
        ('Topic subtopic', 'topic_subtopic', 0.65),
        ('Methods', 'methods', 0.70),
    ]:
        r = results.get(key, {})
        k = r.get('kappa', 0)
        acc = r.get('accuracy', 0)
        n = r.get('n', 0)
        rating, _ = interpret_kappa(k)
        status = '✅' if k >= target else '⚠️' if k >= 0.50 else '❌'
        lines.append(
            f'| {name} | {k:.3f} {status} | {acc:.1%} | {n} | ≥{target} | {rating} |'
        )

    # Per-category accuracy (topic)
    per_cat = results.get('topic_category', {}).get('per_category', {})
    if per_cat:
        lines.append('\n## Per-Category Accuracy (Topic)\n')
        lines.append('| Category | Name | N | Correct | Accuracy |')
        lines.append('|---|---|---|---|---|')
        for cat in sorted(per_cat.keys()):
            info = per_cat[cat]
            name = topic_labels.get(cat, cat)
            lines.append(
                f'| {cat} | {name} | {info["n"]} | {info["correct"]} | '
                f'{info["accuracy"]:.1%} |'
            )

    # Topic confusion pairs
    topic_confusion = results.get('topic_category', {}).get('confusion_pairs', [])
    if topic_confusion:
        lines.append('\n## Top Topic Confusion Pairs\n')
        lines.append('| Human Label | LLM Label | Count | Human Name | LLM Name |')
        lines.append('|---|---|---|---|---|')
        for (h, l), count in topic_confusion:
            h_name = topic_labels.get(h, h)
            l_name = topic_labels.get(l, l)
            lines.append(f'| {h} | {l} | {count} | {h_name} | {l_name} |')

    # Methods confusion pairs
    method_confusion = results.get('methods', {}).get('confusion_pairs', [])
    if method_confusion:
        lines.append('\n## Top Methods Confusion Pairs\n')
        lines.append('| Human Label | LLM Label | Count | Human Name | LLM Name |')
        lines.append('|---|---|---|---|---|')
        for (h, l), count in method_confusion:
            h_name = method_labels.get(h, h)
            l_name = method_labels.get(l, l)
            lines.append(f'| {h} | {l} | {count} | {h_name} | {l_name} |')

    # Confidence calibration
    calibration = results.get('confidence_calibration', {})
    if calibration:
        lines.append('\n## Confidence Calibration (Topic)\n')
        lines.append('Does the LLM\'s self-reported confidence correlate with accuracy?\n')
        lines.append('| Confidence | N | Accuracy |')
        lines.append('|---|---|---|')
        for conf in ['high', 'med', 'low']:
            info = calibration.get(conf, {})
            if info.get('n', 0) > 0:
                lines.append(f'| {conf} | {info["n"]} | {info["accuracy"]:.1%} |')

    # Confusion matrix images
    lines.append('\n## Confusion Matrices\n')
    lines.append('See `validation/confusion_matrices/` for visual confusion matrices:')
    lines.append('- `topic_category_confusion.png`')
    lines.append('- `methods_confusion.png`')

    # Next steps
    lines.append('\n## Recommended Next Steps\n')
    if decision == 'PASS':
        lines.append('1. Proceed to full analysis (Section 8 of execution guide)')
        lines.append('2. Archive this report with the final dataset')
    elif decision == 'REVISE':
        lines.append('1. Run the prompt optimizer: '
                      '`uv run python agents/prompt_optimizer.py`')
        lines.append('2. Re-run classification overnight with revised prompts')
        lines.append('3. Draw a fresh 100-paper sample and re-validate')
    else:
        lines.append('1. Review taxonomy for structural overlap between categories')
        lines.append('2. Consider merging categories that are consistently confused')
        lines.append('3. Revise prompts with explicit disambiguation rules')
        lines.append('4. Re-run classification and re-validate')

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Compute Cohen\'s kappa and generate validation report'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode: generate mock human labels with ~20%% disagreement',
    )
    args = parser.parse_args()

    plt.style.use('seaborn-v0_8-whitegrid')

    print('Validation Kappa Analysis')

    if args.test:
        print('  TEST MODE: generating mock human labels')
        # In test mode, use the unlabeled sample and add mock labels
        if not SAMPLE_PATH.exists():
            print(f'\n  ERROR: {SAMPLE_PATH} not found.')
            print('  Run validation/01_sample.py --test first.')
            sys.exit(1)

        df = pd.read_csv(SAMPLE_PATH)
        df = generate_mock_labels(df)
        # Save labeled version for reference
        LABELED_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(LABELED_PATH, index=False)
        print(f'  Mock-labeled sample saved to {LABELED_PATH}')

    else:
        if not LABELED_PATH.exists():
            print(f'\n  ERROR: {LABELED_PATH} not found.')
            print('  Complete hand-labeling first (see instructions from 01_sample.py).')
            sys.exit(1)

        df = pd.read_csv(LABELED_PATH)

    # Validate required columns
    required = ['human_topic_category', 'human_method', 'topic_category', 'method_type']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f'\n  ERROR: Missing columns: {", ".join(missing)}')
        print('  Ensure the labeled CSV has human_topic_category, human_subtopic, '
              'and human_method columns.')
        sys.exit(1)

    print(f'  Loaded {len(df)} labeled papers')

    # Compute kappa
    print('\n  Computing kappa scores...')
    results = compute_kappa_analysis(df)

    # Print summary
    kappas = {}
    for name, key, target in [
        ('Topic category', 'topic_category', 0.75),
        ('Topic subtopic', 'topic_subtopic', 0.65),
        ('Methods', 'methods', 0.70),
    ]:
        r = results.get(key, {})
        k = r.get('kappa', 0)
        acc = r.get('accuracy', 0)
        rating, _ = interpret_kappa(k)
        status = '✅' if k >= target else '⚠️' if k >= 0.50 else '❌'
        print(f'  {name}: kappa={k:.3f} {status} accuracy={acc:.1%} ({rating})')
        kappas[key] = k

    decision = overall_decision(kappas)
    print(f'\n  Overall decision: {decision}')

    # Generate report
    report = generate_report(results, len(df))
    REPORT_PATH.write_text(report)
    print(f'\n  Report saved to {REPORT_PATH}')

    # Notify
    notify(
        title='Validation Complete',
        message=f'Decision: {decision} — Topic κ={kappas.get("topic_category", 0):.3f}, '
                f'Methods κ={kappas.get("methods", 0):.3f}',
    )


if __name__ == '__main__':
    main()
