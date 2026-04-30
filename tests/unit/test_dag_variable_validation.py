"""Validates that all DAG files use Variable.get with default_var."""

import ast
from pathlib import Path


DAGS_DIR = Path(__file__).resolve().parent.parent.parent / "dags"

SCRAPE_DAGS = [
    DAGS_DIR / "scrape_agencies.py",
    DAGS_DIR / "scrape_ebc.py",
]


def _find_variable_get_calls(filepath: Path) -> list[ast.Call]:
    """Find all Variable.get() calls in a Python file via AST."""
    source = filepath.read_text()
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "get":
            if isinstance(func.value, ast.Name) and func.value.id == "Variable":
                calls.append(node)
    return calls


def _has_default_var(call: ast.Call) -> bool:
    """Check if a Variable.get() call has the default_var keyword argument."""
    return any(kw.arg == "default_var" for kw in call.keywords)


def test_scrape_dags_variable_get_has_default_var():
    """All Variable.get() calls in scrape DAGs must have default_var."""
    for dag_file in SCRAPE_DAGS:
        assert dag_file.exists(), f"{dag_file} not found"
        calls = _find_variable_get_calls(dag_file)
        assert len(calls) > 0, f"No Variable.get() calls found in {dag_file.name}"
        for call in calls:
            assert _has_default_var(call), (
                f"{dag_file.name}:{call.lineno} — Variable.get() missing default_var"
            )
