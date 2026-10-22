"""Every Python example in `docs/` runs as written.

The documentation describes how the library is used, so its examples are part of what a change
delivers. A block that cannot run on its own, because it calls a network service for instance,
carries `<!-- example: skip -->` on the line above its opening fence.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parent.parent / "docs"
EXAMPLE = re.compile(
    r"^(?P<skip><!-- example: skip -->\n)?```python\n(?P<code>.*?)^```$",
    re.DOTALL | re.MULTILINE,
)


def _examples() -> list[tuple[str, str]]:
    """Return every runnable example as (location, code), the location as `page.md:line`."""
    examples: list[tuple[str, str]] = []
    for page in sorted(DOCS.rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        for match in EXAMPLE.finditer(text):
            if match["skip"]:
                continue
            line = text.count("\n", 0, match.start("code")) + 1
            examples.append((f"{page.relative_to(DOCS).as_posix()}:{line}", match["code"]))
    return examples


EXAMPLES = _examples()


@pytest.mark.parametrize(
    "code", [code for _, code in EXAMPLES], ids=[location for location, _ in EXAMPLES]
)
def test_documented_example_runs(code: str, tmp_path: Path) -> None:
    # A fresh interpreter in an empty directory: an example can rely only on what it shows.
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
