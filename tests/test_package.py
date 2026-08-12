"""Smoke tests: assert the package is installed and importable through the src/ layout."""

import ingestio


def test_package_exposes_version() -> None:
    assert isinstance(ingestio.__version__, str)
    assert ingestio.__version__
