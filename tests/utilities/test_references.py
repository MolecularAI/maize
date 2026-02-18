"""References testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name

import pytest

from maize.utilities.references import Author, Reference


@pytest.fixture
def valid_doi() -> str:
    return "10.1186/s13321-024-00812-5"


@pytest.fixture
def invalid_doi() -> str:
    return "10.1000/s99999-021-00522-2"


@pytest.fixture
def author_json() -> dict[str, str | list[str]]:
    return {"given": "Jeffrey", "family": "Lebowski", "sequence": "first", "affiliation": []}


def test_author(author_json: dict[str, str | list[str]]) -> None:
    author = Author.from_json(author_json)
    assert author.family == "Lebowski"
    assert author.given == ["Jeffrey"]
    assert "J." in author.format()

def test_reference(valid_doi: str) -> None:
    ref = Reference.from_doi(valid_doi)
    assert ref.note is None
    assert ref.number == "20"
    assert ref.authors[0].family == "Loeffler"
    assert "Reinvent" in ref.title
    assert ref.year == 2024
    assert ref.volume == 16
    assert "Cheminf" in ref.journal

def test_reference_invalid(invalid_doi: str) -> None:
    ref = Reference.from_doi(invalid_doi)
    assert "Failed to retrieve" in ref.note
    assert ref.doi == invalid_doi
