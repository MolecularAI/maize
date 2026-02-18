"""Reference / citation management for nodes"""

from dataclasses import dataclass
from functools import cache
import requests
from typing import cast, Any
from typing_extensions import Self


@dataclass
class Author:
    """
    Represents an author of a reference.

    Attributes
    ----------
    given
        List of given names
    family
        Family name

    """

    given: list[str]
    family: str

    @classmethod
    def from_json(cls, data: dict[str, str]) -> Self:
        """
        Creates an Author object from JSON data in the format
        returned by ``dx.doi.org``. It should look like this,
        with ``'given'`` and ``'family'`` entries being required:

        .. code-block:: json

           {'given': 'Jeffrey',
            'family': 'Lebowski',
            'sequence': 'first',
            'affiliation': []}

        Parameters
        ----------
        data
            JSON data

        Returns
        -------
        Author
            Author object

        """
        return cls(given=data["given"].split(), family=data["family"])

    @property
    def given_initials(self) -> str:
        """Provides the first name initials"""
        return " ".join(name.capitalize()[0] + "." for name in self.given)

    def format(self) -> str:
        """Formats the name as 'Lebowski, J.'"""
        return f"{self.family}, {self.given_initials}"


@dataclass
class Reference:
    """
    A citable reference to a journal article etc.

    Attributes
    ----------
    doi
        The digital object identifier (DOI) of the item
    authors
        List of authors
    title
        Title of the item
    journal
        Short name of the journal
    volume
        The volume number of the journal
    number
        The article number in the journal
    year
        The year the article was published

    """

    doi: str
    authors: list[Author] | None = None
    title: str | None = None
    journal: str | None = None
    volume: int | None = None
    number: str | None = None
    year: int | None = None
    note: str | None = None

    @classmethod
    def from_doi(cls, doi: str) -> Self:
        """
        Creates a reference object from a DOI.

        Parameters
        ----------
        doi
            The digital object identifier (DOI) of the item

        Returns
        -------
        Reference
            The created reference object

        """
        url = f"http://dx.doi.org/{doi}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return cls(doi=doi, note=f"Failed to retrieve citation (code: {response.status_code})")
        data: dict[str, Any] = response.json()

        missing = set()

        def _get(key: str) -> Any | None:
            val = data.get(key)
            if val is None:
                missing.add(key)
            return val

        volume = _get("volume")
        year = _get("published")
        return cls(
            doi=doi,
            authors=[
                Author.from_json(cast(dict[str, str], entry))
                for entry in data["author"]
                if "author" in data
            ],
            title=_get("title"),
            journal=_get("container-title-short"),
            number=_get("article-number"),
            volume=int(volume) if volume else None,
            year=int(year["date-parts"][0][0]) if year else None,
            note=f"Incomplete JSON response (missing: {missing})" if missing else None,
        )

    def __hash__(self) -> int:
        return hash(self.doi)

    def format(self) -> list[str]:
        """Formats the citation"""
        if all(
            attr is None for attr in (self.title, self.journal, self.volume, self.number, self.year)
        ):
            return [f"doi: {self.doi}", f"note: {self.note}"]
        authors = ", ".join(author.format() for author in self.authors) if self.authors else ""
        return [
            authors,
            f"{self.title}.",
            f"{self.journal} {self.volume}, {self.number} ({self.year}) doi: {self.doi}",
        ]
