"""Conservative normalisation for Australian customer identity fields."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from propertyjourney360.domain.models import IdentityRecord

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")
_ADDRESS_TOKENS = {
    "avenue": "ave",
    "boulevard": "blvd",
    "court": "ct",
    "drive": "dr",
    "highway": "hwy",
    "lane": "ln",
    "parade": "pde",
    "place": "pl",
    "road": "rd",
    "street": "st",
    "terrace": "tce",
}


def _ascii_casefold(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(char for char in decomposed if not unicodedata.combining(char)).casefold()


def normalize_text(value: str | None) -> str | None:
    """Return a stable alphanumeric representation without inventing information."""

    if value is None:
        return None
    normalized = _NON_ALNUM_RE.sub(" ", _ascii_casefold(value))
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized or None


def normalize_email(value: str | None) -> str | None:
    """Normalise case and whitespace only; provider-specific alias rewrites are unsafe."""

    if value is None:
        return None
    normalized = value.strip().casefold()
    return normalized if _EMAIL_RE.fullmatch(normalized) else None


def normalize_au_phone(value: str | None) -> str | None:
    """Normalise plausible Australian numbers to E.164 without accepting short values."""

    if value is None:
        return None
    digits = "".join(char for char in value if char.isdigit())
    if digits.startswith("0061"):
        digits = digits[2:]
    if digits.startswith("61") and len(digits) == 11:
        national = digits[2:]
    elif digits.startswith("0") and len(digits) == 10:
        national = digits[1:]
    else:
        return None
    if len(national) != 9 or national[0] not in "234578":
        return None
    return f"+61{national}"


def normalize_name(first_name: str | None, last_name: str | None) -> str | None:
    parts = [part for raw in (first_name, last_name) if (part := normalize_text(raw))]
    return " ".join(parts) or None


def normalize_postcode(value: str | None) -> str | None:
    if value is None:
        return None
    digits = "".join(char for char in value if char.isdigit())
    return digits if len(digits) == 4 and 200 <= int(digits) <= 9999 else None


def normalize_address(
    address_line: str | None,
    suburb: str | None,
    postcode: str | None,
) -> str | None:
    line = normalize_text(address_line)
    suburb_value = normalize_text(suburb)
    postcode_value = normalize_postcode(postcode)
    if line:
        line = " ".join(_ADDRESS_TOKENS.get(token, token) for token in line.split())
    parts = [part for part in (line, suburb_value, postcode_value) if part]
    return " ".join(parts) or None


@dataclass(frozen=True, slots=True)
class NormalizedIdentity:
    email: str | None
    phone: str | None
    name: str | None
    address: str | None
    postcode: str | None
    trusted_cross_system_id: str | None


def normalize_identity(record: IdentityRecord) -> NormalizedIdentity:
    return NormalizedIdentity(
        email=normalize_email(record.email),
        phone=normalize_au_phone(record.phone),
        name=normalize_name(record.first_name, record.last_name),
        address=normalize_address(record.address_line, record.suburb, record.postcode),
        postcode=normalize_postcode(record.postcode),
        trusted_cross_system_id=normalize_text(record.trusted_cross_system_id),
    )
