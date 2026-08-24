"""Reproducible multi-source identity data with explicit evaluation ground truth.

No real private identity data is used. Email addresses use RFC-reserved example domains and
telephone values use ACMA's fictional Victorian geographic range, so generated contact points
must never reach a real person. Defects are intentional, labelled and deterministic.
"""

from __future__ import annotations

import hashlib
import random
import re
import unicodedata
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from propertyjourney360.domain.models import IdentityRecord, SourceSystem

RESERVED_EMAIL_DOMAINS = ("example.com", "example.net", "example.org")
_FIRST_NAMES = (
    "Aisha",
    "Alex",
    "Amelia",
    "Arjun",
    "Chloe",
    "Daniel",
    "Fatima",
    "Grace",
    "Harper",
    "Jack",
    "Kai",
    "Lachlan",
    "Mei",
    "Noah",
    "Priya",
    "Ruby",
    "Sofia",
    "Thomas",
    "William",
    "Zoe",
)
_LAST_NAMES = (
    "Brown",
    "Chen",
    "Clarke",
    "Davis",
    "Kaur",
    "Khan",
    "Lee",
    "Martin",
    "Nguyen",
    "O'Neil",
    "Patel",
    "Singh",
    "Smith",
    "Taylor",
    "Williams",
    "Wilson",
)
_NON_SLUG_RE = re.compile(r"[^a-z0-9]+")


class DatasetSplit(StrEnum):
    """Entity-level evaluation split; an identity can never leak across the boundary."""

    TRAIN = "train"
    HELD_OUT = "held_out"


class SyntheticDefect(StrEnum):
    """Intentional defects attached to source-record ground truth."""

    DUPLICATE_RECORD = "duplicate_record"
    EMAIL_FORMAT_VARIATION = "email_format_variation"
    MALFORMED_EMAIL = "malformed_email"
    PHONE_FORMAT_VARIATION = "phone_format_variation"
    MISSING_PHONE = "missing_phone"
    STALE_ADDRESS = "stale_address"
    MALFORMED_POSTCODE = "malformed_postcode"
    NAME_TYPO = "name_typo"
    COMMON_NAME_AMBIGUITY = "common_name_ambiguity"
    SHARED_PHONE_AMBIGUITY = "shared_phone_ambiguity"
    MISSING_EMAIL = "missing_email"


@dataclass(frozen=True, slots=True)
class SyntheticAddress:
    """A fictional Victorian-format address used only for private customer attributes."""

    address_line: str
    suburb: str
    postcode: str


DEFAULT_ADDRESSES = (
    SyntheticAddress("10 Example Street", "Melbourne", "3000"),
    SyntheticAddress("24 Sample Road", "Richmond", "3121"),
    SyntheticAddress("8 Demonstration Avenue", "Carlton", "3053"),
    SyntheticAddress("41 Prototype Parade", "Fitzroy", "3065"),
    SyntheticAddress("17 Test Lane", "Footscray", "3011"),
    SyntheticAddress("62 Fiction Court", "St Kilda", "3182"),
    SyntheticAddress("5 Placeholder Drive", "Brunswick", "3056"),
    SyntheticAddress("93 Sandbox Terrace", "Geelong", "3220"),
)


@dataclass(frozen=True, slots=True)
class SyntheticIdentityConfig:
    """Controls deterministic generation without accepting real contact data."""

    customer_count: int = 100
    seed: int = 20260824
    held_out_fraction: float = 0.2
    base_timestamp: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    addresses: tuple[SyntheticAddress, ...] = DEFAULT_ADDRESSES

    def __post_init__(self) -> None:
        if not 1 <= self.customer_count <= 10_000:
            raise ValueError("customer_count must be between 1 and 10,000")
        if not 0 <= self.held_out_fraction <= 1:
            raise ValueError("held_out_fraction must be between 0 and 1")
        if self.base_timestamp.tzinfo is None or self.base_timestamp.utcoffset() is None:
            raise ValueError("base_timestamp must include a timezone")
        if len(self.addresses) < 2:
            raise ValueError(
                "at least two fictional addresses are required for stale-address defects"
            )


@dataclass(frozen=True, slots=True)
class GroundTruthLabel:
    """Evaluation-only label for one source identity."""

    source_key: str
    truth_customer_id: str
    split: DatasetSplit
    defects: tuple[SyntheticDefect, ...]


@dataclass(frozen=True, slots=True)
class SyntheticIdentityDataset:
    """Generated identities, record labels and an entity-safe held-out split."""

    records: tuple[IdentityRecord, ...]
    labels: tuple[GroundTruthLabel, ...]
    train_truth_customer_ids: frozenset[str]
    held_out_truth_customer_ids: frozenset[str]
    defect_counts: tuple[tuple[SyntheticDefect, int], ...]
    seed: int

    def records_for_split(self, split: DatasetSplit) -> tuple[IdentityRecord, ...]:
        truth_ids = (
            self.train_truth_customer_ids
            if split is DatasetSplit.TRAIN
            else self.held_out_truth_customer_ids
        )
        return tuple(record for record in self.records if record.truth_customer_id in truth_ids)

    def label_for(self, source_key: str) -> GroundTruthLabel:
        for label in self.labels:
            if label.source_key == source_key:
                return label
        raise KeyError(source_key)

    def iter_true_match_pairs(self) -> Iterator[tuple[IdentityRecord, IdentityRecord]]:
        """Yield pairs that should resolve to the same canonical customer."""

        by_truth: dict[str, list[IdentityRecord]] = {}
        for record in self.records:
            assert record.truth_customer_id is not None
            by_truth.setdefault(record.truth_customer_id, []).append(record)
        for truth_id in sorted(by_truth):
            group = by_truth[truth_id]
            for left_index, left in enumerate(group):
                for right in group[left_index + 1 :]:
                    yield left, right


@dataclass(frozen=True, slots=True)
class _CanonicalIdentity:
    truth_customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    address: SyntheticAddress
    has_common_name: bool


def _slug(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_value = "".join(char for char in decomposed if not unicodedata.combining(char))
    return _NON_SLUG_RE.sub(".", ascii_value.casefold()).strip(".")


def _fictional_phone(index: int) -> str:
    """Return a number inside ACMA's fictional south-east ``03 5550 xxxx`` range."""

    return f"03 5550 {index:04d}"


def _international_phone(phone: str) -> str:
    digits = "".join(character for character in phone if character.isdigit())
    return f"+61 {digits[1]} {digits[2:6]} {digits[6:]}"


def _punctuated_phone(phone: str) -> str:
    digits = "".join(character for character in phone if character.isdigit())
    return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"


def _held_out_truth_ids(
    truth_ids: tuple[str, ...], *, seed: int, fraction: float
) -> frozenset[str]:
    ranked = sorted(
        truth_ids,
        key=lambda truth_id: hashlib.sha256(
            f"propertyjourney360:{seed}:held-out:{truth_id}".encode()
        ).digest(),
    )
    held_out_count = round(len(ranked) * fraction)
    if fraction > 0 and len(ranked) > 1:
        held_out_count = max(held_out_count, 1)
    if fraction < 1 and len(ranked) > 1:
        held_out_count = min(held_out_count, len(ranked) - 1)
    return frozenset(ranked[:held_out_count])


def _make_record(
    *,
    source_system: SourceSystem,
    source_record_id: str,
    updated_at: datetime,
    identity: _CanonicalIdentity,
    split: DatasetSplit,
    defects: tuple[SyntheticDefect, ...],
    email: str | None,
    email_verified: bool,
    phone: str | None,
    first_name: str | None,
    last_name: str | None,
    address: SyntheticAddress,
) -> IdentityRecord:
    return IdentityRecord(
        source_system=source_system,
        source_record_id=source_record_id,
        source_updated_at=updated_at,
        email=email,
        email_verified=email_verified,
        phone=phone,
        first_name=first_name,
        last_name=last_name,
        address_line=address.address_line,
        suburb=address.suburb,
        postcode=address.postcode,
        truth_customer_id=identity.truth_customer_id,
        attributes={
            "synthetic": True,
            "dataset_split": split.value,
            "defects": tuple(defect.value for defect in defects),
            "contact_safety": {
                "email_domain_policy": "RFC-reserved example domains only",
                "phone_policy": "ACMA fictional range only",
            },
        },
    )


def generate_identity_dataset(
    config: SyntheticIdentityConfig | None = None,
) -> SyntheticIdentityDataset:
    """Generate deterministic source identities with defects and evaluation-only truth labels."""

    settings = config or SyntheticIdentityConfig()
    rng = random.Random(settings.seed)  # noqa: S311 - reproducibility is the security property.
    canonicals: list[_CanonicalIdentity] = []
    first_canonical_name: tuple[str, str] | None = None

    for index in range(settings.customer_count):
        first_name = _FIRST_NAMES[rng.randrange(len(_FIRST_NAMES))]
        last_name = _LAST_NAMES[rng.randrange(len(_LAST_NAMES))]
        has_common_name = index > 0 and index % 7 == 0
        if has_common_name and first_canonical_name is not None:
            first_name, last_name = first_canonical_name
        if first_canonical_name is None:
            first_canonical_name = (first_name, last_name)
        truth_id = f"customer-{index + 1:06d}"
        domain = RESERVED_EMAIL_DOMAINS[index % len(RESERVED_EMAIL_DOMAINS)]
        local_part = f"{_slug(first_name)}.{_slug(last_name)}.{index + 1:06d}"
        canonicals.append(
            _CanonicalIdentity(
                truth_customer_id=truth_id,
                first_name=first_name,
                last_name=last_name,
                email=f"{local_part}@{domain}",
                phone=_fictional_phone(index),
                address=settings.addresses[rng.randrange(len(settings.addresses))],
                has_common_name=has_common_name,
            )
        )

    truth_ids = tuple(identity.truth_customer_id for identity in canonicals)
    held_out = _held_out_truth_ids(
        truth_ids,
        seed=settings.seed,
        fraction=settings.held_out_fraction,
    )
    train = frozenset(truth_ids) - held_out
    records: list[IdentityRecord] = []
    labels: list[GroundTruthLabel] = []

    def add(record: IdentityRecord, defects: tuple[SyntheticDefect, ...]) -> None:
        records.append(record)
        assert record.truth_customer_id is not None
        split = (
            DatasetSplit.HELD_OUT if record.truth_customer_id in held_out else DatasetSplit.TRAIN
        )
        labels.append(
            GroundTruthLabel(
                source_key=record.source_key,
                truth_customer_id=record.truth_customer_id,
                split=split,
                defects=defects,
            )
        )

    base_timestamp = settings.base_timestamp.astimezone(UTC)
    for index, identity in enumerate(canonicals):
        split = (
            DatasetSplit.HELD_OUT if identity.truth_customer_id in held_out else DatasetSplit.TRAIN
        )
        common_defects = (
            (SyntheticDefect.COMMON_NAME_AMBIGUITY,) if identity.has_common_name else ()
        )
        updated_at = base_timestamp + timedelta(days=index)

        salesforce_defects = common_defects
        salesforce = _make_record(
            source_system=SourceSystem.SALESFORCE,
            source_record_id=f"003PJ{index + 1:012d}",
            updated_at=updated_at + timedelta(hours=3),
            identity=identity,
            split=split,
            defects=salesforce_defects,
            email=identity.email,
            email_verified=True,
            phone=identity.phone,
            first_name=identity.first_name,
            last_name=identity.last_name,
            address=identity.address,
        )
        add(salesforce, salesforce_defects)

        legacy_defects: list[SyntheticDefect] = [SyntheticDefect.PHONE_FORMAT_VARIATION]
        legacy_defects.extend(common_defects)
        legacy_email: str | None = identity.email
        if index % 2 == 0:
            legacy_email = f" {identity.email.upper()} "
            legacy_defects.append(SyntheticDefect.EMAIL_FORMAT_VARIATION)
        if index % 5 == 0:
            legacy_email = f"malformed-email-{index + 1:06d}"
            legacy_defects.append(SyntheticDefect.MALFORMED_EMAIL)
        legacy_phone: str | None = _international_phone(identity.phone)
        if index % 7 == 0:
            legacy_phone = None
            legacy_defects.append(SyntheticDefect.MISSING_PHONE)
        legacy_address = identity.address
        if index % 3 == 0:
            address_index = settings.addresses.index(identity.address)
            legacy_address = settings.addresses[(address_index + 1) % len(settings.addresses)]
            legacy_defects.append(SyntheticDefect.STALE_ADDRESS)
        legacy_postcode = legacy_address.postcode
        if index % 8 == 0:
            legacy_postcode = f"{legacy_postcode[0]}O{legacy_postcode[2:]}"
            legacy_defects.append(SyntheticDefect.MALFORMED_POSTCODE)
        legacy_last_name = identity.last_name
        if index % 6 == 0 and len(legacy_last_name) > 2:
            legacy_last_name = legacy_last_name[:-1]
            legacy_defects.append(SyntheticDefect.NAME_TYPO)
        legacy_address_value = SyntheticAddress(
            legacy_address.address_line,
            legacy_address.suburb,
            legacy_postcode,
        )
        legacy_defect_tuple = tuple(dict.fromkeys(legacy_defects))
        legacy = _make_record(
            source_system=SourceSystem.LEGACY_CRM,
            source_record_id=f"LEG-{index + 1:08d}",
            updated_at=updated_at + timedelta(hours=1),
            identity=identity,
            split=split,
            defects=legacy_defect_tuple,
            email=legacy_email,
            email_verified=False,
            phone=legacy_phone,
            first_name=identity.first_name,
            last_name=legacy_last_name,
            address=legacy_address_value,
        )
        add(legacy, legacy_defect_tuple)

        support_defects: list[SyntheticDefect] = [SyntheticDefect.PHONE_FORMAT_VARIATION]
        support_defects.extend(common_defects)
        support_email: str | None = identity.email
        if index % 4 == 0:
            support_email = None
            support_defects.append(SyntheticDefect.MISSING_EMAIL)
        support_phone = _punctuated_phone(identity.phone)
        if index > 0 and index % 4 == 1:
            support_phone = _punctuated_phone(canonicals[index - 1].phone)
            support_defects.append(SyntheticDefect.SHARED_PHONE_AMBIGUITY)
        support_defect_tuple = tuple(dict.fromkeys(support_defects))
        support = _make_record(
            source_system=SourceSystem.SUPPORT,
            source_record_id=f"CASE-CONTACT-{index + 1:08d}",
            updated_at=updated_at + timedelta(hours=2),
            identity=identity,
            split=split,
            defects=support_defect_tuple,
            email=support_email,
            email_verified=False,
            phone=support_phone,
            first_name=identity.first_name,
            last_name=identity.last_name,
            address=identity.address,
        )
        add(support, support_defect_tuple)

        if index % 4 == 0:
            duplicate_defects = (*legacy_defect_tuple, SyntheticDefect.DUPLICATE_RECORD)
            duplicate = legacy.model_copy(
                update={
                    "source_record_id": f"LEG-DUP-{index + 1:08d}",
                    "source_updated_at": legacy.source_updated_at + timedelta(minutes=5),
                    "attributes": {
                        **legacy.attributes,
                        "defects": tuple(defect.value for defect in duplicate_defects),
                        "duplicate_of": legacy.source_key,
                    },
                }
            )
            add(duplicate, duplicate_defects)

    counts: Counter[SyntheticDefect] = Counter(
        defect for label in labels for defect in label.defects
    )
    return SyntheticIdentityDataset(
        records=tuple(records),
        labels=tuple(labels),
        train_truth_customer_ids=train,
        held_out_truth_customer_ids=held_out,
        defect_counts=tuple(sorted(counts.items(), key=lambda item: item[0].value)),
        seed=settings.seed,
    )
