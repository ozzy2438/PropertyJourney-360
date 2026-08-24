from __future__ import annotations

from collections import defaultdict

import pytest

from propertyjourney360.identity.normalization import normalize_au_phone, normalize_email
from propertyjourney360.synthetic.identities import (
    RESERVED_EMAIL_DOMAINS,
    DatasetSplit,
    SyntheticDefect,
    SyntheticIdentityConfig,
    generate_identity_dataset,
)


def test_generation_is_byte_for_byte_deterministic_for_the_same_config() -> None:
    config = SyntheticIdentityConfig(customer_count=24, seed=731, held_out_fraction=0.25)

    first = generate_identity_dataset(config)
    second = generate_identity_dataset(config)

    assert [record.model_dump_json() for record in first.records] == [
        record.model_dump_json() for record in second.records
    ]
    assert first.labels == second.labels
    assert first.train_truth_customer_ids == second.train_truth_customer_ids
    assert first.held_out_truth_customer_ids == second.held_out_truth_customer_ids
    assert first.defect_counts == second.defect_counts


def test_held_out_split_is_deterministic_exact_and_has_no_entity_leakage() -> None:
    dataset = generate_identity_dataset(
        SyntheticIdentityConfig(customer_count=20, seed=99, held_out_fraction=0.25)
    )

    assert len(dataset.held_out_truth_customer_ids) == 5
    assert len(dataset.train_truth_customer_ids) == 15
    assert dataset.held_out_truth_customer_ids.isdisjoint(dataset.train_truth_customer_ids)
    assert {
        record.truth_customer_id for record in dataset.records_for_split(DatasetSplit.HELD_OUT)
    } == set(dataset.held_out_truth_customer_ids)
    assert {
        record.truth_customer_id for record in dataset.records_for_split(DatasetSplit.TRAIN)
    } == set(dataset.train_truth_customer_ids)

    split_by_truth: dict[str, set[DatasetSplit]] = defaultdict(set)
    for label in dataset.labels:
        split_by_truth[label.truth_customer_id].add(label.split)
    assert all(len(splits) == 1 for splits in split_by_truth.values())


def test_dataset_contains_labelled_duplicates_malformed_fields_and_ambiguity() -> None:
    dataset = generate_identity_dataset(
        SyntheticIdentityConfig(customer_count=24, seed=2026, held_out_fraction=0.2)
    )
    defects = {defect for label in dataset.labels for defect in label.defects}

    assert {
        SyntheticDefect.DUPLICATE_RECORD,
        SyntheticDefect.MALFORMED_EMAIL,
        SyntheticDefect.PHONE_FORMAT_VARIATION,
        SyntheticDefect.STALE_ADDRESS,
        SyntheticDefect.MALFORMED_POSTCODE,
        SyntheticDefect.NAME_TYPO,
        SyntheticDefect.COMMON_NAME_AMBIGUITY,
        SyntheticDefect.SHARED_PHONE_AMBIGUITY,
    } <= defects
    assert len(dataset.labels) == len(dataset.records)
    assert all(record.truth_customer_id for record in dataset.records)
    assert all(
        dataset.label_for(record.source_key).truth_customer_id == record.truth_customer_id
        for record in dataset.records
    )

    duplicate_labels = [
        label for label in dataset.labels if SyntheticDefect.DUPLICATE_RECORD in label.defects
    ]
    assert duplicate_labels
    assert all(label.source_key.startswith("legacy_crm:LEG-DUP-") for label in duplicate_labels)

    phone_truth: dict[str, set[str]] = defaultdict(set)
    for record in dataset.records:
        normalized_phone = normalize_au_phone(record.phone)
        if normalized_phone:
            assert record.truth_customer_id is not None
            phone_truth[normalized_phone].add(record.truth_customer_id)
    assert any(len(truth_ids) > 1 for truth_ids in phone_truth.values())


def test_contact_points_are_reserved_and_non_routable() -> None:
    dataset = generate_identity_dataset(SyntheticIdentityConfig(customer_count=40, seed=123))

    for record in dataset.records:
        if record.email and "@" in record.email:
            domain = record.email.strip().casefold().rsplit("@", maxsplit=1)[1]
            assert domain in RESERVED_EMAIL_DOMAINS
        if record.phone:
            normalized = normalize_au_phone(record.phone)
            assert normalized is not None
            assert normalized.startswith("+6135550")

    malformed = [
        record
        for record in dataset.records
        if SyntheticDefect.MALFORMED_EMAIL in dataset.label_for(record.source_key).defects
    ]
    assert malformed
    assert all(normalize_email(record.email) is None for record in malformed)


def test_true_match_pairs_follow_the_labelled_customer_ground_truth() -> None:
    dataset = generate_identity_dataset(SyntheticIdentityConfig(customer_count=8, seed=88))
    pairs = list(dataset.iter_true_match_pairs())

    assert pairs
    assert all(left.truth_customer_id == right.truth_customer_id for left, right in pairs)
    assert all(left.source_key != right.source_key for left, right in pairs)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"customer_count": 0}, "customer_count"),
        ({"held_out_fraction": 1.1}, "held_out_fraction"),
    ],
)
def test_generator_rejects_invalid_configuration(
    kwargs: dict[str, int | float], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        SyntheticIdentityConfig(**kwargs)  # type: ignore[arg-type]
