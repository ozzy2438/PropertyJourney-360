from propertyjourney360.identity.normalization import (
    normalize_address,
    normalize_au_phone,
    normalize_email,
    normalize_name,
    normalize_postcode,
)


def test_normalizes_australian_phone_variants() -> None:
    expected = "+61412345678"
    assert normalize_au_phone("0412 345 678") == expected
    assert normalize_au_phone("+61 412 345 678") == expected
    assert normalize_au_phone("0061 412 345 678") == expected


def test_rejects_short_or_implausible_phone() -> None:
    assert normalize_au_phone("12345") is None
    assert normalize_au_phone("0011 44 7700 900123") is None


def test_email_normalization_is_conservative() -> None:
    assert normalize_email(" Alice.Example+home@Example.COM ") == ("alice.example+home@example.com")
    assert normalize_email("not-an-email") is None


def test_name_address_and_postcode_normalization() -> None:
    assert normalize_name(" José ", " O'Neil ") == "jose o neil"
    assert normalize_postcode("VIC 3000") == "3000"
    assert normalize_address("10 Collins Street", "Melbourne", "3000") == (
        "10 collins st melbourne 3000"
    )
