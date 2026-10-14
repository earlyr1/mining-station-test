"""Заголовок vq-src-offset (§2.3): dispatcher пишет его src_headers, verify и витрина разбирают parse_src_offset.

Правило разбора одно на всех потребителей — только ASCII-цифры: int() принял бы ' 12', '+12', '-1', '1_000' и
не-ASCII цифры, и verify или витрина считали бы своим то, чего dispatcher не пишет.
"""

import pytest
from hypothesis import example, given
from hypothesis import strategies as st

from vq.core.codec import SRC_OFFSET_HEADER, Headers, parse_src_offset, src_headers


def test_header_is_offset_as_decimal_ascii():
    assert SRC_OFFSET_HEADER == "vq-src-offset"
    assert src_headers(0) == [("vq-src-offset", b"0")]
    assert src_headers(1234567) == [("vq-src-offset", b"1234567")]


@pytest.mark.parametrize(
    ("headers", "want"),
    [
        # ASCII-цифры
        ([("vq-src-offset", b"0")], 0),
        ([("vq-src-offset", b"123")], 123),
        ([("vq-src-offset", b"007")], 7),
        # чужие заголовки не мешают; заголовка нет
        ([("other", b"7"), ("vq-src-offset", b"42")], 42),
        ([("other", b"7")], None),
        ([("VQ-SRC-OFFSET", b"7")], None),  # имя сравнивается точно
        ([], None),
        (None, None),
        # решает первый заголовок с этим именем
        ([("vq-src-offset", b"5"), ("vq-src-offset", b"6")], 5),
        ([("vq-src-offset", None), ("vq-src-offset", b"5")], None),
        ([("vq-src-offset", b"x"), ("vq-src-offset", b"5")], None),
        # не ASCII-цифры
        ([("vq-src-offset", None)], None),
        ([("vq-src-offset", b"")], None),
        ([("vq-src-offset", b" 12")], None),
        ([("vq-src-offset", b"12\n")], None),
        ([("vq-src-offset", b"+12")], None),
        ([("vq-src-offset", b"-1")], None),
        ([("vq-src-offset", b"1_000")], None),
        ([("vq-src-offset", b"12a")], None),
        ([("vq-src-offset", "١٢".encode())], None),  # арабско-индийские цифры: int() дал бы 12
        ([("vq-src-offset", b"1" * 5_000)], None),  # длиннее предела int(): None, а не исключение
        # значение-строка
        ([("vq-src-offset", "15")], 15),
        ([("vq-src-offset", " 15")], None),
        ([("vq-src-offset", "١٢")], None),
        # словарь и любая последовательность пар
        ({"vq-src-offset": "15"}, 15),
        ({"vq-src-offset": b"15"}, 15),
        ({"vq-src-offset": None}, None),
        ({"other": b"15"}, None),
        ({}, None),
        ((("vq-src-offset", b"3"),), 3),
    ],
)
def test_parse_src_offset(headers: Headers, want: int | None) -> None:
    assert parse_src_offset(headers) == want


@given(st.integers(min_value=0, max_value=2**63 - 1))  # offset Kafka — неотрицательный int64
@example(0)
@example(2**63 - 1)
def test_parse_reads_back_what_src_headers_writes(offset: int) -> None:
    assert parse_src_offset(src_headers(offset)) == offset
    assert parse_src_offset(dict(src_headers(offset))) == offset
