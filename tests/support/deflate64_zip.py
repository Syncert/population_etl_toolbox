"""Write a one-member zip compressed with Deflate64 (zip method 9).

NCES compresses its CCD membership files this way; ``zipfile`` can read the
directory of such an archive but cannot write or decompress the member. The
fixture and the unit tests build their archives here, with ``inflate64``'s
compressor, so they exercise the same decompression path as NCES's files.
"""

from __future__ import annotations

import struct
import zlib

import inflate64

_METHOD = 9
_VERSION = 21  # 2.1, the version that introduced Deflate64


def deflate64_zip(name: str, data: bytes) -> bytes:
    """A zip holding ``name`` = ``data``, compressed with Deflate64."""
    deflater = inflate64.Deflater()
    compressed = deflater.deflate(data) + deflater.flush()
    encoded = name.encode("utf-8")
    crc = zlib.crc32(data) & 0xFFFFFFFF
    local = struct.pack(
        "<4sHHHHHIIIHH",
        b"PK\x03\x04",
        _VERSION,
        0,
        _METHOD,
        0,
        0x21,
        crc,
        len(compressed),
        len(data),
        len(encoded),
        0,
    )
    central = struct.pack(
        "<4sHHHHHHIIIHHHHHII",
        b"PK\x01\x02",
        _VERSION,
        _VERSION,
        0,
        _METHOD,
        0,
        0x21,
        crc,
        len(compressed),
        len(data),
        len(encoded),
        0,
        0,
        0,
        0,
        0,
        0,
    )
    body = local + encoded + compressed
    directory = central + encoded
    end = struct.pack(
        "<4sHHHHIIH",
        b"PK\x05\x06",
        0,
        0,
        1,
        1,
        len(directory),
        len(body),
        0,
    )
    return body + directory + end
