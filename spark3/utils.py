import ctypes


def _rshift(val: int, n: int) -> int:
    """
    logic right shift
    """
    return val >> n if val >= 0 else (val + 0x100000000) >> n


def _mix_h1(h1: ctypes.c_int32, k1: ctypes.c_int32) -> ctypes.c_int32:
    h1.value ^= k1.value
    h1.value = h1.value << 13 | _rshift(h1.value, 19)
    h1.value = (h1.value * 5 + -430675100)
    return h1


def _mix_k1(k1: ctypes.c_int32) -> ctypes.c_int32:
    k1.value = k1.value * -862048943
    k1.value = k1.value << 15 | _rshift(k1.value, 17)
    k1.value = k1.value * 461845907
    return k1


def _fix_mix(h1: ctypes.c_int32, length: int) -> ctypes.c_int32:
    h1.value ^= length
    h1.value ^= _rshift(h1.value, 16)
    h1.value *= -2048144789
    h1.value ^= _rshift(h1.value, 13)
    h1.value *= -1028477387
    h1.value ^= _rshift(h1.value, 16)
    return h1


def _4_bytes_to_int(b: bytearray, offset: int, length: int) -> ctypes.c_int32:
    assert offset + 4 <= length

    result = b[offset + 3] << 24 | \
             b[offset + 2] << 16 | \
             b[offset + 1] << 8 | \
             b[offset + 0]

    # TODO: BigEndian reverse bytes
    return ctypes.c_int32(result)


def _hash_bytes_by_int(b: bytearray, seed: int) -> ctypes.c_int32:
    length_in_bytes = len(b)
    assert length_in_bytes % 4 == 0

    h1 = ctypes.c_int32(seed)

    for i in range(0, length_in_bytes, 4):
        half_word = _4_bytes_to_int(b, i, length_in_bytes)
        h1 = _mix_h1(h1, _mix_k1(half_word))

    return h1


spark_default_seed = 42


def hash_unsafe_bytes(content: str, seed: int = spark_default_seed) -> int:
    b = bytearray(content.encode())
    length_in_bytes = len(b)
    length_aligned = length_in_bytes - length_in_bytes % 4
    h1 = _hash_bytes_by_int(b[:length_aligned], seed)

    for i in range(length_aligned, length_in_bytes):
        half_word = ctypes.c_int32(b[i])
        k1 = _mix_k1(half_word)
        h1 = _mix_h1(h1, k1)

    return _fix_mix(h1, length_in_bytes).value
