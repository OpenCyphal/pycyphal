"""Hash and CRC utilities"""

from __future__ import annotations

# =====================================================================================================================
# CRC-32C (Castagnoli)
# =====================================================================================================================

CRC32C_INITIAL = 0xFFFFFFFF
CRC32C_OUTPUT_XOR = 0xFFFFFFFF
CRC32C_RESIDUE = 0x48674BC7
# fmt: off
_CRC32C_TABLE = [
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
    0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
    0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
    0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
    0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
    0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
    0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
    0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
    0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
    0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
    0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
    0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
    0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
    0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
    0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
    0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
    0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
    0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
    0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
    0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
    0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
    0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
    0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
]
# fmt: on


def crc32c_add(crc: int, data: bytes | memoryview) -> int:
    """CRC-32C (Castagnoli) one update step without the output XOR."""
    for b in data:
        crc = (crc >> 8) ^ _CRC32C_TABLE[b ^ (crc & 0xFF)]
    return crc


def crc32c_full(data: bytes | memoryview) -> int:
    """CRC-32C (Castagnoli) with the output XOR."""
    return crc32c_add(CRC32C_INITIAL, data) ^ CRC32C_OUTPUT_XOR


# =====================================================================================================================
# CRC-16/CCITT-FALSE
# =====================================================================================================================

CRC16CCITT_FALSE_INITIAL = 0xFFFF
CRC16CCITT_FALSE_RESIDUE = 0x0000
# fmt: off
_CRC16CCITT_FALSE_TABLE = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7, 0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C,
    0xD1AD, 0xE1CE, 0xF1EF, 0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6, 0x9339, 0x8318,
    0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE, 0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4,
    0x5485, 0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D, 0x3653, 0x2672, 0x1611, 0x0630,
    0x76D7, 0x66F6, 0x5695, 0x46B4, 0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC, 0x48C4,
    0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823, 0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969,
    0xA90A, 0xB92B, 0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12, 0xDBFD, 0xCBDC, 0xFBBF,
    0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A, 0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49, 0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13,
    0x2E32, 0x1E51, 0x0E70, 0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78, 0x9188, 0x81A9,
    0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F, 0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046,
    0x6067, 0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E, 0x02B1, 0x1290, 0x22F3, 0x32D2,
    0x4235, 0x5214, 0x6277, 0x7256, 0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D, 0x34E2,
    0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405, 0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E,
    0xC71D, 0xD73C, 0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634, 0xD94C, 0xC96D, 0xF90E,
    0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB, 0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A, 0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1,
    0x1AD0, 0x2AB3, 0x3A92, 0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9, 0x7C26, 0x6C07,
    0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1, 0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9,
    0x9FF8, 0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0,
]
# fmt: on


def crc16ccitt_false_add(crc: int, data: bytes | memoryview) -> int:
    """CRC-16/CCITT-FALSE one update step without output post-processing."""
    for b in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16CCITT_FALSE_TABLE[((crc >> 8) ^ b) & 0xFF]
    return crc


def crc16ccitt_false_full(data: bytes | memoryview) -> int:
    """CRC-16/CCITT-FALSE with the standard initial value."""
    return crc16ccitt_false_add(CRC16CCITT_FALSE_INITIAL, data)


# =====================================================================================================================
# rapidhash V3
# =====================================================================================================================

_RAPID_MASK = 0xFFFFFFFFFFFFFFFF
_RAPID_SECRET = (
    0x2D358DCCAA6C78A5,
    0x8BB84B93962EACC9,
    0x4B33A62ED433D4A3,
    0x4D5A2DA51DE1AA47,
    0xA0761D6478BD642F,
    0xE7037ED1A0B428DB,
    0x90ED1765281C388C,
    0xAAAAAAAAAAAAAAAA,
)


def _rapid_mum(a: int, b: int) -> tuple[int, int]:
    r = a * b
    return r & _RAPID_MASK, (r >> 64) & _RAPID_MASK


def _rapid_mix(a: int, b: int) -> int:
    lo, hi = _rapid_mum(a, b)
    return lo ^ hi


def _r64(d: bytes, o: int) -> int:
    return int.from_bytes(d[o : o + 8], "little")


def _r32(d: bytes, o: int) -> int:
    return int.from_bytes(d[o : o + 4], "little")


def rapidhash(data: bytes | str) -> int:
    """
    A compliant implementation of rapidhash that matches rapidhash.h that can accept strings directly.
    The eponymous package published PyPI is NOT compatible with rapidhash.h, it must not be used!
    """
    data = data if isinstance(data, bytes) else data.encode("utf8")
    assert isinstance(data, bytes)
    s = _RAPID_SECRET
    n = len(data)
    seed = _rapid_mix(s[2], s[1])
    a = b = 0
    i = n
    p = 0
    if n <= 16:
        if n >= 4:
            seed = (seed ^ n) & _RAPID_MASK
            if n >= 8:
                a = _r64(data, 0)
                b = _r64(data, n - 8)
            else:
                a = _r32(data, 0)
                b = _r32(data, n - 4)
        elif n > 0:
            a = (data[0] << 45) | data[n - 1]
            b = data[n >> 1]
    else:
        if n > 112:
            see1 = see2 = see3 = see4 = see5 = see6 = seed
            while True:
                seed = _rapid_mix(_r64(data, p) ^ s[0], _r64(data, p + 8) ^ seed)
                see1 = _rapid_mix(_r64(data, p + 16) ^ s[1], _r64(data, p + 24) ^ see1)
                see2 = _rapid_mix(_r64(data, p + 32) ^ s[2], _r64(data, p + 40) ^ see2)
                see3 = _rapid_mix(_r64(data, p + 48) ^ s[3], _r64(data, p + 56) ^ see3)
                see4 = _rapid_mix(_r64(data, p + 64) ^ s[4], _r64(data, p + 72) ^ see4)
                see5 = _rapid_mix(_r64(data, p + 80) ^ s[5], _r64(data, p + 88) ^ see5)
                see6 = _rapid_mix(_r64(data, p + 96) ^ s[6], _r64(data, p + 104) ^ see6)
                p += 112
                i -= 112
                if i <= 112:
                    break
            seed ^= see1
            see2 ^= see3
            see4 ^= see5
            seed ^= see6
            see2 ^= see4
            seed ^= see2
        if i > 16:
            seed = _rapid_mix(_r64(data, p) ^ s[2], _r64(data, p + 8) ^ seed)
            if i > 32:
                seed = _rapid_mix(_r64(data, p + 16) ^ s[2], _r64(data, p + 24) ^ seed)
                if i > 48:
                    seed = _rapid_mix(_r64(data, p + 32) ^ s[1], _r64(data, p + 40) ^ seed)
                    if i > 64:
                        seed = _rapid_mix(_r64(data, p + 48) ^ s[1], _r64(data, p + 56) ^ seed)
                        if i > 80:
                            seed = _rapid_mix(_r64(data, p + 64) ^ s[2], _r64(data, p + 72) ^ seed)
                            if i > 96:
                                seed = _rapid_mix(_r64(data, p + 80) ^ s[1], _r64(data, p + 88) ^ seed)
        a = _r64(data, p + i - 16) ^ i
        b = _r64(data, p + i - 8)
    a ^= s[1]
    b ^= seed
    a, b = _rapid_mum(a, b)
    return _rapid_mix(a ^ s[7], b ^ s[1] ^ i)
