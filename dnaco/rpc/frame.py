# frames are in the form of | length (u32) | ...data... |
# - the first 4bit are used to identify the protocol version (0-15).
# - the next 1bit is used to identify if the packet should be written to the WAL.
# - the following 27bits are used to identify the packet length (max 128M)
#   +------+---+--------------------------------------+
#   | 1111 | 1 | 111 | 11111111 | 11111111 | 11111111 |
#   +------+---+--------------------------------------+
#   0 rev. 4   5             data length             32

def build_frame_header(rev, write_to_wal, length):
    assert rev < 16
    h4 = (rev << 28) | ((1 << 27) if write_to_wal else 0) | length
    return h4.to_bytes(4, byteorder='big')


def parse_frame_header_u32(header):
    rev = ((header & 0xffffffff) >> 28) & 0x0f
    write_to_wal = (((header & 0xffffffff) >> 27) & 1) != 0
    length = (header & 0x7ffffff)
    return rev, write_to_wal, length


def parse_frame_header(header_bytes):
    h32 = int.from_bytes(header_bytes, byteorder='big')
    return parse_frame_header_u32(h32)


class FrameReader:
    def __init__(self, frame):
        self.offset = 0
        self.frame = frame

    def read(self, n):
        v = self.frame[self.offset:self.offset + n]
        self.offset += n
        return v

    def read_byte(self):
        v = self.frame[self.offset]
        self.offset += 1
        return v

    def read_all(self):
        v = self.frame[self.offset:]
        self.offset = len(self.frame)
        return v
