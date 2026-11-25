"""
protocol.py
-----------
The packet format used by ALL peers. Every other file imports from here.
If you change the protocol, change it ONLY here (and in your report).

Packet = HEADER (22 bytes) + PAYLOAD (0..1000 bytes)

Header fields (network byte order = big endian):
    version      1 byte   protocol version (1)
    type         1 byte   message type (see list below)
    flags        1 byte   bit0 = MORE (this packet is not the last fragment of a message)
    stream       1 byte   0 = control, 1 = chat, 2 = file
    sender_id    4 bytes  unique id of the sending peer
    seq          4 bytes  sequence number (per peer-pair and per stream; starts at 1)
    ack          4 bytes  in an ACK packet: the seq number being acknowledged
    payload_len  2 bytes  number of payload bytes
    crc32        4 bytes  checksum of (header without crc) + payload
"""
import json
import struct
import zlib
from collections import namedtuple

VERSION = 1

HEADER_FORMAT = "!BBBBIIIHI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 22 bytes
MAX_PAYLOAD = 1000                            # bytes of data in one UDP packet

# ---------------- message types ----------------
JOIN = 1             # "I am new, who is here?"   (unreliable, broadcast or direct)
JOIN_REPLY = 2       # list of peers I know       (reliable)
LEAVE = 3            # "I am leaving"             (unreliable)
HEARTBEAT = 4        # "I am alive"               (unreliable)
CHAT = 5             # chat text                  (reliable)
ACK = 6              # acknowledgement            (unreliable)
FILE_LIST = 7        # manifests of shared files  (reliable)
CHUNK_MAP = 8        # who stores which chunk     (reliable)
CHUNK_ANNOUNCE = 9   # "I now store chunk X"      (reliable)
CHUNK_REQUEST = 10   # "send me chunk X"          (reliable)
CHUNK_DATA = 11      # chunk bytes                (reliable)
CHUNK_NACK = 12      # "I don't have chunk X"     (reliable)
HISTORY = 13         # recent chat messages for a newcomer (reliable, chat stream)

TYPE_NAMES = {
    1: "JOIN", 2: "JOIN_REPLY", 3: "LEAVE", 4: "HEARTBEAT", 5: "CHAT", 6: "ACK",
    7: "FILE_LIST", 8: "CHUNK_MAP", 9: "CHUNK_ANNOUNCE", 10: "CHUNK_REQUEST",
    11: "CHUNK_DATA", 12: "CHUNK_NACK", 13: "HISTORY",
}

# ---------------- streams (separate sequence numbers, so chat never waits for files) ----
STREAM_CONTROL = 0
STREAM_CHAT = 1
STREAM_FILE = 2

# ---------------- flags ----------------
FLAG_MORE = 1

# ---------------- chunk payload: file_id(8 bytes) + chunk_index(4) + mode(1) + data ----
CHUNK_HEADER = "!8sIB"
CHUNK_HEADER_SIZE = struct.calcsize(CHUNK_HEADER)
MODE_REPLY = 0   # answer to a CHUNK_REQUEST
MODE_PUSH = 1    # "please store this chunk" (used when a file is shared)

Packet = namedtuple("Packet", "type flags stream sender seq ack payload")


def make_packet(msg_type, stream, sender_id, seq, ack, payload=b"", flags=0):
    """Build the bytes to send over UDP."""
    head = struct.pack("!BBBBIIIH", VERSION, msg_type, flags, stream,
                       sender_id, seq, ack, len(payload))
    crc = zlib.crc32(head + payload) & 0xFFFFFFFF
    return head + struct.pack("!I", crc) + payload


def parse_packet(data):
    """Turn received bytes into a Packet. Returns None if the packet is broken."""
    if len(data) < HEADER_SIZE:
        return None
    version, msg_type, flags, stream, sender, seq, ack, length, crc = \
        struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    payload = data[HEADER_SIZE:]
    if version != VERSION or len(payload) != length:
        return None
    if (zlib.crc32(data[:HEADER_SIZE - 4] + payload) & 0xFFFFFFFF) != crc:
        return None  # corrupted
    return Packet(msg_type, flags, stream, sender, seq, ack, payload)


def pack_chunk(file_id, idx, mode, data):
    return struct.pack(CHUNK_HEADER, file_id.encode(), idx, mode) + data


def unpack_chunk(payload):
    file_id, idx, mode = struct.unpack(CHUNK_HEADER, payload[:CHUNK_HEADER_SIZE])
    return file_id.decode(), idx, mode, payload[CHUNK_HEADER_SIZE:]


def encode_json(obj):
    return json.dumps(obj).encode()


def decode_json(data):
    try:
        return json.loads(data.decode())
    except Exception:
        return {}
