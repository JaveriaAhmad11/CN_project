# Protocol Specification

## 1. Packet Header (22 Bytes)
Every packet sent over UDP in this project begins with a 22-byte header. This header ensures that packets can be identified, ordered, and checked for corruption. All fields use big-endian byte order.

| Field | Size | Meaning |
| :--- | :--- | :--- |
| **version** | 1 Byte | Protocol version (always 1). Allows for future updates. |
| **type** | 1 Byte | Message type (e.g., CHAT, ACK, CHUNK_REQUEST). |
| **flags** | 1 Byte | Bit 0 = MORE. Indicates if more fragments of a message follow. |
| **stream** | 1 Byte | 0 = control, 1 = chat, 2 = file. Keeps traffic separated. |
| **sender_id** | 4 Bytes | Random ID of the sending peer. |
| **seq** | 4 Bytes | Packet sequence number (starts at 1). Used for ordering and ACKs. |
| **ack** | 4 Bytes | In an ACK packet, this holds the `seq` being acknowledged. |
| **payload_len**| 2 Bytes | The size of the payload in bytes. |
| **crc32** | 4 Bytes | Checksum of the header (excluding this field) + payload. |

## 2. Message Types
The protocol supports the following message types, routed to the appropriate handlers:

| Type ID | Name | Reliable? | Purpose |
| :--- | :--- | :--- | :--- |
| 1 | JOIN | No | Sent by a new peer to discover others. |
| 2 | JOIN_REPLY | No | Sent by existing peers to welcome a newcomer and share the peer list. |
| 3 | LEAVE | No | Sent when a peer politely exits the network. |
| 4 | HEARTBEAT | No | Sent every 1 second to prove a peer is still alive. |
| 5 | CHAT | Yes | Carries chat text messages. |
| 6 | ACK | No | Acknowledges receipt of a reliable packet. |
| 7 | FILE_LIST | Yes | Shares metadata/manifests of available files. |
| 8 | CHUNK_MAP | Yes | Shares which peers hold which chunks of a file. |
| 9 | CHUNK_ANNOUNCE| Yes | Informs the network that a peer now stores a specific chunk. |
| 10 | CHUNK_REQUEST | Yes | Requests a specific chunk from a peer. |
| 11 | CHUNK_DATA | Yes | Carries the actual binary data of a 32KB chunk. |
| 12 | CHUNK_NACK | Yes | "Negative ACK" - tells a peer they don't have the requested chunk. |

## 3. Expected Message Sequences

**A. Join Flow:**
1. New peer broadcasts `JOIN` (or sends to a bootstrap peer).
2. Existing peers add the new peer and reply with `JOIN_REPLY` (peer list), `FILE_LIST`, and `CHUNK_MAP`.
3. The newcomer sends `JOIN` to the peers it just learned about.
4. After ~2 seconds, the newcomer runs `take_share()`, sending `CHUNK_REQUEST` to holders.

**B. Chat Flow:**
1. Sender sends `CHAT` reliably via unicast to every alive peer.
2. Receiver processes the message and sends an `ACK` (handled silently by `reliable.py`).
3. If lost, the timer triggers a resend.

**C. Share (Publish) Flow:**
1. Sharer sends `FILE_LIST` to all peers.
2. Sharer sends `CHUNK_DATA` (Mode 1: "push") to the 2 designated replica holders.
3. Holders save the chunk and broadcast `CHUNK_ANNOUNCE` so all chunk maps update.

**D. Download Flow:**
1. Downloader sends `CHUNK_REQUEST` to multiple peers in parallel.
2. Peers reply with `CHUNK_DATA` (Mode 0: "reply").
3. Downloader verifies SHA-256 for each chunk and the final file.

**E. Failure Recovery Flow:**
1. If heartbeats stop for 5 seconds, the peer is declared dead.
2. Pending requests timeout, and other workers take over the dead peer's chunks.
3. 2 seconds later, `repair()` runs to re-create missing replicas.

## 4. Packet Integrity Proof
The protocol uses CRC32 to detect corrupted packets. As proven in my unit test, if even a single bit is flipped in the payload, the CRC32 check fails, and `parse_packet()` returns `None`, preventing corrupted data from entering the system.

*(See attached screenshot: `javeria_crc_proof.png`)*