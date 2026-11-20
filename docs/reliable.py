"""
reliable.py
-----------
ReliableChannel = TCP-like reliability on top of UDP, for ONE (peer, stream) pair.

What it does:
  * splits big messages into packets of <= 1000 bytes (and puts them back together)
  * numbers every packet (seq 1, 2, 3 ...)
  * receiver ACKs every packet (selective ACK: "I got seq X")
  * sender keeps un-ACKed packets and re-sends them after a timeout
  * sliding window (Selective Repeat): at most WINDOW packets in flight
  * receiver drops duplicates, buffers early packets, delivers messages IN ORDER
  * timeout is adaptive:  rtt = 0.875*rtt + 0.125*sample ;  timeout = 2*rtt
"""
import threading
import time
from collections import deque

from protocol import make_packet, ACK, MAX_PAYLOAD, FLAG_MORE

WINDOW = 32          # max packets in flight
MAX_RETRIES = 15     # give up on a packet after this many sends (peer is probably dead)
MIN_TIMEOUT = 0.15   # seconds
MAX_TIMEOUT = 2.0


class ReliableChannel:
    def __init__(self, sock, addr, my_id, stream, deliver):
        self.sock = sock
        self.addr = addr
        self.my_id = my_id
        self.stream = stream
        self.deliver = deliver          # function(msg_type, data) -> called in order
        self.lock = threading.RLock()

        # ---- sender side ----
        self.next_seq = 1
        self.queue = deque()            # packets waiting for window space
        self.unacked = {}               # seq -> {"packet", "sent", "first", "tries"}

        # ---- receiver side ----
        self.expected = 1               # next seq we want to deliver
        self.buffer = {}                # early packets: seq -> Packet
        self.partial = b""              # fragments of the message being rebuilt

        # ---- timing ----
        self.rtt = 0.3
        self.timeout = 0.6

        # ---- statistics (used in experiments) ----
        self.retransmissions = 0
        self.duplicates = 0

    # ------------------------------------------------------------------
    # SENDING
    # ------------------------------------------------------------------
    def send(self, msg_type, data=b""):
        """Queue a message (any size). It is split into packets automatically."""
        pieces = [data[i:i + MAX_PAYLOAD] for i in range(0, len(data), MAX_PAYLOAD)]
        if not pieces:
            pieces = [b""]
        with self.lock:
            for n, piece in enumerate(pieces):
                flags = FLAG_MORE if n < len(pieces) - 1 else 0
                self.queue.append((msg_type, flags, piece))
            self._fill_window()

    def _fill_window(self):
        """Send queued packets while the window has room. (call with lock held)"""
        while self.queue:
            base = min(self.unacked) if self.unacked else self.next_seq
            if self.next_seq - base >= WINDOW:
                break
            msg_type, flags, piece = self.queue.popleft()
            seq = self.next_seq
            self.next_seq += 1
            packet = make_packet(msg_type, self.stream, self.my_id, seq, 0, piece, flags)
            now = time.time()
            self.unacked[seq] = {"packet": packet, "sent": now, "first": now, "tries": 1}
            self.sock.sendto(packet, self.addr)

    def on_ack(self, pkt):
        """An ACK arrived: forget that packet and measure RTT."""
        with self.lock:
            entry = self.unacked.pop(pkt.ack, None)
            if entry is not None and entry["tries"] == 1:   # only use packets sent once
                sample = time.time() - entry["first"]
                self.rtt = 0.875 * self.rtt + 0.125 * sample
                self.timeout = min(max(2 * self.rtt, MIN_TIMEOUT), MAX_TIMEOUT)
            self._fill_window()

    def tick(self):
        """Called ~20 times per second: re-send packets whose timer expired."""
        now = time.time()
        with self.lock:
            for seq, e in list(self.unacked.items()):
                wait = min(self.timeout * min(e["tries"], 3), MAX_TIMEOUT)
                if now - e["sent"] < wait:
                    continue
                if e["tries"] >= MAX_RETRIES:
                    del self.unacked[seq]          # give up (peer is gone)
                    continue
                e["tries"] += 1
                e["sent"] = now
                self.retransmissions += 1
                self.sock.sendto(e["packet"], self.addr)
            self._fill_window()

    def pending(self):
        with self.lock:
            return len(self.queue) + len(self.unacked)

    # ------------------------------------------------------------------
    # RECEIVING
    # ------------------------------------------------------------------
    def on_data(self, pkt):
        """A data packet arrived: ACK it, drop duplicates, deliver in order."""
        ready = []
        with self.lock:
            # always ACK (even duplicates, because our earlier ACK may have been lost)
            self.sock.sendto(make_packet(ACK, self.stream, self.my_id, 0, pkt.seq), self.addr)

            if pkt.seq < self.expected or pkt.seq in self.buffer:
                self.duplicates += 1
                return
            self.buffer[pkt.seq] = pkt

            while self.expected in self.buffer:
                p = self.buffer.pop(self.expected)
                self.expected += 1
                self.partial += p.payload
                if not (p.flags & FLAG_MORE):          # last fragment -> message complete
                    ready.append((p.type, self.partial))
                    self.partial = b""

        for msg_type, data in ready:                    # call outside the lock
            self.deliver(msg_type, data)
