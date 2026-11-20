"""
membership.py
-------------
Who is in the group?
  * peer table (id -> address, name, last time we heard from it)
  * JOIN (broadcast on the LAN, or sent directly to a bootstrap peer)
  * HEARTBEAT every second; a peer silent for 5 seconds is declared dead
"""
import threading
import time

from protocol import make_packet, encode_json, JOIN, HEARTBEAT, STREAM_CONTROL

HEARTBEAT_INTERVAL = 1.0     # seconds between heartbeats
HEARTBEAT_TIMEOUT = 5.0      # silent this long -> dead
BROADCAST_PORTS = range(5000, 5010)   # JOIN is broadcast to these UDP ports


class Membership:
    def __init__(self, my_id, name, sock, on_dead):
        self.my_id = my_id
        self.name = name
        self.sock = sock
        self.on_dead = on_dead          # function(peer_id, name) called when a peer dies
        self.peers = {}                 # id -> {"addr": (ip, port), "name": str, "last_seen": float}
        self.left = set()               # ids of peers that said LEAVE (we ignore them afterwards)
        self.lock = threading.Lock()
        self.running = True

    # ---------------- peer table ----------------
    def add(self, peer_id, addr, name=None):
        """Add a peer or refresh it. Returns True if the peer is NEW."""
        with self.lock:
            if peer_id in self.peers:
                self.peers[peer_id]["last_seen"] = time.time()
                if name:
                    self.peers[peer_id]["name"] = name
                return False
            self.peers[peer_id] = {"addr": addr, "name": name or "?", "last_seen": time.time()}
            return True

    def remove(self, peer_id):
        """Remove a peer. Returns its name (or None if unknown)."""
        with self.lock:
            info = self.peers.pop(peer_id, None)
        return info["name"] if info else None

    def is_alive(self, peer_id):
        with self.lock:
            return peer_id in self.peers

    def alive_ids(self):
        with self.lock:
            return list(self.peers.keys())

    def get_addr(self, peer_id):
        with self.lock:
            info = self.peers.get(peer_id)
            return info["addr"] if info else None

    def name_of(self, peer_id):
        if peer_id == self.my_id:
            return self.name
        with self.lock:
            info = self.peers.get(peer_id)
            return info["name"] if info else str(peer_id)

    def snapshot(self):
        """Peer list in a form that can be sent as JSON."""
        with self.lock:
            return [{"id": pid, "ip": i["addr"][0], "port": i["addr"][1], "name": i["name"]}
                    for pid, i in self.peers.items()]

    # ---------------- JOIN ----------------
    def join_packet(self):
        return make_packet(JOIN, STREAM_CONTROL, self.my_id, 0, 0, encode_json({"name": self.name}))

    def send_join_to(self, addr):
        self.sock.sendto(self.join_packet(), addr)

    def send_join(self, bootstrap=None):
        """Announce ourselves: directly to the bootstrap peer and by LAN broadcast."""
        if bootstrap:
            self.send_join_to(bootstrap)
        for port in BROADCAST_PORTS:
            self.send_join_to(("255.255.255.255", port))

    # ---------------- HEARTBEAT + failure detection (runs in its own thread) -------------
    def heartbeat_loop(self):
        while self.running:
            packet = make_packet(HEARTBEAT, STREAM_CONTROL, self.my_id, 0, 0,
                                 encode_json({"name": self.name}))
            with self.lock:
                addrs = [i["addr"] for i in self.peers.values()]
            for addr in addrs:
                self.sock.sendto(packet, addr)

            now = time.time()
            with self.lock:
                dead = [pid for pid, i in self.peers.items()
                        if now - i["last_seen"] > HEARTBEAT_TIMEOUT]
            for pid in dead:
                name = self.remove(pid)
                self.on_dead(pid, name)
            time.sleep(HEARTBEAT_INTERVAL)

    def stop(self):
        self.running = False
