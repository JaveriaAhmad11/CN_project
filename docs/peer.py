"""
peer.py  -  RUN THIS FILE.   python peer.py --name Javeria --port 5001

Starts all threads of one peer and gives you a command line:
    text                 send a chat message to everybody
    /peers               list active peers
    /files               list shared files
    /share <path>        share a file (split into chunks, spread over peers)
    /download <name>     download a file from several peers in parallel
    /map <name>          show which peer stores how many chunks
    /history [n]         show the last n chat messages (default 20), also from earlier sessions
    /storage             show disk usage, copies per file, orphan chunks
    /scrub               re-check every stored chunk (SHA-256); re-download corrupt ones
    /cleanup             delete extra copies (keep exactly R) and orphan chunk files
    /stats               packets sent / dropped / retransmitted
    /loss <0-1>          simulate packet loss, e.g. /loss 0.1
    /delay <seconds>     slow down downloads (so you have time to kill a peer in the demo)
    /takeover            fetch my share of the chunks again
    /repair              re-create lost replicas
    /crash               die immediately WITHOUT saying goodbye (simulates a crash)
    /quit                leave politely
"""
import argparse
import os
import random
import socket
import threading
import time
import traceback

import storage as storage_mod
from chat import Chat
from download import Downloader
from lossy import LossySocket
from membership import Membership
from protocol import *
from reliable import ReliableChannel
from storage import Storage, make_manifest, chunk_holders


class Peer:
    def __init__(self, name, port, bootstrap=None, loss=0.0, data_dir=None,
                 verbose=True, takeover=True):
        self.name = name
        self.port = port
        self.my_id = random.randint(1, 2 ** 32 - 1)
        self.bootstrap = bootstrap          # (ip, port) of any existing peer, or None
        self.verbose = verbose
        self.takeover = takeover
        self.running = False

        # UDP socket
        self.raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raw.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self.raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        self.raw.bind(("0.0.0.0", port))
        self.raw.settimeout(0.5)
        self.sock = LossySocket(self.raw, loss)     # all sending goes through this

        # the parts of the peer
        self.members = Membership(self.my_id, name, self.sock, self.on_peer_dead)
        self.storage = Storage(data_dir or "data_%d" % port)
        self.chat = Chat(self)
        self.downloader = Downloader(self)

        self.channels = {}                  # (peer_id, stream) -> ReliableChannel
        self.chan_lock = threading.Lock()

    def log(self, msg):
        if self.verbose:
            print(msg, flush=True)

    # ------------------------------------------------------------------
    # start / stop
    # ------------------------------------------------------------------
    def start(self):
        self.running = True
        for target in (self.receive_loop, self.timer_loop, self.members.heartbeat_loop,
                       self.join_network):
            threading.Thread(target=target, daemon=True).start()

    def stop(self):
        """Polite exit: tell everybody, then close."""
        packet = make_packet(LEAVE, STREAM_CONTROL, self.my_id, 0, 0)
        for pid in self.members.alive_ids():
            addr = self.members.get_addr(pid)
            for _ in range(3):
                self.sock.sendto(packet, addr)
        self.kill()

    def kill(self):
        """Crash: stop everything without telling anybody."""
        self.running = False
        self.members.stop()
        try:
            self.raw.close()
        except OSError:
            pass

    def join_network(self):
        for _ in range(3):                           # JOIN may be lost, so try up to 3 times
            if not self.running:
                return
            self.members.send_join(self.bootstrap)
            time.sleep(1)
            if self.members.alive_ids():
                break
        if self.takeover:
            time.sleep(1)                            # give FILE_LIST / CHUNK_MAP time to arrive
            self.reannounce_local()                  # chunks from a previous session count again
            self.downloader.take_share()

    # ------------------------------------------------------------------
    # threads
    # ------------------------------------------------------------------
    def receive_loop(self):
        while self.running:
            try:
                data, addr = self.raw.recvfrom(65535)
            except socket.timeout:
                continue
            except ConnectionResetError:             # Windows: ICMP "port unreachable"
                continue
            except OSError:
                if not self.running:
                    break
                continue
            pkt = parse_packet(data)
            if pkt is None or pkt.sender == self.my_id:
                continue                             # broken packet or our own broadcast
            try:
                self.handle_packet(pkt, addr)
            except Exception:
                traceback.print_exc()

    def timer_loop(self):
        """Retransmission timer for all reliable channels."""
        while self.running:
            with self.chan_lock:
                channels = list(self.channels.values())
            for ch in channels:
                ch.tick()
            time.sleep(0.05)

    # ------------------------------------------------------------------
    # sending helpers
    # ------------------------------------------------------------------
    def get_channel(self, peer_id, stream):
        with self.chan_lock:
            key = (peer_id, stream)
            if key not in self.channels:
                addr = self.members.get_addr(peer_id)
                if addr is None:
                    return None
                deliver = lambda t, d, pid=peer_id: self.deliver(pid, t, d)
                self.channels[key] = ReliableChannel(self.sock, addr, self.my_id, stream, deliver)
            return self.channels[key]

    def send_reliable(self, peer_id, stream, msg_type, data=b""):
        ch = self.get_channel(peer_id, stream)
        if ch is None:
            return False
        ch.send(msg_type, data)
        return True

    def send_to_all(self, stream, msg_type, data=b""):
        for pid in self.members.alive_ids():
            self.send_reliable(pid, stream, msg_type, data)

    # ------------------------------------------------------------------
    # receiving: step 1 = packet arrives (receiver thread)
    # ------------------------------------------------------------------
    def handle_packet(self, pkt, addr):
        if pkt.type == JOIN:
            self.on_join(pkt, addr)
            return
        if pkt.sender in self.members.left:
            return                                   # ignore peers that said goodbye
        if pkt.type == LEAVE:
            name = self.members.remove(pkt.sender)
            self.members.left.add(pkt.sender)
            if name:
                self.log("\n* %s left" % name)
            threading.Thread(target=self.delayed_repair, daemon=True).start()
            return

        name = None
        if pkt.type == HEARTBEAT:
            name = decode_json(pkt.payload).get("name")
        if self.members.add(pkt.sender, addr, name):  # any packet from an unknown peer adds it
            self.log("\n* new peer: %s" % (name or pkt.sender))
        if pkt.type == HEARTBEAT:
            return

        ch = self.get_channel(pkt.sender, pkt.stream)
        if pkt.type == ACK:
            ch.on_ack(pkt)
        else:
            ch.on_data(pkt)                          # -> calls self.deliver() in order

    # step 2 = a complete, in-order message is delivered
    def deliver(self, sender, msg_type, data):
        try:
            if msg_type == CHAT:
                self.chat.on_chat(sender, data)
            elif msg_type == JOIN_REPLY:
                self.on_join_reply(sender, data)
            elif msg_type == FILE_LIST:
                self.on_file_list(data)
            elif msg_type == CHUNK_MAP:
                self.storage.merge_map(decode_json(data))
            elif msg_type == HISTORY:
                self.chat.on_history(sender, data)
            elif msg_type == CHUNK_ANNOUNCE:
                a = decode_json(data)
                if a.get("gone"):                    # that peer deleted its copy
                    self.storage.remove_holder(a["file_id"], a["idx"], a["peer"])
                else:
                    self.storage.add_holder(a["file_id"], a["idx"], a["peer"])
            elif msg_type == CHUNK_REQUEST:
                self.serve_chunk(sender, data)
            elif msg_type == CHUNK_DATA:
                self.downloader.on_chunk_data(sender, data)
            elif msg_type == CHUNK_NACK:
                self.downloader.on_nack(sender, data)
        except Exception:
            traceback.print_exc()

    # ------------------------------------------------------------------
    # membership handlers
    # ------------------------------------------------------------------
    def on_join(self, pkt, addr):
        info = decode_json(pkt.payload)
        self.members.left.discard(pkt.sender)
        if self.members.add(pkt.sender, addr, info.get("name")):
            self.log("\n* %s joined" % info.get("name"))
        s = pkt.sender
        # tell the newcomer everything we know
        self.send_reliable(s, STREAM_CONTROL, JOIN_REPLY, encode_json({"peers": self.members.snapshot()}))
        self.send_reliable(s, STREAM_CONTROL, FILE_LIST, encode_json({"files": self.storage.all_manifests()}))
        self.send_reliable(s, STREAM_CONTROL, CHUNK_MAP, encode_json(self.storage.export_map()))
        self.chat.send_history_to(s)                 # old chat messages for the newcomer

    def on_join_reply(self, sender, data):
        for p in decode_json(data).get("peers", []):
            if p["id"] == self.my_id or p["id"] in self.members.left:
                continue
            addr = (p["ip"], p["port"])
            if self.members.add(p["id"], addr, p["name"]):
                self.log("\n* learned about peer %s" % p["name"])
                self.members.send_join_to(addr)      # introduce ourselves to it

    def on_peer_dead(self, peer_id, name):
        self.log("\n* %s is DEAD (no heartbeat)" % name)
        threading.Thread(target=self.delayed_repair, daemon=True).start()

    def delayed_repair(self):
        time.sleep(2)
        if self.running:
            self.downloader.repair()

    # ------------------------------------------------------------------
    # file handlers
    # ------------------------------------------------------------------
    def on_file_list(self, data):
        for m in decode_json(data).get("files", []):
            if self.storage.add_manifest(m):
                self.log("\n* new shared file: %s (%d bytes)" % (m["name"], m["size"]))

    def serve_chunk(self, sender, data):
        req = decode_json(data)
        chunk = self.storage.load_chunk(req["file_id"], req["idx"])
        if chunk is None:
            self.send_reliable(sender, STREAM_FILE, CHUNK_NACK, data)
        else:
            self.send_reliable(sender, STREAM_FILE, CHUNK_DATA,
                               pack_chunk(req["file_id"], req["idx"], MODE_REPLY, chunk))

    def store_chunk(self, file_id, idx, data):
        """Save a chunk on disk and tell everybody that we store it."""
        self.storage.save_chunk(file_id, idx, data)
        self.storage.add_holder(file_id, idx, self.my_id)
        self.send_to_all(STREAM_CONTROL, CHUNK_ANNOUNCE,
                         encode_json({"file_id": file_id, "idx": idx, "peer": self.my_id}))

    def share(self, path):
        """Split a file into chunks and spread them over the peers (R replicas each)."""
        if not os.path.isfile(path):
            self.log("File not found: " + path)
            return
        manifest, chunks = make_manifest(path)
        file_id = manifest["file_id"]
        everyone = sorted(self.members.alive_ids() + [self.my_id])
        if len(everyone) < 3:
            self.log("Warning: with fewer than 3 peers some peer will hold the whole file.")
        self.storage.add_manifest(manifest)
        self.send_to_all(STREAM_CONTROL, FILE_LIST, encode_json({"files": [manifest]}))
        for idx, data in enumerate(chunks):
            for holder in chunk_holders(file_id, idx, everyone):
                if holder == self.my_id:
                    self.store_chunk(file_id, idx, data)
                else:
                    self.send_reliable(holder, STREAM_FILE, CHUNK_DATA,
                                       pack_chunk(file_id, idx, MODE_PUSH, data))
        self.log("Shared %s: %d chunks, each on %d peers." % (
            manifest["name"], len(chunks), min(storage_mod.REPLICAS, len(everyone))))

    # ------------------------------------------------------------------
    # storage maintenance
    # ------------------------------------------------------------------
    def reannounce_local(self):
        """After a restart we have a NEW id: tell everybody which chunks we still have on disk."""
        count = 0
        for file_id, idx in self.storage.local_chunks():
            if file_id in self.storage.manifests:
                self.storage.add_holder(file_id, idx, self.my_id)
                self.send_to_all(STREAM_CONTROL, CHUNK_ANNOUNCE,
                                 encode_json({"file_id": file_id, "idx": idx, "peer": self.my_id}))
                count += 1
        if count:
            self.log("Re-announced %d chunks found on disk." % count)
        return count

    def scrub(self):
        """Integrity check of everything we store. Corrupt chunks are deleted and fetched again."""
        bad = self.storage.verify_local()
        fixed = 0
        for file_id, idx in bad:
            self.storage.remove_holder(file_id, idx, self.my_id)
            if self.downloader.fetch_any(file_id, idx):
                fixed += 1
        self.log("Scrub: %d corrupt chunks found, %d re-downloaded." % (len(bad), fixed))
        return len(bad), fixed

    def cleanup(self):
        """Delete extra copies and orphan chunk files.
        Rule: when a chunk has more than R live copies, only the R holders with the LOWEST ids keep it.
        Every peer computes the same answer, so the peers never delete the last copies together."""
        alive = set(self.members.alive_ids() + [self.my_id])
        extra = 0
        per_file, orphans = self.storage.disk_report()
        for file_id, idx in orphans:                       # chunk files of files we do not know
            self.storage.delete_chunk(file_id, idx)
        for file_id, idx in self.storage.local_chunks():
            live = sorted(p for p in self.storage.holders(file_id, idx) if p in alive)
            if len(live) > storage_mod.REPLICAS and self.my_id not in live[:storage_mod.REPLICAS]:
                self.storage.delete_chunk(file_id, idx)
                self.storage.remove_holder(file_id, idx, self.my_id)
                self.send_to_all(STREAM_CONTROL, CHUNK_ANNOUNCE, encode_json(
                    {"file_id": file_id, "idx": idx, "peer": self.my_id, "gone": True}))
                extra += 1
        self.log("Cleanup: deleted %d extra copies and %d orphan chunk files." % (extra, len(orphans)))
        return extra, len(orphans)

    def download(self, key, max_sources=None):
        return self.downloader.download(key, max_sources)

    # ------------------------------------------------------------------
    # statistics
    # ------------------------------------------------------------------
    def retransmissions(self, stream=None):
        with self.chan_lock:
            return sum(ch.retransmissions for (pid, s), ch in self.channels.items()
                       if stream is None or s == stream)


# ======================================================================
# command line interface
# ======================================================================
def show_peers(peer):
    print("me: %s (id %d, port %d)" % (peer.name, peer.my_id, peer.port))
    for p in peer.members.snapshot():
        print("  %-12s id=%d  %s:%d" % (p["name"], p["id"], p["ip"], p["port"]))


def show_files(peer):
    for m in peer.storage.all_manifests():
        fid = m["file_id"]
        copies = [len([p for p in peer.storage.holders(fid, i)
                       if p == peer.my_id or peer.members.is_alive(p)])
                  for i in range(m["num_chunks"])]
        print("  %-20s id=%s size=%d chunks=%d  I store %d chunks, min copies=%d" % (
            m["name"], fid, m["size"], m["num_chunks"], peer.storage.my_chunk_count(fid), min(copies)))


def show_map(peer, key):
    m = peer.storage.find_manifest(key)
    if not m:
        print("unknown file")
        return
    count = {}
    for i in range(m["num_chunks"]):
        for p in peer.storage.holders(m["file_id"], i):
            count[p] = count.get(p, 0) + 1
    for p, c in count.items():
        state = "" if p == peer.my_id or peer.members.is_alive(p) else "  (dead)"
        print("  %-12s stores %d of %d chunks%s" % (peer.members.name_of(p), c, m["num_chunks"], state))


def show_storage(peer):
    per_file, orphans = peer.storage.disk_report()
    total = 0
    for m in peer.storage.all_manifests():
        fid = m["file_id"]
        chunks, size = per_file.get(fid, [0, 0])
        total += size
        copies = [len([p for p in peer.storage.holders(fid, i)
                       if p == peer.my_id or peer.members.is_alive(p)])
                  for i in range(m["num_chunks"])]
        over = sum(1 for c in copies if c > storage_mod.REPLICAS)
        print("  %-20s I store %d/%d chunks (%.1f KB)  min copies=%d  chunks with extra copies=%d" % (
            m["name"], chunks, m["num_chunks"], size / 1024.0, min(copies), over))
    print("  total chunk data on disk: %.1f KB, orphan chunk files: %d" % (total / 1024.0, len(orphans)))


def show_stats(peer):
    print("packets sent=%d dropped(simulated)=%d retransmissions=%d" % (
        peer.sock.sent, peer.sock.dropped, peer.retransmissions()))


def cli(peer):
    print(__doc__)
    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        if not line.startswith("/"):
            peer.chat.send(line)
            continue
        parts = line.split(None, 1)
        cmd = parts[0]
        arg = parts[1] if len(parts) > 1 else ""
        try:
            if cmd == "/quit":
                break
            elif cmd == "/peers":
                show_peers(peer)
            elif cmd == "/files":
                show_files(peer)
            elif cmd == "/share":
                peer.share(arg)
            elif cmd == "/download":
                threading.Thread(target=peer.download, args=(arg,), daemon=True).start()
            elif cmd == "/map":
                show_map(peer, arg)
            elif cmd == "/stats":
                show_stats(peer)
            elif cmd == "/history":
                peer.chat.show(int(arg) if arg else 20)
            elif cmd == "/storage":
                show_storage(peer)
            elif cmd == "/scrub":
                threading.Thread(target=peer.scrub, daemon=True).start()
            elif cmd == "/cleanup":
                threading.Thread(target=peer.cleanup, daemon=True).start()
            elif cmd == "/loss":
                peer.sock.loss = float(arg)
                print("loss set to", peer.sock.loss)
            elif cmd == "/delay":
                peer.downloader.chunk_delay = float(arg)
                print("chunk delay set to", arg)
            elif cmd == "/takeover":
                threading.Thread(target=peer.downloader.take_share, daemon=True).start()
            elif cmd == "/repair":
                threading.Thread(target=peer.downloader.repair, daemon=True).start()
            elif cmd == "/crash":
                peer.kill()
                os._exit(0)
            else:
                print("unknown command")
        except Exception as e:
            print("error:", e)
    peer.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default="peer")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--bootstrap", default=None, help="ip:port of any running peer")
    ap.add_argument("--loss", type=float, default=0.0, help="simulated packet loss 0..1")
    ap.add_argument("--data", default=None, help="folder for chunks/downloads")
    args = ap.parse_args()

    bootstrap = None
    if args.bootstrap:
        ip, port = args.bootstrap.split(":")
        bootstrap = (ip, int(port))

    peer = Peer(args.name, args.port, bootstrap, args.loss, args.data)
    peer.start()
    cli(peer)


if __name__ == "__main__":
    main()
