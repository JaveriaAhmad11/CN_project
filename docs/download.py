"""
download.py
-----------
  * request_chunk : ask ONE peer for ONE chunk and wait for the answer
  * download      : multi-source download. One worker thread per source peer; workers take
                    chunks from a shared "still needed" list. If a peer dies or times out,
                    its chunk goes back to the list and a worker of another replica takes it.
  * take_share    : a new peer fetches the chunks it is responsible for
  * repair        : re-create replicas that were lost when a peer died
"""
import os
import threading
import time

import storage as st
from protocol import (CHUNK_REQUEST, STREAM_FILE, MODE_PUSH, unpack_chunk, encode_json,
                      decode_json)

CHUNK_TIMEOUT = 5.0     # seconds to wait for one chunk
FAIL_LIMIT = 2          # a source that times out this many times in a row is dropped


class Downloader:
    def __init__(self, peer):
        self.peer = peer
        self.waiting = {}        # (sender, file_id, idx) -> {"event", "data", "status"}
        self.chunk_delay = 0.0   # extra seconds per chunk (use it to slow down a demo)

    # ------------------------------------------------------------------
    # messages that arrive from other peers
    # ------------------------------------------------------------------
    def on_chunk_data(self, sender, payload):
        file_id, idx, mode, data = unpack_chunk(payload)
        if mode == MODE_PUSH:                         # someone asks us to store this chunk
            self.peer.store_chunk(file_id, idx, data)
            return
        w = self.waiting.get((sender, file_id, idx))  # answer to our request
        if w is not None:
            w["data"] = data
            w["status"] = "ok"
            w["event"].set()

    def on_nack(self, sender, payload):
        req = decode_json(payload)
        w = self.waiting.get((sender, req.get("file_id"), req.get("idx")))
        if w is not None:
            w["status"] = "nack"
            w["event"].set()

    # ------------------------------------------------------------------
    # ask one peer for one chunk
    # ------------------------------------------------------------------
    def request_chunk(self, peer_id, file_id, idx):
        """Returns (data, status). status is 'ok', 'nack' or 'timeout'."""
        key = (peer_id, file_id, idx)
        w = {"event": threading.Event(), "data": None, "status": "timeout"}
        self.waiting[key] = w
        self.peer.send_reliable(peer_id, STREAM_FILE, CHUNK_REQUEST,
                                encode_json({"file_id": file_id, "idx": idx}))
        end = time.time() + CHUNK_TIMEOUT
        while time.time() < end and not w["event"].is_set():
            w["event"].wait(0.2)
            if not self.peer.members.is_alive(peer_id):   # heartbeat says it is dead
                break
        self.waiting.pop(key, None)
        if w["event"].is_set():
            return w["data"], w["status"]
        return None, "timeout"

    def live_holders(self, file_id, idx):
        """Other peers that are alive and store this chunk."""
        return [p for p in self.peer.storage.holders(file_id, idx)
                if p != self.peer.my_id and self.peer.members.is_alive(p)]

    def fetch_any(self, file_id, idx):
        """Get one chunk from any live holder, verify it, store it. Returns True/False."""
        manifest = self.peer.storage.manifests[file_id]
        for holder in self.live_holders(file_id, idx):
            data, status = self.request_chunk(holder, file_id, idx)
            if data is not None and st.sha256(data) == manifest["chunk_hashes"][idx]:
                self.peer.store_chunk(file_id, idx, data)
                return True
        return False

    # ------------------------------------------------------------------
    # MULTI-SOURCE DOWNLOAD
    # ------------------------------------------------------------------
    def download(self, key, max_sources=None):
        peer = self.peer
        storage = peer.storage
        me = peer.my_id

        manifest = storage.find_manifest(key)
        if manifest is None:
            peer.log("Unknown file: " + key)
            return None
        file_id = manifest["file_id"]
        n = manifest["num_chunks"]

        # 1) which peers can serve this file?
        everyone = set()
        for i in range(n):
            everyone |= storage.holders(file_id, i)
        sources = sorted(p for p in everyone if p == me or peer.members.is_alive(p))
        if max_sources:
            sources = sources[:max_sources]
        if not sources:
            peer.log("No source available for " + manifest["name"])
            return None
        peer.log("Downloading %s (%d chunks) from %d peers: %s" % (
            manifest["name"], n, len(sources), ", ".join(peer.members.name_of(p) for p in sources)))

        # shared state of this download (protected by `lock`)
        lock = threading.Lock()
        got = {}                       # idx -> data (verified)
        busy = {}                      # idx -> set of peers currently fetching it
        bad = set()                    # (peer, idx) pairs that failed (nack / wrong hash)
        used = {}                      # peer -> number of chunks it delivered
        allowed = set(sources)         # sources we still trust
        start = time.time()

        def pick(source):
            """Choose the next chunk for this source. Call with lock held."""
            mine = [i for i in range(n) if i not in got and (source, i) not in bad
                    and source in storage.holders(file_id, i)]
            if not mine:
                return "quit"                      # nothing left that this source can give
            for i in mine:
                if i not in busy:                  # a chunk nobody is fetching yet
                    busy[i] = {source}
                    return i
            for i in mine:                         # "end game": help with a chunk someone else
                if source not in busy[i]:          # is fetching (this is what makes failover fast)
                    busy[i].add(source)
                    return i
            return None

        def get_chunk(source, idx):
            """Returns (data or None, problem)."""
            if source == me:
                data = storage.load_chunk(file_id, idx)
                status = "ok" if data is not None else "nack"
            else:
                data, status = self.request_chunk(source, file_id, idx)
            if status == "ok" and st.sha256(data) != manifest["chunk_hashes"][idx]:
                return None, "bad"                 # integrity check failed
            return (data if status == "ok" else None), status

        def worker(source):
            fails = 0
            while True:
                with lock:
                    if len(got) == n or source not in allowed:
                        return
                    if source != me and not peer.members.is_alive(source):
                        allowed.discard(source)
                        return
                    idx = pick(source)
                if idx == "quit":
                    return
                if idx is None:
                    time.sleep(0.05)
                    continue
                if self.chunk_delay:
                    time.sleep(self.chunk_delay)

                data, problem = get_chunk(source, idx)

                with lock:
                    owners = busy.get(idx, set())
                    owners.discard(source)
                    if not owners:
                        busy.pop(idx, None)
                    if data is not None:
                        fails = 0
                        if idx not in got:
                            got[idx] = data
                            used[source] = used.get(source, 0) + 1
                            if len(got) % 16 == 0:
                                peer.log("  progress: %d/%d chunks" % (len(got), n))
                    elif problem == "timeout":
                        fails += 1
                        if fails >= FAIL_LIMIT:
                            allowed.discard(source)
                    else:                           # nack or bad hash
                        bad.add((source, idx))

        # 2) one worker thread per source
        threads = [threading.Thread(target=worker, args=(s,), daemon=True) for s in sources]
        for t in threads:
            t.start()
        while True:
            with lock:
                if len(got) == n:
                    break
            if not any(t.is_alive() for t in threads):
                break
            time.sleep(0.05)

        # 3) assemble + verify the whole file
        with lock:
            complete = len(got) == n
            parts = [got[i] for i in range(n)] if complete else []
        if not complete:
            peer.log("DOWNLOAD FAILED: only %d/%d chunks could be fetched" % (len(got), n))
            return None
        data = b"".join(parts)
        if st.sha256(data) != manifest["file_hash"]:
            peer.log("DOWNLOAD FAILED: whole-file hash does not match")
            return None
        out_path = os.path.join(storage.download_dir, manifest["name"])
        with open(out_path, "wb") as f:
            f.write(data)

        seconds = time.time() - start
        summary = ", ".join("%s=%d" % (peer.members.name_of(p), c) for p, c in sorted(used.items()))
        peer.log("DOWNLOAD OK: %s in %.2fs, hash verified, saved to %s" % (
            manifest["name"], seconds, out_path))
        peer.log("  chunks per source: " + summary)
        return {"seconds": seconds, "bytes": len(data), "path": out_path,
                "chunks_from": {peer.members.name_of(p): c for p, c in used.items()}}

    # ------------------------------------------------------------------
    # JOIN: take over my share of the chunks
    # ------------------------------------------------------------------
    def take_share(self):
        peer = self.peer
        me = peer.my_id
        everyone = sorted(peer.members.alive_ids() + [me])
        taken = 0
        for m in peer.storage.all_manifests():
            file_id = m["file_id"]
            for idx in range(m["num_chunks"]):
                if me in st.chunk_holders(idx, everyone) and not peer.storage.has_chunk(file_id, idx):
                    if self.fetch_any(file_id, idx):
                        taken += 1
        peer.log("Took over %d chunks as my share." % taken)
        return taken

    # ------------------------------------------------------------------
    # PEER LEFT: re-create lost replicas (every chunk should have REPLICAS live copies)
    # ------------------------------------------------------------------
    def repair(self):
        peer = self.peer
        me = peer.my_id
        alive = sorted(peer.members.alive_ids() + [me])
        want = min(st.REPLICAS, len(alive))
        fixed = 0
        for m in peer.storage.all_manifests():
            file_id = m["file_id"]
            for idx in range(m["num_chunks"]):
                live = [p for p in peer.storage.holders(file_id, idx) if p in alive]
                if not live or len(live) >= want:
                    continue                      # healthy, or lost completely (cannot fix)
                candidates = [p for p in alive if p not in live]
                if candidates and candidates[0] == me:    # lowest id without a copy does the work
                    if self.fetch_any(file_id, idx):
                        fixed += 1
        if fixed:
            peer.log("Repair: re-created %d chunk replicas." % fixed)
        return fixed
