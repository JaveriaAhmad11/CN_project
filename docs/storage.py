"""
storage.py
----------
Everything about files:
  * splitting a file into fixed-size chunks + a manifest (hashes for integrity)
  * deciding which peers store which chunk (replica placement)
  * keeping the chunk map  (file -> chunk -> set of peers that store it)
  * saving / loading chunks on disk (folder data_<port>/chunks)
"""
import hashlib
import json
import os
import threading

CHUNK_SIZE = 32 * 1024      # 32 KB per chunk (a 2 MB file = 64 chunks)
REPLICAS = 2                # every chunk is stored on this many peers


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def make_manifest(path):
    """Read a file and return (manifest, list_of_chunks)."""
    with open(path, "rb") as f:
        data = f.read()
    chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
    file_hash = sha256(data)
    manifest = {
        "file_id": file_hash[:8],            # short id (8 hex chars)
        "name": os.path.basename(path),
        "size": len(data),
        "chunk_size": CHUNK_SIZE,
        "num_chunks": len(chunks),
        "chunk_hashes": [sha256(c) for c in chunks],
        "file_hash": file_hash,
    }
    return manifest, chunks

def chunk_holders(file_id, chunk_idx, peer_ids):
    """
    Rendezvous (Highest Random Weight) Hashing.
    score = SHA-256(file_id + chunk_idx + peer_id)
    Return top REPLICAS peers with highest scores.
    """
    ids = list(peer_ids)
    if not ids:
        return []
    r = min(REPLICAS, len(ids))

    def score(peer_id):
        key = f"{file_id}:{chunk_idx}:{peer_id}".encode()
        return hashlib.sha256(key).hexdigest()

    ranked = sorted(ids, key=score, reverse=True)
    return ranked[:r]

class Storage:
    def __init__(self, folder):
        self.folder = folder
        self.chunk_dir = os.path.join(folder, "chunks")
        self.download_dir = os.path.join(folder, "downloads")
        os.makedirs(self.chunk_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        self.manifests = {}      # file_id -> manifest
        self.chunk_map = {}      # file_id -> {chunk_idx: set(peer_ids)}
        self.lock = threading.RLock()
        self.manifest_path = os.path.join(folder, "manifests.json")
        self._load_manifests()

    # ---------------- manifests (also saved on disk, so a restarted peer remembers its files) ----
    def _load_manifests(self):
        try:
            with open(self.manifest_path) as f:
                for m in json.load(f):
                    self.manifests[m["file_id"]] = m
        except (OSError, ValueError):
            pass

    def _save_manifests(self):
        with open(self.manifest_path, "w") as f:
            json.dump(list(self.manifests.values()), f)

    def add_manifest(self, manifest):
        """Returns True if this file was not known before."""
        with self.lock:
            if manifest["file_id"] in self.manifests:
                return False
            self.manifests[manifest["file_id"]] = manifest
            self._save_manifests()
            return True

    def all_manifests(self):
        with self.lock:
            return list(self.manifests.values())

    def find_manifest(self, key):
        """Find a file by name, id, or id prefix."""
        with self.lock:
            for m in self.manifests.values():
                if key == m["name"] or key == m["file_id"]:
                    return m
            for m in self.manifests.values():
                if m["file_id"].startswith(key):
                    return m
        return None

    # ---------------- chunk files on disk ----------------
    def _path(self, file_id, idx):
        return os.path.join(self.chunk_dir, "%s_%d.chunk" % (file_id, idx))

    def has_chunk(self, file_id, idx):
        return os.path.exists(self._path(file_id, idx))

    def save_chunk(self, file_id, idx, data):
        with open(self._path(file_id, idx), "wb") as f:
            f.write(data)

    def load_chunk(self, file_id, idx):
        try:
            with open(self._path(file_id, idx), "rb") as f:
                return f.read()
        except OSError:
            return None

    def my_chunk_count(self, file_id):
        with self.lock:
            m = self.manifests.get(file_id)
        if m is None:
            return 0
        return sum(1 for i in range(m["num_chunks"]) if self.has_chunk(file_id, i))

    # ---------------- chunk map ----------------
    def add_holder(self, file_id, idx, peer_id):
        with self.lock:
            self.chunk_map.setdefault(file_id, {}).setdefault(idx, set()).add(peer_id)

    def holders(self, file_id, idx):
        with self.lock:
            return set(self.chunk_map.get(file_id, {}).get(idx, set()))

    def export_map(self):
        """Chunk map in JSON-friendly form."""
        with self.lock:
            return {fid: {str(i): sorted(h) for i, h in d.items()}
                    for fid, d in self.chunk_map.items()}

    def merge_map(self, data):
        for file_id, d in data.items():
            for idx, holders in d.items():
                for peer_id in holders:
                    self.add_holder(file_id, int(idx), peer_id)

    def remove_holder(self, file_id, idx, peer_id):
        with self.lock:
            self.chunk_map.get(file_id, {}).get(idx, set()).discard(peer_id)

    # ---------------- storage maintenance ----------------
    def local_chunks(self):
        """All (file_id, chunk_index) chunk files that are on disk."""
        found = []
        for name in os.listdir(self.chunk_dir):
            if name.endswith(".chunk"):
                try:
                    file_id, idx = name[:-6].rsplit("_", 1)
                    found.append((file_id, int(idx)))
                except ValueError:
                    pass
        return found

    def delete_chunk(self, file_id, idx):
        try:
            os.remove(self._path(file_id, idx))
        except OSError:
            pass

    def verify_local(self):
        """Check every stored chunk against the SHA-256 in its manifest.
        Corrupt chunks are deleted. Returns the list of (file_id, idx) that were bad."""
        bad = []
        for file_id, idx in self.local_chunks():
            m = self.manifests.get(file_id)
            if m is None or idx >= m["num_chunks"]:
                continue
            data = self.load_chunk(file_id, idx)
            if data is None or sha256(data) != m["chunk_hashes"][idx]:
                self.delete_chunk(file_id, idx)
                bad.append((file_id, idx))
        return bad

    def disk_report(self):
        """Returns (per_file, orphans).
        per_file: file_id -> [number of chunks, bytes];  orphans: chunk files of unknown files."""
        per_file = {}
        orphans = []
        for file_id, idx in self.local_chunks():
            if file_id not in self.manifests:
                orphans.append((file_id, idx))
                continue
            entry = per_file.setdefault(file_id, [0, 0])
            entry[0] += 1
            entry[1] += os.path.getsize(self._path(file_id, idx))
        return per_file, orphans
