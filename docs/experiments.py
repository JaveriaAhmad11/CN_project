"""
experiments.py  -  python experiments.py [quick]
Runs the measurements needed for the report and saves graphs (PNG) + numbers (CSV)
in the folder  results/ .

All peers run inside this one process on localhost, so no network setup is needed.
Needs matplotlib:   pip install matplotlib
Add the word 'quick' to run fewer repetitions.
"""
import csv
import os
import shutil
import sys
import threading
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import storage
from peer import Peer

QUICK = "quick" in sys.argv
TRIALS = 1 if QUICK else 2
FOLDER = "exp_data"
RESULTS = "results"
FILE_SIZE = 2 * 1024 * 1024
next_port = [7000]


def wait_until(condition, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.1)
    return False


def make_file():
    path = os.path.join(FOLDER, "exp.bin")
    if not os.path.exists(path):
        with open(path, "wb") as f:
            f.write(os.urandom(FILE_SIZE))
    return path


def set_loss(peers, loss):
    for p in peers:
        p.sock.loss = loss


def stop_all(peers):
    for p in peers:
        p.kill()


def make_cluster(n_storing=4):
    """Start n_storing peers, then share the file. Returns (storing_peers, file_id)."""
    base = next_port[0]
    next_port[0] += 20
    peers = []
    for i in range(n_storing):
        boot = None if i == 0 else ("127.0.0.1", base)
        p = Peer("P%d" % i, base + i, boot, 0.0, os.path.join(FOLDER, "run%d_p%d" % (base, i)), verbose=False)
        p.start()
        peers.append(p)
        time.sleep(0.2)
    wait_until(lambda: all(len(p.members.alive_ids()) == n_storing - 1 for p in peers))
    peers[0].share(make_file())
    m = peers[0].storage.all_manifests()[0]
    want = min(storage.REPLICAS, n_storing)
    wait_until(lambda: all(len(p.storage.holders(m["file_id"], i)) >= want
                           for p in peers for i in range(m["num_chunks"])))
    return peers, m["file_id"], base


def add_downloader(peers, base):
    """A peer that joins AFTER the file was shared and does not take over any chunks."""
    d = Peer("DL", base + 10, ("127.0.0.1", base), 0.0, os.path.join(FOLDER, "run%d_dl" % base),
             verbose=False, takeover=False)
    d.start()
    m = peers[0].storage.all_manifests()[0]
    wait_until(lambda: len(d.storage.all_manifests()) == 1 and
               all(len(d.storage.holders(m["file_id"], i)) >= min(storage.REPLICAS, len(peers))
                   for i in range(m["num_chunks"])))
    return d


def save_csv(name, header, rows):
    with open(os.path.join(RESULTS, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def save_plot(name, x, y, xlabel, ylabel, title, bar=False):
    plt.figure(figsize=(6, 4))
    if bar:
        plt.bar([str(v) for v in x], y)
    else:
        plt.plot(x, y, marker="o")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, name), dpi=150)
    plt.close()


# ----------------------------------------------------------------------
def exp_loss():
    """Download time vs packet loss."""
    print("Experiment 1: download time vs packet loss")
    storage.REPLICAS = 2
    rows = []
    for loss in [0, 5, 10, 20]:
        times = []
        for _ in range(TRIALS):
            peers, fid, base = make_cluster(4)
            d = add_downloader(peers, base)
            set_loss(peers + [d], loss / 100.0)
            r = d.download("exp.bin")
            times.append(r["seconds"] if r else float("nan"))
            stop_all(peers + [d])
        avg = sum(times) / len(times)
        print("  loss %2d%% -> %.2f s" % (loss, avg))
        rows.append((loss, round(avg, 2)))
    save_csv("download_vs_loss.csv", ["loss_percent", "seconds"], rows)
    save_plot("download_vs_loss.png", [r[0] for r in rows], [r[1] for r in rows],
              "Packet loss (%)", "Download time (s)", "Download time vs packet loss (2 MB file)")


def exp_sources():
    """Download time vs number of source peers (full replication so any peer can serve any chunk)."""
    print("Experiment 2: download time vs number of source peers")
    storage.REPLICAS = 4
    rows = []
    for k in [1, 2, 3, 4]:
        times = []
        for _ in range(TRIALS):
            peers, fid, base = make_cluster(4)
            d = add_downloader(peers, base)
            d.downloader.chunk_delay = 0.02     # small per-chunk delay so parallelism is visible
            r = d.download("exp.bin", max_sources=k)
            times.append(r["seconds"] if r else float("nan"))
            stop_all(peers + [d])
        avg = sum(times) / len(times)
        print("  %d sources -> %.2f s" % (k, avg))
        rows.append((k, round(avg, 2)))
    storage.REPLICAS = 2
    save_csv("download_vs_sources.csv", ["sources", "seconds"], rows)
    save_plot("download_vs_sources.png", [r[0] for r in rows], [r[1] for r in rows],
              "Number of source peers", "Download time (s)", "Download time vs number of sources")


def exp_chat():
    """Chat: messages delivered in order and retransmissions needed, vs loss."""
    print("Experiment 3: chat under packet loss")
    storage.REPLICAS = 2
    rows = []
    for loss in [0, 5, 10, 20]:
        peers, fid, base = make_cluster(4)
        set_loss(peers, loss / 100.0)
        for i in range(100):
            peers[0].chat.send("m%d" % i)
        expected = ["m%d" % i for i in range(100)]
        ok = wait_until(lambda: all([t for (s, t) in p.chat.history if s == peers[0].my_id] == expected
                                    for p in peers[1:]), 60)
        retrans = peers[0].retransmissions(1)
        print("  loss %2d%% -> all delivered in order: %s, retransmissions: %d" % (loss, ok, retrans))
        rows.append((loss, int(ok), retrans))
        stop_all(peers)
    save_csv("chat_vs_loss.csv", ["loss_percent", "all_in_order", "retransmissions"], rows)
    save_plot("chat_retransmissions.png", [r[0] for r in rows], [r[2] for r in rows],
              "Packet loss (%)", "Retransmissions (100 messages x 3 receivers)",
              "Chat retransmissions vs loss")


def exp_recovery():
    """Time to finish a download when a source peer is killed in the middle."""
    print("Experiment 4: recovery after a peer is killed")
    storage.REPLICAS = 2
    results = []
    for kill in [False, True]:
        peers, fid, base = make_cluster(4)
        d = add_downloader(peers, base)
        d.downloader.chunk_delay = 0.1
        box = {}
        t = threading.Thread(target=lambda: box.update(r=d.download("exp.bin")))
        t.start()
        if kill:
            time.sleep(1.0)
            peers[1].kill()
        t.join(90)
        seconds = box["r"]["seconds"] if box.get("r") else float("nan")
        print("  peer killed: %-5s -> download time %.2f s" % (kill, seconds))
        results.append(round(seconds, 2))
        stop_all(peers + [d])
    save_csv("recovery.csv", ["peer_killed", "seconds"], [("no", results[0]), ("yes", results[1])])
    save_plot("recovery.png", ["no kill", "peer killed"], results, "", "Download time (s)",
              "Download with a peer killed mid-transfer", bar=True)


def exp_storage():
    """Storage overhead vs replication factor."""
    print("Experiment 5: storage overhead vs replication")
    rows = []
    for r in [1, 2, 3, 4]:
        storage.REPLICAS = r
        peers, fid, base = make_cluster(4)
        m = peers[0].storage.all_manifests()[0]
        time.sleep(1)
        total = 0
        biggest = 0
        for p in peers:
            size = sum(os.path.getsize(os.path.join(p.storage.chunk_dir, f))
                       for f in os.listdir(p.storage.chunk_dir))
            total += size
            biggest = max(biggest, size)
        print("  R=%d -> total stored %.1f MB (%.1fx file), biggest peer holds %.0f%% of file" % (
            r, total / 1e6, total / m["size"], 100.0 * biggest / m["size"]))
        rows.append((r, round(total / m["size"], 2), round(100.0 * biggest / m["size"], 1)))
        stop_all(peers)
    storage.REPLICAS = 2
    save_csv("storage_overhead.csv", ["replicas", "total_over_file_size", "biggest_peer_percent"], rows)
    save_plot("storage_overhead.png", [r[0] for r in rows], [r[1] for r in rows],
              "Replication factor R", "Total storage / file size", "Storage overhead vs replication")


if __name__ == "__main__":
    shutil.rmtree(FOLDER, ignore_errors=True)
    os.makedirs(FOLDER)
    os.makedirs(RESULTS, exist_ok=True)
    exp_loss()
    exp_sources()
    exp_chat()
    exp_recovery()
    exp_storage()
    print("\nDone. See the folder:", RESULTS)
