"""
selftest.py  -  python selftest.py
Runs 4 peers (+1 late joiner) inside ONE process on localhost and checks everything:
join, chat under loss, file sharing, join takeover, multi-source download,
and a peer being killed in the middle of a download.
Prints PASS/FAIL for each step.
"""
import os
import random
import shutil
import sys
import time

from peer import Peer

PORT = 6100
FOLDER = "selftest_data"


def wait_until(condition, timeout=20):
    end = time.time() + timeout
    while time.time() < end:
        if condition():
            return True
        time.sleep(0.1)
    return False


def check(label, ok):
    print(("PASS  " if ok else "FAIL  ") + label)
    if not ok:
        global failed
        failed = True


failed = False
shutil.rmtree(FOLDER, ignore_errors=True)
os.makedirs(FOLDER)

# a 2 MB random test file
test_file = os.path.join(FOLDER, "test.bin")
with open(test_file, "wb") as f:
    f.write(os.urandom(2 * 1024 * 1024))
test_file2 = os.path.join(FOLDER, "test2.bin")
with open(test_file2, "wb") as f:
    f.write(os.urandom(2 * 1024 * 1024))

# ---- 1. four peers join (first one is the bootstrap) ----
peers = []
for i in range(4):
    boot = None if i == 0 else ("127.0.0.1", PORT)
    p = Peer("P%d" % i, PORT + i, boot, 0.0, os.path.join(FOLDER, "p%d" % i), verbose=False)
    p.start()
    peers.append(p)
    time.sleep(0.3)
check("all 4 peers see 3 others", wait_until(lambda: all(len(p.members.alive_ids()) == 3 for p in peers)))

# ---- 2. chat with 10% packet loss ----
for p in peers:
    p.sock.loss = 0.10
for i in range(50):
    peers[0].chat.send("msg-%d" % i)
expected = ["msg-%d" % i for i in range(50)]


def chat_ok(p):
    got = [t for (s, t) in p.chat.history if s == peers[0].my_id]
    return got == expected


check("chat: every peer got 50 messages in order, no duplicates (10% loss)",
      wait_until(lambda: all(chat_ok(p) for p in peers[1:]), 30))
print("      retransmissions by P0 on chat stream:", peers[0].retransmissions(1))

# ---- 3. share a file ----
for p in peers:
    p.sock.loss = 0.0
peers[0].share(test_file)
peers[0].share(test_file2)
mans = peers[0].storage.all_manifests()
fid, n = mans[0]["file_id"], mans[0]["num_chunks"]
fid2, n2 = mans[1]["file_id"], mans[1]["num_chunks"]
check("both files shared: every chunk has 2 holders on every peer's map",
      wait_until(lambda: all(len(p.storage.holders(fid, i)) >= 2 for p in peers for i in range(n))
                 and all(len(p.storage.holders(fid2, i)) >= 2 for p in peers for i in range(n2))))
check("no single peer holds a whole file",
      all(p.storage.my_chunk_count(fid) < n and p.storage.my_chunk_count(fid2) < n2 for p in peers))
# ---- 4. late joiner takes over its share ----
late = Peer("LATE", PORT + 4, ("127.0.0.1", PORT), 0.0, os.path.join(FOLDER, "late"), verbose=False)
late.start()
check("late joiner received the file list", wait_until(lambda: len(late.storage.all_manifests()) == 2))
check("late joiner took over some chunks", wait_until(lambda: late.storage.my_chunk_count(fid) > 0, 30))

# ---- 5. multi-source download with 10% loss ----
for p in peers + [late]:
    p.sock.loss = 0.10
result = peers[3].download("test.bin")
check("download completed (10% loss)", result is not None)
if result:
    same = open(result["path"], "rb").read() == open(test_file, "rb").read()
    check("downloaded file identical to original", same)
    check("chunks came from at least 2 peers", len(result["chunks_from"]) >= 2)
result2 = peers[3].download("test2.bin")
check("second file download completed (10% loss)", result2 is not None)
if result2:
    same2 = open(result2["path"], "rb").read() == open(test_file2, "rb").read()
    check("second file identical to original", same2)
# ---- 6. kill a peer while downloading ----
for p in peers + [late]:
    p.sock.loss = 0.0
downloader = peers[3]
os.remove(os.path.join(downloader.storage.download_dir, "test.bin"))
downloader.downloader.chunk_delay = 0.05      # slow down so we can kill in the middle
import threading
box = {}
t = threading.Thread(target=lambda: box.update(r=downloader.download("test.bin")))
t.start()
time.sleep(0.5)
peers[1].kill()                               # crash!
t.join(60)
check("download finished although a peer was killed", box.get("r") is not None)
if box.get("r"):
    same = open(box["r"]["path"], "rb").read() == open(test_file, "rb").read()
    check("file still identical after the crash", same)

# ---- 7. failure detection ----
check("others noticed the dead peer (heartbeat)",
      wait_until(lambda: not peers[0].members.is_alive(peers[1].my_id), 15))

def all_chunks_have_2_live_copies():
    for m in peers[0].storage.all_manifests():
        for i in range(m["num_chunks"]):
            live = [h for h in peers[0].storage.holders(m["file_id"], i)
                    if h == peers[0].my_id or peers[0].members.is_alive(h)]
            if len(live) < 2:
                return False
    return True

check("repair restored 2 live copies of every chunk",
      wait_until(all_chunks_have_2_live_copies, 40))

print("\nRESULT:", "SOME TESTS FAILED" if failed else "ALL TESTS PASSED")
for p in peers + [late]:
    p.kill()
sys.exit(1 if failed else 0)
