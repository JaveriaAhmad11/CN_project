# P2P Group Chat + File Sharing over UDP (Computer Networks project)

Python 3.8+ only (no extra libraries, except `matplotlib` for experiments.py).

## Files
| File | What it does |
|---|---|
| protocol.py | packet header, message types, CRC32 |
| lossy.py | drops packets on purpose (testing) |
| reliable.py | TCP-like reliability: seq, ACK, window, retransmit, ordering |
| membership.py | peer list, JOIN, heartbeat, failure detection |
| chat.py | group chat |
| storage.py | chunks, manifests, replica placement, chunk map |
| download.py | multi-source download, failover, join takeover, repair |
| peer.py | MAIN PROGRAM (run this) |
| selftest.py | automatic test of everything (one process) |
| experiments.py | measurements + graphs for the report |

## Quick test (one computer)
    python selftest.py          # should end with: ALL TESTS PASSED
    python experiments.py       # graphs in results/   (add 'quick' for a faster run)

## Run 4 peers by hand (one computer, 4 terminals)
    python peer.py --name A --port 5001
    python peer.py --name B --port 5002 --bootstrap 127.0.0.1:5001
    python peer.py --name C --port 5003 --bootstrap 127.0.0.1:5001
    python peer.py --name D --port 5004 --bootstrap 127.0.0.1:5001

## Run on a real network (4 computers, same Wi-Fi/LAN)
Same command on every computer, no bootstrap needed (LAN broadcast finds the others):
    python peer.py --name A
If broadcast is blocked, add --bootstrap <ip-of-a-running-peer>:5000

## Commands inside a peer
    hello everyone        -> chat to all
    /peers  /files  /map <file>  /stats
    /share <path>         -> share a file (>= 2 MB)
    /download <name>      -> download from several peers
    /loss 0.1             -> simulate 10% packet loss
    /delay 0.2            -> slow the download (time to kill a peer)
    /crash                -> die without saying goodbye
    /quit                 -> leave politely

## Demo script
1. Start 4 peers.  2. On A: `/share bigfile1.bin` and `/share bigfile2.bin`.
3. On D: `/files`, `/delay 0.2`, `/download bigfile1.bin`.
4. While it runs, type `/crash` on B.   5. D prints DOWNLOAD OK + hash verified.

Reset everything: delete the `data_*` folders.
