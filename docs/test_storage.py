import storage
from storage import chunk_holders

peers = [101, 202, 303, 404]
N = 64
storage.REPLICAS = 2

# deterministic + R distinct peers
for i in range(N):
    h = chunk_holders("abcd1234", i, peers)
    assert h == chunk_holders("abcd1234", i, list(reversed(peers)))
    assert len(set(h)) == 2

# minimal movement: removing a peer only changes chunks that peer held
gone = 303
changed = 0
for i in range(N):
    before = chunk_holders("abcd1234", i, peers)
    after = chunk_holders("abcd1234", i, [p for p in peers if p != gone])
    if gone not in before:
        assert before == after
    else:
        changed += 1
print("PASS. chunks that moved after one peer left:", changed, "of", N)

# balance: how many chunks each peer holds
count = {p: 0 for p in peers}
for i in range(N):
    for p in chunk_holders("abcd1234", i, peers):
        count[p] += 1
print("load per peer:", count)