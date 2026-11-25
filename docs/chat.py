"""
chat.py
-------
Group chat + chat history.

SENDING: UDP has no real "send to group", so we send the message to every peer
separately (unicast) through the reliable channel of the CHAT stream.
Ordering and no-duplicates come from reliable.py (sequence numbers per sender).

HISTORY:
  * every message (sent or received) is saved in  data_<port>/chat_history.log  (one JSON per line)
    so the history survives a restart of the peer;
  * when a new peer joins, every existing peer sends it its last 50 messages (HISTORY message);
    the newcomer merges them and skips messages it already has;
  * type  /history 20  to see the last 20 messages.
  (Timestamps come from each sender's own clock, so computers with a wrong clock
   can show slightly wrong order in the history view.)
"""
import json
import os
import threading
import time

from protocol import CHAT, HISTORY, STREAM_CHAT, encode_json, decode_json

HISTORY_SEND = 50      # how many recent messages a newcomer receives


class Chat:
    def __init__(self, peer):
        self.peer = peer
        self.history = []        # (sender_id, text) in delivery order (used by the tests)
        self.entries = []        # saved messages: {"t", "id", "name", "text"}
        self.seen = set()        # (id, t, text) of saved messages, to skip duplicates
        self.lock = threading.Lock()
        self.path = os.path.join(peer.storage.folder, "chat_history.log")
        self.load()

    # ---------------- history file ----------------
    def load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                try:
                    e = json.loads(line)
                    key = (e["id"], e["t"], e["text"])
                except (ValueError, KeyError):
                    continue
                if key not in self.seen:
                    self.seen.add(key)
                    self.entries.append(e)

    def remember(self, e):
        """Save one message. Returns False if we already had it."""
        key = (e["id"], e["t"], e["text"])
        with self.lock:
            if key in self.seen:
                return False
            self.seen.add(key)
            self.entries.append(e)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(e) + "\n")
        return True

    def recent(self, n):
        with self.lock:
            return sorted(self.entries, key=lambda e: e["t"])[-n:]

    def show(self, n=20):
        entries = self.recent(n)
        if not entries:
            print("(no chat history yet)")
        for e in entries:
            clock = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e["t"]))
            print("[%s] %s: %s" % (clock, e["name"], e["text"]))

    # ---------------- sending / receiving ----------------
    def send(self, text):
        t = time.time()
        self.peer.send_to_all(STREAM_CHAT, CHAT, encode_json({"text": text, "time": t}))
        self.history.append((self.peer.my_id, text))
        self.remember({"t": t, "id": self.peer.my_id, "name": self.peer.name, "text": text})

    def on_chat(self, sender_id, data):
        msg = decode_json(data)
        text = msg.get("text", "")
        name = self.peer.members.name_of(sender_id)
        self.history.append((sender_id, text))
        self.remember({"t": msg.get("time", time.time()), "id": sender_id, "name": name, "text": text})
        self.peer.log("\n[" + name + "] " + text)

    # ---------------- history for newcomers ----------------
    def send_history_to(self, peer_id):
        messages = self.recent(HISTORY_SEND)
        if messages:
            self.peer.send_reliable(peer_id, STREAM_CHAT, HISTORY, encode_json({"messages": messages}))

    def on_history(self, sender_id, data):
        added = 0
        for e in decode_json(data).get("messages", []):
            if all(k in e for k in ("t", "id", "name", "text")):
                if self.remember(e):
                    added += 1
        if added:
            self.peer.log("\n* received %d old chat messages (type /history to read them)" % added)
