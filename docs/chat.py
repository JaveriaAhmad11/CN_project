"""
chat.py
-------
Group chat. UDP has no real "send to group", so we send the message to every peer
separately (unicast) through the reliable channel of the CHAT stream.

Ordering and no-duplicates come from reliable.py:
    messages from one sender get seq 1, 2, 3 ... and the receiver delivers them in that order,
    dropping duplicates and waiting for missing ones.
"""
import time

from protocol import CHAT, STREAM_CHAT, encode_json, decode_json


class Chat:
    def __init__(self, peer):
        self.peer = peer
        self.history = []     # list of (sender_id, text) in delivery order

    def send(self, text):
        data = encode_json({"text": text, "time": time.time()})
        self.peer.send_to_all(STREAM_CHAT, CHAT, data)
        self.history.append((self.peer.my_id, text))

    def on_chat(self, sender_id, data):
        msg = decode_json(data)
        text = msg.get("text", "")
        self.history.append((sender_id, text))
        name = self.peer.members.name_of(sender_id)
        self.peer.log("\n[" + name + "] " + text)
