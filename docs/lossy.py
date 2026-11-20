"""
lossy.py
--------
A tiny wrapper around a UDP socket that randomly DROPS outgoing packets.
Used to prove that our reliability layer really works (e.g. loss = 0.1 means 10% lost).
"""
import random


class LossySocket:
    def __init__(self, sock, loss=0.0):
        self.sock = sock
        self.loss = loss      # probability (0.0 - 1.0) that a packet is dropped
        self.sent = 0
        self.dropped = 0

    def sendto(self, data, addr):
        if self.loss > 0 and random.random() < self.loss:
            self.dropped += 1
            return
        self.sent += 1
        try:
            self.sock.sendto(data, addr)
        except OSError:
            pass  # socket closed or network unreachable: just ignore
