import random, time, threading
from protocol import *
from reliable import ReliableChannel

class Wire:   # fake socket: drops 20% of packets, hands the rest to the other side
    def __init__(self, loss): 
        self.loss = loss
        self.target = None
        
    def sendto(self, data, addr):
        if random.random() < self.loss: 
            return # Drop the packet!
        pkt = parse_packet(data)
        if pkt.type == ACK: 
            self.target.on_ack(pkt)
        else: 
            self.target.on_data(pkt)

received = []
wa, wb = Wire(0.2), Wire(0.2)
a = ReliableChannel(wa, None, 1, STREAM_CHAT, lambda t, d: None)
b = ReliableChannel(wb, None, 2, STREAM_CHAT, lambda t, d: received.append(d.decode()))
wa.target, wb.target = b, a

def timer():
    while True:
        a.tick()
        b.tick()
        time.sleep(0.05)

threading.Thread(target=timer, daemon=True).start()

for i in range(100):
    a.send(CHAT, ("msg-%d" % i).encode())

time.sleep(8)
print("received", len(received), "in order:", received == ["msg-%d" % i for i in range(100)])
print("retransmissions:", a.retransmissions, "duplicates dropped by b:", b.duplicates)