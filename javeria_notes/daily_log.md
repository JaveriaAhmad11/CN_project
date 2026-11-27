# Javeria's Daily Log - CN Project
**Role:** Protocol and Reliability Lead
**Files I own:** `protocol.py`, `reliable.py`, `test_reliable.py`
**Report Sections I own:** Protocol specification, Reliability design, Trade-offs table, Final assembly.

---

## Day 1: 20 November (J2 & J3)
**Goal:** Understand the 22-byte header and the Reliable Channel (ACK/Timeout logic).

### 🛠️ What I did today:
- [x] Ran `python selftest.py` (Result: ALL TESTS PASSED).
- [x] Read `protocol.py` and drew the 22-byte header on paper.
- [x] Ran the CRC test in the terminal and took a screenshot (`javeria_crc_proof.png`).
- [x] Read `reliable.py` and filled out the function table.
- [x] Wrote down the 3-packet loss story (how duplicates are handled and order is saved).
- [x] Wrote down the RTT/Timeout formula.

### 📝 My Notes:
- **Header:** 9 fields, 22 bytes total (version, type, flags, stream, sender, seq, ack, length, crc).
- **New Message Type Added:** `13 = HISTORY` (for chat history sync).
- **CRC Proof:** When I ran the test script, it printed `24` (22 header + 2 payload). When I flipped one bit, `parse_packet` returned `None`, proving the CRC32 caught the corruption.
- **RTT Formula:** `rtt = 0.875 * rtt + 0.125 * sample`, `timeout = 2 * rtt`.

---

## Day 2: 21 November (J4)
**Goal:** Write the unit test (`test_reliable.py`).

### 🛠️ What I did today:
- [x] Created `test_reliable.py` with 20% packet loss.
- [x] Ran the test. Received all 100 messages in order!
- [x] Took screenshot (`javeria_unit_test.png`).
- [x] Did the group teach-in for `protocol.py` and `reliable.py`.

### 📝 My Notes:
- The test proves reliability works. Even with 20% loss, the window, ACKs, and timeouts fixed it.
- **Retransmissions:** (Write your number here, e.g., ~50)
- **Duplicates dropped:** (Write your number here, e.g., ~20)

---

## Day 3: 22 November (J5)
**Goal:** Break things on purpose to understand trade-offs.

### 🛠️ What I did today:
- [x] **Exp 1 (No Window):** Changed `WINDOW = 1` in `reliable.py`. Observed: Download time got way bigger. Taught me: Window of 1 is stop-and-wait, very slow.
- [x] **Exp 2 (Huge Loss):** Changed `Wire(0.2)` to `Wire(0.5)` in test. Observed: Test failed to finish in 8 seconds (`received 33 in order: False`). Huge retransmissions. Taught me: At 50% loss, the 8-second timer runs out before all data arrives. It is very slow.
- [x] **Exp 3 (No ACK for dupes):** Commented out ACK line in `on_data`. Observed: Test failed (`received 32 in order: False`) with 118 duplicates dropped. Taught me: We MUST ACK duplicates or the sender gets stuck resending forever.
- [x] **Exp 4 (Long Timeout):** Changed `MIN_TIMEOUT = 1.0`. Observed: Takes longer to finish. Taught me: Timeout trades speed for fewer needless resends.
- [x] Reverted all code changes back to original.

---

## Day 4: 23 November (J6 - Report Part 1)
**Goal:** Write the Protocol Specification section.

### 🛠️ What I did today:
- [x] Created `report/report_draft.md`.
- [x] Wrote the 22-byte Header Table.
- [x] Wrote the Message Types Table (including the new `13 = HISTORY` type).
- [x] Wrote the Expected Message Sequences (Join, Chat, Share, Download, Failure, and Chat History sync).
- [x] Added the `javeria_crc_proof.png` screenshot as proof of integrity.

---

## Day 5: 24 November (J6 - Report Part 2)
**Goal:** Write the Reliability Design and Trade-offs sections.

### 🛠️ What I did today:
- [x] Read the updated project `.docx` to find the new Chat History and Storage Maintenance features.
- [x] Added the "Chat History and Storage Maintenance" sections to the report.
- [x] Wrote the detailed "Design Trade-offs" table (comparing Selective Repeat vs Go-Back-N, R=2 vs R=4, etc.).
- [x] Wrote the "Experimental Results" paragraph, referencing the unit test and the 50% loss / no-ACK experiments from Day 3.
- [x] Pushed all report updates to GitHub.

---

## Day 6: 25-26 November (J7 - Diagrams & Merge)
**Goal:** Draw diagrams and merge the team's report.

### 🛠️ What I plan to do:
- [x] Draw **Figure 2 (Header Layout)** using draw.io.
- [x] Draw **Figure 3 (Join Sequence)** showing JOIN, JOIN_REPLY, and HISTORY.
- [x] Draw **Figure 4 (Reliable Send)** showing packet loss, timeout, and ACK loss.
- [x] Draw **Figure 5 (Group Chat Flow)**.
- [x] Collect Shamin's graphs and Laraib's flow sections.
- [x] Merge all parts into the final `report.md`.

---

## Day 7: 27-28 November (Demo Rehearsal & Viva Prep)
**Goal:** Practice the live demo and viva questions.

### 🛠️ What I plan to do:
- [x] Rehearse demo as Peer A (share files, simulate loss with `/loss 0.2`).
- [x] Prepare viva answer: *"How does chat history work when a new peer joins?"* (Answer: Existing peers send a reliable `HISTORY` message with the last 50 messages; the newcomer merges them and drops duplicates).
- [x] Rehearse the `/crash` mid-download scenario with the team.

---

## Day 8: 29 November (Final Zip & Submit)
**Goal:** Check zip contents and submit on LMS.

### 🛠️ What I plan to do:
- [ ] Run `python selftest.py` one last time (ALL TESTS PASSED).
- [ ] Check that all `[PASTE SCREENSHOT HERE]` placeholders in the report are filled.
- [ ] Ensure no `data_*` folders or `.bin` files are in the final zip.
- [ ] Verify `README.md`, all `.py` files, and the final PDF report are inside the zip.
- [ ] Upload to LMS before the deadline.