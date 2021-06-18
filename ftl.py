#!/usr/bin/env python3

from enum import Enum
from queue import Queue


class Device:
    def __init__(self):
        pass

    def process_writes(self):
        num_active = 0
        num_writes = 0
        for wptr in self.wptr_list:
            num_writes += wptr.process_writes()
            state = wptr.band.state
            if state not in (BandState.FULL,
                             BandState.CLOSING,
                             BandState.CLOSED):
                num_active += 1
        if num_active < 1:
            self.add_wptr()
        return num_writes > 0

    def add_wptr(self):
        band = self.next_wptr_band()
        wptr = band.get_wptr()
        band.prep_write()
        self.wptrs_list.append(wptr)

    def next_write_band(self):
        for band in self.free_bands:
            if band.num_reloc_bands == 0 and band.num_reloc_blocks == 0:
                assert band.state in (Band.State.FREE, Band.State.CLOSED)
                band.erase()
                return band
        else:
            assert False

    def next_wptr_band(self):
        if self.next_band:
            band = self.next_band
            self.next_band = None
            return band
        else:
            return self.next_write_band()


class IO:
    def __init__(self):
        self.dev = None
        self.iov_pos = 0
        self.iov_cnt = 0
        self.children = []
        self.done = False
        self.cb_fn = None
        self.parent = None

    def done(self):
        return self.req_cnt == 0 and self.pos == self.num_blocks

    def _init_iov(self, iov, iov_cnt, iov_off, num_blocks):
        offset = 0
        self.iov_pos = 0
        self.iov_cnt = 0
        while iov_off < num_blocks:
            num_left = min(iov[self.iov_cnt].iov_len / BLOCK_SIZE - iov_off,
                           num_blocks)
            self.iov[self.iov_cnt].iov_base = iov[self.iov_cnt].iov_base + iov_off * BLOCK_SIZE
            self.iov[self.iov_cnt].iov_len = num_left * BLOCK_SIZE
            offset += num_left
            self.iov_cnt += 1
            iov_off = 0

    def _init_from_iovs(self, iovs, num_blocks):
        iov = iovs[0]
        iov_cnt = len(iovs)
        iov_off = 0

    def _init_from_parent(self, parent, num_blocks):
        iov = parent.iov[parent.iov_pos]
        iov_cnt = parent.iov_cnt - parent.iov_pos
        iov_off = parent.iov_off
        if iov_cnt > 0:
            self._init_iov(iov, iov_cnt, iov_off, num_blocks)

    def submit_child_write(self, wptr):
        addr = wptr.addr
        child = IO(dev=self.dev,
                   band=self.band,
                   parent=self)
        child._init_from_parent(self, num_blocks)

    def submit_write(self, wptr):
        while self.iov_pos < self.iov_cnt:
            if (not dev.is_append_supported and wptr.zone.busy):
                wptr.pending_queue.put(self)
                break
            self.submit_child_write(wptr)
            wptr.advance(self.dev.xfer_size)
        if self.done():
            self.complete()

    def complete(self):
        complete = io.children.count() == 0
        self.done = True
        if complete:
            if io.cb_fn:
                io.cb_fn(io, io.cb_ctx, io.status)
        if self.parent:
            if self.parent.remove_child(self):
                self.parent.complete()
        del self


class WritePtr:
    def __init__(self, dev, band, zone, addr):
        self.dev = dev
        self.band = band
        self.zone = zone
        self.addr = addr
        self.pending_queue = Queue()

    def submit_write(self, io):
        while
    def process_writes(self):
        io = self.pending_queue.pop()
        io


BLOCK_SIZE = 4096


class BandState(Enum):
    FREE = 1
    PREP = 2
    OPENING = 3
    OPEN = 4
    FULL = 5
    CLOSING = 6
    CLOSED = 7
    MAX = 8


class Band:
    def __init__(self):
        pass

    def alloc_lba_map(self):
        assert self.lba_map.ref_cnt == 0
        assert self.lba_map.map is None
        self.lba_map.dma_buf = self.dev.lba_pool.alloc()
        self.lba_map.map = (self.lba_map.dma_buf +
                            BLOCK_SIZE * (self.dev.tail_md.header_num_blocks() +
                                          self.dev.valid_map_num_blocks()))
        self.lba_map.segments = (self.lba_map.dma_buf +
                                 BLOCK_SIZE * self.dev.valid_map_num_blocks())
        self.lba_map.ref_cnt += 1

    def prep_write(self):
        self.alloc_lba_map()
        self.dev.seq += 1
        self.seq = self.dev.seq

    def get_wptr(self):
        zone = self.zone[0]
        return WritePtr(dev=self.dev,
                        band=self,
                        zone=zone,
                        addr=zone.info.zone_id)

    def erase(self):
        for zone in self.zones:
            if zone.state == Zone.State.EMPTY:
                continue
            zone.erase()


class ZoneAction(Enum):
    CLOSE = 1
    FINISH = 2
    OPEN = 3
    RESET = 4
    OFFLINE = 5


class ZoneState(Enum):
    FREE = 1
    PREP = 2
    OPENING = 3
    OPEN = 4
    FULL = 5
    CLOSING = 6
    CLOSED = 7
    MAX = 8


class Zone:
    def erase(self):
        self.state = ZoneState.PREP
        self.management(self.info.zone_id, ZoneAction.RESET)
