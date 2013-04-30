"""
This file is part of HyperLogLogDB.

HyperLogLogDB is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

HyperLogLogDB is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with HyperLogLogDB.  If not, see <http://www.gnu.org/licenses/>.
"""

import json
import struct
import mmap
import os

import hll

class HyperLogLogDB(object):
    fobj = None
    mfile = None
    idx = None
    f_header = None
    f_idx = None
    header_struct = None
    idx_offset = 0
    idx_length = 0
    last_pos = 0
    file_size = 0
    m = 0
    error_rate = 0.01
    bitcount_arr = None

    def __init__(self, file_path=None, fileobj=None, error_rate=0.01):
        """
        Header structure:
        unsigned long   - index offset
        unsigned long   - index length (bytes)
        unsigned long   - last position
        float           - error_rate
        unsigned long   - m value for this error_rate (from hll)
        """

        self.header_struct = struct.Struct('LLLfL')
        # self.header_struct = struct.Struct('iiifi')
        self.error_rate = error_rate

        if fileobj:
            self.fobj = fileobj
        elif file_path:
            if not os.access(file_path, os.R_OK | os.W_OK):
                self.fobj = open(file_path, 'wb')
                self.fobj.close()
            self.fobj = open(file_path, 'r+b')
        else:
            raise ValueError("Must include either file_path or fileobj")

        if self.fobj.mode != 'r+b':
            raise ValueError("fileobj must be opened in mode w+b")

        self.fobj.seek(0)
        data = self.fobj.read(mmap.PAGESIZE)
        self.bitcount_arr = hll.HyperLogLog._get_bitcount_arr(error_rate)

        if not data:
            # print "Writing blank header"
            self.write_bytes(0, self.header_struct.size)
            self.file_size = self.header_struct.size
            self.mfile = mmap.mmap(self.fobj.fileno(), 0)
            self.f_header = hll.MmapSlice(self.mfile, self.header_struct.size, 0)
            self.write_header()
            self.idx = {}
            self.idx_length = 0
            self.idx_offset = self.header_struct.size
            self.last_pos = self.header_struct.size
            self.error_rate = error_rate
            self.m = hll.HyperLogLog._get_size(error_rate)
            self.flush_idx()
        else:
            self.fobj.seek(0, os.SEEK_END)
            self.file_size = self.fobj.tell()
            self.mfile = mmap.mmap(self.fobj.fileno(), 0)
            self.f_header = hll.MmapSlice(self.mfile, self.header_struct.size, 0)
            self.read_header()
            self.f_idx = hll.MmapSlice(self.mfile, self.idx_length, offset=self.idx_offset)
            self.read_idx()

    def resize(self, new_size):
        blocksize = mmap.PAGESIZE*1000

        if self.file_size >= new_size:
            return

        expand_to = new_size + blocksize - new_size % blocksize
        self.mfile.flush()
        self.write_bytes(self.file_size, expand_to-self.file_size)
        self.file_size = expand_to
        self.mfile = mmap.mmap(self.fobj.fileno(), 0)

        if self.f_header:
            self.f_header.data = self.mfile
        if self.f_idx:
            self.f_idx.data = self.mfile
        for obj in self.idx.itervalues():
            obj['mmap'].data = self.mfile

    def write_bytes(self, start, length):
        self.fobj.seek(start+length-1)
        self.fobj.write('\x00')
        self.fobj.flush()

    def read_header(self):
        data = self.header_struct.unpack(self.f_header.read(self.header_struct.size))
        self.idx_offset, self.idx_length, self.last_pos, self.error_rate, self.m = data

    def write_header(self):
        data = self.header_struct.pack(self.idx_offset, self.idx_length, self.last_pos, self.error_rate, self.m)
        self.f_header.write(data)
        self.mfile.flush()

    def flush_idx(self):
        idx_str = json.dumps(dict([(k, v['offset']) for k,v in self.idx.items()]))
        if len(idx_str) > self.idx_length:
            #move the index to a new location
            self.idx_length = len(idx_str)
            self.idx_offset = self.last_pos
            self.resize(self.idx_offset+self.idx_length)
            self.last_pos = self.idx_offset + self.idx_length
            self.write_header()
            self.f_idx = hll.MmapSlice(self.mfile, self.idx_length, offset=self.idx_offset)

        self.f_idx.write(idx_str)
        self.mfile.flush()

    def read_idx(self):
        new_idx = json.loads(self.f_idx.read(self.idx_length))

        self.idx = dict([(k,{'offset':v}) for k,v in new_idx.iteritems()])
        for k, obj in self.idx.iteritems():
            obj['mmap'] = hll.MmapSlice(self.mfile, self.m, obj['offset'])
            obj['hll'] = hll.HyperLogLog(self.error_rate, obj['mmap'], bitcount_arr=self.bitcount_arr)



    def flush(self):
        self.flush_idx()
        self.write_header()
        self.mfile.flush()
        os.fsync(self.fobj)

    def __exit__(self, type, value, traceback):
        self.flush()


    def create(self, key):
        new_size = self.last_pos+self.m
        self.resize(self.last_pos+self.m)
        obj = {}
        obj['offset'] = self.last_pos
        obj['mmap'] = hll.MmapSlice(self.mfile, self.m, offset=obj['offset'])
        obj['hll'] = hll.HyperLogLog(self.error_rate, obj['mmap'], bitcount_arr=self.bitcount_arr)
        self.idx[key] = obj
        self.last_pos = obj['offset'] + self.m
        return obj['hll']

    def get_hll(self, key):
        if key not in self.idx:
            return None
        else:
            return self.idx[key]['hll']

    def merge(self, others):
        if not isinstance(others, list):
            others = [others]

        # get a list of all keys in other
        all_other_keys = set()
        for other in others:
            all_other_keys.update(other.idx.keys())

        for k in all_other_keys:
            self.update(k, filter(lambda o: o, map(lambda other: other.get_hll(k), others)))

    def update(self, key, others):
        if not isinstance(others, list):
            others = [others]

        if key not in self.idx and len(others) == 1:
            self.create(key)
            self.copy_hll(others[0], self.idx[key]['hll'])
        elif key not in self.idx:
            self.create(key)
            self.idx[key]['hll'].update(others)
        else:
            self.idx[key]['hll'].update(others)

    def copy_hll(self, from_hll, to_hll):
        to_hll.M.write(from_hll.M.read(self.m))

    def add(self, key, val):
        if key not in self.idx:
            self.create(key)
        self.idx[key]['hll'].add(val)

    def count(self, key):
        if key not in self.idx:
            return 0
        else:
            return len(self.idx[key]['hll'])

