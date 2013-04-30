import unittest
import random
import string
import mmap
import tempfile
import math
import struct
import os
import json

from hlldb import HyperLogLogDB
import hll

class TestHLL(unittest.TestCase):

    test_data1 = set()
    test_data2 = set()
    test_data3 = set()
    test_set_size = 1000
    error_rate = 0.01
    m = 16384

    @classmethod
    def setUpClass(cls):
        for i in range(cls.test_set_size):
            cls.test_data1.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))
        for i in range(cls.test_set_size):
            cls.test_data2.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))
        for i in range(cls.test_set_size):
            cls.test_data3.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))

    def init_hll_file(self, fobj):
        length = self.m + mmap.PAGESIZE - self.m % mmap.PAGESIZE
        fobj.write(''.join(['\x00' for i in range(length)]))
        fobj.seek(0)
        mfile = mmap.mmap(fobj.fileno(), self.m)
        return hll.MmapSlice(mfile, self.m)


    def test_header_saving(self):
        f = tempfile.NamedTemporaryFile(mode='r+b', delete=False)
        filename = f.name
        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        self.assertEqual(test.idx_offset, test.header_struct.size)
        size_of_empty_index = 2
        self.assertEqual(test.last_pos, test.header_struct.size+size_of_empty_index)
        test.flush()
        test = None
        f.close()

        f = open(filename, 'r+b')
        header_struct = struct.Struct('LLLfL')
        f.seek(0)
        data = f.read(mmap.PAGESIZE)
        data = header_struct.unpack_from(data)
        idx_offset, idx_length, last_pos, error_rate, m = data
        self.assertAlmostEqual(error_rate, self.error_rate)
        self.assertEqual(idx_length, size_of_empty_index)
        self.assertEqual(last_pos, header_struct.size+size_of_empty_index)
        f.close()
        os.remove(filename)


    def test_header_loading(self):
        header_struct = struct.Struct('LLLfL')
        error_rate = self.error_rate
        idx_offset = header_struct.size
        idx_length = 2
        last_pos = idx_offset+idx_length
        m = 16384

        f = tempfile.NamedTemporaryFile(mode='r+b')
        f.seek(0)
        f.write(''.join(['\x00' for i in range(last_pos)]))

        data = header_struct.pack(idx_offset, idx_length, last_pos, error_rate, m)
        unpacked_data = header_struct.unpack(data)

        f.seek(0)
        f.write(data)
        data = json.dumps({})
        f.seek(idx_offset)
        f.write(data)
        f.seek(0)

        data = f.read(header_struct.size)
        f.seek(0)

        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        self.assertEqual(test.idx_offset, idx_offset)


    def test_hll_counting(self):
        f = tempfile.NamedTemporaryFile(mode='r+b')
        test1 = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        test1.add('test_key', 'test_val')

        f2 = tempfile.NamedTemporaryFile(mode='r+b')
        mfile = self.init_hll_file(f2)
        test2 = hll.HyperLogLog(self.error_rate, mfile)
        test2.add('test_val')

        self.assertEqual(test1.count('test_key'), 1)
        self.assertEqual(len(test2), 1)

    def test_add_hll(self):
        f = tempfile.NamedTemporaryFile(mode='r+b')
        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        test.add('test_key', 'test_val')
        self.assertEqual(test.count('test_key'), 1)

    def test_add_hll2(self):
        f = tempfile.NamedTemporaryFile(mode='r+b')
        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        test.add('test_key', 'test_val')
        test.add('test_key2', 'test_val2')
        test.add('test_key2', 'test_val3')
        self.assertEqual(test.count('test_key'), 1)
        self.assertEqual(test.count('test_key2'), 2)

    def test_loading_hll(self):
        f = tempfile.NamedTemporaryFile(mode='r+b')
        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        test.add('test_key', 'test_val')
        test.add('test_key2', 'test_val2')
        test.add('test_key2', 'test_val3')
        test.flush()

        test = None

        test = HyperLogLogDB(fileobj=f, error_rate=self.error_rate)
        self.assertEqual(test.count('test_key'), 1)
        self.assertEqual(test.count('test_key2'), 2)

    def test_merging_hll(self):
        f1 = tempfile.NamedTemporaryFile(mode='r+b')
        test1 = HyperLogLogDB(fileobj=f1, error_rate=self.error_rate)
        test1.add('test_key', 'test_val')
        test1.add('test_key2', 'test_val2')
        test1.add('test_key2', 'test_val3')

        f2 = tempfile.NamedTemporaryFile(mode='r+b')
        test2 = HyperLogLogDB(fileobj=f2, error_rate=self.error_rate)
        test2.add('test_key', 'test_val2')
        test2.add('test_key2', 'test_val22')
        test2.add('test_key3', 'test_val32')

        test1.merge(test2)

        self.assertEqual(test1.count('test_key'), 2)
        self.assertEqual(test1.count('test_key2'), 3)
        self.assertEqual(test1.count('test_key3'), 1)

    def test_merging_hll3(self):
        f1 = tempfile.NamedTemporaryFile(mode='r+b')
        test1 = HyperLogLogDB(fileobj=f1, error_rate=self.error_rate)
        test1.add('test_key', 'test_val')
        test1.add('test_key2', 'test_val2')
        test1.add('test_key2', 'test_val3')

        f2 = tempfile.NamedTemporaryFile(mode='r+b')
        test2 = HyperLogLogDB(fileobj=f2, error_rate=self.error_rate)
        test2.add('test_key', 'test_val2')
        test2.add('test_key2', 'test_val22')
        test2.add('test_key3', 'test_val32')

        f3 = tempfile.NamedTemporaryFile(mode='r+b')
        test3 = HyperLogLogDB(fileobj=f3, error_rate=self.error_rate)
        test3.add('test_key', 'test_val3')
        test3.add('test_key4', 'test_val22')
        test3.add('test_key5', 'test_val32')

        test1.merge([test2, test3])

        self.assertEqual(test1.count('test_key'), 3)
        self.assertEqual(test1.count('test_key2'), 3)
        self.assertEqual(test1.count('test_key3'), 1)
        self.assertEqual(test1.count('test_key4'), 1)
        self.assertEqual(test1.count('test_key5'), 1)

    def test_copy_hll(self):
        f1 = tempfile.NamedTemporaryFile(mode='r+b')
        test1 = HyperLogLogDB(fileobj=f1, error_rate=self.error_rate)
        for v in self.test_data1:
            test1.add('test_key', v)

        f2 = tempfile.NamedTemporaryFile(mode='r+b')
        test2 = HyperLogLogDB(fileobj=f2, error_rate=self.error_rate)
        test2.merge(test1)

        self.assertEqual(test1.count('test_key'), test2.count('test_key'))

    def test_move_index(self):
        # import yappi
        # yappi.start(builtins=True)

        f1 = tempfile.NamedTemporaryFile(mode='r+b')
        test1 = HyperLogLogDB(fileobj=f1, error_rate=self.error_rate)
        test1.flush()
        keys = set()

        for i in range(500):
            key = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20))
            keys.add(key)
            for v in self.test_data1:
                test1.add(key, v)

        test1.flush()

        # yappi.stop()
        # f = open('hyperloglogdb.profile', 'w+')
        # yappi.print_stats(out=f, sort_type=yappi.SORTTYPE_TSUB)

        test1 = None

        test1 = HyperLogLogDB(fileobj=f1, error_rate=self.error_rate)
        for key in keys:
            self.assertAlmostEqual(test1.count(key), len(self.test_data1), delta=len(self.test_data1)*self.error_rate)



unittest.main()
# tester = TestHLL('test_add_hll')
# tester.run()

