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

import unittest
import random
import string
import mmap
import tempfile

from hll import HyperLogLog, MmapSlice

class TestMmapSlice(unittest.TestCase):
    def test_eq(self):
        f1 = tempfile.NamedTemporaryFile('r+b')
        f1.write(''.join('\x00' for i in range(40)))
        f1.flush()
        mfile1 = mmap.mmap(f1.fileno(),0)
        mslice1 = MmapSlice(mfile1, 20, 0)
        mslice2 = MmapSlice(mfile1, 20, 20)
        test3 = [ '\x00' for i in range(20)]

        for i in range(20):
            v = random.choice(string.ascii_uppercase + string.digits)
            mslice1[i] = v
            mslice2[i] = v
            test3[i] = v

        self.assertEqual(mslice1, mslice2)
        self.assertEqual(mslice1, test3)

    def test_not_eq(self):
        f1 = tempfile.NamedTemporaryFile('r+b')
        f1.write(''.join('\x00' for i in range(40)))
        f1.flush()
        mfile1 = mmap.mmap(f1.fileno(),0)
        mslice1 = MmapSlice(mfile1, 20, 0)
        mslice2 = MmapSlice(mfile1, 20, 20)
        test3 = [ '\x00' for i in range(20)]

        for i in range(20):
            mslice1[i] = random.choice(string.ascii_uppercase + string.digits)
            mslice2[i] = random.choice(string.ascii_uppercase + string.digits)
            test3[i] = random.choice(string.ascii_uppercase + string.digits)

        self.assertNotEqual(mslice1, mslice2)
        self.assertNotEqual(mslice1, test3)

    def test_hll(self):
        m = 16384
        f1 = tempfile.NamedTemporaryFile('r+b')
        f1.write(''.join('\x00' for i in range(m)))
        f1.flush()
        mfile1 = mmap.mmap(f1.fileno(),0)
        mslice1 = MmapSlice(mfile1, m, 0)
        test = HyperLogLog(0.01, mslice1)

        self.assertEqual(len(test), 0)
        test.add('test_val')
        self.assertEqual(len(test), 1)


class TestHLL(unittest.TestCase):

    test_data1 = set()
    test_data2 = set()
    test_data3 = set()
    test_set_size = 10000
    error_rate = 0.01

    @classmethod
    def setUpClass(cls):
        for i in range(cls.test_set_size):
            cls.test_data1.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))
        for i in range(cls.test_set_size):
            cls.test_data2.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))
        for i in range(cls.test_set_size):
            cls.test_data3.add(''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)))


    def test_add_twice(self):
        f = tempfile.TemporaryFile()
        m = 16384
        flen = m + mmap.PAGESIZE - m % mmap.PAGESIZE

        self.assertGreater(flen, m)

        f.write(''.join(['\x00' for i in range(flen)]))
        f.seek(0)
        fmap = mmap.mmap(f.fileno(), m)
        mslice = MmapSlice(fmap, m)

        hll = HyperLogLog(self.error_rate, mslice)
        for v in self.test_data1:
            hll.add(v)
        for v in self.test_data1:
            hll.add(v)
        self.assertAlmostEqual(self.test_set_size, len(hll), delta=self.test_set_size*self.error_rate)

    def test_mmap(self):
        f = tempfile.TemporaryFile()
        m = 16384
        flen = m + mmap.PAGESIZE - m % mmap.PAGESIZE

        self.assertGreater(flen, m)

        f.write(''.join(['\x00' for i in range(flen)]))
        fmap = mmap.mmap(f.fileno(), m)

        self.assertEqual(len(fmap), m)

        mslice = MmapSlice(fmap, m)
        hll = HyperLogLog(self.error_rate, mslice)
        for v in self.test_data1:
            hll.add(v)
        self.assertAlmostEqual(self.test_set_size, len(hll), delta=self.test_set_size*self.error_rate)

    def test_update(self):
        f = tempfile.TemporaryFile()
        m = 16384
        flen = (m*3) + mmap.PAGESIZE - (m*3) % mmap.PAGESIZE

        self.assertGreater(flen, m*3)

        f.write(''.join(['\x00' for i in range(flen)]))
        fmap = mmap.mmap(f.fileno(), m*3)

        self.assertEqual(len(fmap), m*3)


        mslice1 = MmapSlice(fmap, m, 0)
        mslice2 = MmapSlice(fmap, m, m)
        mslice3 = MmapSlice(fmap, m, m*2)

        hll1 = HyperLogLog(self.error_rate, mslice1)
        hll2 = HyperLogLog(self.error_rate, mslice2)
        hll3 = HyperLogLog(self.error_rate, mslice3)

        for v in self.test_data1:
            hll1.add(v)
        for v in self.test_data2:
            hll2.add(v)

        hll1.update(hll2)

        self.assertAlmostEqual(self.test_set_size*2, len(hll1), delta=self.test_set_size*2*self.error_rate)

    def test_update3(self):
        f = tempfile.TemporaryFile()
        m = 16384
        flen = (m*3) + mmap.PAGESIZE - (m*3) % mmap.PAGESIZE

        self.assertGreater(flen, m*3)

        f.write(''.join(['\x00' for i in range(flen)]))
        fmap = mmap.mmap(f.fileno(), m*3)

        self.assertEqual(len(fmap), m*3)

        mslice1 = MmapSlice(fmap, m, 0)
        mslice2 = MmapSlice(fmap, m, m)
        mslice3 = MmapSlice(fmap, m, m*2)

        hll1 = HyperLogLog(self.error_rate, mslice1)
        hll2 = HyperLogLog(self.error_rate, mslice2)
        hll3 = HyperLogLog(self.error_rate, mslice3)

        for v in self.test_data1:
            hll1.add(v)
        for v in self.test_data2:
            hll2.add(v)
        for v in self.test_data3:
            hll3.add(v)

        hll1.update([hll2, hll3])

        self.assertAlmostEqual(self.test_set_size*3, len(hll1), delta=self.test_set_size*3*self.error_rate)

unittest.main()

