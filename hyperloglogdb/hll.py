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

----

This module implements probabilistic data structure which is able to
calculate the cardinality of large multisets in a single pass using
little auxiliary memory.

This is a modified version of the implementation by Vasiliy Evseenko
https://github.com/svpcom/hyperloglog

The algorithm is described here:
http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.76.4286
"""

import math
from hashlib import sha1
from bisect import bisect_right
import numpy

class HyperLogLog(object):
    """
    HyperLogLog cardinality counter
    """

    def __init__(self, error_rate, data, bitcount_arr=None):
        """
        Implementes a HyperLogLog

        error_rate = abs_err / cardinality
        """

        if not (0 < error_rate < 1):
            raise ValueError("Error_Rate must be between 0 and 1.")

        # error_rate = 1.04 / sqrt(m)
        # m = 2 ** b
        # M(1)... M(m) = 0

        b = int(math.ceil(math.log((1.04 / error_rate) ** 2, 2)))

        self.alpha = self._get_alpha(b)
        self.b = b
        self.m = 1 << b

        if not isinstance(data, MmapSlice):
            raise ValueError("data must be of type MmapSlice")

        self.M = data
        self.bitcount_arr = bitcount_arr or self._get_bitcount_arr(error_rate, b)

    @staticmethod
    def _get_size(error_rate):
        b = int(math.ceil(math.log((1.04 / error_rate) ** 2, 2)))
        m = 1 << b
        return m

    @staticmethod
    def _get_bitcount_arr(error_rate, b=None):
        if not b:
            b = int(math.ceil(math.log((1.04 / error_rate) ** 2, 2)))
        return [ 1L << i for i in range(160 - b + 1) ]

    @staticmethod
    def _get_alpha(b):
        if not (4 <= b <= 16):
            raise ValueError("b=%d should be in range [4 : 16]" % b)

        if b == 4:
            return 0.673

        if b == 5:
            return 0.697

        if b == 6:
            return 0.709

        return 0.7213 / (1.0 + 1.079 / (1 << b))

    @staticmethod
    def _get_rho(w, arr):
        rho = len(arr) - bisect_right(arr, w)
        if rho == 0:
            raise ValueError('w overflow')
        return rho

    def add(self, value):
        """
        Adds the item to the HyperLogLog
        """
        # h: D -> {0,1} ** 160
        # x = h(v)
        # j = <x_1x_2..x_b>
        # w = <x_{b+1}x_{b+2}..>
        # M[j] = max(M[j], rho(w))

        x = long(sha1(value).hexdigest(), 16)
        j = x & ((1 << self.b) - 1)
        w = x >> self.b

        self.M[j] = max(self.M[j], chr(self._get_rho(w, self.bitcount_arr)))


    def update(self, others):
        """
        Merge other counters
        """

        if not isinstance(others, list):
            others = [others]

        for other in others:
            if self.m != other.m:
                raise ValueError('Counters precisions should be equal')

        arr = numpy.array(map(lambda other: bytearray(other.M.read(other.m)), others) + [bytearray(self.M.read(self.m))])

        M1 = bytearray(numpy.amax(arr,axis=0))

        self.M.write(str(M1))


    # def __eq__(self, other):
    #     if self.m != other.m:
    #         raise ValueError('Counters precisions should be equal')

    #     #TODO: make this work with mmap files
    #     return self.M == other.M


    # def __ne__(self, other):
    #     return not self.__eq__(other)


    def __len__(self):
        return self.length()

    def length(self):
        """
        Returns the estimate of the cardinality
        """
        M1 = self.M.read(self.m)
        E = self.alpha * float(self.m ** 2) / sum([2.0 ** -x for x in bytearray(M1)])

        if E <= 2.5 * self.m:             # Small range correction
            #print 'Small corr'
            V = M1.count('\x00') #count number or registers equal to 0
            return self.m * math.log(self.m / float(V)) if V > 0 else E
        elif E <= float(1L << 160) / 30.0:  #intermidiate range correction -> No correction
            #print 'No corr'
            return E
        else:
            # print 'Large corr'
            return -(1L << 160) * math.log(1.0 - E / (1L << 160))

class MmapSlice(object):
    data = None
    length = None
    offset = None

    def __init__(self, mmap_file, length, offset=0):
        self.data = mmap_file
        self.length = length
        self.offset = offset

    # def __iter__(self):
    #   for x in self.data[self.offset:self.offset+self.length]:
    #       yield x

    def __getitem__(self, index):
        # if index >= self.length or index < 0:
        #   raise ValueError("Index %s out of bounds" % index)
        return self.data[self.offset+index]

    def __setitem__(self, index, val):
        # if index >= self.length or index < 0:
        #   raise ValueError("Index %s out of bounds" % index)
        self.data[self.offset+index]=val

    def __len__(self):
        return self.length

    def __eq__(self, other):
        if self.length != len(other):
            return False
        for i in range(self.length):
            if self[i] != other[i]:
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def count(self, val):
        self.data.seek(self.offset)
        return self.data.read(self.length).count(val)

    def write(self, data):
        self.data.seek(self.offset)
        return self.data.write(data)

    def read(self, length):
        self.data.seek(self.offset)
        return self.data.read(length)

    def seek(self, index):
        return

