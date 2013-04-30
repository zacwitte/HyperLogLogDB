# HyperLogLogDB

A disk-backed database of HyperLogLog data structures for estimating
cardinality of many distinct sets. It uses memory mapped files to keep
recently used data in cache and let's the OS layer move data between disk
and memory. It requires the numpy library to do more efficient merges of sets.

This library contains a modified version of the implementation by Vasiliy Evseenko
<https://github.com/svpcom/hyperloglog>

The original description of the HyperLogLog data structure can be found here:
<http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.76.4286>

## Install


Either:

    sudo pip install hyperloglogdb

Or:

    sudo python setup.py install

## Dependencies

 * [numpy](http://www.numpy.org/) is used for efficient merging of sets

## Examples

```pycon
>>> import hyperloglogdb
>>>
>>> my_hlldb = hyperloglogdb.HyperLogLogDB(file_path='my_hlldb.db', error_rate=0.01)
>>>
>>> # or:
>>> f = open('my_hlldb.db', 'r+b')
>>> my_hlldb = hyperloglogdb.HyperLogLogDB(fileobj=f, error_rate=0.01)
>>>
>>> # Add 'test_val' to the set stored at key 'test_key'
>>> my_hlldb.add('test_key', 'test_val')
>>> print my_hlldb.count('test_key')
1
>>> print len(my_hlldb.get_hll('test_key'))
1
>>> my_hlldb.flush()
>>> # you can now copy or compress my_hlldb.db and open it at a later time
>>>
>>> my_hlldb2 = hyperloglogdb.HyperLogLogDB(file_path='my_hlldb2.db', error_rate=0.01)
>>> my_hlldb2.add('test_key', 'test_val2')
>>> my_hlldb.merge(my_hlldb2)
>>> print my_hlldb.count('test_key')
2
```

## Documentation

### _class_ `hyperloglogdb.MmapSlice`( _mmap_file_, _length_, _offset=0_ )

A module abstracting a slice of a larger memory mapped file (python `mmap`)

 * **mmap_file** - the `mmap` file object
 * **length** - length in bytes of this slice
 * **offset** - offset in bytes from the start of the _mmap_file_

> #### count( _val_ )
> Returns the number of occurances of val in the slice
>
> * **val** - a byte to search for

### _class_ `hyperloglogdb.HyperLogLog`( _error_rate_, _data_, _bitcount_arr=None_ )

A single instance of a HyperLogLog data structure

 * **error_rate** - ( _float_ ) the approx. percentage error rate. This determines the size of the data.
 * **data** - ( _MmapSlice_ ) the data slice where this hyper log log should be stored
 * **bitcount_arr** - ( _list_ ) optionally include a pre-generated bitcount array so it doesn't need to be regenerated for each HLL in the DB.

> #### add( _val_ )
> Adds a single value to the set
>
> * **val** - ( _string_ ) to add to the set


> #### update( _others_ )
> Merges either a single `HyperLogLog` or a list of `HyperLogLog`s into the current data structure
>
> * **others** - ( _HyperLogLog_ or _list of HyperLogLogs_ ) to merge into the set

> #### length()
>
> Returns the estimated cardinality of the set


### _class_ `hyperloglogdb.HyperLogLogDB`( _file_path=None_, _fileobj=None_, _error_rate=0.01_ )

A disk-backed key-value stores of `HyperLogLog` data structures

 * **file_path** - ( _string_ ) a relative path to the location of the file storing the data. If the file does not exist it will be created. Either _file_path_ or _fileobj_ must be provided.
 * **fileobj** - ( _file_ object ) a file object containing the file for storing data. Either _file_path_ or _fileobj_ must be provided.
 * **error_rate** - ( _float_ ) the approx. percentage error rate. This determines the size of each `HyperLogLog`.

> #### flush()
> Syncs any in-memory updates to disk

> #### create( _key_ )
> Creates an empty `HyperLogLog` data structure and returns it.
>
> * **key** - ( _string_ ) the key that the HyperLogLog is associated with

> #### get_hll( _key_ )
> Returns the `HyperLogLog` associated with _key_ or `None` if the key does not exist
>
> * **key** - ( _string_ ) the key that the HyperLogLog is associated with

> #### merge( _others_ )
> Merges either a single `HyperLogLogDB` or a list of `HyperLogLogDB`s into the current database. If a key in _others_ does not exist in the current structure it will be created.
>
> * **others** - ( _HyperLogLogDB_ or list of _HyperLogLogDBs_ ) to merge into the database

> #### update( _key_, _others_ )
> Merges either a single `HyperLogLog` or a list of `HyperLogLog`s into the HLL associated with _key_. If _key_  does not exist in the current structure it will be created.
>
> * **key** - ( _string_ ) the key that the HyperLogLog is associated with
> * **others** - ( _HyperLogLog_ or list of _HyperLogLogs_ ) to merge into the HLL associated with _key_

> #### copy_hll( _from_hll_, _to_hll_ )
> Copies the data from _from_hll_ over the data in _to_hll_
>
> * **from_hll** - ( _HyperLogLog_ ) the HyperLogLog instance to copy data from
> * **to_hll** - ( _HyperLogLog_ ) the HyperLogLog instance to copy data to

> #### add( _key_, _val_ )
> Add a value to the `HyperLogLog` associated with _key_ and create _key_ if it does not exist.
>
> * **key** - ( _string_ ) the key that the HyperLogLog is associated with
> * **val** - ( _string_ ) the value to add to the set

> #### count( _key_ )
> Returns the estimated cardinality of the `HyperLogLog` associated with _key_ or 0 if _key_ does not exist
>
> * **key** - ( _string_ ) the key that the HyperLogLog is associated with
