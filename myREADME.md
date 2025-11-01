Τeam Members:
 Stefanos Tzaferis (sdi2200183)
 Eleftheria Galiatsatou (sdi2200025)

Parts implemented by each member:
    - RobinHood Hash Table implementation: We implemented it together.
    - Hopscotch Hash Table implementation: Eleftheria Galiatsatou
    - Cuckoo Hash Table implementation: Stefanos Tzaferis
    - Final works and automated running : Both
 
 <Runnincg the new executables>
    compiles with :
        cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev 
        cmake --build build -- -j $(nproc) 
        or
        cmake --build build -- -j $(nproc) fast
        to build the new executables.               

    New executables have been created in the build directory: run, run_robin, run_cuckoo, and run_hopscotch.
    (if made with cache support, they will be named fast, fast_robin, fast_cuckoo, and fast_hopscotch).

    You can run the executables from the build directory like so:
    ./build/run_robin plans.json or ./build/fast_robin plans.json etc.


<TESTING>
    Concerning tests, we changed the CMakeLists.txt file so as to create test executables for each hash table implementation (robin hood, hopscotch, cuckoo) both for the base tests (the ones you provided us) and the extended tests that we made fo each implementation.

    There is a script -- tests.sh -- that can be used to run all tests on all hash table implementations.
    (./tests.sh runs all tests, ./tests.sh clean removes all generated test executables and ./tests.sh <testname> runs a specific test.)
    for example, ./tests.sh robinhood runs only the robin hood hash table tests. both base and extended tests are run.


<RobinHood Hash Table implementation>

First of all , we implemented a multimap , where each key can map to multiple values. In the base implementation, that was done by the user of the hash table passing a vector of values for each key.
We tried both ways but finally (since there was not a significant difference in performance) we kept the vector of values inside the hash node itself, because it seemed to us more correct in terms of encapsulation.

Default hash function is MurmurHash3 finalizer but the user can provide their own hash function as a template parameter when constructing the hash table. We did that so as to be able to set an easy hash function for testing purposes in order to know exactly where each key will be mapped in the table.

We ensured that the capacity of the table is always a power of 2, so that we can use bitwise AND instead of modulo for calculating the index from the hash value. This improves performance.


<Hopscotch Hash Table implementation>

Similar to the robinhood implementation, we used a multimap and a vector for the duplicate values. The hop range can be given trough the constructor, but a default value of 32 is given which we kept as best(in case of change the type of the bitmap number should be changed, but we supposed this will not be needed since the hop range is a standard number).
The hash function, can be provided from the user as well, through the constructor, but std::hash is declared as the default one. 

- Hash insert: consists of 5 helper functions so it can be more readable. Initially, we check if the element already exists in the table through *insertDuplicateKey()*. If so, the value is appended to the existing vector. Next we check if it can be inserted in its original hashing position or its neighborhood through *insertWithinHopRange()* . If not, the procedure goes on with checking wether or not the neighborhood is full or not. 
*isHopInfoFull()* returns true if all bits of the bitmap are true, in which case the table needs rehashing. *FindFreeSlot* starts checking linearly the table for a free position or rehashes if there is none. Since that free position cannot be within the hop range, we have to start swapping elements. *insertAndSwap()* implements the main logic of swapping the empty position with an appropriate element and brings the former as close to the neighborhood of the original hashed position as possible. After that, the free position is now within the hop range so 
*insertWithinHopRange()* is called once again to perform the final insertion.

- Hash search: this is the function where the performance of the hopscotch algorithm is based on and for a given key it calculates its hash value and only checks its bitmap for the existence of the key. Specifically, it checks the positions which have a valid bit and compares their keys to the one we are looking for.

We tried optimizing the algorithm to decrease its execution time, but did not manage to. Possible critical parts which could slow down the execution might be the insertAndSwap logic where hashInsert is called recursively after rehashing. 


<Cuckoo Hash Table implementation>