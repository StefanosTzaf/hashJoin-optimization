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
    -- cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev 
    -- cmake --build build -- -j $(nproc) OR cmake --build build -- -j $(nproc) fast
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

Default hash function is MurmurHash3 finalizer but the user can provide their own hash function as a template parameter when constructing the hash table. We did that so as to be able to set an easy nhash function for testing purposes in order to know exactly where each key will be mapped in the table.


We ensured that the capacity of the table is always a power of 2, so that we can use bitwise AND instead of modulo for calculating the index from the hash value. This improves performance.


<Hopscotch Hash Table implementation>



<Cuckoo Hash Table implementation>