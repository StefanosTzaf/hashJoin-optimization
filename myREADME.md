compiles with 
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev 
cmake --build build -- -j $(nproc) or cmake --build build -- -j $(nproc) fast
to build the new executables.               

New executables have been created in the build directory: run, run_robin, run_cuckoo, and run_hopscotch.
(if made with cache support, they will be named fast, fast_robin, fast_cuckoo, and fast_hopscotch).

You can run the different engines with the following commands:
./run_engine.sh base plans.json
./run_engine.sh cuckoo plans.json
./run_engine.sh robin plans.json

