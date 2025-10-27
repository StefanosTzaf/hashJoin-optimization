compiles with 
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev 
as before, and then with
cmake --build build -- -j $(nproc) run run_cuckoo run_robin
to build the new executables.               

new executables are running with script run_engine.sh:

./run_engine.sh base plans.json
./run_engine.sh cuckoo plans.json
./run_engine.sh robin plans.json
