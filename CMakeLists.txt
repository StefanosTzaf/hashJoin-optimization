cmake_minimum_required(VERSION 3.16)

project(SigmodContest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenMP REQUIRED)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")

Include(FetchContent)

FetchContent_Declare(
    Catch2
    URL https://github.com/catchorg/Catch2/archive/refs/tags/v3.8.0.tar.gz
)

FetchContent_MakeAvailable(Catch2)

FetchContent_Declare(
    abseil
    URL https://github.com/abseil/abseil-cpp/releases/download/20240722.1/abseil-cpp-20240722.1.tar.gz
)

set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
FetchContent_MakeAvailable(abseil)

FetchContent_Declare(
    re2
    URL https://github.com/google/re2/releases/download/2024-07-02/re2-2024-07-02.tar.gz
)

FetchContent_MakeAvailable(re2)

FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)

FetchContent_MakeAvailable(json)

FetchContent_Declare(
    sql-parser
    URL https://github.com/a858438680/sql-parser/archive/refs/tags/win-port-2.tar.gz
)
set(HSQL_ENABLE_WERROR OFF)
FetchContent_MakeAvailable(sql-parser)

FetchContent_Declare(
    range-v3
    URL https://github.com/ericniebler/range-v3/archive/refs/tags/0.12.0.tar.gz
)

FetchContent_MakeAvailable(range-v3)

FetchContent_Declare(
    fmtlib
    URL https://github.com/fmtlib/fmt/releases/download/11.1.3/fmt-11.1.3.zip
)

FetchContent_MakeAvailable(fmtlib)

FetchContent_Declare(
    duckdb
    URL https://github.com/duckdb/duckdb/archive/refs/tags/v1.2.1.tar.gz
)
set(ENABLE_SANITIZER OFF)
set(ENABLE_UBSAN OFF)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc|powerpc|ppc64|ppc64le")
    message("Disabling jemalloc extension of DuckDB on Power.")
    set(SKIP_EXTENSIONS jemalloc)
endif()
FetchContent_MakeAvailable(duckdb)

# Explicitly list sources to avoid pulling both engine variants into the same target.
set(COMMON_SOURCES
    src/build_table.cpp
    src/csv_parser.cpp
    src/statement.cpp
    src/LateMaterialization.cpp
    src/ColumnStore.cpp
    src/unchained.cpp
)

set(BASE_ENGINE src/execute.cpp)

# Enable server-specific compiler optimizations.
# Use march=native for all but Power servers, which results in the following error:
#    clang++-18: error: unsupported option '-march=' for target 'powerpc64le-unknown-linux-gnu'
# This flag works on other Power systems, but for now, we disable march=native on all Power machines.
if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "ppc|powerpc|ppc64|ppc64le")
    add_compile_options(-march=native)
endif()

# Targets
# Default run uses the base engine implementation
add_executable(run ${COMMON_SOURCES} ${BASE_ENGINE} tests/read_sql.cpp)

# Unit tests - main test suite
add_executable(unit_tests ${COMMON_SOURCES} ${BASE_ENGINE} tests/unit_tests.cpp)
add_executable(unit_tests_columnStore ${COMMON_SOURCES} ${BASE_ENGINE} tests/unit_tests_columnStore.cpp)
add_executable(unit_tests_unchained ${COMMON_SOURCES} ${BASE_ENGINE} tests/unit_tests_Unchained.cpp)


add_executable(build_database tests/build_database.cpp)

# For all executables
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    target_compile_definitions(run PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(build_database PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(unit_tests PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(unit_tests_columnStore PRIVATE _CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(unit_tests_unchained PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Windows")
    # Exclude fast targets from default 'all' build; build via explicit target 'fast'
    add_executable(build_cache EXCLUDE_FROM_ALL ${COMMON_SOURCES} ${BASE_ENGINE} tests/read_sql.cpp)

    add_executable(leaderboard EXCLUDE_FROM_ALL ${COMMON_SOURCES} ${BASE_ENGINE} tests/read_sql.cpp)


    # Ensure 'cmake --build ... fast' builds all fast executables including all unit tests
    add_dependencies(leaderboard unit_tests unit_tests_columnStore unit_tests_unchained)

    target_link_libraries(build_cache PRIVATE re2 fmt range-v3 nlohmann_json::nlohmann_json sqlparser duckdb)
    target_compile_definitions(build_cache PRIVATE TEAMOPT_USE_DUCKDB TEAMOPT_BUILD_CACHE)
    target_link_libraries(leaderboard PRIVATE re2 fmt range-v3 nlohmann_json::nlohmann_json sqlparser)
    target_include_directories(build_cache PRIVATE include)
    target_include_directories(leaderboard PRIVATE include)
endif()

target_link_libraries(run PRIVATE re2 fmt range-v3 nlohmann_json::nlohmann_json sqlparser duckdb)
target_compile_definitions(run PRIVATE TEAMOPT_USE_DUCKDB)
target_link_libraries(build_database PRIVATE fmt duckdb)
target_link_libraries(unit_tests PRIVATE re2 range-v3 fmt Catch2::Catch2WithMain)
target_link_libraries(unit_tests_columnStore PRIVATE re2 range-v3 fmt Catch2::Catch2WithMain)
target_link_libraries(unit_tests_unchained PRIVATE re2 range-v3 fmt Catch2::Catch2WithMain)

target_include_directories(run PRIVATE include)
target_include_directories(build_database PRIVATE include)
target_include_directories(unit_tests PRIVATE include)
target_include_directories(unit_tests_columnStore PRIVATE include)
target_include_directories(unit_tests_unchained PRIVATE include)