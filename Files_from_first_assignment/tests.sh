#!/bin/bash

# A simple script to run tests

if [ $# -eq 0 ]; then
    echo "No tests specified. Running all tests for Robinhood, Hopscotch, and Cuckoo."
    if [[ ! -f ./build/unit_tests_base_robin ]]; then
        echo "Missing test: unit_tests_base_robin -- Run Cmake to build it."
        exit 1
    fi
    if [[ ! -f ./build/unit_tests_robin ]]; then
        echo "Missing test: unit_tests_robin -- Run Cmake to build it."
        exit 1
    fi
    if [[ ! -f ./build/unit_tests_base_hopscotch ]]; then
        echo "Missing test: unit_tests_base_hopscotch -- Run Cmake to build it."
        exit 1
    fi
    if [[ ! -f ./build/unit_tests_hopscotch ]]; then
        echo "Missing test: unit_tests_hopscotch -- Run Cmake to build it."
        exit 1
    fi
    if [[ ! -f ./build/unit_tests_base_cuckoo ]]; then
        echo "Missing test: unit_tests_base_cuckoo -- Run Cmake to build it."
        exit 1
    fi
    if [[ ! -f ./build/unit_tests_cuckoo ]]; then
        echo "Missing test: unit_tests_cuckoo -- Run Cmake to build it."
        exit 1
    fi
    echo -e "\n\tRunning existing Tests with Robinhood implementation \n"
    ./build/unit_tests_base_robin
    echo -e "\n\tRunning our Robinhood-specific Tests \n"
    ./build/unit_tests_robin
    echo -e "\n\tRunning existing Tests with Hopscotch implementation \n"
    ./build/unit_tests_base_hopscotch
    echo -e "\n\tRunning our Hopscotch-specific Tests \n"
    ./build/unit_tests_hopscotch
    echo -e "\n\tRunning existing Tests with Cuckoo implementation \n"
    ./build/unit_tests_base_cuckoo
    echo -e "\n\tRunning our Cuckoo-specific Tests \n"
    ./build/unit_tests_cuckoo
elif [ -n "$1" ]; then
    # Normalize to lowercase so input is case-insensitive (e.g. rObin, ROBIN, Robinhood)
    arg="$(printf "%s" "$1" | tr '[:upper:]' '[:lower:]')"
    case $arg in
        robinhood)
            if [[ ! -f ./build/unit_tests_base_robin ]]; then
                echo "Missing test: unit_tests_base_robin -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_robin ]]; then
                echo "Missing test: unit_tests_robin -- Run Cmake to build it."
                exit 1
            fi

            echo -e "\n\tRunning existing Tests with Robinhood implementation \n"
            ./build/unit_tests_base_robin
            echo -e "\n\tRunning our Robinhood-specific Tests \n"
            ./build/unit_tests_robin
            echo
        
            ;;
        hopscotch)
            if [[ ! -f ./build/unit_tests_base_hopscotch ]]; then
                echo "Missing test: unit_tests_base_hopscotch -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_hopscotch ]]; then
                echo "Missing test: unit_tests_hopscotch -- Run Cmake to build it."
                exit 1
            fi

            echo -e "\n\tRunning existing Tests with Hopscotch implementation \n"
            ./build/unit_tests_base_hopscotch
            echo -e "\n\tRunning our Hopscotch-specific Tests \n"
            ./build/unit_tests_hopscotch
            echo

            ;;
        cuckoo)
            if [[ ! -f ./build/unit_tests_base_cuckoo ]]; then
                echo "Missing test: unit_tests_base_cuckoo -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_cuckoo ]]; then
                echo "Missing test: unit_tests_cuckoo -- Run Cmake to build it."
                exit 1
            fi
            echo -e "\n\tRunning existing Tests with Cuckoo implementation \n"
            ./build/unit_tests_base_cuckoo
            echo -e "\n\tRunning our Cuckoo-specific Tests \n"
            ./build/unit_tests_cuckoo
            echo

            ;;
        clean)
            echo "Cleaning all built test executables..."
            rm -f ./build/unit_tests_base_robin
            rm -f ./build/unit_tests_robin
            rm -f ./build/unit_tests_base_hopscotch
            rm -f ./build/unit_tests_hopscotch
            rm -f ./build/unit_tests_base_cuckoo
            rm -f ./build/unit_tests_cuckoo
            echo "Clean complete."
            ;;
        all)
            if [[ ! -f ./build/unit_tests_base_robin ]]; then
                echo "Missing test: unit_tests_base_robin -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_robin ]]; then
                echo "Missing test: unit_tests_robin -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_base_hopscotch ]]; then
                echo "Missing test: unit_tests_base_hopscotch -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_hopscotch ]]; then
                echo "Missing test: unit_tests_hopscotch -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_base_cuckoo ]]; then
                echo "Missing test: unit_tests_base_cuckoo -- Run Cmake to build it."
                exit 1
            fi
            if [[ ! -f ./build/unit_tests_cuckoo ]]; then
                echo "Missing test: unit_tests_cuckoo -- Run Cmake to build it."
                exit 1
            fi
            echo -e "\n\tRunning existing Tests with Robinhood implementation \n"
            ./build/unit_tests_base_robin
            echo -e "\n\tRunning our Robinhood-specific Tests \n"
            ./build/unit_tests_robin
            echo -e "\n\tRunning existing Tests with Hopscotch implementation \n"
            ./build/unit_tests_base_hopscotch
            echo -e "\n\tRunning our Hopscotch-specific Tests \n"
            ./build/unit_tests_hopscotch
            echo -e "\n\tRunning existing Tests with Cuckoo implementation \n"
            ./build/unit_tests_base_cuckoo
            echo -e "\n\tRunning our Cuckoo-specific Tests \n"
            ./build/unit_tests_cuckoo

            ;;
        *)
            echo "Usage : $0 <Robinhood | Hopscotch | Cuckoo | All>"
            ;;
    esac
else
    echo "Usage : $0 <Robinhood | Hopscotch | Cuckoo | All>"
fi

exit 0