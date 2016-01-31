#!/usr/bin/env python3

#
# These are the functional tests for the file merger
# Currently running some dodgy home-made test runner
# Plan is to move this crap over to nosetests or something similar
#

import os
import sys
import time
import filecmp
import subprocess

from shlex import split

VERBOSE = False

# Define context keys
NAME = "name"
BINARY = "binary"
TEST_TYPE = "test_type"
DATA_GLOB = "data_glob"
DATA_DELIMITER = "data_delimiter"
DATA_INDEX = "data_index"
DATA_START = "data_start"
DATA_END = "data_end"
DATA_CACHE = "data_cache"
DATA_OUTPUT = "data_output"
DEFAULT_REQUIRED_CONTEXT = (NAME, BINARY, DATA_DELIMITER, DATA_INDEX)

# Define test types
BASE_CMD = "{binary} -v -v -v --delimiter {data_delimiter} --index {data_index}"

BUILD_CACHE = "--glob {data_glob} --cache-file {data_cache}"
MERGE_FILES_FROM_CACHE = "--cache-file {data_cache} --key-start {data_start} --key-end {data_end}"
MERGE_FILES_FROM_GLOB = "--glob {data_glob} --key-start {data_start} --key-end {data_end}"
TEST_TYPES = (BUILD_CACHE, MERGE_FILES_FROM_CACHE, MERGE_FILES_FROM_GLOB)
REQUIRED_TEST_TYPE_CONTEXT = {
    BUILD_CACHE: (DATA_GLOB, DATA_CACHE),
    MERGE_FILES_FROM_CACHE: (DATA_CACHE, DATA_START, DATA_END, DATA_OUTPUT),
    MERGE_FILES_FROM_GLOB: (DATA_GLOB, DATA_START, DATA_END, DATA_OUTPUT),
}

def run_test(context):
    # Define the required keys in a tests context
    required_context = set(DEFAULT_REQUIRED_CONTEXT + REQUIRED_TEST_TYPE_CONTEXT[context[TEST_TYPE]])
    missing_context = required_context.difference(context)

    # Ensure the test has the required context
    if missing_context:
        print("ERROR: Missing test context: {0}".format(", ".join(missing_context)))
        sys.exit(1)

    cmd_line = " ".join([BASE_CMD, context[TEST_TYPE]]).format(**context)

    if VERBOSE:
        print("-" * 20)
        print("Running '{name}' -> '{cmd_line}'".format(name=context[NAME], cmd_line=cmd_line))

    # Run the test
    process = subprocess.Popen(split(cmd_line), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if VERBOSE:
        print("-> Communicating")

    stdout, stderr = process.communicate()
    stdout, stderr = stdout.decode("utf-8").strip("\n"), stderr.decode("utf-8").strip("\n")

    if VERBOSE:
        print("--- vvv stdout vvv ---")
        print(stdout)
        print("--- ^^^ stdout ^^^ ---")
        
        print("--- vvv stderr vvv ---")
        print(stderr)
        print("--- ^^^ stderr ^^^ ---")

    # Assert and clean up
    if context[TEST_TYPE] == BUILD_CACHE:
        if not os.path.exists(context[DATA_CACHE]):
            if VERBOSE:
                print("ERROR: Missing cache file???")
            assert(False)

        assert(filecmp.cmp(context[DATA_CACHE], "./files/test1.cache"))

    elif context[TEST_TYPE] in [MERGE_FILES_FROM_CACHE, MERGE_FILES_FROM_GLOB]:
        with open(context[DATA_OUTPUT], "rb") as data_output_file:
            data_output = data_output_file.read().decode("utf-8").strip("\n")

            if VERBOSE and data_output != stdout:
                print("Data Mismatch!")
                print("Known output:")
                print(data_output)
                print("Test output:")
                print(stdout)

            #if data_output.decode("utf-8") == stdout.decode("utf-8"):
            assert(data_output == stdout)

# Define our tests
tests = []

tests.append({
    NAME: "Build a cache based of valid data files",
    TEST_TYPE: BUILD_CACHE,
    BINARY: "../target/debug/file-merge",
    DATA_DELIMITER: "tsv",
    DATA_INDEX: "0",
    DATA_GLOB: os.path.realpath(os.path.join(os.getcwd(), "./files/data?.tsv")),
    DATA_CACHE: "./test1.cache.tmp",
})

tests.append({
    NAME: "Merge files with a valid cache",
    TEST_TYPE: MERGE_FILES_FROM_CACHE,
    BINARY: "../target/debug/file-merge",
    DATA_DELIMITER: "tsv",
    DATA_INDEX: "0",
    DATA_CACHE: "./files/test2.cache",
    DATA_OUTPUT: "./files/test2.output",
    DATA_START: "12345",
    DATA_END: "12347",
})

tests.append({
    NAME: "Merge files directly from valid data files",
    TEST_TYPE: MERGE_FILES_FROM_GLOB,
    BINARY: "../target/debug/file-merge",
    DATA_DELIMITER: "tsv",
    DATA_INDEX: "0",
    DATA_GLOB: os.path.realpath(os.path.join(os.getcwd(), "./files/data?.tsv")),
    DATA_OUTPUT: "./files/test3.output",
    DATA_START: "12345",
    DATA_END: "12347",
})

results = []
print("Results: ", end="")

for test in tests:
    # Ensure the cache file doesn't exist, delete it if it does
    if test[TEST_TYPE] == BUILD_CACHE and os.path.exists(test[DATA_CACHE]):
        os.remove(test[DATA_CACHE])

    try:
        run_test(context=test)
        results.append((True, test[NAME]))
        print(".", end="")
    except AssertionError:
        results.append((False, test[NAME]))
        print("F", end="")

    # Clean up after ourselves
    if test[TEST_TYPE] == BUILD_CACHE:
        os.remove(test[DATA_CACHE])
    elif test[TEST_TYPE] == MERGE_FILES_FROM_CACHE:
        pass
    elif test[TEST_TYPE] == MERGE_FILES_FROM_GLOB:
        pass

print("")

for i, (result, test_name) in enumerate(results):
    print("test_id='{0}' result='{1}' test_name='{2}'".format(i+1, result, test_name))
