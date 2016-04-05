# File Merger

File Merger will perform a [k-way merge](https://en.wikipedia.org/wiki/Merge_algorithm#K-way_merging) of all input files. This is a partial implementation of an [external merge sort](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort), only performing the second half of the function. It assumes all external files it will merge are themselves already sorted based on a merge key.

It is written in the Rust programming language as an initial foray into the language.

## Features
* Ability to generate, store and later utilize a cache of files to perform the sort on (this is useful for batch processing)
* Able to merge on any single column
* Supports any delimiter you throw at it (single character)
* Low memory overhead as we only store the 'current' line of each merge file in memory
* Supports different specializations of the merge key, allowing faster merges

## Bucket list of features
* Ability to merge on multiple columns (non-trivial)
* Support more decompression types

## Installation
### From source (assuming you have Rust & Cargo installed)
1. Clone the repository: ```git clone https://github.com/michael-robbins/filemerger.git```

2. ```cd filemerger; cargo build --release```. The binary will now be in ```./target/release/filemerger```

3. Done! Test it out by generating a cache file or performing a direct merge!

## Usage
    Usage: ./file-merger [-h] [-v] -- See below for all options

    Options:
        -h, --help          Print out this help.
        -v, --verbose       Prints out more info (able to be applied up to 3 times)
        --config-file /path/to/config.yaml
                        Configuration file in YAML that contains most other settings
        --delimiter '	' || ',' || '|'
                        Raw character we split the line on
        --index 0 -> len(line) - 1
                        Column index we will use for the merge key (0 based)
        --glob /path/to/specific_*_files.*.gz
                        File glob that will provide all required files
        --cache-file /path/to/file.cache
                        Cache file containing files we could merge and their upper and lower merge keys
        --key-start 1   Lower bound (starting from and including) merge key
        --key-end 10    Upper bound (up to but not including) merge key
        --key-type 'Unsigned32Integer' || 'Signed32Integer' || 'String'
                        The data type of the key used for optimization
