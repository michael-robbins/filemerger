# File Merger

File Merger will perform a single k-way merge of all input files. This is a partial implementation of an [external merge sort](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort), only performing the second half of the function. It assumes all external files it will merge are themselves already sorted based on a merge key.

It is written in the Rust programming language as an initial foray into the language.

## Features
* Ability to generate, store and later utilize a cache of files to perform the sort on (this is useful for batch processing)
* Able to merge on any single column
* Supports TSV/CSV/PSV files as currently we hard code the supported delimiters
* Low memory overhead as we only store the 'current' line of each merge file in memory

## Bucket list of features
* Ability to merge on multiple columns
* Store the delimiter/merge key in the cache
* Offer different specialisations for the merge key data type (currently the column is cast into a String for comparison)
* Support more decompression types
* Support more delimiters (or remove the hardcoding of them altogether, letting the user specify the character to split on themselves)
* Optimise for merging from the first column (no need to split the line, just slice up to the first occurence of the delimiter)

## Installation
### From source (assuming you have Rust & Cargo installed)
1. Clone the repository: ```git clone https://github.com/michael-robbins/filemerger.git```

2. ```cd filemerger; cargo build --release```. The binary will now be in ```./target/release/filemerger```

3. Done! Test it out by generating a cache file or performing a direct merge!

## Usage
    Usage: ./target/debug/file-merger [-h] [-v] -- See below for all options

    Options:
        -h, --help          Print out this help.
        -v, --verbose       Prints out more info (able to be applied up to 3
                            times)
            --delimiter tsv || csv || psv
                            Delimiter we split the line on
            --index 0 -> len(line) - 1
                            Column index we will use for the merge key (0 based)
            --glob /path/to/specific_*_files.*.gz
                            File glob that will provide all required files
            --cache-file /path/to/file.cache
                            Cache file containing files we could merge and their
                            upper and lower merge keys
            --key-start 1   Lower bound (starting from and including) merge key
            --key-end 10    Upper bound (up to but not including) merge key
