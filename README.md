# File Merger

File Merger will perform a single k-way merge of all input files. This is a partial implementation of an [external merge sort](https://en.wikipedia.org/wiki/External_sorting#External_merge_sort), only performing the second half of the function. It assumes all external files it will merge are themselves already sorted based on a merge key.

It is written in the Rust programming language as an initial foray into the language.

##Features
* Ability to generate, store and later utilize a cache of files to perform the sort on (this is useful for batch processing)
* Able to merge on any column within a file for any type that implements the PartialOrd trait
* Supports TSV/CSV/PSV files as currently we hard code the supported delimiters
* Low memory overhead as we only store the 'current' line of each merge file in memory
