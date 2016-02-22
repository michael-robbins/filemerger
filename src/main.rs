/// Filemerge
/// Takes a series of files that are assumed to be sorted based on your merge key
/// Splits each line based on a user supplied delimiter
/// Extracts a specific column to use as the merge key
/// Merges all files together into a single stream based on the merge key

#[macro_use] extern crate log;
extern crate env_logger;
extern crate getopts;
extern crate flate2;
extern crate bzip2;
extern crate glob;

mod merge_file_manager;
mod merge_file;

use merge_file_manager::MergeFileManager;
use std::collections::HashMap;
use getopts::Options;
use std::path::Path;
use std::process;
use std::env;

fn print_usage(program: &str, opts: &Options) {
    let usage = format!("\nUsage: {} [-h] [-v] -- See below for all options", program);
    println!("{}", opts.usage(&usage));
}

fn error_usage_and_bail(message: &str, program: &str, opts: &Options) {
    error!("{}", message);
    print_usage(&program, &opts);
    process::exit(1);
}

fn main() {
    // Set up argument parsing
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();

    // General options
    opts.optflag("h", "help", "Print out this help.");
    opts.optflagmulti("v", "verbose", "Prints out more info (able to be applied up to 3 times)");

    // Merge key options (required for both emitting and caching)
    opts.optopt("", "delimiter", "Delimiter we split the line on", "tsv || csv || psv");
    opts.optopt("", "index", "Column index we will use for the merge key (0 based)", "0 -> len(line) - 1");

    // File selection options
    // * If either the glob or cache-file options are provided, we will perform a merge
    // * If both the glob and cache-file options are provided, we will cache the glob results
    opts.optmulti("", "glob", "File glob that will provide all required files", "/path/to/specific_*_files.*.gz");
    opts.optopt("", "cache-file", "Cache file containing files we could merge and their upper and lower merge keys", "/path/to/file.cache");

    // Merge options (only required if merging)
    opts.optopt("", "key-start", "Lower bound (starting from and including) merge key", "1");
    opts.optopt("", "key-end", "Upper bound (up to but not including) merge key", "10");

    // Parse the user provided parameters matching them to the options specified above
    let matches = match opts.parse(&args[1..]) {
        Ok(matches) => matches,
        Err(failure) => panic!(failure.to_string()),
    };

    // Check if the 'h' flag was present, print the usage if it was, then exit
    if matches.opt_present("h") {
        print_usage(&program, &opts);
        return;
    }

    // Configure logging verbosity and initialise the logger
    match matches.opt_count("v") {
        0 => {env::set_var("RUST_LOG", "warn")},
        1 => {env::set_var("RUST_LOG", "info")},
        2 => {env::set_var("RUST_LOG", "debug")},
        _ => {env::set_var("RUST_LOG", "trace")}, // Provided > 2 -v flags
    }

    env_logger::init().unwrap();

    debug!("Applied log level: {}", env::var("RUST_LOG").unwrap());

    // Verify the --delimiter parameter
    if ! matches.opt_present("delimiter") {
        error_usage_and_bail("We need a --delimiter parameter", &program, &opts);
    }

    let delimiter = match matches.opt_str("delimiter").unwrap().as_ref() {
        "tsv" => {'\t'},
        "csv" => {','},
        "psv" => {'|'},
        _ => {
            error!("Unknown delimiter, valid choices: tsv, csv or psv");
            return;
        }
    };
    debug!("We got a delimiter of: {}", matches.opt_str("delimiter").unwrap());

    // Verify the --index parameter
    if ! matches.opt_present("index") {
        error_usage_and_bail("We need a --index parameter", &program, &opts);
    }

    let index = matches.opt_str("index").unwrap().parse::<usize>().unwrap();
    debug!("We got an --index of {}", index);

    // Verify the --glob parameter(s)
    let mut glob_present = false;
    let mut glob_choices = Vec::new();

    if matches.opt_present("glob") {
        glob_present = true;
        glob_choices = matches.opt_strs("glob");

        for glob_choice in &glob_choices {
            debug!("We got the following glob: {:?}", glob_choice);
        }
    } else {
        debug!("We didn't get any globs!");
    }

    // Verify the --cache-file parameter
    let mut cache_present = false;
    let mut cache_filename = String::new();

    if matches.opt_present("cache-file") {
        cache_present = true;
        cache_filename = matches.opt_str("cache-file").unwrap();
        debug!("We got the following cache-file: {}", cache_filename);
    } else {
        debug!("We didn't get any cache-file!");
    }

    let cache_exists = Path::new(&cache_filename).is_file();

    if cache_exists {
        debug!("Cache file you provided already exists, we will attempt to load it first.");
    } else {
        debug!("Cache file you provided doesn't exist, not loading it first.");
    }

    // Check that at least one required arg is present
    if ! glob_present && ! cache_present {
        error_usage_and_bail("Missing both glob and cache-file, we need at least one of them.", &program, &opts);
    } else if ! glob_present && ! cache_exists {
        error_usage_and_bail("No glob provided and the cache file doesn't exist? Nothing we can do here", &program, &opts);
    }

    // key-start
    let mut key_start = String::new();
    if matches.opt_present("key-start") {
        let result = matches.opt_str("key-start");

        if result.is_some() {
            key_start = result.unwrap();
            debug!("key-start was set to: {}", key_start);
        } else if result.is_none() {
            error_usage_and_bail("Failed to extract they starting key from --key-start", &program, &opts);
        }
    } else {
        info!("--key-start was not supplied, merging from the start of each file");
    }

    // key-end
    let mut key_end = String::new();
    if matches.opt_present("key-end") {
        let result = matches.opt_str("key-end");

        if result.is_some() {
            key_end = result.unwrap();
            debug!("key-end was set to: {}", key_end);
        } else if result.is_none() {
            error_usage_and_bail("Failed to extract they starting key from --key-end", &program, &opts);
        }
    } else {
        info!("--key-end was not supplied, merging until the end of each file");
    }

    // Allocate an empty cache
    let mut mergefile_cache = HashMap::new();

    if cache_present && cache_exists {
        let result = MergeFileManager::retrieve_from_cache(&cache_filename, delimiter, index);

        match result {
            Ok(merge_files) => {
                mergefile_cache.extend(merge_files);
                debug!("Added cachefile {} to the cache", cache_filename)
            },
            Err(error) => {
                error!("Unable to load from cache file: {}", cache_filename);
                error!("Error was: {}", error);
            }
        }
    } else {
        if cache_present && glob_present {
            info!("No cache provided, but we will write out one.")
        } else {
            info!("We didn't get any cache file, loading from globs and merging directly!");
        }
    }

    if glob_present {
        debug!("Getting all files from the glob(s)!");
        for glob_choice in glob_choices {
            let result = MergeFileManager::retrieve_from_glob(&glob_choice, delimiter, index);

            match result {
                Ok(merge_files) => {
                    mergefile_cache.extend(merge_files);
                    debug!("Added glob {} to the cache", glob_choice);
                },
                Err(error) => {
                    error!("Unable to load from glob: {}", glob_choice);
                    error!("Error was: {}", error);
                }
            }
        }

        if cache_present {
            // Write out the merge_cache to disk as the new cache file
            debug!("Creating new cache file");

            match MergeFileManager::write_cache(&cache_filename, mergefile_cache) {
                Ok(result) => {info!("{}", result)},
                Err(result) => {error!("{}", result)},
            }

            // Bail early as glob + cache == don't perform merge
            return;
        }
    }

    // Begin the merge process
    if matches.opt_present("key-end") {
        info!("Beginning merge -> {}", &key_end);
    } else {
        info!("Beginning merge -> EOF");
    }

    MergeFileManager::begin_merge(mergefile_cache, &key_start, &key_end, true);
}
