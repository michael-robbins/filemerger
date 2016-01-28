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
use getopts::Options;
use std::path::Path;
use std::env;

fn print_usage(program: &str, opts: Options) {
    let usage = format!("\nUsage: {} [-h] [-v] -- See below for all options", program);
    println!("{}", opts.usage(&usage));
}

fn main() {
    // Get all the arguments given by the user
    let args: Vec<String> = env::args().collect();

    // Take a copy of the first parameter (usually relative/path/to/name of binary)
    let program = args[0].clone();

    // Create a new Options class and assign the parameters
    let mut opts = Options::new();

    // General options
    opts.optflag("h", "help", "Print out this help.");
    opts.optflagmulti("v", "verbose", "Prints out more info (able to be applied up to 3 times)");

    // Merge key options (required for both emitting and caching)
    opts.optopt("", "delimiter", "Delimiter we split the line on", "tsv || csv || psv");
    opts.optopt("", "index", "Column index we will use for the merge key (0 based)", "0 -> len(line) - 1");

    // File selection options
    // * If just glob(s) are provided, we will emit directly from all files (slower!)
    // * If just the cache-file is provided we will emit from the stored cache (quicker!)
    // * If both options are provided we will cache what the glob(s) provide into the cache-file
    opts.optmulti("", "glob", "File glob that will provide all required files", "/path/to/specific_*_files.*.gz");
    opts.optopt("", "cache-file", "Cache file containing files we could merge and their upper and lower merge keys", "/path/to/file.cache");

    // Emit options (only required if emitting logs)
    opts.optopt("", "key-start", "Lower bound (starting from and including) merge key", "1");
    opts.optopt("", "key-end", "Upper bound (up to but not including) merge key", "10");

    // Parse the user provided parameters matching them to the options specified above
    let matches = match opts.parse(&args[1..]) {
        Ok(matches) => { matches }
        Err(failure) => {
            panic!(failure.to_string())
        }
    };

    // TODO: Turn all the opts stuff to write into an 'Options' struct, then just read out of that

    // Check if the 'h' flag was present, print the usage if it was, then exit
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    // If the user wants verbose, THEN GIVE THEM MORE
    match matches.opt_count("v") {
        0 => {env::set_var("RUST_LOG", "warn")},
        1 => {env::set_var("RUST_LOG", "info")},
        2 => {env::set_var("RUST_LOG", "debug")},
        _ => {env::set_var("RUST_LOG", "trace")}, // Provided >2 -v flags
    }

    env_logger::init().unwrap();

    debug!("Applied log level: {}", env::var("RUST_LOG").unwrap());

    // Verify the --delimiter parameter
    if ! matches.opt_present("delimiter") {
        print_usage(&program, opts);
        return;
    }

    let delimiter = match matches.opt_str("delimiter").unwrap().as_ref() {
        "tsv" => {'\t'},
        "csv" => {','},
        "psv" => {'|'},
        _ => {panic!("Unknown delimiter, valid choices: tsv, csv or psv")}
    };

    debug!("We got a delimiter of: {}", matches.opt_str("delimiter").unwrap());

    // Verify the --index parameter
    if ! matches.opt_present("index") {
        print_usage(&program, opts);
        return;
    }

    let index = matches.opt_str("index").unwrap().parse::<usize>().unwrap();

    debug!("We got an --index of {}", index);

    // Verify the --glob parameter(s)
    let mut glob_present = false;
    let mut glob_choices = Vec::new();

    if matches.opt_present("glob") {
        glob_present = true;
        glob_choices = matches.opt_strs("glob");
    }

    if glob_present {
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
    }

    if cache_present {
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
        error!("Missing both glob and cache-file, we need at least one of them.");
        print_usage(&program, opts);
        return;
    } else if ! glob_present && ! cache_exists {
        error!("No glob provided and the cache file doesn't exist? Nothing we can do here");
        print_usage(&program, opts);
        return;
    }

    // key-start
    let mut key_start = String::new();
    if matches.opt_present("key-start") {
        key_start = matches.opt_str("key-start").unwrap();
    }

    if key_start != String::new() {
        debug!("key-start was set to: {}", key_start);
    }

    // key-end
    let mut key_end = String::new();
    if matches.opt_present("key-end") {
        key_end = matches.opt_str("key-end").unwrap();
    }

    if key_end != String::new() {
        debug!("key-end was set to: {}", key_end);
    }

    // Set up the merge_cache
    let mut merge_manager = MergeFileManager::new();

    if cache_present && cache_exists {
        let result = merge_manager.load_from_cache(&cache_filename, delimiter, index);
        if result.is_err() {
            error!("Unable to load cache ({}) correctly, bailing!", &cache_filename);
            error!("Error message was: {}", result.unwrap());
            return;
        } else {
            info!("{}", result.unwrap())
        }
    } else {
        if cache_present && glob_present {
            info!("No cache provided, but we will write out one.")
        } else {
            info!("We didn't get any cache-file, loading from globs and merging directly!");
        }
    }

    if glob_present {
        debug!("Getting all files from the glob(s)!");
        for glob_choice in glob_choices {
            let result = merge_manager.load_from_glob(&glob_choice, delimiter, index);
            if result.is_err() {
                error!("Unable to load glob ({}) ???", &glob_choice);
                error!("Error message is: {}", result.unwrap());
                return;
            }
            debug!("Added glob {} to the cache with: {}", glob_choice, result.unwrap());
        }

        if cache_present {
            // Write out the merge_cache to disk as the new cache file
            debug!("Creating new cache file");

            match merge_manager.write_cache(&cache_filename) {
                Ok(result) => {info!("{}", result)},
                Err(result) => {error!("{}", result)},
            }

            // Bail early as glob + cache == don't perform merge
            return;
        }
    }

    // We prepare for the merge by fast forwarding all the merge files to the start of the merge key
    merge_manager.fast_forward_cache(&key_start);

    // Begin the merge process
    if matches.opt_present("key-end") {
        info!("Beginning merge -> {}", &key_end);
    } else {
        info!("Beginning merge -> EOF");
    }

    // TODO: Make key_end be passed in as a reference
    merge_manager.begin_merge(key_end);
}
