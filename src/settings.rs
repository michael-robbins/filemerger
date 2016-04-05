extern crate env_logger;

use std::path::PathBuf;
use getopts::Options;
use std::process;
use std::env;

#[derive(Clone, Debug)]
pub enum KeyType {
    Unsigned32Integer,
    Signed32Integer,
    String,
}

pub struct MergeSettings {
    pub delimiter: char,
    pub key_index: usize,
    pub key_start: Option<String>,
    pub key_end: Option<String>,
    pub key_type: KeyType,
    pub cache_path: Option<PathBuf>,
    pub glob_choices: Vec<String>,
}

fn print_usage(program: &str, opts: &Options) {
    let usage = format!("\nUsage: {} [-h] [-v] -- See below for all options", program);
    println!("{}", opts.usage(&usage));
    process::exit(1);
}

fn error_usage_and_bail(message: &str, program: &str, opts: &Options) {
    error!("{}", message);
    print_usage(&program, &opts);
    process::exit(1);
}

pub fn load(args: Vec<String>) -> Option<MergeSettings> {
    let program = args[0].clone();
    let mut opts = Options::new();

    // General options
    opts.optflag("h", "help", "Print out this help.");
    opts.optflagmulti("v", "verbose", "Prints out more info (able to be applied up to 3 times)");
    opts.optopt("", "config-file", "Configuration file in YAML that contains most other settings", "/path/to/config.yaml");

    // File selection options
    // * If either the glob or cache-file options are provided, we will perform a merge
    // * If both the glob and cache-file options are provided, we will cache the glob results
    opts.optmulti("", "glob", "File glob that will provide all required files", "/path/to/specific_*_files.*.gz");
    opts.optopt("", "cache-file", "Cache file containing files we could merge and their upper and lower merge keys", "/path/to/file.cache");
    opts.optopt("", "delimiter", "Raw character we split the line on", "'\t' || ',' || '|'");

    // Merge options (only required if merging)
    opts.optopt("", "key-index", "Column index we will use for the merge key (0 based)", "0 -> len(line) - 1");
    opts.optopt("", "key-start", "Lower bound (starting from and including) merge key", "1");
    opts.optopt("", "key-end", "Upper bound (up to but not including) merge key", "10");
    opts.optopt("", "key-type", "The data type of the key used for optimization", "'Unsigned32Integer' || 'Signed32Integer' || 'String'");

    // Parse the user provided parameters matching them to the options specified above
    let matches = match opts.parse(args) {
        Ok(matches) => matches,
        Err(failure) => panic!(failure.to_string()),
    };

    // Check if the 'h' flag was present, print the usage if it was, then exit
    if matches.opt_present("h") {
        print_usage(&program, &opts);
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

    let delimiter_string = matches.opt_str("delimiter").unwrap();
    let delimiter_char = delimiter_string.chars().next().unwrap();

    if delimiter_string.len() > 1 {
        error_usage_and_bail("Delimiter can only be a single character", &program, &opts);
    }

    debug!("We got a delimiter of: {}", delimiter_char);

    // Verify the --index parameter
    if ! matches.opt_present("key_index") {
        error_usage_and_bail("We need a --key-index parameter", &program, &opts);
    }

    let key_index = matches.opt_str("key_index").unwrap().parse::<usize>().unwrap();
    debug!("We got an --key-index of {}", key_index);

    // Verify the --glob parameter(s)
    let mut glob_present = false;
    let mut glob_choices: Vec<String> = Vec::new();

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
    let mut cache_path = None;

    if matches.opt_present("cache-file") {
        let result = matches.opt_str("cache-filename").unwrap();
        debug!("We got the following cache-file: {}", result);
        cache_path = Some(PathBuf::from(result));
    } else {
        debug!("We didn't get any cache-file!");
    }

    // Check that at least one required arg is present
    if ! glob_present && cache_path.is_none() {
        error_usage_and_bail("Missing both glob and cache-file, we need at least one of them.", &program, &opts);
    } else if ! glob_present && ! cache_path.is_some() && cache_path.clone().unwrap().is_file() {
        error_usage_and_bail("No glob provided and the cache file doesn't exist? Nothing we can do here", &program, &opts);
    }

    // Verify the --key-start parameter
    let mut key_start = None;
    if matches.opt_present("key-start") {
        let result = matches.opt_str("key-start");

        match result {
            Some(result) => {
                debug!("key-start was set to: {}", result);
                key_start = Some(result);
            },
            None => {
                error_usage_and_bail("Failed to extract what you provided from --key-start", &program, &opts);
            }
        }
    } else {
        info!("--key-start was not supplied, merging from the start of each file");
    }

    // Verify the --key-end parameter
    let mut key_end = None;
    if matches.opt_present("key-end") {
        let result = matches.opt_str("key-end");

        match result {
            Some(result) => {
                debug!("key-end was set to: {}", result);
                key_end = Some(result);
            },
            None => {
                error_usage_and_bail("Failed to extract what you provided from --key-end", &program, &opts);
            }
        }
    } else {
        info!("--key-end was not supplied, merging until the end of each file");
    }

    // Verify the --key-type parameter
    // We default to KeyType::String as it's the slowest/most supported
    let mut key_type = KeyType::String;
    if matches.opt_present("key-type") {
        let result = matches.opt_str("key-type");

        if result.is_some() {
            let result = result.unwrap();

            match result.trim() {
                "Unsigned32Integer" => key_type = KeyType::Unsigned32Integer,
                "Signed32Integer" => key_type = KeyType::Signed32Integer,
                "String"  => key_type = KeyType::String,
                _         => warn!("--key-type unsupported, assuming a String type (slowest)"),
            }
        } else {
            error!("Unable to parse the --key-type parameter? Defaulting to String type (slowest)");
        }
    } else {
        info!("--key-type was not supplied, defaulting to String type (slowest)");
    }

    Some(MergeSettings {
        cache_path: cache_path,
        glob_choices: glob_choices,
        delimiter: delimiter_char,
        key_index: key_index,
        key_start: key_start,
        key_end: key_end,
        key_type: key_type,
    })
}
