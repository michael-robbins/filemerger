extern crate env_logger;

use std::path::PathBuf;
use getopts::{Options, Matches};
use std::process;
use std::env;

#[derive(Clone, Debug)]
pub enum KeyType {
    Unsigned32Integer,
    Signed32Integer,
    String,
}

#[derive(Clone, Debug)]
pub struct MergeSettings {
    pub delimiter: char,
    pub key_index: usize,
    pub key_start: Option<String>,
    pub key_end: Option<String>,
    pub key_type: KeyType,
    pub cache_path: Option<PathBuf>,
    pub glob_choices: Option<Vec<String>>,
}

pub struct MergeSettingsParser {
    program: String,
    opts: Options,
    matches: Matches,
}

impl MergeSettingsParser {
    pub fn new(args: Vec<String>) -> MergeSettingsParser {
        let opts = MergeSettingsParser::build();

        let matches = match opts.parse(&args) {
            Ok(matches) => matches,
            Err(failure) => panic!(failure.to_string()),
        };

        MergeSettingsParser {
            program: args[0].clone(),
            opts: opts,
            matches: matches,
        }
    }

    pub fn parse(&self) -> Result<MergeSettings, String> {
        // Check if the 'h' flag was present, print the usage if it was, then exit
        if self.matches.opt_present("h") {
            self.print_usage();
        }

        self.init_logging();

        let delimiter_char = try!(self.parse_delimiter());
        let key_index = try!(self.parse_key_index());
        let glob_choices = try!(self.parse_glob());
        let cache_path = try!(self.parse_cache_file());

        // Check that at least one required arg is present
        if glob_choices.is_none() && cache_path.is_none() {
            self.error_usage_and_bail("Missing both glob and cache-file, we need at least one of them.");
        } else if glob_choices.is_none() && cache_path.is_none() && cache_path.clone().unwrap().is_file() {
            self.error_usage_and_bail("No glob provided and the cache file doesn't exist? Nothing we can do here.");
        }

        let key_start = try!(self.parse_key_generic("key-start"));
        let key_end = try!(self.parse_key_generic("key-end"));

        let key_type = try!(self.parse_key_type());

        Ok(MergeSettings {
            cache_path: cache_path,
            glob_choices: glob_choices,
            delimiter: delimiter_char,
            key_index: key_index,
            key_start: key_start,
            key_end: key_end,
            key_type: key_type,
        })
    }

    fn build() -> Options {
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

        opts
    }

    fn print_usage(&self) {
        let usage = format!("\nUsage: {} [-h] [-v] -- See below for all options", self.program);
        println!("{}", self.opts.usage(&usage));
        process::exit(1);
    }

    pub fn error_usage_and_bail(&self, message: &str) {
        error!("{}", message);
        self.print_usage();
        process::exit(1);
    }

    fn init_logging(&self) {
        // Configure logging verbosity and initialise the logger
        match self.matches.opt_count("v") {
            0 => {env::set_var("RUST_LOG", "warn")},
            1 => {env::set_var("RUST_LOG", "info")},
            2 => {env::set_var("RUST_LOG", "debug")},
            _ => {env::set_var("RUST_LOG", "trace")}, // Provided > 2 -v flags
        }

        env_logger::init();

        debug!("Applied log level: {}", env::var("RUST_LOG").unwrap());
    }

    fn parse_delimiter(&self) -> Result<char, &str> {
        // Verify the --delimiter parameter
        if ! self.matches.opt_present("delimiter") {
            return Err("We need a --delimiter parameter")
        }

        match self.matches.opt_str("delimiter") {
            Some(ref x) if x == "tsv" => Ok('\t'),
            Some(ref x) if x == "csv" => Ok(','),
            Some(ref x) if x == "psv" => Ok('|'),
            Some(ref x) if x.len() == 1 => Ok(x.chars().next().unwrap()),
            _ => Err("Delimiter can only be a single character")
        }
    }

    fn parse_key_index(&self) -> Result<usize, &str> {
        if ! self.matches.opt_present("key-index") {
            return Err("We need a --key-index parameter");
        }

        let key_index = self.matches.opt_str("key-index").unwrap().parse::<usize>().unwrap();

        Ok(key_index)
    }

    fn parse_glob(&self) -> Result<Option<Vec<String>>, String> {
        if self.matches.opt_present("glob") {
            Ok(Some(self.matches.opt_strs("glob")))
        } else {
            Ok(None)
        }
    }

    fn parse_cache_file(&self) -> Result<Option<PathBuf>, String> {
        if self.matches.opt_present("cache-file") {
            let result = self.matches.opt_str("cache-file").unwrap();
            Ok(Some(PathBuf::from(result)))
        } else {
            Ok(None)
        }
    }

    fn parse_key_generic(&self, param: &str) -> Result<Option<String>, String> {
        if self.matches.opt_present(param) {
            let result = self.matches.opt_str(param);

            match result {
                Some(result) => {
                    Ok(Some(result))
                },
                None => {
                    Err(format!("Failed to extract what you provided from {}", param))
                }
            }
        } else {
            Ok(None)
        }
    }

    fn parse_key_type(&self) -> Result<KeyType, &str> {
        if self.matches.opt_present("key-type") {
            let result = self.matches.opt_str("key-type");

            if result.is_some() {
                let result = result.unwrap();

                match result.trim() {
                    "Unsigned32Integer" => Ok(KeyType::Unsigned32Integer),
                    "Signed32Integer"   => Ok(KeyType::Signed32Integer),
                    "String"            => Ok(KeyType::String),
                    _                   => Err("Your key-type is wrong?"),
                }
            } else {
                Err("Your key-type is wrong?")
            }
        } else {
            Ok(KeyType::String)
        }
    }
}
