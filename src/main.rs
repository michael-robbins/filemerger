/// Filemerger
///
/// Takes a series of files that are assumed to be sorted based on your merge key
/// Splits each line based on a user supplied delimiter
/// Extracts a specific column to use as the merge key
/// Merges all files together into a single stream based on the merge key
///
/// We have a large number of code paths below due to Rust's type checker!

#[macro_use] extern crate log;
extern crate rustc_serialize;
extern crate yaml_rust;
extern crate getopts;
extern crate flate2;
extern crate bzip2;
extern crate glob;
extern crate csv;

mod merge_file_manager;
mod merge_file;
mod settings;

use merge_file_manager::MergeFileManager;
use std::collections::HashMap;
use merge_file::Mergeable;
use merge_file::MergeFile;
use std::path::PathBuf;
use settings::KeyType;
use std::env;
use std::fmt;

fn retrieve_from_cache<T>(cache_path: &PathBuf, default_key: T, key_type: KeyType, mut merge_cache: HashMap<String, MergeFile<T>>)
    -> HashMap<String, MergeFile<T>>
    where T: Mergeable, T::Err: fmt::Debug {
    match MergeFileManager::retrieve_from_cache(&cache_path, default_key, key_type) {
        Ok(merge_files) => {
            merge_cache.extend(merge_files);
            debug!("Added cachefile {} to the cache", cache_path.display())
        },
        Err(error) => {
            error!("Unable to load from cache file: {}", cache_path.display());
            error!("Error was: {}", error);
        }
    }
    merge_cache
}

fn retrieve_from_glob<T>(glob_choice: &str, delimiter: char, index: usize, default_key: T, key_type: KeyType, mut merge_cache: HashMap<String, MergeFile<T>>)
    -> HashMap<String, MergeFile<T>>
    where T: Mergeable, T::Err: fmt::Debug {
    match MergeFileManager::retrieve_from_glob(&glob_choice, delimiter, index, default_key, key_type) {
        Ok(merge_files) => {
            merge_cache.extend(merge_files);
            debug!("Added glob {} to the cache", glob_choice);
        },
        Err(error) => {
            error!("Unable to load from glob: {}", glob_choice);
            error!("Error was: {}", error);
        }
    }
    merge_cache
}

fn write_cache<T>(cache_path: &PathBuf, merge_cache: HashMap<String, MergeFile<T>>, default_key: T)
    where T: Mergeable, T::Err: fmt::Debug {
    match MergeFileManager::write_cache(&cache_path, merge_cache, default_key) {
        Ok(result) => {info!("{}", result)},
        Err(result) => {error!("{}", result)},
    }
}

fn begin_merge<T>(mut merge_cache: HashMap<String, MergeFile<T>>, key_start: Option<String>, key_end: Option<String>, print_merge_output: bool)
    where T: Mergeable, T::Err: fmt::Debug {
    // If we have a start position, then fast forward to it
    if key_start.is_some() {
        merge_cache = MergeFileManager::fast_forward_cache(merge_cache, key_start.unwrap());
    }

    if key_end.is_some() {
        info!("Beginning merge -> {}", key_end.clone().unwrap());
    } else {
        info!("Beginning merge -> EOF");
    }

    MergeFileManager::begin_merge(merge_cache, key_end, print_merge_output);
}

fn main() {
    // Allocate an empty cache for each KeyType variant, it's hacky and there's plenty of code duplication
    // but we need to do it as Rust cannot have a single HashMap that can contiain two types of MergeFile<T>
    // and because each match arm of KeyType is a different type we would have to use some box trait magic.
    let mut merge_cache_string = HashMap::new();
    let mut merge_cache_i32 = HashMap::new();
    let mut merge_cache_u32 = HashMap::new();

    // Set up argument parsing
    let args = env::args().collect::<Vec<String>>();
    let settings = settings::load(args).unwrap();

    let cache_present = settings.cache_path.is_some();
    let glob_present = settings.glob_choices.len() > 0;

    let mut cache_path = PathBuf::from("");

    if cache_present {
        cache_path = settings.cache_path.unwrap();

        if cache_path.exists() {
            match settings.key_type {
                KeyType::Unsigned32Integer => {
                    merge_cache_u32 = retrieve_from_cache(&cache_path,
                                                          0u32,
                                                          settings.key_type.clone(),
                                                          merge_cache_u32);
                },
                KeyType::Signed32Integer => {
                    merge_cache_i32 = retrieve_from_cache(&cache_path,
                                                          0i32,
                                                          settings.key_type.clone(),
                                                          merge_cache_i32);
                },
                KeyType::String => {
                    merge_cache_string = retrieve_from_cache(&cache_path,
                                                             "0".to_string(),
                                                             settings.key_type.clone(),
                                                             merge_cache_string);
                }
            }
        }
    }

    if glob_present {
        for glob_choice in settings.glob_choices {
            match settings.key_type {
                KeyType::Unsigned32Integer => {
                    merge_cache_u32 = retrieve_from_glob(&glob_choice,
                                                         settings.delimiter,
                                                         settings.key_index,
                                                         0u32,
                                                         settings.key_type.clone(),
                                                         merge_cache_u32);
                },
                KeyType::Signed32Integer => {
                    merge_cache_i32 = retrieve_from_glob(&glob_choice,
                                                         settings.delimiter,
                                                         settings.key_index,
                                                         0i32, settings.key_type.clone(),
                                                         merge_cache_i32);
                },
                KeyType::String => {
                    merge_cache_string = retrieve_from_glob(&glob_choice,
                                                            settings.delimiter,
                                                            settings.key_index,
                                                            "0".to_string(),
                                                            settings.key_type.clone(),
                                                            merge_cache_string);
                }
            }
        }

        if cache_present {
            match settings.key_type {
                KeyType::Unsigned32Integer => write_cache(&cache_path, merge_cache_u32, 0u32),
                KeyType::Signed32Integer => write_cache(&cache_path, merge_cache_i32, 0i32),
                KeyType::String => write_cache(&cache_path, merge_cache_string, "0".to_string()),
            }

            // Bail early as glob + cache == don't perform merge
            return;
        }
    }

    // Begin the merge process
    match settings.key_type {
        KeyType::Unsigned32Integer => begin_merge(merge_cache_u32, settings.key_start, settings.key_end, true),
        KeyType::Signed32Integer => begin_merge(merge_cache_i32, settings.key_start, settings.key_end, true),
        KeyType::String => begin_merge(merge_cache_string, settings.key_start, settings.key_end, true),
    }
}
