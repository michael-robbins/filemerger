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
use std::path::PathBuf;
use settings::KeyType;
use std::env;

fn main() {
    // Set up argument parsing
    let args = env::args().collect::<Vec<String>>();

    // Allocate an empty cache for each KeyType variant, it's hacky and there's plenty of code duplication
    // but we need to do it as Rust cannot have a single HashMap that can contiain two types of MergeFile<T>
    // and because each match arm of KeyType is a different type we would have to use some box trait magic.
    let mut mergefile_cache_string = HashMap::new();
    let mut mergefile_cache_i32 = HashMap::new();
    let mut mergefile_cache_u32 = HashMap::new();

    let settings = settings::load(args).unwrap();

    // Load cache path
    let cache_present = settings.cache_path.is_some();
    let glob_present = settings.glob_choices.len() > 0;

    let mut cache_path = PathBuf::from("");

    if cache_present {
        cache_path = settings.cache_path.unwrap();

        if cache_path.exists() {
            match settings.key_type {
                KeyType::Unsigned32Integer => {
                    match MergeFileManager::retrieve_from_cache(&cache_path, 0u32, settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_u32.extend(merge_files);
                            debug!("Added cachefile {} to the cache", cache_path.display())
                        },
                        Err(error) => {
                            error!("Unable to load from cache file: {}", cache_path.display());
                            error!("Error was: {}", error);
                        }
                    }
                },
                KeyType::Signed32Integer => {
                    match MergeFileManager::retrieve_from_cache(&cache_path, 0i32, settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_i32.extend(merge_files);
                            debug!("Added cachefile {} to the cache", cache_path.display())
                        },
                        Err(error) => {
                            error!("Unable to load from cache file: {}", cache_path.display());
                            error!("Error was: {}", error);
                        }
                    }
                },
                KeyType::String => {
                    match MergeFileManager::retrieve_from_cache(&cache_path, "0".to_string(), settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_string.extend(merge_files);
                            debug!("Added cachefile {} to the cache", cache_path.display())
                        },
                        Err(error) => {
                            error!("Unable to load from cache file: {}", cache_path.display());
                            error!("Error was: {}", error);
                        }
                    }
                }
            }
        }
    }

    if glob_present {
        debug!("Getting all files from the glob(s)!");
        for glob_choice in settings.glob_choices {
            match settings.key_type {
                KeyType::Unsigned32Integer => {
                    match MergeFileManager::retrieve_from_glob(&glob_choice, settings.delimiter, settings.key_index, 0u32, settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_u32.extend(merge_files);
                            debug!("Added glob {} to the cache", glob_choice)
                        },
                        Err(error) => {
                            error!("Unable to load from glob: {}", glob_choice);
                            error!("Error was: {}", error);
                        }
                    }
                },
                KeyType::Signed32Integer => {
                    match MergeFileManager::retrieve_from_glob(&glob_choice, settings.delimiter, settings.key_index, 0i32, settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_i32.extend(merge_files);
                            debug!("Added glob {} to the cache", glob_choice)
                        },
                        Err(error) => {
                            error!("Unable to load from glob: {}", glob_choice);
                            error!("Error was: {}", error);
                        }
                    }
                },
                KeyType::String => {
                    match MergeFileManager::retrieve_from_glob(&glob_choice, settings.delimiter, settings.key_index, "0".to_string(), settings.key_type.clone()) {
                        Ok(merge_files) => {
                            mergefile_cache_string.extend(merge_files);
                            debug!("Added glob {} to the cache", glob_choice)
                        },
                        Err(error) => {
                            error!("Unable to load from glob: {}", glob_choice);
                            error!("Error was: {}", error);
                        }
                    }
                }
            }
        }

        if cache_present {
            match settings.key_type {
                KeyType::Unsigned32Integer => {
                    match MergeFileManager::write_cache(&cache_path, mergefile_cache_u32) {
                        Ok(result) => {info!("{}", result)},
                        Err(result) => {error!("{}", result)},
                    }
                },
                KeyType::Signed32Integer => {
                    match MergeFileManager::write_cache(&cache_path, mergefile_cache_u32) {
                        Ok(result) => {info!("{}", result)},
                        Err(result) => {error!("{}", result)},
                    }
                },
                KeyType::String => {
                    match MergeFileManager::write_cache(&cache_path, mergefile_cache_string) {
                        Ok(result) => {info!("{}", result)},
                        Err(result) => {error!("{}", result)},
                    }
                },
            }

            // Bail early as glob + cache == don't perform merge
            return;
        }
    }

    // Begin the merge process
    match settings.key_type {
        KeyType::Unsigned32Integer => {
            let cache = MergeFileManager::fast_forward_cache(mergefile_cache_u32, settings.key_start);

            if settings.key_end.is_some() {
                info!("Beginning merge -> {}", settings.key_end.clone().unwrap());
            } else {
                info!("Beginning merge -> EOF");
            }

            MergeFileManager::begin_merge(cache, settings.key_end, true);
        },
        KeyType::Signed32Integer => {
            let cache = MergeFileManager::fast_forward_cache(mergefile_cache_i32, settings.key_start);

            if settings.key_end.is_some() {
                info!("Beginning merge -> {}", settings.key_end.clone().unwrap());
            } else {
                info!("Beginning merge -> EOF");
            }

            MergeFileManager::begin_merge(cache, settings.key_end, true);
        },
        KeyType::String => {
            let cache = MergeFileManager::fast_forward_cache(mergefile_cache_string, settings.key_start);

            if settings.key_end.is_some() {
                info!("Beginning merge -> {}", settings.key_end.clone().unwrap());
            } else {
                info!("Beginning merge -> EOF");
            }

            MergeFileManager::begin_merge(cache, settings.key_end, true);
        }
    }
}
