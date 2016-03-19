/// Filemerge
/// Takes a series of files that are assumed to be sorted based on your merge key
/// Splits each line based on a user supplied delimiter
/// Extracts a specific column to use as the merge key
/// Merges all files together into a single stream based on the merge key

#[macro_use] extern crate log;
extern crate getopts;
extern crate flate2;
extern crate bzip2;
extern crate glob;
extern crate yaml_rust;

mod merge_file_manager;
mod merge_file;
mod settings;

use merge_file_manager::MergeFileManager;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::env;

fn main() {
    // Set up argument parsing
    let args: Vec<String> = env::args().collect();

    // Allocate an empty cache
    let mut mergefile_cache = HashMap::new();

    let settings = settings::load(args).unwrap();

    let cache_present = settings.cache_path.to_str().unwrap() != "";
    let cache_exists = settings.cache_path.is_file();
    let glob_present = settings.glob_choices.len() > 0;

    // If we have a cache file, preload it
    if cache_exists {
        match MergeFileManager::retrieve_from_cache(&settings.cache_path, settings.delimiter, settings.index) {
            Ok(merge_files) => {
                mergefile_cache.extend(merge_files);
                debug!("Added cachefile {} to the cache", settings.cache_path.display())
            },
            Err(error) => {
                error!("Unable to load from cache file: {}", settings.cache_path.display());
                error!("Error was: {}", error);
            }
        }
    }

    if glob_present {
        debug!("Getting all files from the glob(s)!");
        for glob_choice in settings.glob_choices {
            match MergeFileManager::retrieve_from_glob(&glob_choice, settings.delimiter, settings.index) {
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
            match MergeFileManager::write_cache(&settings.cache_path, mergefile_cache) {
                Ok(result) => {info!("{}", result)},
                Err(result) => {error!("{}", result)},
            }

            // Bail early as glob + cache == don't perform merge
            return;
        }
    }

    // Begin the merge process
    let cache = MergeFileManager::fast_forward_cache(mergefile_cache, settings.key_start);
    let heap = BinaryHeap::from(MergeFileManager::cache_to_vec(cache));

    if settings.key_end.is_some() {
        info!("Beginning merge -> {}", settings.key_end.clone().unwrap());
    } else {
        info!("Beginning merge -> EOF");
    }

    MergeFileManager::begin_merge(heap, settings.key_end, true);
}
