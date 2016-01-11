use std::collections::BinaryHeap;
use std::io::{BufReader,BufWriter};
use std::io::{Error, ErrorKind};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use glob::glob;
use std::io;

use merge_file::MergeFile;

pub struct MergeFileManager {
    //pub cache: Vec<MergeFile>,
    pub cache: HashMap<String, MergeFile>,
}

impl MergeFileManager {
    pub fn new() -> MergeFileManager {
        // Allocate an empty cache and return it
        let cache = HashMap::new();
        MergeFileManager{cache: cache}
    }

    pub fn add_file(&mut self, filepath: &String, delimiter: char, index: usize) -> io::Result<&'static str> {
        // Create the merge file
        let merge_file = try!(MergeFile::new(filepath, delimiter, index));

        let mut merge_file = merge_file;
        let iter_result = merge_file.next(); // (Merge Key, Line)

        if iter_result.is_none() {
            return Err(Error::new(ErrorKind::Other, "Failed to iterate on the merge file"));
        }

        // Remember the initial merge_key of the file
        merge_file.beginning_merge_key = iter_result.unwrap().0;

        let cache_key = filepath.clone();

        if self.cache.contains_key(&cache_key) {
            self.cache.remove(&cache_key);
        }

        self.cache.insert(cache_key, merge_file);
        return Ok("File added to cache!");
    }

    pub fn load_from_glob(&mut self, glob_choice: &String, delimiter: char, index: usize) -> Result<String, String> {
        // Fill the merge_cache with all files from the glob
        for result in glob(glob_choice.as_ref()).unwrap() {
            match result {
                Ok(path) => {
                    let filename = path.to_str().unwrap().to_string();

                    // Check if the file is already in the cache and the same size
                    {
                        let cache_entry = self.cache.get(&filename);
                        if cache_entry.is_some() {
                            let cache_entry = cache_entry.unwrap();
                            let cache_filesize = File::open(path).unwrap().metadata().unwrap().len();

                            if cache_filesize == cache_entry.filesize {
                                warn!("{} is already in the cache and is the same size, skipping", filename);
                                continue;
                            } else {
                                warn!("{} is already in the cache, but a different size? Resetting it to the on-disk glob version", filename);
                            }
                        } else {
                            debug!("{} wasn't found in the cache", filename);
                        }
                    }

                    // Add it into the cache if it isn't
                    debug!("Adding {} to the cache!", filename);
                    let _ = self.add_file(&filename, delimiter, index);
                },
                Err(e) => {
                    debug!("Unable to load {:?} we got from the glob {}", e, glob_choice);
                },
            }
        }

        Ok(format!("Loaded all files we could from glob {:?}", glob_choice))
    }

    pub fn load_from_cache(&mut self, cache_filepath: &String, delimiter: char, index: usize) -> io::Result<String> {

        // Cache file layout: file_name,mergekey_start,mergekey_end,file_size
        // Load the file
        let cache_file = BufReader::new(try!(File::open(Path::new(cache_filepath))));

        // Iterate over cache_file reading in and creating new CacheFileEntry instances
        for cache_line in cache_file.lines() {
            if cache_line.is_err() {
                return Err(cache_line.unwrap_err());
            }

            let cache_line = cache_line.unwrap();
            debug!("CacheFile line: {}", cache_line);

            let cache_line: Vec<&str> = cache_line.split(",").collect();

            if cache_line.len() != 4 {
                warn!("Cache entry has {} columns, we expect only 4", cache_line.len());
                continue;
            }

            // We should totally verify the filename exists locally first?
            let cache_entry_filename = cache_line[0].to_string();
            let cache_entry_filepath = Path::new(&cache_entry_filename);
            let cache_entry_file_result = File::open(cache_entry_filepath.clone());

            if cache_entry_file_result.is_err() {
                warn!("Skipping cache entry for {} as it doesn't exist?", cache_line[0].to_string());
                continue;
            }

            let cache_entry_file = cache_entry_file_result.unwrap();
            let ondisk_filesize = cache_entry_file.metadata().unwrap().len();
            let cache_filesize = cache_line[3].parse::<u64>().unwrap();

            if ondisk_filesize != cache_filesize {
                warn!("Skipping cache entry for {:?} as it's filesize is wrong ({} != {})", cache_entry_filepath, ondisk_filesize, cache_filesize);
                continue;
            }

            let _ = self.add_file(&cache_entry_filename, delimiter, index);
        }

        Ok(format!("Loaded all files we could from {:?}", cache_filepath))
    }

    pub fn fast_forward_cache(&mut self, merge_start: &String) {
        for (_, merge_file) in self.cache.iter_mut() {
            info!("Fast Forwarding MergeFile {:?} -> {}", &merge_file, &merge_start);
            merge_file.fast_forward(&merge_start);
        }
    }

    pub fn begin_merge(&mut self, merge_end: String) {
        info!("Beginning merge => {}", merge_end);

        // HeapSort-ify the self.cache vector based on the merge_key column
        // Drain removes an item from the cache and returns it, this means we don't need to worry about cloning
        let cache_vec: Vec<MergeFile> = self.cache.drain().map(|(_, file)| file).collect();
        let mut cache = BinaryHeap::from(cache_vec);

        // Create a discarded_cache of files, as once we pop the file off the heap, we can't insert it after it's bad
        let mut discarded_cache: Vec<MergeFile> = Vec::new();

        while cache.len() > 0 {
            let earliest_file = cache.pop();

            if earliest_file.is_none() {
                info!("We reached the end of the cache! All done!");
                break
            }

            // Unwrap earliest_file and attempt to read a line`
            let mut earliest_file = earliest_file.unwrap();
            let result = earliest_file.next();

            // Report on the line or EOF the file and add it to the discarded pile
            if result.is_some() {
                let result = result.unwrap();

                // Check if the line has exceeped the merge_end key
                if result.0 > merge_end {
                    info!("MergeFile<{}> has hit end bound ({}>{}), discarding from cache", earliest_file.filename, result.0, merge_end);
                    discarded_cache.push(earliest_file);
                } else {
                    // Print the line the push the MergeFile back into the heap
                    println!("{}", result.1);
                    cache.push(earliest_file);
                }
            } else {
                println!("We hit EOF for {} with a final merge key of {}", earliest_file.filename, earliest_file.ending_merge_key);
                discarded_cache.push(earliest_file);
            }
        }
    }

    pub fn write_cache(&mut self, cache_filename: &String) -> io::Result<&str> {
        info!("Writing out cache to disk => {}!", cache_filename);

        // Open the file
        let mut cache_file = BufWriter::new(try!(File::create(Path::new(cache_filename))));

        for (_, merge_file) in self.cache.iter_mut() {
            info!("Fast Forwarding MergeFile {:?} -> end", &merge_file);
            merge_file.fast_forward_to_end();
            try!(cache_file.write(format!("{},{},{},{}\n", merge_file.filename,
                                                         merge_file.beginning_merge_key,
                                                         merge_file.ending_merge_key,
                                                         merge_file.filesize).as_ref()));
        }

        Ok("Written cache out to disk.")
    }
}
