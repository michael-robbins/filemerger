use std::io::{BufReader,BufWriter};
use std::collections::BinaryHeap;
use std::io::{Error, ErrorKind};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use glob::glob;
use std::io;

use merge_file::MergeFile;

pub struct MergeFileManager {
    pub cache: HashMap<String, MergeFile>,
    print_merge_output: bool,
}

impl MergeFileManager {
    pub fn new() -> MergeFileManager {
        // Allocate an empty cache and return it
        let cache = HashMap::new();
        MergeFileManager{cache: cache, print_merge_output: true}
    }

    pub fn add_file(&mut self, filepath: &String, delimiter: char, index: usize) -> io::Result<&'static str> {
        // Create the merge file
        let mut merge_file = try!(MergeFile::new(filepath, delimiter, index));

        let iter_result = merge_file.next(); // (Merge Key, Line)

        if iter_result.is_none() {
            return Err(Error::new(ErrorKind::Other, "Failed to iterate on the merge file"));
        }

        // Remember the initial merge_key of the file
        merge_file.beginning_merge_key = iter_result.unwrap().0;

        let cache_key = filepath.clone();

        if self.cache.contains_key(&cache_key) {
            warn!("{} is already in the cache, removing it and adding in this one", &cache_key);
            self.cache.remove(&cache_key);
        }

        self.cache.insert(cache_key, merge_file);
        return Ok("File added to cache!");
    }

    pub fn load_from_glob(&mut self, glob_choice: &String, delimiter: char, index: usize) -> Result<String, String> {
        // TODO: Unit test: Given a glob test the number of files found
        // TODO: Unit test: Given an invalid glob test the number of files found

        // Fill the merge_cache with all files from the glob
        let glob_result = glob(glob_choice.as_ref());

        if glob_result.is_err() {
            return Err(format!("Unable to perform glob over: {}",glob_choice))
        }

        for result in glob_result.unwrap() {
            match result {
                Ok(path) => {
                    let filename = path.to_string_lossy().into_owned();

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
        // TODO: Unit test: Given a known cache, determine the loaded entries
        // TODO: Unit test: Given an invalid cache, determine the loaded entries

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
        // TODO: Handle the deletion stuff better
        let mut files_to_delete: Vec<String>  = vec!();
        for (_, merge_file) in self.cache.iter_mut() {
            info!("Fast Forwarding MergeFile {:?} -> {}", &merge_file, &merge_start);
            if merge_file.fast_forward(&merge_start).is_err() {
                info!("Failed to fastforward or we hit EOF for {}, removing from cache", merge_file.filename);
                files_to_delete.push(merge_file.filename.clone());
            }
        }

        for filename in files_to_delete {
            info!("Removing file {} from cache", filename);
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
                    // Print the line (if required) then push the MergeFile back into the heap
                    if self.print_merge_output {
                        println!("{}", result.1);
                    }

                    cache.push(earliest_file);
                }
            } else {
                println!("We hit EOF for {} with a final merge key of {}", earliest_file.filename, earliest_file.ending_merge_key);
                discarded_cache.push(earliest_file);
            }
        }
    }

    pub fn write_cache(&mut self, cache_filename: &String) -> io::Result<&str> {
        // TODO: Make the function accept a cache instead of using self
        // TODO: Unit test: Given a filename, test the provided cache writing
        // TODO: Unit test: Given an invalid filename, test the writing ability
        info!("Writing out cache to disk => {}!", cache_filename);

        // Open the file
        let mut cache_file = BufWriter::new(try!(File::create(Path::new(cache_filename))));

        //TODO: Need to sort the output of iter_mut() before iterating over it (maybe collect -vec-> sort?)
        //      This is required for functional test(s) because we need to garuentee the output ordering of the cache
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

#[test]
fn test_add_file() {
    // Set up the test data
    // TODO: Add the PID of the process into the filename
    let test_filename_1 = "/tmp/test_add_file.file1.tsv".to_string();
    let mut test_file_1 = BufWriter::new(File::create(Path::new(&test_filename_1)).unwrap());
    test_file_1.write(format!("{}\t{}\t{}\n", "123", "bbb", "999").as_ref()).unwrap();
    test_file_1.write(format!("{}\t{}\t{}\n", "124", "bbb", "999").as_ref()).unwrap();
    test_file_1.write(format!("{}\t{}\t{}\n", "125", "bbb", "999").as_ref()).unwrap();
    let _ = test_file_1.flush();

    let test_filename_2 = "/tmp/test_add_file.file2.tsv".to_string();
    let mut test_file_2 = BufWriter::new(File::create(Path::new(&test_filename_2)).unwrap());
    test_file_2.write(format!("{},{},{}\n", "123", "aaa", "888").as_ref()).unwrap();
    test_file_2.write(format!("{},{},{}\n", "124", "aaa", "888").as_ref()).unwrap();
    test_file_2.write(format!("{},{},{}\n", "127", "aaa", "888").as_ref()).unwrap();
    let _ = test_file_2.flush();

    // Create an instance of MergeFileManager
    let cache = HashMap::new();
    let mut test_manager = MergeFileManager{cache: cache, print_merge_output: false};

    // Add the first file and sanity check
    assert!(test_manager.add_file(&test_filename_1, '\t', 0).is_ok());
    assert_eq!(test_manager.cache.len(), 1);
    assert_eq!(test_manager.cache.get(&test_filename_1).unwrap().filename, test_filename_1);

    // Add the second file and sanity check
    assert!(test_manager.add_file(&test_filename_2, ',', 0).is_ok());
    assert_eq!(test_manager.cache.len(), 2);
    assert_eq!(test_manager.cache.get(&test_filename_2).unwrap().filename, test_filename_2);

    // Add the second file *again* and sure there isn't 3 files
    assert!(test_manager.add_file(&test_filename_2, ',', 0).is_ok());
    assert_eq!(test_manager.cache.len(), 2);

    // Check the first file's number of lines
    assert!(test_manager.cache.get_mut(&test_filename_1).unwrap().next().is_some());
    assert!(test_manager.cache.get_mut(&test_filename_1).unwrap().next().is_some());
    assert!(test_manager.cache.get_mut(&test_filename_1).unwrap().next().is_none());

    // Check the second file's number of lines
    assert!(test_manager.cache.get_mut(&test_filename_2).unwrap().next().is_some());
    assert!(test_manager.cache.get_mut(&test_filename_2).unwrap().next().is_some());
    assert!(test_manager.cache.get_mut(&test_filename_2).unwrap().next().is_none());
}

#[test]
fn test_load_from_glob() {
    // load_from_glob function signiture
    // (&mut self, glob_choice: &String, delimiter: char, index: usize) -> Result<String, String>
    let glob_choice_1 = "tests/files/data1.tsv";
    let glob_choice_2 = "tests/files/data?.tsv";

    assert_eq!(glob_choice_1, "tests/files/data1.tsv");
    assert_eq!(glob_choice_2, "tests/files/data?.tsv");
}
