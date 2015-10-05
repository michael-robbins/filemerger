

use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::error::Error;
use std::path::Path;
use std::io::Lines;
use std::fs::File;
use glob::glob;
use std::cmp;
use std::fmt;

// Optional decompressors for merge files
use flate2::read::GzDecoder;
use bzip2::reader::BzDecompressor;

pub struct MergeFile {
    filename: String,
    filesize: u64,
    lines: Lines<BufReader<Box<Read>>>,
    delimiter: char,
    index: usize,
    line: String,
    current_merge_key: String,
    beginning_merge_key: String,
    ending_merge_key: String,
}

pub struct MergeFileManager {
    pub cache: HashMap<String, MergeFile>,
}

impl MergeFile {
    pub fn new(filename: &String, delimiter: char, index: usize) -> Result<MergeFile, String> {
        // Open the input file
        let filepath = Path::new(filename);
        let file = match File::open(filepath) {
            Err(_) => return Err(format!("Failed to open file {:?}", filepath)),
            Ok(file) => file,
        };

        let file_metadata = file.metadata().unwrap();
        let filesize = file_metadata.len();

        // Figure out the input file's decompressor
        let decompressor: Box<Read> = match filepath.extension().unwrap().to_str().unwrap() {
            "bz2" => {
                debug!("Using BzDecompressor as the input decompressor.");
                Box::new(BzDecompressor::new(file))
            },
            "gz" => {
                debug!("Using GzDecoder as the input decompressor.");
                Box::new(GzDecoder::new(file).unwrap())
            },
            _ => {
                debug!("Assuming the file is uncompressed.");
                Box::new(file)
            },
        };

        let merge_file = MergeFile {
            filename: filepath.to_str().unwrap().to_string(),
            filesize: filesize,
            lines: BufReader::new(decompressor).lines(),
            delimiter: delimiter,
            index: index,
            line: "".to_string(),
            current_merge_key: "".to_string(),
            beginning_merge_key: "".to_string(),
            ending_merge_key: "".to_string(),
        };

        Ok(merge_file)
    }

    fn fast_forward(&mut self, merge_start: &String) {
        while self.current_merge_key < *merge_start {
            let _ = self.lines.next();
        }
    }
}

impl MergeFileManager {
    pub fn new() -> MergeFileManager {
        // Allocate an empty cache and return it
        let cache = HashMap::new();
        MergeFileManager{cache: cache}
    }

    pub fn add_file(&mut self, filepath: &String, delimiter: char, index: usize) -> Result<&'static str, &'static str> {
        // Create the merge file
        let merge_file = MergeFile::new(filepath, delimiter, index);

        if merge_file.is_err() {
            return Err("Failed to create the merge file")
        }

        let mut merge_file = merge_file.unwrap();
        let iter_result = merge_file.next();

        if iter_result.is_none() {
            return Err("Failed to iterate on the merge file");
        }

        // Remember the initial merge_key of the file
        merge_file.beginning_merge_key = iter_result.unwrap().0;

        self.cache.insert(filepath.clone(), merge_file);
        return Ok("File added to cache!");
    }

    pub fn load_from_glob(&mut self, glob_choice: &String, delimiter: char, index: usize) -> Result<String, String> {
        // Fill the merge_cache with all files from the glob
        for result in glob(glob_choice.as_ref()).unwrap() {
            match result {
                Ok(path) => {
                    let filename = path.to_str().unwrap().to_string();

                    // Check if the file is already in the cache and the same size

                    // Add it into the cache if it isn't
                    let _ = self.add_file(&filename, delimiter, index);
                },
                Err(e) => {
                    debug!("Unable to load {:?} we got from the glob {}", e, glob_choice);
                },
            }
        }

        Ok(format!("Loaded all files we could from glob {:?}", glob_choice))
    }

    pub fn load_from_cache(&mut self, cache_filepath: &String, delimiter: char, index: usize) -> Result<String, String> {
        // Load the file
        let cache_file = match File::open(Path::new(cache_filepath)) {
            Err(why) => return Err(format!("ERROR: Count't open input file {}: {}", cache_filepath, Error::description(&why))),
            Ok(file) => BufReader::new(file),
        };

        // Iterate over cache_file reading in and creating new CacheFileEntry instances
        for cache_line in cache_file.lines() {
            let cache_line = cache_line.unwrap();
            println!("CacheFile line: {}", cache_line);

            let cache_line: Vec<&str> = cache_line.split("|").collect();

            if cache_line.len() != 4 {
                warn!("Skipping cache entry as the cache line's invalid!");
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
            println!("Fast Forwarding MergeFile {:?} -> {}", &merge_file, &merge_start);
            merge_file.fast_forward(&merge_start);
        }
    }

    pub fn begin_merge(self, merge_end: &String) {
        info!("Beginning merge => {}!", merge_end);
        // The heapsort goes in here
    }
}

impl Iterator for MergeFile {
    type Item = (String, String);

    // This is just a thin wrapper around Lines
    // It extracts the merge_key and passes that upstream
    fn next(&mut self) -> Option<(String, String)> {
        match self.lines.next() {
            Some(result) => {
                match result {
                    Ok(line) => {
                        self.line = line.clone();
                        self.current_merge_key = line.split(self.delimiter).nth(self.index).unwrap().to_string();

                        trace!("file='{}' current_merge_key='{}'", self.filename, self.current_merge_key);
                        Some((self.current_merge_key.clone(), self.line.clone()))
                    },
                    Err(_) => {
                        // Problems reading the file
                        debug!("Problem reading the next line for {}", self.filename);
                        None
                    },
                }
            },
            None => {
                // We've reached the end of the file, save it's merge_key
                debug!("Reached EOF for {}", self.filename);
                self.ending_merge_key = self.current_merge_key.clone();
                None
            },
        }
    }
}

impl fmt::Debug for MergeFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.filename)
    }
}

impl cmp::Ord for MergeFile {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.current_merge_key < other.current_merge_key {
            return cmp::Ordering::Less;
        } else if self.current_merge_key > other.current_merge_key {
            return cmp::Ordering::Greater;
        }
        cmp::Ordering::Equal
    }
}

impl cmp::PartialOrd for MergeFile {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.current_merge_key < other.current_merge_key {
            return Some(cmp::Ordering::Less);
        } else if self.current_merge_key > other.current_merge_key {
            return Some(cmp::Ordering::Greater);
        }
        Some(cmp::Ordering::Equal)
    }
}

impl cmp::Eq for MergeFile {}

impl cmp::PartialEq for MergeFile {
    fn eq(&self, other: &Self) -> bool {
        if self.filename == other.filename && self.filesize == other.filesize {
            return true;
        }
        false
    }
}
