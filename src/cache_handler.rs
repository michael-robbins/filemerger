
use std::fmt;
use std::io::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::error::Error;
use std::path::Path;

#[derive(Default)]
pub struct CacheFileEntry {
    filename: String,
    merge_key_start: String,
    merge_key_end: String,
    filesize: f64,
}

pub struct CacheFileManager {
    cache: Vec<CacheFileEntry>,
}

impl CacheFileManager {
    pub fn new(filename: &String) -> CacheFileManager {


        // Open the input file
        let cache_file = match File::open(Path::new(filename)) {
            Err(why) => panic!("ERROR: Count't open input file {}: {}", filename, Error::description(&why)),
            Ok(file) => BufReader::new(file),
        };
        let mut cache: Vec<CacheFileEntry> = Vec::new();

        // Iterate over cache_file reading in and creating new CacheFileEntry instances
        for cache_line in cache_file.lines() {
            let cache_line = cache_line.unwrap();
            println!("CacheFile line: {}", cache_line);

            let cache_line_vec: Vec<&str> = cache_line.split("|").collect();

            // We should totally verify the filename exists locally first?
            // Maybe also if its filesize is different invalidate its merge_key_{start,end}?

            cache.push(CacheFileEntry{filename: cache_line_vec[0].to_string(),
                                      merge_key_start: cache_line_vec[1].to_string(),
                                      merge_key_end: cache_line_vec[2].to_string(),
                                      filesize: cache_line_vec[3].parse::<f64>().unwrap(),
                                     });
        }

        // Return with the vec of CacheFileEntry's
        CacheFileManager {cache: cache}
    }
}

impl Iterator for CacheFileManager {
    type Item = CacheFileEntry;

    // Just returns the next internal CacheFileEntry until there's no more!
    fn next(&mut self) -> Option<CacheFileEntry> {
        self.cache.pop()
    }
}

impl fmt::Debug for CacheFileEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {} -> {} ({})", self.filename, self.merge_key_start, self.merge_key_end, self.filesize)
    }
}
