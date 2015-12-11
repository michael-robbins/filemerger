// File IO modules
use std::io::{Error, ErrorKind};
use std::io::prelude::*;
use std::io::BufReader;
use std::io::Lines;
use std::path::Path;
use std::fs::File;
use std::cmp;
use std::fmt;
use std::io;

// Optional decompressors for merge files
use flate2::read::GzDecoder;
use bzip2::reader::BzDecompressor;

pub struct MergeFile {
    pub filename: String,
    pub filesize: u64,
    lines: Lines<BufReader<Box<Read>>>,
    delimiter: char,
    index: usize,
    line: String,
    current_merge_key: String,
    pub beginning_merge_key: String,
    pub ending_merge_key: String,
}

impl MergeFile {
    pub fn new(filename: &String, delimiter: char, index: usize) -> io::Result<MergeFile> {
        // Open the input file
        let filepath = Path::new(filename);

        let file_ext = match filepath.extension() {
            Some(extension) => extension,
            None => return Err(Error::new(ErrorKind::Other, format!("Couldn't find file extension in {:?}", filepath))),
        };

        let file = try!(File::open(filepath));

        let file_metadata = try!(file.metadata());
        let filesize = file_metadata.len();

        // Figure out the input file's decompressor
        let decompressor: Box<Read> = match file_ext.to_str().unwrap() {
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

    pub fn fast_forward(&mut self, merge_start: &String) {
        while self.current_merge_key < *merge_start {
            let _ = self.lines.next();
        }
    }

    pub fn fast_forward_to_end(&mut self) {
        while self.lines.next().is_some() {
            continue;
        }

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
