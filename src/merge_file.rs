
// File IO stuff
use std::io::prelude::*;
use std::fs::File;
use std::io::Lines;
use std::io::BufReader;
use std::error::Error;
use std::path::Path;

// Optional decompressors
use flate2::read::GzDecoder;
use bzip2::reader::BzDecompressor;

pub struct MergeFile {
    lines: Lines<BufReader<Box<Read>>>,
    delimiter: char,
    index: usize,
    line: String,
    merge_key: String,
}

impl MergeFile {
    pub fn new(filepath: &Path, delimiter: char, index: usize) -> MergeFile {

        // Open the input file
        let file = match File::open(filepath) {
            Err(why) => panic!("ERROR: Count't open input file {:?}: {}", filepath, Error::description(&why)),
            Ok(file) => file,
        };
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

        MergeFile {
            lines: BufReader::new(decompressor).lines(),
            delimiter: delimiter,
            index: index,
            line: "".to_string(),
            merge_key: "".to_string(),
        }
    }

    pub fn fast_forward(&mut self, merge_start: f64) {
        println!("{}", merge_start);
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

                        if self.index == 1 {
                            // Figure out how to optimise this where instead of splitting
                            // we just index from character 0 -> lines.find(self.delimiter)
                            self.merge_key = line.split(self.delimiter).nth(self.index).unwrap().to_string();
                        } else {
                            self.merge_key = line.split(self.delimiter).nth(self.index).unwrap().to_string();
                        }

                        println!("Using merge_key: {}", self.merge_key);

                        Some((self.merge_key.clone(), self.line.clone()))
                    },
                    Err(_) => {None},
                }
            },
            None => {None},
        }
    }
}
