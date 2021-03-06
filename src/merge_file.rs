// File IO modules
use std::io::{Error, ErrorKind};
use std::io::prelude::*;
use std::io::BufReader;
use std::str::FromStr;
use std::path::Path;
use std::io::Lines;
use std::fs::File;
use std::cmp;
use std::fmt;
use std::io;

// Optional decompressors for merge files
use flate2::read::GzDecoder;
use bzip2::read::BzDecoder;

// Other project dependencies
use settings::KeyType;

pub trait Mergeable: Clone + FromStr + fmt::Display + fmt::Debug + PartialOrd + Ord {}

impl Mergeable for u32 {}
impl Mergeable for i32 {}
impl Mergeable for String {}

pub struct MergeFile<T> {
    pub filename: String,
    pub filesize: u64,
    lines: Lines<BufReader<Box<Read>>>,
    pub line: String,
    pub delimiter: char,
    pub key_index: usize,
    pub current_merge_key: T,
    pub beginning_merge_key: T,
    pub ending_merge_key: T,
    pub key_type: KeyType,
}

impl<T: Mergeable> MergeFile<T> where T::Err: fmt::Debug {
    /// Constructs a new `MergeFile`.
    /// A `MergeFile` can be specialised for anything that can be converted to from an str.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut merge_file = MergeFile::new("/path/to/data.psv", '|', 1);
    /// ```
    pub fn new(filename: &str, delimiter: char, key_index: usize, default_key: T, key_type: KeyType) -> io::Result<MergeFile<T>> {
        // Unit test: Create MergeFile with valid test data
        // Unit test: Create MergeFile with invalid test data
        let filepath = Path::new(filename);

        let file_ext = match filepath.extension() {
            Some(extension) => extension,
            None => return Err(Error::new(ErrorKind::Other, format!("Couldn't find file extension in {:?}", filepath))),
        };

        let file = try!(File::open(filepath));
        let filesize = try!(file.metadata()).len();

        // Figure out the input file's decompressor
        let decompressor: Box<Read> = match file_ext.to_str() {
            Some("bz2") => {
                debug!("Using BzDecompressor as the input decompressor.");
                Box::new(BzDecoder::new(file))
            },
            Some("gz") => {
                debug!("Using GzDecoder as the input decompressor.");
                Box::new(GzDecoder::new(file).unwrap())
            },
            Some(_) => {
                debug!("Assuming the file is uncompressed.");
                Box::new(file)
            },
            None => {
                warn!("Unable to aquire file extention for {}", filename);
                return Err(Error::new(ErrorKind::Other, "File extension invalid?"))
            },
        };

        let mut merge_file = MergeFile {
            filename: filename.to_string(),
            filesize: filesize,
            lines: BufReader::new(decompressor).lines(),
            delimiter: delimiter,
            key_index: key_index,
            line: "".to_string(),
            current_merge_key: default_key.clone(),
            beginning_merge_key: default_key.clone(),
            ending_merge_key: default_key.clone(),
            key_type: key_type,
        };

        if let Some(merge_key) = merge_file.next() {
            merge_file.beginning_merge_key = merge_key;
            Ok(merge_file)
        } else {
            Err(Error::new(ErrorKind::Other, format!("Error performing initial iteration over {:?}", filepath)))
        }
    }

    pub fn fast_forward(&mut self, merge_start: &str) -> Result<&'static str,&'static str> {
        debug!("MergeFile<{}>: Fastforwarding -> {}", self.filename, merge_start);
        let merge_start = merge_start.parse::<T>().unwrap();
        while self.current_merge_key < merge_start {
            if self.next().is_none() {
                debug!("MergeFile<{}>: Fast forward hit EOF or failed to read, bailing", self.filename);
                return Err("Hit EOF or failed to read");
            }
        }
        debug!("MergeFile<{}>: Fastforwarded correctly!", self.filename);
        Ok("Fastwarded correctly")
    }

    pub fn fast_forward_to_end(&mut self) {
        while self.next().is_some() {
            continue;
        }
    }
}

impl<T: Mergeable> Iterator for MergeFile<T> where T::Err: fmt::Debug {
    type Item = T;

    // This is just a thin wrapper around Lines
    // It saves the line, extracts the merge_key and passes them upstream
    fn next(&mut self) -> Option<T> {
        match self.lines.next() {
            Some(Ok(line)) => {
                // Clone all required parts and return the new merge key and the line
                self.line = line.clone();

                let new_merge_key = line.splitn(self.key_index + 2, self.delimiter).next().unwrap();

                self.current_merge_key = new_merge_key.parse::<T>().unwrap();
                Some(self.current_merge_key.clone())
            },
            Some(Err(_)) => {
                // Problems reading the file
                debug!("Problem reading the next line for {}", self.filename);
                None
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

impl<T: fmt::Display> fmt::Debug for MergeFile<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.filename)
    }
}

impl<T: fmt::Display> fmt::Display for MergeFile<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.filename)
    }
}

impl<T: cmp::Ord + fmt::Display> cmp::Ord for MergeFile<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.current_merge_key > other.current_merge_key {
            return cmp::Ordering::Greater;
        } else if self.current_merge_key < other.current_merge_key {
            return cmp::Ordering::Less;
        }
        cmp::Ordering::Equal
    }
}

impl<T: cmp::PartialOrd + fmt::Display> cmp::PartialOrd for MergeFile<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.current_merge_key > other.current_merge_key {
            return Some(cmp::Ordering::Greater);
        } else if self.current_merge_key < other.current_merge_key {
            return Some(cmp::Ordering::Less);
        }
        Some(cmp::Ordering::Equal)
    }
}

impl<T: cmp::Eq> cmp::Eq for MergeFile<T> {}

impl<T: cmp::PartialEq> cmp::PartialEq for MergeFile<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.filename == other.filename && self.filesize == other.filesize {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::io::prelude::*;
    use std::io::BufWriter;
    use std::path::Path;
    use std::fs::File;
    use std::fs;

    use super::MergeFile;
    use settings::KeyType;

    fn create_file(filename: &str, contents: String) {
        let mut temp_file = BufWriter::new(File::create(Path::new(filename)).unwrap());
        temp_file.write(contents.as_ref()).unwrap();
        let _ = temp_file.flush();
    }

    #[test]
    fn new() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_new.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // Add the first file and sanity check
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mergefile = result.unwrap();
        assert_eq!(mergefile.filename, test_filename_1);

        let test_file_1 = File::open(&test_filename_1).unwrap();
        let test_filesize_1 = test_file_1.metadata().unwrap().len();
        assert_eq!(mergefile.filesize, test_filesize_1);

        assert_eq!(mergefile.delimiter, '\t');
        assert_eq!(mergefile.key_index, 0);

        assert_eq!(mergefile.line, "123\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "123");
        assert_eq!(mergefile.ending_merge_key, "0");

        let _ = fs::remove_file(test_filename_1);
    }

    #[test]
    fn fast_forward() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_fast_forward.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // Add the first file and sanity check
        let mut mergefile = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String).unwrap();

        // Test a fast forward to the middle of the file
        assert!(mergefile.fast_forward(&"124".to_string()).is_ok());
        assert_eq!(mergefile.line, "124\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "124");
        assert_eq!(mergefile.ending_merge_key, "0");

        // Test a fast forward past the end of the file
        assert!(mergefile.fast_forward(&"126".to_string()).is_err());
        assert_eq!(mergefile.line, "125\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "125");
        assert_eq!(mergefile.ending_merge_key, "125");

        let _ = fs::remove_file(test_filename_1);
    }


    #[test]
    fn fast_forward_to_end() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_fast_forward_to_end.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // Add the first file and sanity check
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mut mergefile = result.unwrap();

        // Ensure the current line is the last one in the above contents
        mergefile.fast_forward_to_end();
        assert_eq!(mergefile.line, "125\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "125");
        assert_eq!(mergefile.ending_merge_key, "125");

        let _ = fs::remove_file(test_filename_1);
    }

    #[test]
    fn impl_iterator() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        // TODO: Can we create temporary files?
        let test_filename_1 = "/tmp/test_impl_formatting.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // Add the first file and sanity check
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mut mergefile = result.unwrap();

        // Test line 1
        assert_eq!(mergefile.line, "123\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "123");
        assert_eq!(mergefile.ending_merge_key, "0");

        // Test line 2
        let result = mergefile.next();
        assert_eq!(result, Some("124".to_string()));

        assert_eq!(mergefile.line, "124\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "124");
        assert_eq!(mergefile.ending_merge_key, "0");

        // Test line 3
        let result = mergefile.next();
        assert_eq!(result, Some("125".to_string()));

        assert_eq!(mergefile.line, "125\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "125");
        assert_eq!(mergefile.ending_merge_key, "0");

        // Test EOF
        let result = mergefile.next();
        assert_eq!(result, None);

        assert_eq!(mergefile.line, "125\tbbb\t999");
        assert_eq!(mergefile.beginning_merge_key, "123");
        assert_eq!(mergefile.current_merge_key, "125");
        assert_eq!(mergefile.ending_merge_key, "125");
    }

    #[test]
    fn impl_formatting() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_impl_formatting.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // Add the first file and sanity check
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mergefile = result.unwrap();
        assert_eq!(format!("{}", mergefile), test_filename_1); // Test fmt::Display
        assert_eq!(format!("{:?}", mergefile), test_filename_1); // Test fmt::Debug

        let _ = fs::remove_file(test_filename_1);
    }

    #[test]
    fn impl_ordering_and_equality() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_impl_ordering.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        // TODO: Add the PID of the process into the filename
        let test_filename_2 = "/tmp/test_impl_ordering.file2.tsv";
        let test_contents_2 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "124", "aaa", "888",
                                        "125", "aaa", "888",
                                        "126", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        // Create the first file and initialise it
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mut mergefile_1 = result.unwrap();
        let result = mergefile_1.fast_forward(&"123".to_string());
        assert!(result.is_ok());

        // Create the second file and initialise it
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mut mergefile_2 = result.unwrap();
        let result = mergefile_2.fast_forward(&"124".to_string());
        assert!(result.is_ok());

        assert!(mergefile_1 < mergefile_2); // File 1 (123) < File 2 (124)

        // Increment File 1
        let result = mergefile_1.next();
        assert!(result.is_some());
        assert!(mergefile_1 == mergefile_2); // File 1 (124) == File 2 (124)

        // Increment File 1 again
        let result = mergefile_1.next();
        assert!(result.is_some());
        assert!(mergefile_1 > mergefile_2); // File 1 (125) > File 2 (124)

        // Increment File 2
        let result = mergefile_2.next();
        assert!(result.is_some());
        assert!(mergefile_1 == mergefile_2); // File 1 (125) == File 2 (125)

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
    }
}
