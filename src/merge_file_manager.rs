use std::collections::BinaryHeap;
use std::io::{Error, ErrorKind};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::PathBuf;
use std::fmt;
use std::fs;
use std::io;
use glob;
use csv;

use merge_file::MergeFile;
use merge_file::Mergeable;
use settings::KeyType;

/// A MergeFile manager that maintains an internal cache and will perform the merge over all added files.
///
/// The idea is you add various files into the cache, configuring a delimiter and the column to perform
/// the merge on. Then you either write a new cache file to be used later, or you perform the merge.
pub struct MergeFileManager;

impl MergeFileManager {
    /// For the provided glob, we load all resolved files into an internal cache, returning the cache.
    ///
    /// # Examples
    ///
    /// ```
    /// # Provide a cache specialised for MergeFile<i32>
    /// let cache = MergeFileManager::load_from_glob("/data/files/*.csv", ',', 0, 0i32);
    /// ```
    pub fn retrieve_from_glob<T>(glob_choice: &str, delimiter: char, index: usize, default_key: T, key_type: KeyType) -> io::Result<HashMap<String, MergeFile<T>>>
        where T: Mergeable, T::Err: fmt::Debug {
        let mut cache: HashMap<String, MergeFile<T>> = HashMap::new();

        let glob_result = glob::glob(glob_choice);

        if glob_result.is_err() {
            return Err(Error::new(ErrorKind::Other, format!("Unable to perform glob over: {}",glob_choice)));
        }

        let mut glob_result = glob_result.unwrap();

        while let Some(Ok(path)) = glob_result.next() {
            debug!("Attempting to load path: {}", path.display());

            if let Some(path) = path.to_str() {
                if let Ok(merge_file) = MergeFile::new(path, delimiter, index, default_key.clone(), key_type.clone()) {
                    cache.insert(path.to_string(), merge_file);
                    debug!("Added {} to the cache successfully!", path);
                } else {
                    error!("We failed to load {} into the cache!", path);
                }
            } else {
                error!("Unable to convert path into unicode?");
            }
        }

        Ok(cache)
    }

    /// Loads a bunch of files into an internal cache that are returned from a pregenerated
    /// cache file from a previous invocation of this program. Returns the number of files
    /// the cache file loaded successfully.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut merge_manager = MergeFileManager::new();
    /// merge_manager.load_from_cache("/data/cache/file.cache", ',', 0);
    /// ```
    pub fn retrieve_from_cache<T>(filename: &PathBuf, default_key: T, key_type: KeyType) -> io::Result<HashMap<String, MergeFile<T>>>
        where T: Mergeable, T::Err: fmt::Debug {
        let mut cache: HashMap<String, MergeFile<T>> = HashMap::new();

        // Attempt to read the cache file
        let mut cache_reader = csv::Reader::from_file(filename).unwrap().has_headers(false);
        debug!("Opened cache file: {}", filename.display());

        #[derive(RustcDecodable,Debug)]
        struct CacheFileLine {
            filename: String,
            beginning_merge_key: String,
            ending_merge_key: String,
            delimiter: String,
            key_index: String,
            filesize: String,
        }

        // Iterate over cache file reading in and creating new CacheFileEntry instances
        for record in cache_reader.decode() {
            let record: CacheFileLine = record.unwrap();
            debug!("CacheFileLine Record: {:?}", record);

            // Check if the file is already in the cache
            if cache.get(&record.filename).is_some() {
                let metadata = try!(fs::metadata(filename));
                if metadata.len() == record.filesize.parse::<u64>().unwrap() {
                    // File is already in cache and filesize is the same, skip it
                    continue;
                }
            }

            let delimiter = match record.delimiter.as_ref() {
                "tsv" => '\t',
                "csv" => ',',
                "psv" => '|',
                // Assume it's a single character
                _ => {
                    warn!("Assuming delimiter is the first character in the field");
                    record.delimiter.chars().next().unwrap()
                },
            };

            // Add it into the cache if it isn't
            if let Ok(mut merge_file) = MergeFile::new(&record.filename,
                                                   delimiter,
                                                   record.key_index.parse::<usize>().unwrap(),
                                                   default_key.clone(),
                                                   key_type.clone()) {

                // Because the cache knows the ending_merge_key, set it as well
                // this will help if we're writing a new cache, as we can skip the fastforward
                merge_file.ending_merge_key = record.ending_merge_key.parse::<T>().unwrap();
                cache.insert(record.filename.clone(), merge_file);
                debug!("Added {} to the cache successfully!", record.filename);
            } else {
                error!("We failed to load {} into the cache!", record.filename);
            }
        }

        Ok(cache)
    }

    /// Consumes a HashMap<K,V> turning it into a Vec<V>
    pub fn cache_to_vec<T>(mut hashmap: HashMap<String, MergeFile<T>>) -> Vec<MergeFile<T>> {
        hashmap.drain().map(|(_, v)| v).collect()
    }

    /// Consumes a HashMap<K, MergeFile> and returns one with only existing MergeFile(s)
    pub fn fast_forward_cache<T>(mut cache: HashMap<String, MergeFile<T>>, merge_start: String) -> HashMap<String, MergeFile<T>>
        where T: Mergeable, T::Err: fmt::Debug {
        let mut files_to_delete: Vec<String> = vec!();

        for (_, merge_file) in cache.iter_mut() {
            if merge_file.fast_forward(&merge_start).is_err() {
                files_to_delete.push(merge_file.filename.clone());
            }
        }

        for filename in files_to_delete {
            info!("Removing file {} from cache", filename);
            cache.remove(&filename);
        }

        cache
    }

    /// Starts the k-way merge on the cache in its current state.
    /// It will forward all files to the start of the merge,
    ///
    /// # Examples
    ///
    /// ```
    /// let mut merge_manager = MergeFileManager::new();
    /// merge_manager.load_from_glob("/data/*.tsv", '\t', 0);
    /// merge_manager.begin_merge("zzz");
    /// ```
    pub fn begin_merge<T>(cache: HashMap<String, MergeFile<T>>, merge_end: Option<String>, print_merge_output: bool) -> Vec<MergeFile<T>>
        where T: Mergeable, T::Err: fmt::Debug {
        let mut heap = BinaryHeap::from(MergeFileManager::cache_to_vec(cache));
        let mut discarded = Vec::new();

        if merge_end.is_some() {
            let merge_end = merge_end.unwrap().parse::<T>().unwrap();
            info!("Beginning merge -> {}", merge_end);

            while let Some(mut next_file) = heap.pop() {
                // Report on the line or EOF the file and add it to the discarded pile
                if let Some(result) = next_file.next() {
                    // Check if the line has exceeded the merge_end key
                    if result > merge_end {
                        info!("MergeFile<{}> has hit end bound ({}>{}), discarding from cache", next_file.filename, result, merge_end);
                        discarded.push(next_file);
                    } else {
                        // Print the line (if required) then push the MergeFile back into the heap
                        if print_merge_output {
                            println!("{}", next_file.line);
                        }

                        heap.push(next_file);
                    }
                } else {
                    info!("We hit EOF for {} with a final merge key of {}", next_file.filename, next_file.ending_merge_key);
                    discarded.push(next_file);
                }
            }
        } else {
            info!("Beginning merge -> EOF");

            while let Some(mut next_file) = heap.pop() {
                // Report on the line or EOF the file and add it to the discarded pile
                if let Some(_) = next_file.next() {
                    if print_merge_output {
                        println!("{}", next_file.line);
                    }

                    heap.push(next_file);
                } else {
                    info!("We hit EOF for {} with a final merge key of {}", next_file.filename, next_file.ending_merge_key);
                    discarded.push(next_file);
                }
            }
        }


        discarded
    }

    /// Consumes the cache, turning it into a sorted vector.
    /// It then fast forwards each file and writes it out into the cache file.
    /// The cache file layout is: file_name, mergekey_start, mergekey_end, file_size
    ///
    /// # Examples
    ///
    /// ```
    /// let mut merge_manager = MergeFileManager::new();
    /// let cache = merge_manager.load_from_glob("/data/*.tsv", '\t', 0);
    /// merge_manager.write_cache("/data/caches/data.cache".to_string(), cache);
    /// ```
    pub fn write_cache<T>(filename: &PathBuf, cache: HashMap<String, MergeFile<T>>, default_key: T) -> Result<String, String>
        where T: Mergeable, T::Err: fmt::Debug {
        info!("Writing out cache to disk => {}!", filename.display());

        // Open the file
        let mut cache_writer = csv::Writer::from_file(filename)
                                            .unwrap()
                                            .delimiter(b',');

        // Drain the cache into a vec, sort it, then write its contents out to disk
        let mut merge_files = MergeFileManager::cache_to_vec(cache);
        merge_files.sort();

        for mut merge_file in merge_files {

            if merge_file.ending_merge_key == default_key {
                info!("MergeFile {} was loaded from glob, fastwarding to EOF", &merge_file);
                merge_file.fast_forward_to_end();
            } else {
                info!("MergeFile {} was loaded from cache, skipping fastforward", &merge_file);
            }

            let pretty_delimiter = match merge_file.delimiter {
                '\t' => "tsv".to_string(),
                ',' => "csv".to_string(),
                '|' => "psv".to_string(),
                _   => merge_file.delimiter.to_string(),
            };

            let cache_line = [
                merge_file.filename,
                merge_file.beginning_merge_key.to_string(),
                merge_file.ending_merge_key.to_string(),
                pretty_delimiter,
                merge_file.key_index.to_string(),
                merge_file.filesize.to_string()
            ];

            cache_writer.write(cache_line.iter()).unwrap();
        }

        cache_writer.flush().unwrap();
        Ok("Written cache out to disk.".to_string())
    }
}


#[cfg(test)]
mod tests {
    use std::io::prelude::*;
    use std::io::BufWriter;
    use std::path::PathBuf;
    use std::fs::File;
    use std::fs;

    use super::MergeFileManager;
    use merge_file::MergeFile;
    use settings::KeyType;

    fn create_file(filename: &str, contents: String) {
        let mut temp_file = BufWriter::new(File::create(PathBuf::from(filename)).unwrap());
        temp_file.write(contents.as_ref()).unwrap();
        let _ = temp_file.flush();
    }

    #[test]
    fn new_merge_file() {
        // Set up the test data
        // TODO: Add the PID of the process into the filename
        let test_filename_1 = "/tmp/test_add_file.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_add_file.file2.csv";
        let test_contents_2 = format!("{},{},{}\n\
                                       {},{},{}\n\
                                       {},{},{}\n",
                                        "123", "aaa", "888",
                                        "124", "aaa", "888",
                                        "127", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        // Add the first file and sanity check
        let result = MergeFile::new(&test_filename_1, '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mergefile = result.unwrap();
        assert_eq!(mergefile.filename, test_filename_1);
        assert_eq!(mergefile.current_merge_key, "123");

        // Add the second file and sanity check
        let result = MergeFile::new(&test_filename_2, ',', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let mergefile = result.unwrap();
        assert_eq!(mergefile.filename, test_filename_2);
        assert_eq!(mergefile.current_merge_key, "123");

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);

    }

    #[test]
    fn retrieve_from_glob() {
        let test_filename_1 = "/tmp/test_retrieve_from_glob.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n
                                       {}\t{}\t{}\n
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_retrieve_from_glob.file2.tsv";
        let test_contents_2 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "aaa", "888",
                                        "124", "aaa", "888",
                                        "127", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        // Load a glob with a single file into the cache
        let result = MergeFileManager::retrieve_from_glob("/tmp/test_retrieve_from_glob.file1.tsv", '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let merge_files = result.unwrap();
        assert_eq!(merge_files.len(), 1);
        assert!(merge_files.values().any(|x|x.filename == test_filename_1));

        // Load a glob with a single file into the cache
        let result = MergeFileManager::retrieve_from_glob("/tmp/test_retrieve_from_glob.file?.tsv", '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let merge_files = result.unwrap();
        assert_eq!(merge_files.len(), 2);
        assert!(merge_files.values().any(|x|x.filename == test_filename_1));
        assert!(merge_files.values().any(|x|x.filename == test_filename_2));

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
    }

    #[test]
    fn retrieve_from_cache() {
        let test_filename_1 = "/tmp/test_retrieve_from_cache.file1.tsv";
        let test_contents_1 = format!("{key_1}\t{foo}\t{bar}\n\
                                       {key_2}\t{foo}\t{bar}\n\
                                       {key_3}\t{foo}\t{bar}\n",
                                       key_1="123", key_2="124", key_3="125", foo="bbb", bar="999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_retrieve_from_cache.file2.tsv";
        let test_contents_2 = format!("{key_1}\t{foo}\t{bar}\n\
                                       {key_2}\t{foo}\t{bar}\n\
                                       {key_3}\t{foo}\t{bar}\n",
                                       key_1="123", key_2="124", key_3="127", foo="aaa", bar="888");

        create_file(test_filename_2, test_contents_2);

        let cache_filename = "/tmp/test_retrieve_from_cache.cache";
        let cache_contents = format!(
            "{},{},{},{},{},{}\n\
             {},{},{},{},{},{}\n",
            test_filename_1, "", "", '\t', 0, "",
            test_filename_2, "", "", '\t', 0, ""
        );

        create_file(&cache_filename, cache_contents);

        let cache_path = PathBuf::from(&cache_filename);
        let result = MergeFileManager::retrieve_from_cache(&cache_path, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let merge_files = result.unwrap();
        assert_eq!(merge_files.len(), 2);
        assert!(merge_files.values().any(|x|x.filename == test_filename_1));
        assert!(merge_files.values().any(|x|x.filename == test_filename_2));

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
        let _ = fs::remove_file(cache_filename);
    }

    #[test]
    fn cache_to_vec() {
        // Build up a cache
        let test_filename_1 = "/tmp/test_cache_to_vec.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_cache_to_vec.file2.tsv";
        let test_contents_2 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "aaa", "888",
                                        "124", "aaa", "888",
                                        "127", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        let result = MergeFileManager::retrieve_from_glob("/tmp/test_cache_to_vec.file?.tsv", '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());
        let cache = result.unwrap();

        // Create the vec and ensure it only contains the two elements from above
        let test_vec = MergeFileManager::cache_to_vec(cache);

        assert_eq!(test_vec.len(), 2);
        assert!(test_vec.iter().any(|x|x.filename == test_filename_1));
        assert!(test_vec.iter().any(|x|x.filename == test_filename_2));

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
    }

    #[test]
    fn begin_merge() {
        //pub fn begin_merge(mut cache: HashMap<String, MergeFile>, merge_start: &String, merge_end: &String, print_merge_output: bool) {
        let test_filename_1 = "/tmp/test_begin_merge.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_begin_merge.file2.tsv";
        let test_contents_2 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "aaa", "888",
                                        "124", "aaa", "888",
                                        "127", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        // Load a glob with a single file into the cache
        let result = MergeFileManager::retrieve_from_glob("/tmp/test_begin_merge.file?.tsv", '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());
        let cache = result.unwrap();

        let initial_cache_len = cache.len();
        assert_eq!(initial_cache_len, 2);

        let merge_start = "124".to_string();
        let merge_end = "126".to_string();

        let cache = MergeFileManager::fast_forward_cache(cache, merge_start);
        let discarded = MergeFileManager::begin_merge(cache, Some(merge_end.clone()), false);

        // Both original files should exist and have correct final merge keys
        assert_eq!(initial_cache_len, discarded.len());
        assert!(discarded.iter().any(|x|x.filename == test_filename_1 && x.ending_merge_key <= merge_end));
        assert!(discarded.iter().any(|x|x.filename == test_filename_2 && x.ending_merge_key <= merge_end));

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
    }

    #[test]
    fn write_cache() {
        let test_filename_1 = "/tmp/test_write_cache.file1.tsv";
        let test_contents_1 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "bbb", "999",
                                        "124", "bbb", "999",
                                        "125", "bbb", "999");

        create_file(test_filename_1, test_contents_1);

        let test_filename_2 = "/tmp/test_write_cache.file2.tsv";
        let test_contents_2 = format!("{}\t{}\t{}\n\
                                       {}\t{}\t{}\n\
                                       {}\t{}\t{}\n",
                                        "123", "aaa", "888",
                                        "124", "aaa", "888",
                                        "127", "aaa", "888");

        create_file(test_filename_2, test_contents_2);

        // Load a glob with a single file into the cache
        let result = MergeFileManager::retrieve_from_glob("/tmp/test_write_cache.file?.tsv", '\t', 0, "0".to_string(), KeyType::String);
        assert!(result.is_ok());
        let cache = result.unwrap();

        assert_eq!(cache.len(), 2);

        let test_cache_filename = "/tmp/test_cache.cache";
        let test_cache_path = PathBuf::from(&test_cache_filename);
        let result = MergeFileManager::write_cache(&test_cache_path, cache, "0".to_string());
        assert!(result.is_ok());

        let result = MergeFileManager::retrieve_from_cache(&test_cache_path, "0".to_string(), KeyType::String);
        assert!(result.is_ok());

        let merge_files = result.unwrap();
        assert_eq!(merge_files.len(), 2);

        let _ = fs::remove_file(test_filename_1);
        let _ = fs::remove_file(test_filename_2);
        let _ = fs::remove_file(test_cache_filename);
    }
}
