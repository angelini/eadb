use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use bitvec::prelude as bv;
use bitvec::vec::BitVec;
use byteorder;
use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};
use env_logger;
use log::debug;
use lru::LruCache;
use snap;
use uuid::Uuid;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum Type {
    Bool,
    Int,
    Float,
    String,
}

#[derive(Clone)]
struct Bound<T: PartialOrd> {
    min: T,
    max: T,
}

struct PageData {
    bytes: Vec<u8>,
    nulls: BitVec<bv::LittleEndian, u8>,
    offsets: Vec<usize>,
    typ: Type,
}

impl PageData {
    fn from_bools(data: &[Option<bool>]) -> io::Result<PageData> {
        let mut bits = BitVec::<bv::LittleEndian, u8>::new();
        let mut nulls = BitVec::new();

        for entry in data.iter() {
            bits.push(entry.unwrap_or(false));
            nulls.push(entry.is_none());
        }
        Ok(PageData {
            bytes: bits.as_slice().to_vec(),
            nulls: nulls,
            offsets: vec![],
            typ: Type::Bool,
        })
    }

    fn from_ints(data: &[Option<i64>]) -> io::Result<PageData> {
        let mut bytes = vec![];
        let mut nulls = BitVec::new();

        for entry in data.iter() {
            bytes.write_i64::<byteorder::LittleEndian>(entry.unwrap_or(0))?;
            nulls.push(entry.is_none());
        }
        Ok(PageData {
            bytes: bytes,
            nulls: nulls,
            offsets: vec![],
            typ: Type::Int,
        })
    }

    fn from_floats(data: &[Option<f64>]) -> io::Result<PageData> {
        let mut nulls = BitVec::new();
        let mut bytes = vec![];
        for entry in data.iter() {
            nulls.push(entry.is_none());
            bytes.write_f64::<byteorder::LittleEndian>(entry.unwrap_or(0.0))?;
        }
        Ok(PageData {
            bytes: bytes,
            nulls: nulls,
            offsets: vec![],
            typ: Type::Float,
        })
    }

    fn from_strings(data: &[Option<&str>]) -> io::Result<PageData> {
        let mut bytes = vec![];
        let mut nulls = BitVec::new();
        let mut offset = 0;
        let mut offsets = vec![];

        for entry in data.iter() {
            let value = entry.unwrap_or("");
            bytes.extend(value.bytes());
            nulls.push(entry.is_none());
            offsets.push(offset);
            offset += value.len();
        }
        offsets.push(offset);

        Ok(PageData {
            bytes: bytes,
            nulls: nulls,
            offsets: offsets,
            typ: Type::String,
        })
    }

    fn len(&self) -> usize {
        self.nulls.len()
    }

    fn get_bool(&self, idx: usize) -> Option<bool> {
        if self.nulls[idx] {
            None
        } else {
            let bits = BitVec::<bv::LittleEndian, u8>::from_slice(&self.bytes);
            bits.get(idx)
        }
    }

    fn get_int(&self, idx: usize) -> Option<i64> {
        if self.nulls[idx] {
            None
        } else {
            let mut slice = self.bytes.get(idx * 8..(idx + 1) * 8).unwrap();
            Some(slice.read_i64::<byteorder::LittleEndian>().unwrap())
        }
    }

    fn get_float(&self, idx: usize) -> Option<f64> {
        if self.nulls[idx] {
            None
        } else {
            let mut slice = self.bytes.get(idx * 8..(idx + 1) * 8).unwrap();
            Some(slice.read_f64::<byteorder::LittleEndian>().unwrap())
        }
    }

    fn get_string(&self, idx: usize) -> Option<String> {
        if self.nulls[idx] {
            None
        } else {
            let slice = self
                .bytes
                .get(self.offsets[idx]..self.offsets[idx + 1])
                .unwrap();
            Some(String::from_utf8(slice.to_vec()).unwrap())
        }
    }
}

#[derive(Clone, Default)]
struct PageStats {
    contains_nulls: bool,
    int_bound: Option<Bound<usize>>,
    float_bound: Option<Bound<f64>>,
    string_bound: Option<Bound<String>>,
}

#[derive(Clone)]
struct PageMeta {
    id: Uuid,
    offset: usize,
    path: PathBuf,
    size: usize,
    stats: PageStats,
    typ: Type,
}

impl PageMeta {
    fn new(typ: Type, path: &Path, offset: usize, size: usize) -> Self {
        PageMeta {
            id: Uuid::new_v4(),
            offset: offset,
            path: path.to_path_buf(),
            size: size,
            stats: PageStats::default(),
            typ: typ,
        }
    }

    fn load_page(&self) -> io::Result<PageData> {
        debug!("loading page: {:?}", self.path);
        let mut file = File::open(&self.path)?;

        let mut size_bytes = [0; 8];
        file.read(&mut size_bytes)?;
        let size = byteorder::LittleEndian::read_u64(&size_bytes);

        let mut null_bytes = vec![0; size as usize];
        file.read(&mut null_bytes)?;
        let nulls = BitVec::from_slice(&null_bytes);

        let mut offsets = vec![];
        if self.typ == Type::String {
            let mut offset_bytes = vec![0; (self.size + 1) * 8];
            file.read(&mut offset_bytes)?;
            offsets = offset_bytes
                .chunks(8)
                .map(|word| byteorder::LittleEndian::read_u64(word) as usize)
                .collect();
        }

        let mut bytes = vec![];
        let mut decompressed_file = snap::Reader::new(file);
        decompressed_file.read_to_end(&mut bytes)?;

        Ok(PageData {
            bytes: bytes,
            nulls: nulls,
            offsets: offsets,
            typ: self.typ,
        })
    }
}

type PageKey = (Uuid, usize);

struct Page {
    data: PageData,
    meta: PageMeta,
}

impl Page {
    fn get_bool(&self, idx: usize) -> Option<bool> {
        assert!(self.meta.typ == Type::Bool);
        self.data.get_bool(idx)
    }

    fn get_int(&self, idx: usize) -> Option<i64> {
        assert!(self.meta.typ == Type::Int);
        self.data.get_int(idx)
    }

    fn get_float(&self, idx: usize) -> Option<f64> {
        assert!(self.meta.typ == Type::Float);
        self.data.get_float(idx)
    }

    fn get_string(&self, idx: usize) -> Option<String> {
        assert!(self.meta.typ == Type::String);
        self.data.get_string(idx)
    }
}

impl Page {
    fn new(meta: &PageMeta, data: PageData) -> Self {
        Page {
            data: data,
            meta: meta.clone(),
        }
    }
}

struct PageCache {
    pages: LruCache<PageKey, Page>,
}

impl PageCache {
    const SIZE: usize = 256;

    fn new() -> Self {
        PageCache {
            pages: LruCache::new(PageCache::SIZE),
        }
    }

    fn get(&mut self, key: &PageKey, meta: &PageMeta) -> io::Result<&Page> {
        if !self.pages.contains(key) {
            let data = meta.load_page()?;
            self.pages.put(key.clone(), Page::new(meta, data));
        }
        Ok(self.pages.get(key).unwrap())
    }
}

struct Collection {
    id: Uuid,
    page_metas: BTreeMap<PageKey, PageMeta>,
    size: usize,
    typ: Type,
}

impl Collection {
    fn new(page_metas: Vec<PageMeta>) -> Self {
        let typ = {
            let mut types = page_metas
                .iter()
                .map(|meta| meta.typ)
                .collect::<HashSet<Type>>()
                .into_iter();
            let t = types.next();
            assert!(t.is_some() && types.next().is_none());
            t.unwrap()
        };

        let id = Uuid::new_v4();
        let size = page_metas.iter().fold(0, |acc, meta| acc + meta.size);
        Collection {
            id: id,
            page_metas: page_metas
                .into_iter()
                .enumerate()
                .map(|(page_idx, meta)| ((id, page_idx), meta))
                .collect(),
            size: size,
            typ: typ,
        }
    }

    fn get_bool(&self, cache: &mut PageCache, idx: usize) -> Option<bool> {
        self.find_page(cache, idx)
            .and_then(|(page, offset)| page.get_bool(idx - offset))
    }

    fn get_int(&self, cache: &mut PageCache, idx: usize) -> Option<i64> {
        self.find_page(cache, idx)
            .and_then(|(page, offset)| page.get_int(idx - offset))
    }

    fn get_float(&self, cache: &mut PageCache, idx: usize) -> Option<f64> {
        self.find_page(cache, idx)
            .and_then(|(page, offset)| page.get_float(idx - offset))
    }

    fn get_string<'a>(&self, cache: &'a mut PageCache, idx: usize) -> Option<String> {
        self.find_page(cache, idx)
            .and_then(|(page, offset)| page.get_string(idx - offset))
    }

    fn bool_iter<'a>(&'a self, cache: &'a mut PageCache) -> CollectionBoolIter<'a> {
        CollectionBoolIter::new(cache, self)
    }

    fn int_iter<'a>(&'a self, cache: &'a mut PageCache) -> CollectionIntIter<'a> {
        CollectionIntIter::new(cache, self)
    }

    fn float_iter<'a>(&'a self, cache: &'a mut PageCache) -> CollectionFloatIter<'a> {
        CollectionFloatIter::new(cache, self)
    }

    fn string_iter<'a>(&'a self, cache: &'a mut PageCache) -> CollectionStringIter<'a> {
        CollectionStringIter::new(cache, self)
    }

    fn find_page<'a>(&self, cache: &'a mut PageCache, idx: usize) -> Option<(&'a Page, usize)> {
        for (key, meta) in self.page_metas.iter() {
            let offset = key.1 * meta.size;
            if idx >= offset && idx < offset + meta.size {
                return Some((
                    cache
                        .get(key, meta)
                        .expect(&format!("Cannot load page {:?} {:?}", key, meta.path)),
                    offset,
                ));
            }
        }
        None
    }
}

struct CollectionBoolIter<'a> {
    idx: usize,
    cache: &'a mut PageCache,
    collection: &'a Collection,
}

impl<'a> CollectionBoolIter<'a> {
    fn new(cache: &'a mut PageCache, collection: &'a Collection) -> Self {
        CollectionBoolIter {
            idx: 0,
            cache: cache,
            collection: collection,
        }
    }
}

impl<'a> Iterator for CollectionBoolIter<'a> {
    type Item = Option<bool>;

    fn next(&mut self) -> Option<Option<bool>> {
        if self.idx == self.collection.size {
            return None;
        }

        let entry = self.collection.get_bool(self.cache, self.idx);
        self.idx += 1;
        Some(entry)
    }
}

struct CollectionIntIter<'a> {
    idx: usize,
    cache: &'a mut PageCache,
    collection: &'a Collection,
}

impl<'a> CollectionIntIter<'a> {
    fn new(cache: &'a mut PageCache, collection: &'a Collection) -> Self {
        CollectionIntIter {
            idx: 0,
            cache: cache,
            collection: collection,
        }
    }
}

impl<'a> Iterator for CollectionIntIter<'a> {
    type Item = Option<i64>;

    fn next(&mut self) -> Option<Option<i64>> {
        if self.idx == self.collection.size {
            return None;
        }

        let entry = self.collection.get_int(self.cache, self.idx);
        self.idx += 1;
        Some(entry)
    }
}

struct CollectionFloatIter<'a> {
    idx: usize,
    cache: &'a mut PageCache,
    collection: &'a Collection,
}

impl<'a> CollectionFloatIter<'a> {
    fn new(cache: &'a mut PageCache, collection: &'a Collection) -> Self {
        CollectionFloatIter {
            idx: 0,
            cache: cache,
            collection: collection,
        }
    }
}

impl<'a> Iterator for CollectionFloatIter<'a> {
    type Item = Option<f64>;

    fn next(&mut self) -> Option<Option<f64>> {
        if self.idx == self.collection.size {
            return None;
        }

        let entry = self.collection.get_float(self.cache, self.idx);
        self.idx += 1;
        Some(entry)
    }
}

struct CollectionStringIter<'a> {
    idx: usize,
    cache: &'a mut PageCache,
    collection: &'a Collection,
}

impl<'a> CollectionStringIter<'a> {
    fn new(cache: &'a mut PageCache, collection: &'a Collection) -> Self {
        CollectionStringIter {
            idx: 0,
            cache: cache,
            collection: collection,
        }
    }
}

impl<'a> Iterator for CollectionStringIter<'a> {
    type Item = Option<String>;

    fn next(&mut self) -> Option<Option<String>> {
        if self.idx == self.collection.size {
            return None;
        }

        let entry = self.collection.get_string(self.cache, self.idx);
        self.idx += 1;
        Some(entry)
    }
}

struct PageWriter {}

impl PageWriter {
    fn write(path: &Path, offset: usize, data: &PageData) -> io::Result<PageMeta> {
        let meta = PageMeta::new(data.typ, path, offset, data.len());
        let mut file = File::create(path)?;

        PageWriter::write_nulls(&mut file, &data)?;
        PageWriter::write_offsets(&mut file, &data)?;

        let mut compressed_file = snap::Writer::new(file);
        compressed_file.write_all(&data.bytes).unwrap();

        Ok(meta)
    }

    fn write_nulls(file: &mut File, data: &PageData) -> io::Result<()> {
        let nulls_slice = data.nulls.as_slice();

        let mut size_bytes = [0; 8];
        byteorder::LittleEndian::write_u64(&mut size_bytes, nulls_slice.len() as u64);

        file.write_all(&size_bytes)?;
        file.write_all(data.nulls.as_slice())?;

        Ok(())
    }

    fn write_offsets(file: &mut File, data: &PageData) -> io::Result<()> {
        let mut bytes = [0; 8];
        for offset in &data.offsets {
            byteorder::LittleEndian::write_u64(&mut bytes, *offset as u64);
            file.write(&bytes)?;
        }
        Ok(())
    }
}

fn test_bools(cache: &mut PageCache) -> io::Result<()> {
    let page_metas = vec![
        PageWriter::write(
            Path::new("./example/bool_1"),
            0,
            &PageData::from_bools(&[Some(true), None, Some(true)])?,
        )?,
        PageWriter::write(
            Path::new("./example/bool_2"),
            3,
            &PageData::from_bools(&[None, Some(false), Some(false)])?,
        )?,
    ];
    let collection = Collection::new(page_metas);

    println!("0: {:?}", collection.get_bool(cache, 0));
    println!("1: {:?}", collection.get_bool(cache, 1));
    println!("2: {:?}", collection.get_bool(cache, 2));
    println!("3: {:?}", collection.get_bool(cache, 3));

    println!("---");

    for entry in collection.bool_iter(cache) {
        println!("entry: {:?}", entry);
    }

    Ok(())
}

fn test_ints(cache: &mut PageCache) -> io::Result<()> {
    let page_metas = vec![
        PageWriter::write(
            Path::new("./example/int_1"),
            0,
            &PageData::from_ints(&[Some(2), None, Some(4)])?,
        )?,
        PageWriter::write(
            Path::new("./example/int_2"),
            3,
            &PageData::from_ints(&[None, Some(6), None])?,
        )?,
    ];
    let collection = Collection::new(page_metas);

    println!("0: {:?}", collection.get_int(cache, 0));
    println!("1: {:?}", collection.get_int(cache, 1));
    println!("2: {:?}", collection.get_int(cache, 2));
    println!("3: {:?}", collection.get_int(cache, 3));

    println!("---");

    for entry in collection.int_iter(cache) {
        println!("entry: {:?}", entry);
    }

    Ok(())
}

fn test_floats(cache: &mut PageCache) -> io::Result<()> {
    let page_metas = vec![
        PageWriter::write(
            Path::new("./example/float_1"),
            0,
            &PageData::from_floats(&[Some(1.2), None, Some(4.5)])?,
        )?,
        PageWriter::write(
            Path::new("./example/float_2"),
            3,
            &PageData::from_floats(&[None, Some(-6.1), None])?,
        )?,
    ];
    let collection = Collection::new(page_metas);

    println!("0: {:?}", collection.get_float(cache, 0));
    println!("1: {:?}", collection.get_float(cache, 1));
    println!("2: {:?}", collection.get_float(cache, 2));
    println!("3: {:?}", collection.get_float(cache, 3));

    println!("---");

    for entry in collection.float_iter(cache) {
        println!("entry: {:?}", entry);
    }

    Ok(())
}

fn test_strings(cache: &mut PageCache) -> io::Result<()> {
    let page_metas = vec![
        PageWriter::write(
            Path::new("./example/string_1"),
            0,
            &PageData::from_strings(&[Some("abc"), None, Some("def")])?,
        )?,
        PageWriter::write(
            Path::new("./example/string_2"),
            3,
            &PageData::from_strings(&[None, Some(""), None])?,
        )?,
    ];
    let collection = Collection::new(page_metas);

    println!("0: {:?}", collection.get_string(cache, 0));
    println!("1: {:?}", collection.get_string(cache, 1));
    println!("2: {:?}", collection.get_string(cache, 2));
    println!("3: {:?}", collection.get_string(cache, 3));

    println!("---");

    for entry in collection.string_iter(cache) {
        println!("entry: {:?}", entry);
    }

    Ok(())
}

fn main() -> io::Result<()> {
    env_logger::init();

    let mut cache = PageCache::new();
    test_bools(&mut cache)?;
    test_ints(&mut cache)?;
    test_floats(&mut cache)?;
    test_strings(&mut cache)?;

    Ok(())
}
