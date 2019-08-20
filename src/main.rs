use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use bitvec::vec::BitVec;
use bitvec::prelude as bv;
use byteorder;
use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};
use lru::LruCache;
use snap;
use uuid::Uuid;

#[derive(Clone, Eq, PartialEq)]
enum Type {
    Boolean,
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
}

impl PageData {
    fn from_ints(data: Vec<Option<i64>>) -> io::Result<PageData> {
        let mut nulls = BitVec::new();
        let mut bytes = vec![];
        for entry in data.iter() {
            nulls.push(entry.is_none());
            bytes.write_i64::<byteorder::LittleEndian>(entry.unwrap_or(0))?;
        }
        Ok(
            PageData {
                bytes: bytes,
                nulls: nulls,
            }
        )
    }

    fn get_int(&self, idx: usize) -> Option<i64> {
        if self.nulls[idx] {
            None
        } else {
            let mut slice = self.bytes.get(idx * 8..(idx + 1) * 8).unwrap();
            Some(slice.read_i64::<byteorder::LittleEndian>().unwrap())
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
    fn new(typ: Type, path: &Path, offset: usize, size: usize) -> PageMeta {
        PageMeta {
            id: Uuid::new_v4(),
            offset: offset,
            path: path.to_path_buf(),
            size: size,
            stats: PageStats::default(),
            typ: typ,
        }
    }

    fn load(&self) -> io::Result<PageData> {
        let mut file = File::open(&self.path)?;

        let mut size_bytes = [0; 8];
        file.read(&mut size_bytes)?;
        let size = byteorder::LittleEndian::read_u64(&size_bytes);

        let mut null_bytes = Vec::with_capacity(size as usize);
        file.read(&mut null_bytes)?;

        let mut bytes = vec![];
        let mut decompressed_file = snap::Reader::new(file);
        decompressed_file.read_to_end(&mut bytes)?;

        Ok(
            PageData {
                bytes: bytes,
                nulls: BitVec::from_slice(&null_bytes),
            }
        )
    }
}

type PageKey = (Uuid, Uuid);

struct Page {
    data: PageData,
    meta: PageMeta,
}

impl Page {
    fn get_int(&self, idx: usize) -> Option<i64> {
        assert!(self.meta.typ == Type::Int);
        self.data.get_int(idx)
    }
}

impl Page {
    fn new(meta: &PageMeta, data: PageData) -> Page {
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

    fn new() -> PageCache {
        PageCache {
            pages: LruCache::new(PageCache::SIZE)
        }
    }

    fn get(&mut self, key: &PageKey, meta: &PageMeta) -> &Page {
        if !self.pages.contains(key) {
            let data = meta.load().unwrap();
            self.pages.put(key.clone(), Page::new(meta, data));
        }
        self.pages.get(key).unwrap()
    }
}

struct Collection {
    id: Uuid,
    page_metas: BTreeMap<PageKey, PageMeta>,
    typ: Type,
}

impl Collection {
    fn get_int(&self, cache: &mut PageCache, idx: usize) -> Option<i64> {
        let mut count = 0;
        for (key, meta) in self.page_metas.iter() {
            if idx >= count && idx < count + meta.size {
                cache.get(key, meta).get_int(idx - count);
            }
            count += 1;
        }
        None
    }
}

fn write_int_page(path: &Path, offset: usize, data: Vec<Option<i64>>) -> io::Result<PageMeta> {
    let meta = PageMeta::new(Type::Int, path, offset, data.len());
    let mut file = File::create(path)?;

    let data = PageData::from_ints(data)?;
    let nulls_slice = data.nulls.as_slice();

    let mut size_bytes = [0; 8];
    byteorder::LittleEndian::write_u64(&mut size_bytes, nulls_slice.len() as u64);

    file.write_all(&size_bytes).unwrap();
    file.write_all(data.nulls.as_slice()).unwrap();

    let mut compressed_file = snap::Writer::new(file);
    compressed_file.write_all(&data.bytes).unwrap();

    Ok(meta)
}

fn main() -> io::Result<()> {
    let collection_id = Uuid::new_v4();
    let mut page_metas = BTreeMap::new();
    page_metas.insert((collection_id, Uuid::new_v4()),
                      write_int_page(Path::new("./example/1"), 0, vec![Some(2), None, Some(4)])?);
    page_metas.insert((collection_id, Uuid::new_v4()),
                      write_int_page(Path::new("./example/2"), 3, vec![None, Some(6), None])?);

    let collection = Collection {
        id: collection_id,
        page_metas: page_metas,
        typ: Type::Int,
    };
    let mut cache = PageCache::new();

    println!("0: {:?}", collection.get_int(&mut cache, 0));
    println!("1: {:?}", collection.get_int(&mut cache, 0));
    println!("2: {:?}", collection.get_int(&mut cache, 0));
    println!("3: {:?}", collection.get_int(&mut cache, 0));

    Ok(())
}
