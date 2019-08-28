use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use bitvec::prelude as bv;
use bitvec::vec::BitVec;
use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt};
use log::debug;
use uuid::Uuid;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum Type {
    Bool,
    Int,
    Float,
    String,
}

#[derive(Clone)]
pub struct Bound<T: PartialOrd> {
    min: T,
    max: T,
}

pub struct PageData {
    bytes: Vec<u8>,
    nulls: BitVec<bv::LittleEndian, u8>,
    offsets: Vec<usize>,
    typ: Type,
}

impl PageData {
    pub fn from_bools(data: &[Option<bool>]) -> io::Result<PageData> {
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

    pub fn from_ints(data: &[Option<i64>]) -> io::Result<PageData> {
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

    pub fn from_floats(data: &[Option<f64>]) -> io::Result<PageData> {
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

    pub fn from_strings(data: &[Option<&str>]) -> io::Result<PageData> {
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

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        if self.nulls[idx] {
            None
        } else {
            let bits = BitVec::<bv::LittleEndian, u8>::from_slice(&self.bytes);
            bits.get(idx)
        }
    }

    pub fn get_int(&self, idx: usize) -> Option<i64> {
        if self.nulls[idx] {
            None
        } else {
            let mut slice = self.bytes.get(idx * 8..(idx + 1) * 8).unwrap();
            Some(slice.read_i64::<byteorder::LittleEndian>().unwrap())
        }
    }

    pub fn get_float(&self, idx: usize) -> Option<f64> {
        if self.nulls[idx] {
            None
        } else {
            let mut slice = self.bytes.get(idx * 8..(idx + 1) * 8).unwrap();
            Some(slice.read_f64::<byteorder::LittleEndian>().unwrap())
        }
    }

    pub fn get_string(&self, idx: usize) -> Option<String> {
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
pub struct PageStats {
    contains_nulls: bool,
    int_bound: Option<Bound<usize>>,
    float_bound: Option<Bound<f64>>,
    string_bound: Option<Bound<String>>,
}

#[derive(Clone)]
pub struct PageMeta {
    pub id: Uuid,
    pub path: PathBuf,
    pub size: usize,
    pub typ: Type,
    offset: usize,
    stats: PageStats,
}

impl PageMeta {
    pub fn new(typ: Type, path: &Path, offset: usize, size: usize) -> Self {
        PageMeta {
            id: Uuid::new_v4(),
            offset: offset,
            path: path.to_path_buf(),
            size: size,
            stats: PageStats::default(),
            typ: typ,
        }
    }
}

pub type PageKey = (Uuid, usize);

pub struct Page {
    data: PageData,
    meta: PageMeta,
}

impl Page {
    pub fn new(meta: &PageMeta, data: PageData) -> Self {
        Page {
            data: data,
            meta: meta.clone(),
        }
    }

    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        assert!(self.meta.typ == Type::Bool);
        self.data.get_bool(idx)
    }

    pub fn get_int(&self, idx: usize) -> Option<i64> {
        assert!(self.meta.typ == Type::Int);
        self.data.get_int(idx)
    }

    pub fn get_float(&self, idx: usize) -> Option<f64> {
        assert!(self.meta.typ == Type::Float);
        self.data.get_float(idx)
    }

    pub fn get_string(&self, idx: usize) -> Option<String> {
        assert!(self.meta.typ == Type::String);
        self.data.get_string(idx)
    }
}

pub struct PageReader {}

impl PageReader {
    pub fn read(meta: &PageMeta) -> io::Result<Page> {
        debug!("loading page: {:?}", meta.path);
        let mut file = File::open(&meta.path)?;

        let mut size_bytes = [0; 8];
        file.read(&mut size_bytes)?;
        let size = byteorder::LittleEndian::read_u64(&size_bytes);

        let mut null_bytes = vec![0; size as usize];
        file.read(&mut null_bytes)?;
        let nulls = BitVec::from_slice(&null_bytes);

        let mut offsets = vec![];
        if meta.typ == Type::String {
            let mut offset_bytes = vec![0; (meta.size + 1) * 8];
            file.read(&mut offset_bytes)?;
            offsets = offset_bytes
                .chunks(8)
                .map(|word| byteorder::LittleEndian::read_u64(word) as usize)
                .collect();
        }

        let mut bytes = vec![];
        let mut decompressed_file = snap::Reader::new(file);
        decompressed_file.read_to_end(&mut bytes)?;

        Ok(Page::new(
            meta,
            PageData {
                bytes: bytes,
                nulls: nulls,
                offsets: offsets,
                typ: meta.typ,
            },
        ))
    }
}

pub struct PageWriter {}

impl PageWriter {
    pub fn write(page: &Page) -> io::Result<()> {
        let mut file = File::create(&page.meta.path)?;

        PageWriter::write_nulls(&mut file, &page.data)?;
        PageWriter::write_offsets(&mut file, &page.data)?;

        let mut compressed_file = snap::Writer::new(file);
        compressed_file.write_all(&page.data.bytes).unwrap();
        Ok(())
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
