use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::Path;

use env_logger;
use lru::LruCache;
use uuid::Uuid;

mod page;

use page::{Page, PageData, PageKey, PageMeta, PageReader, PageWriter, Type};

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
            self.pages.put(key.clone(), PageReader::read(&meta)?);
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

fn test_bools(cache: &mut PageCache) -> io::Result<()> {
    let page_metas = vec![
        PageMeta::new(Type::Bool, &Path::new("./example/bool_1"), 0, 3),
        PageMeta::new(Type::Bool, &Path::new("./example/bool_2"), 3, 3),
    ];

    let pages = [
        Page::new(&page_metas[0], PageData::from_bools(&[Some(true), None, Some(true)])?),
        Page::new(&page_metas[1], PageData::from_bools(&[None, Some(false), Some(false)])?),
    ];

    let collection = Collection::new(page_metas);

    PageWriter::write(&pages[0])?;
    PageWriter::write(&pages[1])?;

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
        PageMeta::new(Type::Int, &Path::new("./example/int_1"), 0, 3),
        PageMeta::new(Type::Int, &Path::new("./example/int_2"), 3, 3),
    ];

    let pages = [
        Page::new(&page_metas[0], PageData::from_ints(&[Some(2), None, Some(4)])?),
        Page::new(&page_metas[0], PageData::from_ints(&[None, Some(6), None])?),
    ];

    let collection = Collection::new(page_metas);

    PageWriter::write(&pages[0])?;
    PageWriter::write(&pages[1])?;

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
        PageMeta::new(Type::Float, &Path::new("./example/float_1"), 0, 3),
        PageMeta::new(Type::Float, &Path::new("./example/float_2"), 3, 3),
    ];

    let pages = vec![
        Page::new(&page_metas[0], PageData::from_floats(&[Some(1.2), None, Some(4.5)])?),
        Page::new(&page_metas[1], PageData::from_floats(&[None, Some(-6.1), None])?),
    ];

    let collection = Collection::new(page_metas);

    PageWriter::write(&pages[0])?;
    PageWriter::write(&pages[1])?;

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
        PageMeta::new(Type::String, &Path::new("./example/string_1"), 0, 3),
        PageMeta::new(Type::String, &Path::new("./example/string_2"), 3, 3),
    ];

    let pages = [
        Page::new(&page_metas[0], PageData::from_strings(&[Some("abc"), None, Some("def")])?),
        Page::new(&page_metas[1], PageData::from_strings(&[None, Some(""), None])?),
    ];

    let collection = Collection::new(page_metas);

    PageWriter::write(&pages[0])?;
    PageWriter::write(&pages[1])?;

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
