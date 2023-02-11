use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::{FileMetaData, PageHeader, Type};

use std::{fs::File, result::Result};
use std::io::{Read, Seek, SeekFrom, Error};

const STARTING_READ_SIZE: usize = 1024;
const MAGIC_NUMBER : [u8; 4] = [b'P', b'A', b'R', b'1'];

fn read_footer(reader: &mut File) -> Result<FileMetaData, Box<dyn std::error::Error>> {
    // TODO lets avoid reading twice by reading a larger page to start.
    let mut buffer = Vec::with_capacity(STARTING_READ_SIZE);

    reader.seek(SeekFrom::End(-(STARTING_READ_SIZE as i64)))?;

    reader
        .take(STARTING_READ_SIZE as u64)
        .read_to_end(&mut buffer)?;

    assert!(buffer[buffer.len()-4..] == MAGIC_NUMBER);

    let file_metadata_size = u32::from_le_bytes(buffer[buffer.len() - 8..buffer.len() - 4].try_into()?);

    reader.seek(SeekFrom::End(-8 -(file_metadata_size as i64)))?;

    let metadata: &[u8];

    if file_metadata_size as usize > buffer.capacity() {
        // if the buffer is not big enough lets reserver more capacity
        buffer.try_reserve((file_metadata_size as usize) - STARTING_READ_SIZE)?;
        buffer.resize(file_metadata_size as usize, 0);
        reader.read_exact(&mut buffer)?;
        metadata = &buffer;
    } else {
        // If the buffer is too big lets take a slice of the bytes read
        let remaining = buffer.len() - file_metadata_size as usize;
        metadata = &buffer[remaining..]
    }

    let mut protocol = TCompactInputProtocol::new(metadata, (file_metadata_size as usize)*2);

    Ok(FileMetaData::read_from_in_protocol(&mut protocol).unwrap())
}

fn read_page_header(reader: &mut File, page_offset: u64) -> Result<PageHeader, Box<dyn std::error::Error>> {
    reader.seek(SeekFrom::Start(page_offset))?;
    let mut protocol = TCompactInputProtocol::new(reader, 4096);
    Ok(PageHeader::read_from_in_protocol(&mut protocol).unwrap())
}

fn read_column(reader: &mut File, page_offset: u64, column_type: Type) -> Result<(), Box<dyn std::error::Error>> {
    let page_header = read_page_header(reader, page_offset).unwrap();
    let num_values = page_header.data_page_header.unwrap().num_values as usize;
    let compressed_size = page_header.compressed_page_size as usize;
    let uncompressed_size = page_header.uncompressed_page_size as usize;
    let mut compressed_buffer = Vec::with_capacity(compressed_size);

    // TODO Reuse this buffer for decompressing other pages
    let mut output_buffer = vec![0u8; uncompressed_size];

    let bytes_read = match reader.take(compressed_size as u64).read_to_end(&mut compressed_buffer) {
        Ok(bytes) => bytes,
        Err(err) => return Err(Box::new(err)),        
    };

    if bytes_read != compressed_size {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Read was the wrong size")));
    }
    
    let mut decoder = zstd::Decoder::new(&*compressed_buffer)?;
    decoder.read_exact(&mut output_buffer)?;

    match column_type {
        Type::INT64 => {
            let values_start = uncompressed_size - (num_values * 8);
            read_plain_int64(&output_buffer[values_start..])?
        }
        _ => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Unknown Column Encoding")))
    };

    Ok(())
}

fn read_plain_int64(buffer: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in buffer.chunks_exact(8) {
        let value = u64::from_le_bytes(chunk.try_into()?);
        println!("{:?}\n", value);
    }
    Ok(())
}

fn main() {
    let file_path = "./test_data/tpcds_call_center/tpcds_call_center.parquet";
    let mut reader = File::open(file_path).unwrap();
    let file_metadata = read_footer(&mut reader).expect("what");
    let page_offset = file_metadata.row_groups[0].columns[0].meta_data.as_ref().unwrap().data_page_offset as u64;
    println!("{:?}", file_metadata.row_groups[0].columns[0].meta_data);
    println!("{:?}", read_page_header(&mut reader, page_offset).expect("page header missing"));
    read_column(&mut reader, page_offset, file_metadata.row_groups[0].columns[0].meta_data.as_ref().unwrap().type_);
}
