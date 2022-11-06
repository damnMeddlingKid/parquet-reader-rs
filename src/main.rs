use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::FileMetaData;
use std::{fs::File, result::Result};
use std::io::{Read, Seek, SeekFrom};


const DEFAULT_FOOTER_READ_SIZE: usize = 64;
const MAGIC_NUMBER : [u8; 4] = [b'P', b'A', b'R', b'1'];

fn read_footer(reader: &mut File) -> Result<FileMetaData, Box<dyn std::error::Error>> {
    let mut buffer = Vec::with_capacity(DEFAULT_FOOTER_READ_SIZE);

    reader.seek(SeekFrom::End(-(DEFAULT_FOOTER_READ_SIZE as i64)))?;

    reader
        .take(DEFAULT_FOOTER_READ_SIZE as u64)
        .read_to_end(&mut buffer)?;

    assert!(buffer[buffer.len()-4..] == MAGIC_NUMBER);

    let file_metadata_size = u32::from_le_bytes(buffer[buffer.len() - 8..buffer.len() - 4].try_into()?);

    reader.seek(SeekFrom::End(-8 -(file_metadata_size as i64)))?;

    buffer.clear();
    buffer.try_reserve((file_metadata_size as usize) - DEFAULT_FOOTER_READ_SIZE)?;

    reader.take(file_metadata_size as u64).read_to_end(&mut buffer)?;

    let reader: &[u8] = &buffer;

    let mut protocol = TCompactInputProtocol::new(reader, (file_metadata_size as usize)*2);

    Ok(FileMetaData::read_from_in_protocol(&mut protocol).unwrap())
}

fn main() {
    let file_path = "./test_data/tpcds_call_center/tpcds_call_center.parquet";
    let mut reader = File::open(file_path).unwrap();
    let file_metadata = read_footer(&mut reader).expect("what");

    println!("{}", file_metadata.num_rows);
}
