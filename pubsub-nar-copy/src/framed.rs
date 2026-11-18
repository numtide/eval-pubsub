use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::wire::{NixDeserialize, NixSerialize, ProtocolVersion};

pub struct FramedNixWriter<W> {
    write_buffer: BytesMut,
    writer: W,
}

impl<W> FramedNixWriter<W> {
    pub fn new(writer: W) -> Self {
        let write_buffer = BytesMut::new();

        FramedNixWriter {
            write_buffer,
            writer,
        }
    }
}

impl<W> FramedNixWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn write<A>(&mut self, protocol_version: ProtocolVersion, value: &A) -> Result<(), ()>
    where
        A: NixSerialize,
    {
        value.serialize(protocol_version, &mut self.write_buffer);
        self.writer
            .write_all_buf(&mut self.write_buffer)
            .await
            .unwrap();
        self.write_buffer.truncate(0);

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), ()> {
        Ok(self.writer.flush().await.unwrap())
    }
}

pub struct FramedNixReader<R> {
    read_buffer: BytesMut,
    reader: R,
}

impl<R> FramedNixReader<R> {
    pub fn new(reader: R) -> Self {
        let read_buffer = BytesMut::new();

        FramedNixReader {
            read_buffer,
            reader,
        }
    }
}

impl<R> FramedNixReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn get_u64_le(&mut self) -> u64 {
        while self.read_buffer.len() < size_of::<u64>() {
            self.reader.read_buf(&mut self.read_buffer).await.unwrap();
        }

        self.read_buffer.get_u64_le()
    }

    pub async fn nom_read<A>(&mut self, protocol_version: ProtocolVersion) -> Result<A, ()>
    where
        A: for<'a> NixDeserialize<'a>,
    {
        loop {
            match A::deserialize(protocol_version, &self.read_buffer) {
                Ok((input, ok)) => {
                    let _ = self
                        .read_buffer
                        .split_to(self.read_buffer.len() - input.len());
                    break Ok(ok);
                }
                Err(nom::Err::Incomplete(_)) => {}
                Err(nom::Err::Error(error)) => panic!("error: {:?}", error),
                Err(nom::Err::Failure(failure)) => panic!("failure: {:?}", failure),
            }

            self.reader.read_buf(&mut self.read_buffer).await.unwrap();
        }
    }
}
