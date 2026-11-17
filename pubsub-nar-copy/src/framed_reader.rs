use bytes::{Buf, BufMut as _, BytesMut};
use futures::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::wire::{NixDeserialize, ProtocolVersion};

pub struct FramedNixReader<R> {
    read_buffer: BytesMut,
    reader: R,
}

impl<R> FramedNixReader<R> {
    pub fn new(reader: R) -> Self {
        FramedNixReader {
            read_buffer: BytesMut::new(),
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
        A: NixDeserialize,
    {
        loop {
            let _ = self.reader.read_buf(&mut self.read_buffer).await.unwrap();

            match A::deserialize(protocol_version, &self.read_buffer) {
                Ok((input, ok)) => {
                    let _ = self
                        .read_buffer
                        .split_to(self.read_buffer.len() - input.len());
                    break Ok(ok);
                }
                Err(nom::Err::Incomplete(_)) => continue,
                Err(nom::Err::Error(error)) => panic!("error: {:?}", error),
                Err(nom::Err::Failure(failure)) => panic!("failure: {:?}", failure),
            }
        }
    }
}
