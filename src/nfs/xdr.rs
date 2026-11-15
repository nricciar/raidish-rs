//! XDR (RFC 4506): big-endian, everything padded to 4 bytes.

/// The arguments didn't decode (an RPC `GARBAGE_ARGS`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XdrError(pub &'static str);

impl std::fmt::Display for XdrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad XDR: {}", self.0)
    }
}

pub type XdrResult<T> = Result<T, XdrError>;

fn pad(n: usize) -> usize {
    (4 - n % 4) % 4
}

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> XdrResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or(XdrError("truncated"))?;
        let out = &self.buf[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    pub fn u32(&mut self) -> XdrResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> XdrResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn bool(&mut self) -> XdrResult<bool> {
        match self.u32()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(XdrError("bad bool")),
        }
    }

    /// Fixed-length opaque data (`opaque x[n]`).
    pub fn fixed(&mut self, n: usize) -> XdrResult<&'a [u8]> {
        let out = self.take(n)?;
        self.take(pad(n))?;
        Ok(out)
    }

    /// Variable-length opaque data of at most `max` bytes.
    pub fn opaque(&mut self, max: usize) -> XdrResult<&'a [u8]> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(XdrError("opaque too long"));
        }
        self.fixed(n)
    }

    /// A string of at most `max` bytes (UTF-8, as names here are).
    pub fn string(&mut self, max: usize) -> XdrResult<String> {
        let b = self.opaque(max)?;
        String::from_utf8(b.to_vec()).map_err(|_| XdrError("string isn't UTF-8"))
    }

    /// An optional value (`T *x`, a bool then the value).
    pub fn optional<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> XdrResult<T>,
    ) -> XdrResult<Option<T>> {
        if self.bool()? {
            Ok(Some(f(self)?))
        } else {
            Ok(None)
        }
    }

    /// What's left to read, without reading it.
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }
}

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u32(v as u32)
    }

    pub fn fixed(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self.buf.extend(std::iter::repeat_n(0, pad(b.len())));
        self
    }

    pub fn opaque(&mut self, b: &[u8]) -> &mut Self {
        self.u32(b.len() as u32);
        self.fixed(b)
    }

    pub fn string(&mut self, s: &str) -> &mut Self {
        self.opaque(s.as_bytes())
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_padding() {
        let mut w = Writer::new();
        w.u32(7)
            .u64(1 << 40)
            .bool(true)
            .opaque(b"abcde")
            .string("hi")
            .fixed(&[1, 2, 3]);
        let bytes = w.into_bytes();
        // 4 + 8 + 4 + (4 + 5 + 3) + (4 + 2 + 2) + (3 + 1)
        assert_eq!(bytes.len(), 40);
        let mut r = Reader::new(&bytes);
        assert_eq!(r.u32().unwrap(), 7);
        assert_eq!(r.u64().unwrap(), 1 << 40);
        assert!(r.bool().unwrap());
        assert_eq!(r.opaque(16).unwrap(), b"abcde");
        assert_eq!(r.string(16).unwrap(), "hi");
        assert_eq!(r.fixed(3).unwrap(), &[1, 2, 3]);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn refuses_bad_input() {
        let mut w = Writer::new();
        w.opaque(&[0u8; 10]);
        let bytes = w.into_bytes();
        assert!(
            Reader::new(&bytes).opaque(9).is_err(),
            "longer than allowed"
        );
        assert!(Reader::new(&bytes[..8]).opaque(16).is_err(), "truncated");
        assert!(Reader::new(&2u32.to_be_bytes()).bool().is_err());
        let huge = u32::MAX.to_be_bytes();
        assert!(Reader::new(&huge).opaque(usize::MAX).is_err());
    }
}
