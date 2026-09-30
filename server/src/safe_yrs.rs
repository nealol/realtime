//! Checked Yjs v1 decoding for untrusted wire and persisted bytes.
//!
//! `yrs` 0.19's default `Read::read_string` uses `from_utf8_unchecked`, so a
//! malformed update can construct an invalid `str` and abort the process. This
//! decoder mirrors `DecoderV1` but validates every variable-length string before
//! exposing it to `yrs`.

use std::sync::Arc;

use yrs::encoding::read::{Error, Read};
use yrs::updates::decoder::{Decode, Decoder};
use yrs::updates::encoder::Encode;
use yrs::{Any, DeleteSet, Doc, StateVector, Transact, Update, ID};

struct SafeCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SafeCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
}

impl Read for SafeCursor<'_> {
    fn read_exact(&mut self, len: usize) -> Result<&[u8], Error> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(Error::EndOfBuffer(len))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(Error::EndOfBuffer(len))?;
        self.position = end;
        Ok(bytes)
    }

    fn read_string(&mut self) -> Result<&str, Error> {
        let bytes = self.read_buf()?;
        std::str::from_utf8(bytes)
            .map_err(|error| Error::Custom(format!("invalid UTF-8 string: {error}")))
    }
}

struct SafeDecoderV1<'a> {
    cursor: SafeCursor<'a>,
    delete_set_clock: Option<u32>,
}

impl<'a> SafeDecoderV1<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: SafeCursor::new(bytes),
            delete_set_clock: None,
        }
    }

    fn read_id(&mut self) -> Result<ID, Error> {
        let client: u32 = self.read_var()?;
        let clock = self.read_var()?;
        Ok(ID::new(client as _, clock))
    }
}

impl Read for SafeDecoderV1<'_> {
    fn read_exact(&mut self, len: usize) -> Result<&[u8], Error> {
        self.cursor.read_exact(len)
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        self.cursor.read_u8()
    }

    fn read_string(&mut self) -> Result<&str, Error> {
        self.cursor.read_string()
    }
}

impl Decoder for SafeDecoderV1<'_> {
    fn reset_ds_cur_val(&mut self) {
        self.delete_set_clock = None;
    }

    fn read_ds_clock(&mut self) -> Result<u32, Error> {
        let clock = self.read_var()?;
        self.delete_set_clock = Some(clock);
        Ok(clock)
    }

    fn read_ds_len(&mut self) -> Result<u32, Error> {
        let len = self.read_var()?;
        let clock = self.delete_set_clock.ok_or(Error::UnexpectedValue)?;
        clock
            .checked_add(len)
            .ok_or_else(|| Error::Custom("delete-set clock range overflows u32".to_string()))?;
        self.delete_set_clock = Some(clock + len);
        Ok(len)
    }

    fn read_left_id(&mut self) -> Result<ID, Error> {
        self.read_id()
    }

    fn read_right_id(&mut self) -> Result<ID, Error> {
        self.read_id()
    }

    fn read_client(&mut self) -> Result<u64, Error> {
        let client: u32 = self.read_var()?;
        Ok(client as u64)
    }

    fn read_info(&mut self) -> Result<u8, Error> {
        self.read_u8()
    }

    fn read_parent_info(&mut self) -> Result<bool, Error> {
        Ok(self.read_var::<u32>()? == 1)
    }

    fn read_type_ref(&mut self) -> Result<u8, Error> {
        self.read_u8()
    }

    fn read_len(&mut self) -> Result<u32, Error> {
        self.read_var()
    }

    fn read_any(&mut self) -> Result<Any, Error> {
        Any::decode(self)
    }

    fn read_json(&mut self) -> Result<Any, Error> {
        Any::from_json(self.read_string()?)
    }

    fn read_key(&mut self) -> Result<Arc<str>, Error> {
        Ok(self.read_string()?.into())
    }

    fn read_to_end(&mut self) -> Result<&[u8], Error> {
        Ok(&self.cursor.bytes[self.cursor.position..])
    }
}

pub(crate) fn decode_v1<T: Decode>(bytes: &[u8]) -> Result<T, Error> {
    decode_v1_inner(bytes)
}

fn decode_v1_inner<T: Decode>(bytes: &[u8]) -> Result<T, Error> {
    if std::any::type_name::<T>() == std::any::type_name::<Update>() {
        let mut cursor = SafeCursor::new(bytes);
        let clients: u32 = cursor.read_var()?;
        if clients as usize > bytes.len() {
            return Err(Error::Custom(
                "update client count exceeds encoded input".to_string(),
            ));
        }
    }
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        T::decode(&mut SafeDecoderV1::new(bytes))
    }))
    .unwrap_or(Err(Error::UnexpectedValue))
}

/// Decode a y-protocol `Message` from untrusted wire bytes with the same
/// safety envelope as `decode_v1`, plus explicit bounds on the two
/// sub-payloads that yrs otherwise preallocates from an attacker-controlled
/// varint:
///
/// - `AwarenessUpdate::decode` uses `HashMap::with_capacity(len)` and
///   `from_utf8_unchecked` on an internal `DecoderV1`, so we parse the
///   awareness payload ourselves with validated UTF-8 and a count bounded by
///   the actual sub-buffer length.
/// - `StateVector::decode` (SyncStep1) also preallocates from a varint; we
///   reject a count that exceeds the remaining input before delegating.
///
/// All other message kinds are delegated to `decode_v1`'s envelope
/// (`SafeDecoderV1`: validated strings, bounded buffers).
pub(crate) fn decode_message(bytes: &[u8]) -> Result<yrs::sync::Message, Error> {
    use std::collections::HashMap;
    use yrs::sync::awareness::{AwarenessUpdate, AwarenessUpdateEntry};
    use yrs::sync::protocol::{
        MSG_AWARENESS, MSG_SYNC, MSG_SYNC_STEP_1, MSG_SYNC_STEP_2, MSG_SYNC_UPDATE,
    };
    use yrs::sync::Message;

    let mut cursor = SafeCursor::new(bytes);
    let tag: u8 = cursor.read_var()?;
    match tag {
        MSG_SYNC => {
            let sub: u8 = cursor.read_var()?;
            match sub {
                MSG_SYNC_STEP_1 => {
                    // `SyncMessage::decode` reads `read_buf()` to get the
                    // state-vector sub-buffer, then `StateVector::decode`
                    // preallocates `HashMap::with_capacity(client_count)`
                    // from the first varint inside that sub-buffer. Bound the
                    // attacker-controlled client count against the sub-buffer
                    // length before delegating to yrs.
                    let sv_buf = cursor.read_buf()?;
                    let mut sv_inner = SafeCursor::new(sv_buf);
                    let sv_count = sv_inner.read_var::<usize>()?;
                    if sv_count.saturating_mul(2) > sv_buf.len() {
                        return Err(Error::Custom(
                            "state-vector length exceeds encoded input".to_string(),
                        ));
                    }
                    decode_v1_inner::<Message>(bytes)
                }
                MSG_SYNC_STEP_2 | MSG_SYNC_UPDATE => {
                    let _ = cursor.read_buf()?;
                    decode_v1_inner::<Message>(bytes)
                }
                _ => Err(Error::Custom(format!("unknown sync subtag {sub}"))),
            }
        }
        MSG_AWARENESS => {
            let data = cursor.read_buf()?;
            let mut inner = SafeCursor::new(data);
            let count = inner.read_var::<usize>()?;
            if count.saturating_mul(3) > data.len() {
                return Err(Error::Custom(
                    "awareness client count exceeds payload".to_string(),
                ));
            }
            let mut clients = HashMap::new();
            clients.try_reserve(count)?;
            for _ in 0..count {
                let client_id: u64 = inner.read_var()?;
                let clock: u32 = inner.read_var()?;
                let json = inner.read_string()?.to_string();
                clients.insert(client_id, AwarenessUpdateEntry { clock, json });
            }
            Ok(Message::Awareness(AwarenessUpdate { clients }))
        }
        _ => decode_v1_inner::<Message>(bytes),
    }
}

/// Preflight an encoded update by decoding it (through `decode_v1`) and applying
/// it to a disposable empty document under a panic guard. yrs' integrator can
/// still abort on malformed block structures that survive decoding (e.g. a GC or
/// Skip block with `len = 0` whose `end = clock + len - 1` underflows, or any
/// other invariant yrs only checks at integrate time). Those panics are
/// intrinsic to the update bytes, not to the receiving document, so replaying
/// against a fresh `Doc` surfaces them without ever touching durable storage or
/// the live awareness document.
///
/// Callers MUST run this before appending the update to the log or mutating any
/// live state. The returned decoded `Update` is the validated value and may be
/// reused so the caller does not decode twice.
pub(crate) fn validate_update(bytes: &[u8]) -> Result<Update, Error> {
    // `Update` is not `Clone`, so the preflight consumes its own decoded value;
    // the value returned to the caller is re-decoded on the proven-safe input.
    let preflight = decode_v1::<Update>(bytes)?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let doc = Doc::new();
        doc.transact_mut().apply_update(preflight);
    }));
    match result {
        Ok(()) => decode_v1::<Update>(bytes),
        Err(_) => Err(Error::Custom(
            "update panics when applied to an empty document".to_string(),
        )),
    }
}

/// What a validated update carries, without integrating it anywhere: the
/// upper bound of its block clocks and its delete set.
pub(crate) fn update_contents(bytes: &[u8]) -> Result<(StateVector, DeleteSet), Error> {
    let state_vector = validate_update(bytes)?.state_vector();
    // A diff against the update's own clocks keeps none of its blocks, only
    // the delete set. The bytes have just decoded safely, so yrs' own decoder
    // sees valid strings; the panic guard covers anything else.
    let own_clocks = state_vector.encode_v1();
    let diff = std::panic::catch_unwind(|| yrs::diff_updates_v1(bytes, &own_clocks))
        .unwrap_or(Err(Error::UnexpectedValue))?;
    match diff.split_first() {
        Some((0, delete_set)) => Ok((state_vector, decode_v1::<DeleteSet>(delete_set)?)),
        _ => Err(Error::Custom("update diff retained blocks".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::{Map, ReadTxn, StateVector};

    #[test]
    fn decodes_valid_updates_and_rejects_invalid_utf8() {
        let source = Doc::new();
        source
            .get_or_insert_map("values")
            .insert(&mut source.transact_mut(), "key", "value");
        let encoded = source
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        let update = validate_update(&encoded).unwrap();
        let target = Doc::new();
        target.transact_mut().apply_update(update);
        assert_eq!(
            target
                .get_or_insert_map("values")
                .get(&target.transact(), "key")
                .and_then(|value| value.cast::<String>().ok())
                .as_deref(),
            Some("value")
        );

        let mut malformed = encoded;
        let value_offset = malformed
            .windows(b"value".len())
            .position(|window| window == b"value")
            .unwrap();
        malformed[value_offset] = 0xff;
        let error = decode_v1::<Update>(&malformed).unwrap_err();
        assert!(error.to_string().contains("invalid UTF-8"));
    }

    #[test]
    fn rejects_overflowing_delete_set_range() {
        // Empty struct store followed by one delete-set range:
        // client 0, clock u32::MAX, length 1.
        let malformed = [0, 1, 0, 1, 0xff, 0xff, 0xff, 0xff, 0x0f, 1];
        let error = decode_v1::<Update>(&malformed).unwrap_err();
        assert!(error.to_string().contains("clock range overflows"));
    }

    #[test]
    fn rejects_impossible_update_client_count_before_allocation() {
        let malformed = [0xb2, 0xb2, 0xb2, 0xb2, 0x0a];
        let error = decode_v1::<Update>(&malformed).unwrap_err();
        assert!(error.to_string().contains("client count"));
    }

    #[test]
    fn preflight_rejects_update_that_panics_integration() {
        // A GC block with length 0 makes `GC::len` compute `end - start` with
        // `end = start + 0 - 1`, underflowing during integration and aborting
        // the process. Payload: 1 client, 1 block, clock 0, info =
        // BLOCK_GC_REF_NUMBER (0), len 0, followed by an empty delete set.
        // Payload: 1 client, 1 block, clock 0, info = BLOCK_GC_REF_NUMBER (0),
        // len 0. The delete set is empty (0 clients).
        let malformed = [1, 1, 0, 0, 0, 0, 0];
        let error = validate_update(&malformed).unwrap_err();
        assert!(error.to_string().contains("panics when applied"));
    }

    #[test]
    fn decode_message_rejects_hostile_state_vector_client_count() {
        // Wire: MSG_SYNC (0), MSG_SYNC_STEP_1 (0), then a length-prefixed
        // sub-buffer whose first varint claims 0xffff clients but the
        // sub-buffer is only 3 bytes. The outer buffer length (3) is valid,
        // so a naive guard that only checks the outer length would pass; the
        // inner client count (0xffff >> 3 bytes) must be rejected.
        // Sub-buffer: [0xff, 0xff, 0x03] = varint 0xffff (65535), no payload.
        let sv_buf = [0xff, 0xff, 0x03];
        let mut msg = vec![0u8, 0u8, sv_buf.len() as u8];
        msg.extend_from_slice(&sv_buf);
        let error = decode_message(&msg).unwrap_err();
        assert!(error.to_string().contains("state-vector length"));
    }

    #[test]
    fn decode_message_rejects_hostile_awareness_client_count() {
        // Wire: MSG_AWARENESS (1), then a length-prefixed sub-buffer whose
        // first varint claims 0xffff clients but the sub-buffer is only 3
        // bytes. `AwarenessUpdate::decode` would call
        // `HashMap::with_capacity(65535)` then read past EOF.
        let aw_buf = [0xff, 0xff, 0x03];
        let mut msg = vec![1u8, aw_buf.len() as u8];
        msg.extend_from_slice(&aw_buf);
        let error = decode_message(&msg).unwrap_err();
        assert!(error.to_string().contains("awareness client count"));
    }

    #[test]
    fn decode_message_accepts_valid_state_vector() {
        use yrs::updates::encoder::Encode;
        use yrs::Text;
        let doc = Doc::new();
        doc.get_or_insert_text("x")
            .insert(&mut doc.transact_mut(), 0, "a");
        let sv = doc.transact().state_vector();
        let msg = yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep1(sv)).encode_v1();
        let decoded = decode_message(&msg).unwrap();
        assert!(matches!(
            decoded,
            yrs::sync::Message::Sync(yrs::sync::SyncMessage::SyncStep1(_))
        ));
    }
}
