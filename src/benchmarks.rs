use anyhow::{Context, Result};
use std::{
    convert::TryInto,
    mem::size_of,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) const BYTES_LEN: usize = size_of::<u128>();

#[inline]
pub(super) fn now() -> Result<[u8; BYTES_LEN]> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .with_context(|| "Never before 1970")?
        .as_millis()
        .to_le_bytes())
}

#[inline]
pub(super) fn elapsed(t: [u8; BYTES_LEN]) -> Result<Duration> {
    Ok(SystemTime::now()
        .duration_since(
            UNIX_EPOCH
                + Duration::from_millis(
                    u128::from_le_bytes(t)
                        .try_into()
                        .with_context(|| "Cannot convert millis to u64")?,
                ),
        )
        .with_context(|| "Wrong time system")?)
}
