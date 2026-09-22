//! `Unified::next` must yield the smallest buffer sorting after every buffer with that prefix.
use kivis::{BufferOverflowError, Unified};

fn successor(bytes: &[u8]) -> Result<Vec<u8>, BufferOverflowError> {
    let mut buffer = bytes.to_vec();
    buffer.next()?;
    Ok(buffer)
}

#[test]
fn increments_the_last_byte() -> anyhow::Result<()> {
    assert_eq!(successor(&[1, 2, 3])?, vec![1, 2, 4]);
    assert_eq!(successor(&[0])?, vec![1]);
    Ok(())
}

#[test]
fn trailing_max_bytes_are_dropped() -> anyhow::Result<()> {
    // [01, FF] must become [02], not [02, 00]: the latter still sorts after the bare key [02],
    // which does not share the prefix and would be swept into a prefix scan.
    assert_eq!(successor(&[1, 255])?, vec![2]);
    assert_eq!(successor(&[1, 255, 255])?, vec![2]);
    Ok(())
}

#[test]
fn successor_always_sorts_after_the_input() -> anyhow::Result<()> {
    for bytes in [
        vec![0],
        vec![1, 2, 3],
        vec![1, 255],
        vec![0, 255, 255],
        vec![254, 255],
    ] {
        let next = successor(&bytes)?;
        assert!(
            next > bytes,
            "next({bytes:?}) = {next:?} does not sort after it"
        );
    }
    Ok(())
}

#[test]
fn all_max_bytes_have_no_successor() {
    // Used to zero every byte and push one, producing a buffer that sorted *before* the input.
    assert_eq!(successor(&[255]), Err(BufferOverflowError));
    assert_eq!(successor(&[255, 255]), Err(BufferOverflowError));
    assert_eq!(successor(&[]), Err(BufferOverflowError));
}
