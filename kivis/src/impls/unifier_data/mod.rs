#[cfg(any(feature = "std", feature = "alloc"))]
mod alloc;

#[cfg(feature = "heapless")]
mod heapless;
