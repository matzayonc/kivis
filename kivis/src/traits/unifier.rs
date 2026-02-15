use core::error::Error;
use core::fmt::{Debug, Display};
use serde::{Serialize, de::DeserializeOwned};

use crate::{BufferOverflowError, BufferOverflowOr};

pub trait UnifierData: Default + Clone {
    /// The borrowed view type for this buffer (e.g., &[u8] for Vec<u8>, &str for String)
    type View<'a>;

    /// Converts the buffer to a view with explicit lifetime
    fn as_view(&self) -> Self::View<'_> {
        self.extract_range(0, self.len())
    }

    /// Creates a buffer from a view
    fn from_view(data: Self::View<'_>) -> Self;

    /// Increments the buffer to the next value.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn next(&mut self) -> Result<(), BufferOverflowError>;

    /// Appends a single part to the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn extend_from(&mut self, part: Self::View<'_>) -> Result<(), BufferOverflowError>;

    /// Returns the current length of the buffer.
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Extracts a range from the buffer as a reference.
    /// This is used to extract individual values from buffered data.
    #[must_use]
    fn extract_range(&self, start: usize, end: usize) -> Self::View<'_>;

    /// Duplicates data by cloning the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate(data: Self::View<'_>) -> Result<Self, BufferOverflowError> {
        let mut result = Self::default();
        result.extend_from(data)?;
        Ok(result)
    }

    /// Duplicates a range from the buffer and appends it to the same buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer overflows.
    fn duplicate_within(&mut self, start: usize, end: usize) -> Result<(), BufferOverflowError>;
}

pub trait Unifiable: Serialize + DeserializeOwned {}
pub trait UnifiableRef: Unifiable + Clone {}

impl<T: Serialize + DeserializeOwned> Unifiable for T {}
impl<T: Serialize + DeserializeOwned + Clone> UnifiableRef for T {}

pub trait Unifier {
    type D: UnifierData;
    type SerError: Debug + Display + Error;
    type DeError: Debug + Display + Error;

    /// Serializes data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize(
        &self,
        buffer: &mut Self::D,
        data: impl Unifiable,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>>;

    /// Serializes borrowed data directly into an existing buffer and returns the start and end positions.
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize_ref<R: UnifiableRef>(
        &self,
        buffer: &mut Self::D,
        data: &R,
    ) -> Result<(usize, usize), BufferOverflowOr<Self::SerError>> {
        self.serialize(buffer, data.clone())
    }

    /// Deserializes data from the given buffer.
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize<T: Unifiable>(&self, data: &Self::D) -> Result<T, Self::DeError>;
}
