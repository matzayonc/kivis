use kivis::Record;
#[derive(Record, serde::Serialize, serde::Deserialize, Debug)]
struct Generic<T> {
    value: T,
}
fn main() {}
