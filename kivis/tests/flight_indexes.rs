use kivis::{Database, Lexicographic, MemoryStorage, Record, manifest};

#[derive(
    Record, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Flight {
    #[index]
    flight_number: Lexicographic<String>,
    #[index]
    departure_airport: Lexicographic<String>,
    #[index]
    arrival_airport: Lexicographic<String>,
    airline: String,
    departure_time: u64,
    arrival_time: u64,
    capacity: u32,
    available_seats: u32,
}

manifest![Manifest: Flight];

#[test]
fn test_insertion_and_deletion() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let flights = [
        Flight {
            flight_number: "AA100".into(),
            departure_airport: "JFK".into(),
            arrival_airport: "LAX".into(),
            airline: "American Airlines".to_string(),
            departure_time: 1640000000,
            arrival_time: 1640020000,
            capacity: 200,
            available_seats: 150,
        },
        Flight {
            flight_number: "UA200".into(),
            departure_airport: "JFK".into(),
            arrival_airport: "SFO".into(),
            airline: "United Airlines".to_string(),
            departure_time: 1640010000,
            arrival_time: 1640030000,
            capacity: 180,
            available_seats: 100,
        },
    ];

    let key1 = store.put(flights[0].clone())?;
    let key2 = store.put(flights[1].clone())?;

    assert_eq!(store.get(&key1)?, Some(flights[0].clone()));
    assert_eq!(store.get(&key2)?, Some(flights[1].clone()));

    store.remove(&key1)?;
    assert_eq!(store.get(&key1)?, None);
    assert_eq!(store.get(&key2)?, Some(flights[1].clone()));
    Ok(())
}

#[test]
fn test_iter_by_flight_number() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    for i in 1..=5 {
        store.put(Flight {
            flight_number: format!("AA{}", i * 100).into(),
            departure_airport: "JFK".into(),
            arrival_airport: "LAX".into(),
            airline: "American Airlines".to_string(),
            departure_time: 1640000000,
            arrival_time: 1640020000,
            capacity: 200,
            available_seats: 150,
        })?;
    }

    let aa_flights = store
        .iter_by_index(
            FlightFlightNumberIndex("AA200".into())..FlightFlightNumberIndex("AA400".into()),
        )?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(aa_flights.len(), 2);
    Ok(())
}

#[test]
fn test_iter_by_departure_airport() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let airports = ["JFK", "JFK", "ATL", "ORD"];
    for (i, airport) in airports.iter().enumerate() {
        store.put(Flight {
            flight_number: format!("FL{}", i).into(),
            departure_airport: (*airport).into(),
            arrival_airport: "LAX".into(),
            airline: "Test Airlines".to_string(),
            departure_time: 1640000000,
            arrival_time: 1640020000,
            capacity: 200,
            available_seats: 150,
        })?;
    }

    let jfk_flights = store
        .iter_by_index_exact(FlightDepartureAirportIndex("JFK".into()))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(jfk_flights.len(), 2);
    Ok(())
}

#[test]
fn test_iter_by_arrival_airport() -> anyhow::Result<()> {
    let mut store = Database::<_, Manifest>::new(MemoryStorage::default())?;

    let destinations = ["LAX", "SFO", "LAX", "MIA"];
    for (i, dest) in destinations.iter().enumerate() {
        store.put(Flight {
            flight_number: format!("FL{}", i).into(),
            departure_airport: "JFK".into(),
            arrival_airport: (*dest).into(),
            airline: "Test Airlines".to_string(),
            departure_time: 1640000000,
            arrival_time: 1640020000,
            capacity: 200,
            available_seats: 150,
        })?;
    }

    let lax_flights = store
        .iter_by_index_exact(FlightArrivalAirportIndex("LAX".into()))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(lax_flights.len(), 2);
    Ok(())
}
