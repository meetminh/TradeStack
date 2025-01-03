use crate::portfolio::blocks::models::Block;
use crate::portfolio::execution::strategy_executor::{execute_strategy, Allocation};
use crate::market::database_functions::DatabaseError;
use chrono::{NaiveDate, NaiveDateTime, Utc, Datelike, TimeZone, Months};
use deadpool_postgres::{Client, Pool};

impl From<chrono::format::ParseError> for DatabaseError {
    fn from(err: chrono::format::ParseError) -> Self {
        DatabaseError::InvalidInput(err.to_string())
    }
}


async fn get_last_market_open_day_of_previous_month(
  client: &Client,
  date: &str,
) -> Result<String, DatabaseError> {
  // Parse the input date
  let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;

  // Get the last day of the previous month
  let last_day_of_previous_month = date
      .with_day(1).unwrap() // Start of the current month
      .pred_opt().unwrap() // Last day of the previous month
      .format("%Y-%m-%d")
      .to_string();

  //println!("Last day of previous month: {}", last_day_of_previous_month);

  // Check if the last day of the previous month is a market open day
  let query = format!(
      r#"
      SELECT Date, Date_of_Previous_Trading_Day, Is_Holiday, Is_Weekend
      FROM nasdaq_closed_days
      WHERE Date = '{}'
      "#,
      last_day_of_previous_month
  );

  // println!("Executing query: {}", query);

  // Execute the query and handle the case where no rows are returned
  let row_result = client.query_opt(&query, &[]).await?;

  let last_market_open_day = match row_result {
      Some(row) => {
          // The date is a closing day (holiday or weekend)
          let is_holiday: bool = row.get::<_, bool>("Is_Holiday");
          let is_weekend: bool = row.get::<_, bool>("Is_Weekend");
          let is_market_open = !is_holiday && !is_weekend;

          // println!(
          //     "Is holiday: {}, Is weekend: {}, Is market open: {}",
          //     is_holiday, is_weekend, is_market_open
          // );

          if is_market_open {
              // This should not happen since the table only contains closing days
              last_day_of_previous_month
          } else {
              // Use the last trading day before the closing day
              let previous_trading_day: NaiveDateTime = row.get::<_, NaiveDateTime>("Date_of_Previous_Trading_Day");
              previous_trading_day.format("%Y-%m-%d").to_string()
          }
      }
      None => {
          // The date is a market open day
          println!("Date is a market open day: {}", last_day_of_previous_month);
          last_day_of_previous_month
      }
  };

  // println!("Last market open day: {}", last_market_open_day);

  // Format the date with the fixed time component
  let last_market_open_day = Utc
      .with_ymd_and_hms(
          last_market_open_day[0..4].parse().unwrap(),
          last_market_open_day[5..7].parse().unwrap(),
          last_market_open_day[8..10].parse().unwrap(),
          16, 0, 0,
      )
      .unwrap()
      .format("%Y-%m-%dT%H:%M:%S.000000Z")
      .to_string();

  // println!("Formatted last market open day: {}", last_market_open_day);

  Ok(last_market_open_day)
}
/// Main function to execute the strategy over a time span
pub async fn execute_strategy_over_time_span(
    pool: &Pool,
    strategy: &Block,
    start_date: &str,
    end_date: Option<&str>,
    frequency: &str, // "monthly", "quarterly", "yearly"
) -> Result<Vec<(String, Vec<Allocation>)>, DatabaseError> {
    let client = pool.get().await?;
    let end_date = end_date.map(|s| s.to_string()).unwrap_or_else(|| {
        Utc::now().format("%Y-%m-%dT%H:%M:%S.000000Z").to_string()
    });

    // println!("Start date: {}, End date: {}", start_date, end_date);

    let mut current_date = start_date.to_string();
    let mut results = Vec::new();

    while &current_date <= &end_date {
        // println!("Current date: {}", current_date);

        // Get the last market open trading day of the previous month
        let last_market_open_day = get_last_market_open_day_of_previous_month(&client, &current_date).await?;

        // println!("Executing strategy for date: {}", last_market_open_day);

        // Execute the strategy on the last market open trading day
        let allocations = execute_strategy(strategy, pool, &last_market_open_day).await?;

        // println!("Strategy executed successfully for date: {}", last_market_open_day);

        // Store the results
        results.push((last_market_open_day, allocations));

        // Move to the next month (or quarter/year)
        let next_date = match frequency {
            "monthly" => {
                let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
                date.checked_add_months(Months::new(1)).ok_or(DatabaseError::InvalidInput("Invalid month".to_string()))?
            }
            "quarterly" => {
                let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
                date.checked_add_months(Months::new(3)).ok_or(DatabaseError::InvalidInput("Invalid month".to_string()))?
            }
            "yearly" => {
                let date = NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")?;
                date.checked_add_months(Months::new(12)).ok_or(DatabaseError::InvalidInput("Invalid year".to_string()))?
            }
            _ => return Err(DatabaseError::InvalidInput("Invalid frequency".to_string())),
        };
        current_date = next_date.format("%Y-%m-%d").to_string();
    }

    // println!("Strategy execution completed. Results: {:?}", results);

    Ok(results)
}