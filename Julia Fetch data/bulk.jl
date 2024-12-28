#!/usr/bin/env julia

using YFinance
using DataFrames
using Dates
using LibPQ
using HTTP
using JSON
using ProgressMeter
using Logging
using OrderedCollections
using Base.Threads
using ProgressMeter


# Configuration
const DB_CONNECTION = "host=questdb.orb.local port=8812 dbname=qdb user=admin password=quest"
const MAX_RETRIES = 3
const THREAD_RESULTS = Dict{Int, DataFrame}()

# Type checking functions
function validate_types(df::DataFrame)
    type_issues = String[]
    
    if !(eltype(df.time) <: DateTime)
        push!(type_issues, "time should be DateTime, got $(eltype(df.time))")
    end
    
    if !(eltype(df.ticker) <: String)
        push!(type_issues, "ticker should be String, got $(eltype(df.ticker))")
    end
    
    for col in [:open, :high, :low, :close]
        if !(eltype(df[!, col]) <: Float64)
            push!(type_issues, "$(col) should be Float64, got $(eltype(df[!, col]))")
        end
    end
    
    if !(eltype(df.volume) <: Int64)
        push!(type_issues, "volume should be Int64, got $(eltype(df.volume))")
    end
    
    return type_issues
end

function normalize_market_time(df::DataFrame)
    # Convert all timestamps to 16:00 ET
    df.time = map(t -> DateTime(Date(t)) + Hour(16), df.time)
    return df
end

function fetch_historical_data(symbol::String, start_date::Union{DateTime, Nothing}=nothing, retries=MAX_RETRIES)
    for attempt in 1:retries
        try
            @info "Thread $(threadid()): Fetching data for $symbol (attempt $attempt)"
            
            data = if start_date !== nothing
                @info "Fetching data from $start_date for $symbol"
                get_prices(
                    symbol,
                    range="max",
                    interval="1d",
                    autoadjust=true
                )
            else
                @info "Fetching complete history for $symbol"
                get_prices(
                    symbol,
                    range="max",
                    interval="1d",
                    autoadjust=true
                )
            end
            
            required_keys = ["timestamp", "open", "high", "low", "adjclose", "vol"]
            if !all(k -> haskey(data, k), required_keys)
                missing_keys = filter(k -> !haskey(data, k), required_keys)
                @error "Missing required keys: $missing_keys"
                return DataFrame()
            end
            
            n = length(data["timestamp"])
            
            # Handle NaN values in volume data
            volumes = data["vol"]
            volumes = map(v -> isnan(v) ? 0 : v, volumes)  # Replace NaN with 0
            
            df = DataFrame(
                time = data["timestamp"],
                ticker = String.(fill(symbol, n)),
                open = Float64.(data["open"]),
                high = Float64.(data["high"]),
                low = Float64.(data["low"]),
                close = Float64.(data["adjclose"]),
                volume = Int64.(round.(volumes))  # Use cleaned volumes
            )

            df = normalize_market_time(df)
            
            # If start_date is provided, filter data
            if start_date !== nothing
                df = df[df.time .> start_date, :]
            end
            
            type_issues = validate_types(df)
            if !isempty(type_issues)
                @error "Data type issues detected" issues=type_issues
                return DataFrame()
            end
            
            return df
        catch e
            @error "Attempt $attempt failed for $symbol" exception=e
            if attempt == retries
                return DataFrame()
            end
            sleep(2^attempt)
        end
    end
    return DataFrame()
end

function process_symbols_parallel(symbols::Vector{String}, latest_dates::Dict{String, DateTime}, n_threads::Int=7)
    empty!(THREAD_RESULTS)
    
    symbols_per_thread = ceil(Int, length(symbols)/n_threads)
    thread_symbol_groups = [symbols[i:min(i+symbols_per_thread-1, length(symbols))] 
                          for i in 1:symbols_per_thread:length(symbols)]
    
    @threads for thread_num in 1:length(thread_symbol_groups)
        thread_symbols = thread_symbol_groups[thread_num]
        thread_data = DataFrame[]
        
        for symbol in thread_symbols
            if haskey(latest_dates, symbol)
                # Existing ticker - fetch from last date
                start_date = latest_dates[symbol]
                df = fetch_historical_data(symbol, start_date)
            else
                # New ticker - fetch all history
                df = fetch_historical_data(symbol)
            end
            
            if !isempty(df)
                push!(thread_data, df)
            end
            sleep(1)
        end
        
        if !isempty(thread_data)
            THREAD_RESULTS[thread_num] = vcat(thread_data...)
        end
    end
    
    all_data = DataFrame[]
    for thread_num in 1:length(thread_symbol_groups)
        if haskey(THREAD_RESULTS, thread_num)
            push!(all_data, THREAD_RESULTS[thread_num])
        end
    end
    
    return isempty(all_data) ? DataFrame() : vcat(all_data...)
end

function bulk_insert_batch(df::DataFrame)
    if isempty(df)
        return false
    end
    
    conn = LibPQ.Connection(DB_CONNECTION)
    try
        total_rows = nrow(df)
        chunk_size = 5000
        chunks = ceil(Int, total_rows/chunk_size)
        
        @info "Starting bulk insert of $total_rows rows in $chunks chunks"
        
        for chunk_idx in 1:chunks
            start_idx = (chunk_idx-1) * chunk_size + 1
            end_idx = min(chunk_idx * chunk_size, total_rows)
            chunk = df[start_idx:end_idx, :]
            
            values_parts = String[]
            for row in eachrow(chunk)
                # Convert DateTime to Unix microseconds
                unix_micro = Int64(Dates.datetime2unix(row.time) * 1_000_000)
                
                values_row = string(
                    "(",
                    unix_micro, ",'",  # No quotes around timestamp number
                    row.ticker, "',",
                    row.open, ",",
                    row.high, ",",
                    row.low, ",",
                    row.close, ",",
                    row.volume,
                    ")"
                )
                push!(values_parts, values_row)
            end

            bulk_query = """
                INSERT INTO stock_data_daily (time, ticker, open, high, low, close, volume)
                VALUES $(join(values_parts, ","));
            """
            
            try
                execute(conn, bulk_query)
                @info "Inserted chunk $chunk_idx/$chunks ($(length(values_parts)) rows)"
            catch e
                @error "Failed to insert chunk $chunk_idx" exception=e
                if isa(e, LibPQ.Errors.UnknownError)
                    error_result = execute(conn, "SELECT pg_last_error();")
                    if !isempty(error_result)
                        error_message = fetch(error_result)[1][1]
                        @error "Database error details: $error_message"
                    end
                end
                continue
            end
        end
        
        @info "Completed inserting $total_rows rows"
        return true
    catch e
        @error "Error in bulk insert" exception=e
        return false
    finally
        close(conn)
    end
end

using HTTP
using CSV
using DataFrames

using LibPQ, HTTP, CSV, DataFrames, Logging, Dates

function main()
  @info "Starting market data fetcher with $(nthreads()) threads..."
  
  # First, get existing data from DB with error handling
  local existing_data
  try
      conn = LibPQ.Connection(DB_CONNECTION)
      existing_query = """
          SELECT 
              ticker,
              max(time) as max_time
          FROM stock_data_daily
          GROUP BY ticker;
      """
      existing_data = execute(conn, existing_query) |> DataFrame
      close(conn)
  catch e
      @error "Failed to fetch existing data from database" exception=e
      return
  end
  
  # Create dictionary for faster lookups with type safety
  latest_dates = Dict{String, DateTime}()
  for row in eachrow(existing_data)
      ticker = string(row.ticker)  # Ensure ticker is a String
      if !ismissing(row.max_time)  # Skip missing values
          try
              max_time = DateTime(row.max_time)  # Ensure max_time is a DateTime
              latest_dates[ticker] = max_time
          catch e
              @warn "Failed to process date for ticker $ticker" exception=e
              continue
          end
      end
  end
  @info "Found $(length(latest_dates)) existing tickers in database"
  
  # Try to get NASDAQ symbols from URL first
  local symbols
  try
      @info "Fetching NYSE symbols from URL..."
      url = "https://r2.datahub.io/clt98mjxo000pl708niy4jpmy/main/raw/data/other-listed.csv"
      response = HTTP.get(url)
      data = String(response.body)
      nasdaq_df = CSV.read(IOBuffer(data), DataFrame)
      
      # Inspect the column names in the DataFrame
      @info "Columns in the DataFrame: $(names(nasdaq_df))"
      
      # Replace :Symbol with the correct column name (e.g., :ACT Symbol)
      symbols = String.(coalesce.(nasdaq_df."ACT Symbol", ""))  # Use the correct column name
      symbols = filter(!isempty, symbols)  # Remove empty strings
  catch e
      @error "Failed to fetch symbols from URL. Exiting." exception=e
      return  # Exit the function if URL fetch fails
  end
  
  # Validate symbols
  if isempty(symbols)
      @error "No valid symbols found. Exiting."
      return
  end
  @info "Processing $(length(symbols)) valid symbols"
  
  # Process in batches with progress tracking
  n_batches = 7
  batch_size = ceil(Int, length(symbols)/n_batches)
  symbol_batches = [symbols[i:min(i+batch_size-1, end)] for i in 1:batch_size:length(symbols)]
  
  @info "Processing $(length(symbols)) symbols in $n_batches batches"
  
  # Create progress meter
  prog = ProgressMeter.Progress(length(symbol_batches), desc="Processing batches: ", barlen=50)
  
  for (batch_num, symbol_batch) in enumerate(symbol_batches)
      @info "Processing batch $batch_num/$(length(symbol_batches)) ($(length(symbol_batch)) symbols)"
      
      try
          batch_data = process_symbols_parallel(symbol_batch, latest_dates, 7)  # Use 7 threads
          
          if !isempty(batch_data)
              @info "Inserting batch $batch_num data ($(nrow(batch_data)) rows)"
              if !bulk_insert_batch(batch_data)
                  @warn "Failed to insert batch $batch_num"
              end
          else
              @warn "No data available for batch $batch_num"
          end
      catch e
          @error "Failed to process batch $batch_num" exception=e
          continue  # Continue with next batch even if this one fails
      end
      
      # Update progress
      ProgressMeter.next!(prog)
      
      # Add small delay between batches to avoid overwhelming APIs
      sleep(1)
      
      GC.gc()  # Run garbage collection between batches
  end
  
  @info "Data fetching and insertion completed!"
end

# Add error handling for script execution
if abspath(PROGRAM_FILE) == @__FILE__
  try
      main()
  catch e
      @error "Script failed" exception=e
      exit(1)
  end
end