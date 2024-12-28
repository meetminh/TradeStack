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
                INSERT INTO stock_data_daily_staging (time, ticker, open, high, low, close, volume)
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
                continue  # Continue with next chunk instead of failing entire batch
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
endusing YFinance
using DataFrames
using Dates
using LibPQ
using ProgressMeter
using Logging
using Base.Threads

const DB_CONNECTION = "host=questdb.go-server-devcontainer.orb.local port=8812 dbname=qdb user=admin password=quest"
const MAX_RETRIES = 3
const THREAD_RESULTS = Dict{Int, DataFrame}()

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
    df.time = map(t -> DateTime(Date(t)) + Hour(16), df.time)
    return df
end

function fetch_historical_data(symbol::String, start_date::Union{DateTime, Nothing}=nothing, end_date::Union{DateTime, Nothing}=nothing, retries=MAX_RETRIES)
    for attempt in 1:retries
        try
            @info "Thread $(threadid()): Fetching data for $symbol (attempt $attempt)"
            
            # Always use the same get_prices approach as in working code
            data = get_prices(
                symbol,
                range="max",
                interval="1d",
                autoadjust=true
            )
            
            
            required_keys = ["timestamp", "open", "high", "low", "adjclose", "vol"]
            if !all(k -> haskey(data, k), required_keys)
                missing_keys = filter(k -> !haskey(data, k), required_keys)
                @error "Missing required keys: $missing_keys"
                return DataFrame()
            end
            
            n = length(data["timestamp"])
            
            volumes = data["vol"]
            volumes = map(v -> isnan(v) ? 0 : v, volumes)
            
            df = DataFrame(
                time = data["timestamp"],
                ticker = String.(fill(symbol, n)),
                open = Float64.(data["open"]),
                high = Float64.(data["high"]),
                low = Float64.(data["low"]),
                close = Float64.(data["adjclose"]),
                volume = Int64.(round.(volumes))
            )
            
            df = normalize_market_time(df)
            
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

function process_symbols_parallel(symbols::Vector{String}, existing_data::DataFrame)
    empty!(THREAD_RESULTS)
    
    n_threads = 4
    symbols_per_thread = ceil(Int, length(symbols)/n_threads)
    thread_symbol_groups = [symbols[i:min(i+symbols_per_thread-1, length(symbols))] 
                          for i in 1:symbols_per_thread:length(symbols)]
    
    @threads for thread_num in 1:length(thread_symbol_groups)
        thread_symbols = thread_symbol_groups[thread_num]
        thread_data = DataFrame[]
        
        for symbol in thread_symbols
        # Always fetch all data first
        df = fetch_historical_data(symbol)
        
        if !isempty(df)
            # If symbol exists, filter to only get new data
            if symbol in existing_data.ticker
                latest_date = existing_data[existing_data.ticker .== symbol, :max_time][1]
                df = df[df.time .> latest_date, :]
                @info "Filtered $(symbol) data after $(latest_date)"
            end
            
            if !isempty(df)
                push!(thread_data, df)
            end
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
                unix_micro = Int64(Dates.datetime2unix(row.time) * 1_000_000)
                values_row = string(
                    "(",
                    unix_micro, ",'",
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
                INSERT INTO stock_data_daily_staging (time, ticker, open, high, low, close, volume)
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

function is_valid_ticker(ticker::String)
    if contains(ticker, "^") || contains(ticker, ".WS") || contains(ticker, ".CL")
        return false
    end
    return true
end

function main(exchanges::Vector{String}=["NYSE"])
    @info "Starting market data fetcher with $(nthreads()) threads..."
    
    for exchange in exchanges
        @info "Processing exchange: $exchange"
        
        # First, get existing data from DB
        conn = LibPQ.Connection(DB_CONNECTION)
        existing_query = """
            SELECT 
                ticker,
                min(time) as min_time,
                max(time) as max_time
            FROM stock_data_daily_staging
            GROUP BY ticker
            ORDER BY ticker;
        """
        existing_data = execute(conn, existing_query) |> DataFrame
        close(conn)
        @info "Found $(nrow(existing_data)) existing tickers in database"
        
        # Get all tickers using YFinance.get_all_symbols
        @info "Fetching symbols for $exchange"
        all_tickers = YFinance.get_all_symbols(exchange)  # Returns Vector{String}
        valid_tickers = filter(t -> !contains(t, "^") && !contains(t, ".WS") && !contains(t, ".CL"), all_tickers)
        
        if isempty(valid_tickers)
            @warn "No valid tickers found for exchange $exchange"
            continue
        end
        
        @info "Found $(length(valid_tickers)) valid tickers for $exchange"
        
        n_batches = 7
        batch_size = ceil(Int, length(valid_tickers)/n_batches)
        symbol_batches = [valid_tickers[i:min(i+batch_size-1, length(valid_tickers))] 
                         for i in 1:batch_size:length(valid_tickers)]
        
        @info "Processing $(length(valid_tickers)) symbols in $n_batches batches"
        
        for (batch_num, symbol_batch) in enumerate(symbol_batches)
            @info "Processing batch $batch_num/$(length(symbol_batches)) ($(length(symbol_batch)) symbols)"
            
            batch_data = process_symbols_parallel(symbol_batch, existing_data)
            
            if !isempty(batch_data)
                @info "Inserting batch $batch_num data ($(nrow(batch_data)) rows)"
                bulk_insert_batch(batch_data)
            else
                @warn "No data available for batch $batch_num"
            end
            
            GC.gc()
        end
    end
    
    @info "Data fetching and insertion completed!"
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(["NYSE", "NASDAQ"])
end