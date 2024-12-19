#!/usr/bin/env julia

# setup.jl
using Pkg
using LibPQ

println("📦 Setting up Market Data Fetcher environment...")

# List of required packages
const REQUIRED_PACKAGES = [
    "YFinance",
    "DataFrames",
    "Dates",
    "LibPQ",
    "TimeZones",
    "HTTP",
    "JSON",
    "ProgressMeter",
    "Distributed",
    "LoggingExtras"
]

# Orbstack-specific database configuration
const DB_HOST = "questdb.go-server-devcontainer.orb.local"
const DB_PORT = "8812"
const DB_NAME = "qdb"
const DB_USER = "admin"
const DB_PASSWORD = "quest"

function create_log_directory()
    log_dir = "logs"
    if !isdir(log_dir)
        mkdir(log_dir)
        println("\n📁 Created logs directory")
    end
end

function test_database_connection()
    println("\n🔌 Testing database connection to $DB_HOST...")
    try
        conn_string = "host=$DB_HOST port=$DB_PORT dbname=$DB_NAME user=$DB_USER password=$DB_PASSWORD"
        conn = LibPQ.Connection(conn_string)
        println("✅ Database connection successful!")
        
        # Test if table exists
        result = execute(conn, """
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'stock_data'
            );
        """)
        table_exists = fetch(result)[1][1]
        
        if !table_exists
            println("\n📝 Creating stock_data table...")
            execute(conn, """
                CREATE TABLE IF NOT EXISTS stock_data (
                    time TIMESTAMP,
                    ticker SYMBOL,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume LONG,
                    PRIMARY KEY(time, ticker)
                ) timestamp(time) PARTITION BY DAY;
            """)
            println("✅ Table created successfully!")
        else
            println("✅ stock_data table already exists!")
        end
        
        close(conn)
    catch e
        println("❌ Database connection failed. Please ensure QuestDB is running and credentials are correct.")
        println("Error: ", e)
    end
end

function main()
    # Add required packages
    println("\n🔧 Installing required packages...")
    Pkg.add(REQUIRED_PACKAGES)
    
    # Create log directory
    create_log_directory()
    
    # Test database connection and create table if needed
    test_database_connection()
    
    println("\n✨ Setup complete! You can now run the market data fetcher.")
    println("\nTo run the fetcher, use:")
    println("julia market_data_fetcher.jl")
end

main()