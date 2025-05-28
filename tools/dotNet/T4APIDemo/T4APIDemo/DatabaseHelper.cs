using Microsoft.Data.Sqlite;
using System;

namespace T4APIDemo
{
    public class DatabaseHelper
    {
        private readonly string _connectionString;

        public DatabaseHelper(string dbPath)
        {
            _connectionString = $"Data Source={dbPath}";
            InitializeDatabase();
        }

        private void InitializeDatabase()
        {
            using var connection = new SqliteConnection(_connectionString);
            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE IF NOT EXISTS Trades (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderUniqueId TEXT NOT NULL,
                    Symbol TEXT NOT NULL,
                    Price REAL NOT NULL,
                    TradeVolume INTEGER NOT NULL,
                    BuySell TEXT NOT NULL,
                    Status TEXT NOT NULL,
                    Timestamp TEXT NOT NULL,
                    SequenceOrder INTEGER NOT NULL,
                    UNIQUE(OrderUniqueId, SequenceOrder)
                )";
            command.ExecuteNonQuery();
        }

        public bool TradeExists(string orderUniqueId, int sequenceOrder)
        {
            using var connection = new SqliteConnection(_connectionString);
            connection.Open();

            var command = connection.CreateCommand();
            command.CommandText = @"
                SELECT COUNT(*) FROM Trades 
                WHERE OrderUniqueId = $orderUniqueId AND SequenceOrder = $sequenceOrder";
            command.Parameters.AddWithValue("$orderUniqueId", orderUniqueId);
            command.Parameters.AddWithValue("$sequenceOrder", sequenceOrder);

            var result = command.ExecuteScalar();
            return result != null && Convert.ToInt64(result) > 0;
        }

        public void InsertTrade(string orderUniqueId, string symbol, decimal price, int tradeVolume, string buySell, string status, DateTime timestamp, int sequenceOrder)
        {
            if (TradeExists(orderUniqueId, sequenceOrder))
            {
                return; // Skip duplicate trade
            }

            using var connection = new SqliteConnection(_connectionString);
            connection.Open();
            using var transaction = connection.BeginTransaction();

            try
            {
                var command = connection.CreateCommand();
                command.CommandText = @"
                    INSERT INTO Trades (OrderUniqueId, Symbol, Price, TradeVolume, BuySell, Status, Timestamp, SequenceOrder)
                    VALUES ($orderUniqueId, $symbol, $price, $tradeVolume, $buySell, $status, $timestamp, $sequenceOrder)";
                command.Parameters.AddWithValue("$orderUniqueId", orderUniqueId);
                command.Parameters.AddWithValue("$symbol", symbol ?? "Unknown");
                command.Parameters.AddWithValue("$price", price);
                command.Parameters.AddWithValue("$tradeVolume", tradeVolume);
                command.Parameters.AddWithValue("$buySell", buySell);
                command.Parameters.AddWithValue("$status", status);
                command.Parameters.AddWithValue("$timestamp", timestamp.ToString("o"));
                command.Parameters.AddWithValue("$sequenceOrder", sequenceOrder);
                command.ExecuteNonQuery();

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }
}
















