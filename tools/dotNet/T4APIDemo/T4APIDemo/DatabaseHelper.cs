using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using System;

namespace T4APIDemo
{
    public class DatabaseHelper : IDisposable
    {
        private readonly SqliteConnection _connection;
        private readonly ILogger<DatabaseHelper> _logger;
        private bool _disposed;

        public DatabaseHelper(string dbPath, ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<DatabaseHelper>();
            _connection = new SqliteConnection($"Data Source={dbPath}");
            _connection.Open();
            InitializeDatabase();
        }

        private void InitializeDatabase()
        {
            using var command = _connection.CreateCommand();
            command.CommandText = @"
                CREATE TABLE IF NOT EXISTS Orders (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    OrderUniqueId TEXT NOT NULL,
                    Symbol TEXT NOT NULL,
                    Status TEXT NOT NULL,
                    BuySell TEXT NOT NULL,
                    CurrentVolume INTEGER NOT NULL,
                    CurrentPrice REAL,
                    TotalFillVolume INTEGER NOT NULL,
                    Timestamp TEXT NOT NULL,
                    StatusDetail TEXT,
                    Change TEXT,
                    UNIQUE(OrderUniqueId)
                );";
            try
            {
                command.ExecuteNonQuery();
                _logger.LogInformation("Orders table initialized.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Orders table.");
            }
        }

        public bool OrderExists(string orderUniqueId)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM Orders WHERE OrderUniqueId = $orderUniqueId";
            command.Parameters.AddWithValue("$orderUniqueId", orderUniqueId);
            return Convert.ToInt32(command.ExecuteScalar()) > 0;
        }

        public void InsertOrUpdateOrder(string orderUniqueId, string symbol, string status, string buySell, int currentVolume, decimal? currentPrice, int totalFillVolume, DateTime timestamp, string statusDetail, string change)
        {
            if (string.IsNullOrEmpty(orderUniqueId))
            {
                _logger.LogError("Invalid order: OrderUniqueId is null or empty.");
                return;
            }

            try
            {
                using var command = _connection.CreateCommand();
                if (OrderExists(orderUniqueId))
                {
                    command.CommandText = @"
                        UPDATE Orders 
                        SET Symbol = $symbol, Status = $status, BuySell = $buySell, 
                            CurrentVolume = $currentVolume, CurrentPrice = $currentPrice, 
                            TotalFillVolume = $totalFillVolume, Timestamp = $timestamp, 
                            StatusDetail = $statusDetail, Change = $change
                        WHERE OrderUniqueId = $orderUniqueId";
                    _logger.LogInformation("Updating order: OrderUniqueId={OrderUniqueId}, Status={Status}", orderUniqueId, status);
                }
                else
                {
                    command.CommandText = @"
                        INSERT INTO Orders (OrderUniqueId, Symbol, Status, BuySell, CurrentVolume, CurrentPrice, TotalFillVolume, Timestamp, StatusDetail, Change)
                        VALUES ($orderUniqueId, $symbol, $status, $buySell, $currentVolume, $currentPrice, $totalFillVolume, $timestamp, $statusDetail, $change)";
                    _logger.LogInformation("Inserting order: OrderUniqueId={OrderUniqueId}, Status={Status}", orderUniqueId, status);
                }

                command.Parameters.AddWithValue("$orderUniqueId", orderUniqueId);
                command.Parameters.AddWithValue("$symbol", symbol);
                command.Parameters.AddWithValue("$status", status);
                command.Parameters.AddWithValue("$buySell", buySell);
                command.Parameters.AddWithValue("$currentVolume", currentVolume);
                command.Parameters.AddWithValue("$currentPrice", currentPrice.HasValue ? (object)currentPrice.Value : DBNull.Value);
                command.Parameters.AddWithValue("$totalFillVolume", totalFillVolume);
                command.Parameters.AddWithValue("$timestamp", timestamp.ToString("o"));
                command.Parameters.AddWithValue("$statusDetail", statusDetail ?? string.Empty);
                command.Parameters.AddWithValue("$change", change ?? string.Empty);
                command.ExecuteNonQuery();
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
            {
                _logger.LogWarning("Constraint violation for order: OrderUniqueId={OrderUniqueId}, Error={Error}", orderUniqueId, ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to insert/update order: OrderUniqueId={OrderUniqueId}", orderUniqueId);
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _connection?.Dispose();
                _disposed = true;
            }
        }
    }
}



