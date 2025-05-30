using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace T4APIDemo;

public class DatabaseHelper
{
    private readonly string _connectionString;
    private readonly ILogger<DatabaseHelper>? _logger;

    public DatabaseHelper(string dbPath = "trades.db")
        : this(dbPath, null)
    {
    }

    public DatabaseHelper(string dbPath, ILogger<DatabaseHelper>? logger)
    {
        _connectionString = $"Data Source={dbPath}";
        _logger = logger;
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS Orders (
                OrderUniqueId TEXT PRIMARY KEY,
                Symbol TEXT,
                Status TEXT,
                BuySell TEXT,
                CurrentVolume INTEGER,
                Price INTEGER,
                TotalFillVolume INTEGER,
                Timestamp TEXT,
                StatusDetail TEXT,
                Change TEXT
            );

            CREATE TABLE IF NOT EXISTS Trades (
                TradeId INTEGER PRIMARY KEY AUTOINCREMENT,
                BuyOrderId TEXT,
                SellOrderId TEXT,
                Symbol TEXT,
                Volume INTEGER,
                BuyPrice INTEGER,
                SellPrice INTEGER,
                BuyTime TEXT,
                SellTime TEXT,
                TradeDate INTEGER,
                FOREIGN KEY (BuyOrderId) REFERENCES Orders(OrderUniqueId),
                FOREIGN KEY (SellOrderId) REFERENCES Orders(OrderUniqueId)
            );

            CREATE TABLE IF NOT EXISTS ProcessedTrades (
                OrderUniqueId TEXT,
                SequenceOrder INTEGER,
                PRIMARY KEY (OrderUniqueId, SequenceOrder)
            );";

        command.ExecuteNonQuery();
        _logger?.LogInformation("Orders, Trades, and ProcessedTrades tables initialized.");
    }

    public void InsertOrUpdateOrder(string orderId, string symbol, string status, string buySell,
        int volume, long price, int totalFillVolume, string timestamp, string statusDetail, string change)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT OR REPLACE INTO Orders (OrderUniqueId, Symbol, Status, BuySell, CurrentVolume, Price, TotalFillVolume, Timestamp, StatusDetail, Change)
            VALUES ($orderId, $symbol, $status, $buySell, $volume, $price, $totalFillVolume, $timestamp, $statusDetail, $change)";
        command.Parameters.AddWithValue("$orderId", orderId);
        command.Parameters.AddWithValue("$symbol", symbol);
        command.Parameters.AddWithValue("$status", status);
        command.Parameters.AddWithValue("$buySell", buySell);
        command.Parameters.AddWithValue("$volume", volume);
        command.Parameters.AddWithValue("$price", price);
        command.Parameters.AddWithValue("$totalFillVolume", totalFillVolume);
        command.Parameters.AddWithValue("$timestamp", timestamp);
        command.Parameters.AddWithValue("$statusDetail", statusDetail ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("$change", change ?? (object)DBNull.Value);

        command.ExecuteNonQuery();
        _logger?.LogInformation("Inserted/Updated order: {OrderId}", orderId);
    }

    public void InsertOrUpdateTrades(List<(string buyOrderId, string sellOrderId, string symbol, int volume,
        long buyPrice, long sellPrice, string buyTime, string sellTime, long tradeDate)> trades)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        try
        {
            foreach (var trade in trades)
            {
                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = @"
                    INSERT INTO Trades (BuyOrderId, SellOrderId, Symbol, Volume, BuyPrice, SellPrice, BuyTime, SellTime, TradeDate)
                    VALUES ($buyOrderId, $sellOrderId, $symbol, $volume, $buyPrice, $sellPrice, $buyTime, $sellTime, $tradeDate)";
                command.Parameters.AddWithValue("$buyOrderId", trade.buyOrderId);
                command.Parameters.AddWithValue("$sellOrderId", trade.sellOrderId);
                command.Parameters.AddWithValue("$symbol", trade.symbol);
                command.Parameters.AddWithValue("$volume", trade.volume);
                command.Parameters.AddWithValue("$buyPrice", trade.buyPrice);
                command.Parameters.AddWithValue("$sellPrice", trade.sellPrice);
                command.Parameters.AddWithValue("$buyTime", trade.buyTime);
                command.Parameters.AddWithValue("$sellTime", trade.sellTime);
                command.Parameters.AddWithValue("$tradeDate", trade.tradeDate);

                command.ExecuteNonQuery();
                _logger?.LogInformation("Inserted trade: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}, Volume={Volume}", trade.buyOrderId, trade.sellOrderId, trade.volume);
            }
            transaction.Commit();
        }
        catch (SqliteException ex)
        {
            transaction.Rollback();
            _logger?.LogError(ex, "Failed to insert trades batch");
            throw;
        }
    }

    public bool IsFillProcessed(string orderId, int sequenceOrder)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM ProcessedTrades WHERE OrderUniqueId = $orderId AND SequenceOrder = $sequenceOrder";
        command.Parameters.AddWithValue("$orderId", orderId);
        command.Parameters.AddWithValue("$sequenceOrder", sequenceOrder);

        return Convert.ToInt32(command.ExecuteScalar()) > 0;
    }

    public void MarkFillProcessed(string orderId, int sequenceOrder)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO ProcessedTrades (OrderUniqueId, SequenceOrder) VALUES ($orderId, $sequenceOrder)";
        command.Parameters.AddWithValue("$orderId", orderId);
        command.Parameters.AddWithValue("$sequenceOrder", sequenceOrder);

        command.ExecuteNonQuery();
        _logger?.LogDebug("Marked fill as processed: OrderId={OrderId}, SequenceOrder={SequenceOrder}", orderId, sequenceOrder);
    }
}




