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
                ProfitLoss REAL,
                TradeDate INTEGER,
                FOREIGN KEY (BuyOrderId) REFERENCES Orders(OrderUniqueId),
                FOREIGN KEY (SellOrderId) REFERENCES Orders(OrderUniqueId),
                UNIQUE(BuyOrderId, SellOrderId)
            );";

        command.ExecuteNonQuery();
        _logger?.LogInformation("Orders and Trades tables initialized.");
    }

    public void InsertOrUpdateOrder(string orderUniqueId, string symbol, string status, string buySell,
        int currentVolume, long price, int totalFillVolume, string timestamp, string statusDetail, string change)
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT OR REPLACE INTO Orders (OrderUniqueId, Symbol, Status, BuySell, CurrentVolume, Price, TotalFillVolume, Timestamp, StatusDetail, Change)
            VALUES ($orderUniqueId, $symbol, $status, $buySell, $currentVolume, $price, $totalFillVolume, $timestamp, $statusDetail, $change)";
        command.Parameters.AddWithValue("$orderUniqueId", orderUniqueId);
        command.Parameters.AddWithValue("$symbol", symbol);
        command.Parameters.AddWithValue("$status", status);
        command.Parameters.AddWithValue("$buySell", buySell);
        command.Parameters.AddWithValue("$currentVolume", currentVolume);
        command.Parameters.AddWithValue("$price", price);
        command.Parameters.AddWithValue("$totalFillVolume", totalFillVolume);
        command.Parameters.AddWithValue("$timestamp", timestamp);
        command.Parameters.AddWithValue("$statusDetail", statusDetail ?? (object)DBNull.Value);
        command.Parameters.AddWithValue("$change", change ?? (object)DBNull.Value);

        command.ExecuteNonQuery();
        _logger?.LogInformation("Inserted/Updated order: {OrderUniqueId}", orderUniqueId);
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
                    INSERT OR IGNORE INTO Trades (BuyOrderId, SellOrderId, Symbol, Volume, BuyPrice, SellPrice, BuyTime, SellTime, ProfitLoss, TradeDate)
                    VALUES ($buyOrderId, $sellOrderId, $symbol, $volume, $buyPrice, $sellPrice, $buyTime, $sellTime, $profitLoss, $tradeDate)";
                command.Parameters.AddWithValue("$buyOrderId", trade.buyOrderId);
                command.Parameters.AddWithValue("$sellOrderId", trade.sellOrderId);
                command.Parameters.AddWithValue("$symbol", trade.symbol);
                command.Parameters.AddWithValue("$volume", trade.volume);
                command.Parameters.AddWithValue("$buyPrice", trade.buyPrice);
                command.Parameters.AddWithValue("$sellPrice", trade.sellPrice);
                command.Parameters.AddWithValue("$buyTime", trade.buyTime);
                command.Parameters.AddWithValue("$sellTime", trade.sellTime);
                command.Parameters.AddWithValue("$profitLoss", 0.0); // P&L ignored
                command.Parameters.AddWithValue("$tradeDate", trade.tradeDate);

                command.ExecuteNonQuery();
                _logger?.LogInformation("Inserted/Updated trade: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}", trade.buyOrderId, trade.sellOrderId);
            }
            transaction.Commit();
        }
        catch (SqliteException ex)
        {
            transaction.Rollback();
            _logger?.LogError(ex, "Failed to insert/update trades batch");
            throw;
        }
    }
}










