using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using T4APIDemo.T4;
using T4APIDemo.T4.Util;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using T4Proto.V1.Common;

namespace T4APIDemo.DemoClient;

public class DemoClient : BackgroundService
{
    private readonly ILogger<DemoClient> _logger;
    private readonly T4APIClient _apiClient;
    private readonly IConfiguration _configuration;
    private readonly DatabaseHelper _dbHelper;
    private readonly CancellationTokenSource _shutdownCts = new CancellationTokenSource();
    private bool _isSnapshotProcessed;

    public DemoClient(ILogger<DemoClient> logger, T4APIClient apiClient, IConfiguration configuration, DatabaseHelper dbHelper)
    {
        _logger = logger;
        _apiClient = apiClient;
        _configuration = configuration;
        _dbHelper = dbHelper;
        _isSnapshotProcessed = false;

        _apiClient.OnConnectionStatusChanged += _apiClient_OnConnectionStatusChanged;
        _apiClient.OnAccountUpdate += _apiClient_OnAccountUpdate;
    }

    public override void Dispose()
    {
        _apiClient.OnConnectionStatusChanged -= _apiClient_OnConnectionStatusChanged;
        _apiClient.OnAccountUpdate -= _apiClient_OnAccountUpdate;
        _shutdownCts.Dispose();
        _logger.LogInformation("DemoClient resources released");
        base.Dispose();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DemoClient starting up...");

        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _shutdownCts.Token);

            await _apiClient.StartAsync();
            await _apiClient.SubscribeAllAccounts();

            _logger.LogInformation("Subscribed to all accounts, waiting for updates...");

            while (!linkedCts.Token.IsCancellationRequested)
            {
                await Task.Delay(5000, linkedCts.Token);
            }
        }
        catch (TaskCanceledException)
        {
            _logger.LogInformation("DemoClient execution canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in DemoClient");
        }
        finally
        {
            _logger.LogInformation("DemoClient shutting down...");
            try
            {
                _apiClient.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing T4APIClient during shutdown");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("StopAsync called, initiating shutdown...");
        _shutdownCts.Cancel();
        await base.StopAsync(cancellationToken);
    }

    private void _apiClient_OnConnectionStatusChanged(object? sender, ConnectionStatusEventArgs e)
    {
        _logger.LogInformation(
            "Connection status changed: Connected={IsConnected}, Uptime={Uptime}, Disconnections={DisconnectionCount}",
            e.IsConnected,
            e.Uptime.ToString(@"hh\:mm\:ss"),
            e.DisconnectionCount);
    }

    private void _apiClient_OnAccountUpdate(object? sender, T4.AccountData.AccountUpdateEventArgs e)
    {
        var account = e.Account;
        var positions = e.UpdatedPositions;
        var orders = e.UpdatedOrders;
        var trades = e.UpdatedTrades;

        _logger.LogInformation(
            "Account Update: AccountId={AccountId}, Positions={PositionCount}, Orders={OrderCount}, Trades={TradeCount}",
            account.Details.AccountId,
            positions.Count,
            orders.Count,
            trades.Count);

        foreach (var position in positions)
        {
            _logger.LogInformation(
                "Position: Market={MarketId}, NetQty={NetQty}, AvgPrice={AvgPrice}",
                position.Data.MarketId,
                position.Data.Buys - position.Data.Sells,
                position.Data.AverageOpenPrice?.ToStringValue() ?? "N/A");
        }

        foreach (var order in orders)
        {
            _logger.LogInformation(
                "Order: ID={UniqueId}, Market={MarketId}, Status={Status}, BuySell={BuySell}, Qty={CurrentVolume}, Price={CurrentPrice}, TotalFillVolume={TotalFillVolume}",
                order.Data.UniqueId,
                order.Data.MarketId,
                order.Data.Status,
                order.Data.BuySell,
                order.Data.CurrentVolume,
                order.Data.CurrentLimitPrice?.ToStringValue() ?? "Market",
                order.Data.TotalFillVolume);
        }

        if (!_isSnapshotProcessed && orders.Count > 50)
        {
            _logger.LogInformation("Processing initial snapshot, inserting all trade events.");
            foreach (var order in orders)
            {
                foreach (var trade in order.Data.Trades)
                {
                    if (trade.Price != null && trade.Time != null)
                    {
                        try
                        {
                            _dbHelper.InsertTrade(
                                order.Data.UniqueId,
                                order.Data.MarketId,
                                trade.Price.ToDecimal() ?? 0.0m,
                                trade.Volume,
                                order.Data.BuySell.ToString(),
                                order.Data.Status.ToString(),
                                trade.Time.ProtobufTimestampToCST(),
                                trade.SequenceOrder
                            );
                            _logger.LogInformation("Inserted snapshot trade: OrderUniqueId={OrderUniqueId}, SequenceOrder={SequenceOrder}", order.Data.UniqueId, trade.SequenceOrder);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to insert snapshot trade: OrderUniqueId={OrderUniqueId}, SequenceOrder={SequenceOrder}", order.Data.UniqueId, trade.SequenceOrder);
                        }
                    }
                }
            }
            foreach (var trade in trades)
            {
                var priceString = trade.Data.Price != null ? trade.Data.Price.ToStringValue() : "N/A";
                var timestampString = trade.Data.Time != null ? trade.Data.Time.ProtobufTimestampToCST().ToString("o") : "N/A";
                _logger.LogInformation(
                    "Snapshot Trade: OrderUniqueId={OrderUniqueId}, TradeVolume={TradeVolume}, TotalFillVolume={TotalFillVolume}, BuySell={BuySell}, Status={Status}, Price={Price}, Market={MarketId}, Time={Timestamp}, SequenceOrder={SequenceOrder}",
                    trade.Order.Data.UniqueId,
                    trade.Data.Volume,
                    trade.Order.Data.TotalFillVolume,
                    trade.Order.Data.BuySell,
                    trade.Order.Data.Status,
                    priceString,
                    trade.Order.MarketId,
                    timestampString,
                    trade.Data.SequenceOrder);
            }
            _isSnapshotProcessed = true;
            return;
        }

        foreach (var trade in trades)
        {
            var priceString = trade.Data.Price != null ? trade.Data.Price.ToStringValue() : "N/A";
            var timestampString = trade.Data.Time != null ? trade.Data.Time.ProtobufTimestampToCST().ToString("o") : "N/A";

            _logger.LogInformation(
                "Trade: OrderUniqueId={OrderUniqueId}, TradeVolume={TradeVolume}, TotalFillVolume={TotalFillVolume}, BuySell={BuySell}, Status={Status}, Price={Price}, Market={MarketId}, Time={Timestamp}, SequenceOrder={SequenceOrder}",
                trade.Order.Data.UniqueId,
                trade.Data.Volume,
                trade.Order.Data.TotalFillVolume,
                trade.Order.Data.BuySell,
                trade.Order.Data.Status,
                priceString,
                trade.Order.MarketId,
                timestampString,
                trade.Data.SequenceOrder);

            if (trade.Data.Price != null && trade.Data.Time != null)
            {
                try
                {
                    _dbHelper.InsertTrade(
                        trade.Order.Data.UniqueId,
                        trade.Order.MarketId,
                        trade.Data.Price.ToDecimal() ?? 0.0m,
                        trade.Data.Volume,
                        trade.Order.Data.BuySell.ToString(),
                        trade.Order.Data.Status.ToString(),
                        trade.Data.Time.ProtobufTimestampToCST(),
                        trade.Data.SequenceOrder
                    );
                    _logger.LogInformation("Successfully inserted trade into database: OrderUniqueId={OrderUniqueId}, SequenceOrder={SequenceOrder}", trade.Order.Data.UniqueId, trade.Data.SequenceOrder);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to insert trade into database: OrderUniqueId={OrderUniqueId}, SequenceOrder={SequenceOrder}", trade.Order.Data.UniqueId, trade.Data.SequenceOrder);
                }
            }
            else
            {
                _logger.LogWarning("Skipped trade insertion due to missing Price or Time: OrderUniqueId={OrderUniqueId}, SequenceOrder={SequenceOrder}", trade.Order.Data.UniqueId, trade.Data.SequenceOrder);
            }
        }

        if (!_isSnapshotProcessed)
        {
            _isSnapshotProcessed = true;
        }
    }
}

