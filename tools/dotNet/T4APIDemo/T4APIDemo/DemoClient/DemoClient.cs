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

        _logger.LogInformation(
            "Account Update: AccountId={AccountId}, Positions={PositionCount}, Orders={OrderCount}",
            account.Details.AccountId,
            positions.Count,
            orders.Count);

        foreach (var position in positions)
        {
            _logger.LogInformation(
                "Position: Market={MarketId}, NetQty={NetQty}, AvgPrice={AvgPrice}",
                position.Data.MarketId,
                position.Data.Buys - position.Data.Sells,
                position.Data.AverageOpenPrice?.ToStringValue() ?? "N/A");
        }

        if (!_isSnapshotProcessed && orders.Count > 50)
        {
            _logger.LogInformation("Processing initial snapshot, inserting Finished orders.");
            foreach (var order in orders.Where(o => o.Data.Status == OrderStatus.Finished))
            {
                _logger.LogInformation("Order StatusDetail: {StatusDetail}", order.Data.StatusDetail ?? "null");
                _logger.LogInformation(
                    "Order: ID={UniqueId}, Market={MarketId}, Status={Status}, BuySell={BuySell}, Qty={CurrentVolume}, Price={CurrentPrice}, TotalFillVolume={TotalFillVolume}, StatusDetail={StatusDetail}, Change={Change}",
                    order.Data.UniqueId,
                    order.Data.MarketId,
                    order.Data.Status,
                    order.Data.BuySell,
                    order.Data.CurrentVolume,
                    order.Data.CurrentLimitPrice?.ToStringValue() ?? "Market",
                    order.Data.TotalFillVolume,
                    order.Data.StatusDetail ?? "null",
                    order.Data.Change.ToString());

                try
                {
                    _dbHelper.InsertOrUpdateOrder(
                        order.Data.UniqueId,
                        order.Data.MarketId,
                        order.Data.Status.ToString(),
                        order.Data.BuySell.ToString(),
                        order.Data.CurrentVolume,
                        order.Data.CurrentLimitPrice?.ToDecimal(),
                        order.Data.TotalFillVolume,
                        order.Data.Time?.ProtobufTimestampToCST() ?? DateTime.Now,
                        order.Data.StatusDetail ?? string.Empty,
                        order.Data.Change.ToString());
                    _logger.LogInformation("Inserted/Updated order: OrderUniqueId={OrderUniqueId}, Status={Status}", order.Data.UniqueId, order.Data.Status);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to insert/update order: OrderUniqueId={OrderUniqueId}", order.Data.UniqueId);
                }
            }
            _isSnapshotProcessed = true;
            return;
        }

        foreach (var order in orders)
        {
            _logger.LogInformation("Order StatusDetail: {StatusDetail}", order.Data.StatusDetail ?? "null");
            _logger.LogInformation(
                "Order: ID={UniqueId}, Market={MarketId}, Status={Status}, BuySell={BuySell}, Qty={CurrentVolume}, Price={CurrentPrice}, TotalFillVolume={TotalFillVolume}, StatusDetail={StatusDetail}, Change={Change}",
                order.Data.UniqueId,
                order.Data.MarketId,
                order.Data.Status,
                order.Data.BuySell,
                order.Data.CurrentVolume,
                order.Data.CurrentLimitPrice?.ToStringValue() ?? "Market",
                order.Data.TotalFillVolume,
                order.Data.StatusDetail ?? "null",
                order.Data.Change.ToString());

            try
            {
                _dbHelper.InsertOrUpdateOrder(
                    order.Data.UniqueId,
                    order.Data.MarketId,
                    order.Data.Status.ToString(),
                    order.Data.BuySell.ToString(),
                    order.Data.CurrentVolume,
                    order.Data.CurrentLimitPrice?.ToDecimal(),
                    order.Data.TotalFillVolume,
                    order.Data.Time?.ProtobufTimestampToCST() ?? DateTime.Now,
                    order.Data.StatusDetail ?? string.Empty,
                    order.Data.Change.ToString());
                _logger.LogInformation("Inserted/Updated order: OrderUniqueId={OrderUniqueId}, Status={Status}", order.Data.UniqueId, order.Data.Status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to insert/update order: OrderUniqueId={OrderUniqueId}", order.Data.UniqueId);
            }
        }

        if (!_isSnapshotProcessed)
        {
            _isSnapshotProcessed = true;
        }
    }
}

