using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using T4APIDemo.T4;
using T4APIDemo.T4.AccountData;
using T4APIDemo.T4.Util;
using T4Proto.V1.Common;
using T4Proto.V1.Orderrouting;

namespace T4APIDemo.DemoClient;

public class DemoClient : BackgroundService
{
    private readonly T4APIClient _apiClient;
    private readonly DatabaseHelper _databaseHelper;
    private readonly IConfiguration _configuration;
    private readonly ILogger<DemoClient> _logger;
    private readonly CancellationTokenSource _shutdownCts = new();

    private readonly ConcurrentDictionary<string, int> _netQtyBySymbol = new();
    private readonly ConcurrentDictionary<string, List<(string OrderId, long Price, string Time, int Volume)>> _pendingBuys = new();
    private readonly ConcurrentDictionary<string, List<(string OrderId, long Price, string Time, int Volume)>> _pendingSells = new();
    private readonly ConcurrentDictionary<string, bool> _processedTradeIds = new();

    private static readonly HashSet<string> ValidSymbols = new() { "XCME_Eq ES (M25)", "XCME_Eq MES (M25)", "XCME_NY CL (N25)" };

    public DemoClient(T4APIClient apiClient, DatabaseHelper databaseHelper, IConfiguration configuration, ILogger<DemoClient> logger)
    {
        _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
        _databaseHelper = databaseHelper ?? throw new ArgumentNullException(nameof(databaseHelper));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting DemoClient...");
        _apiClient.OnAccountUpdate += OnAccountUpdate;
        _apiClient.OnConnectionStatusChanged += OnConnectionStatusChanged;

        try
        {
            await _apiClient.StartAsync();
            await _apiClient.SubscribeAllAccounts();

            _logger.LogInformation("DemoClient started. Press Ctrl+C to stop.");
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("DemoClient stopping...");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DemoClient encountered an error");
        }
        finally
        {
            Dispose();
        }
    }

    private void OnConnectionStatusChanged(object? sender, ConnectionStatusEventArgs args)
    {
        _logger.LogInformation("Connection status changed: Connected={Connected}, Uptime={Uptime}, Disconnections={Disconnections}",
            args.IsConnected, args.Uptime, args.DisconnectionCount);
    }

    private string GetSymbol(OrderUpdate orderData)
    {
        var marketId = orderData?.MarketId ?? "Unknown";
        if (!ValidSymbols.Contains(marketId))
        {
            _logger.LogWarning("Invalid MarketId: {MarketId}, OrderId={OrderId}", marketId, orderData?.UniqueId);
            return "Unknown";
        }
        return marketId;
    }

    private void OnAccountUpdate(object? sender, AccountUpdateEventArgs args)
    {
        // Update NetQty from AccountPosition
        foreach (var position in args.UpdatedPositions)
        {
            var symbol = position.Data.MarketId ?? "Unknown";
            if (!ValidSymbols.Contains(symbol))
            {
                _logger.LogWarning("Invalid position symbol: {Symbol}", symbol);
                continue;
            }
            _netQtyBySymbol.AddOrUpdate(symbol, position.Data.TotalOpenVolume, (_, _) => position.Data.TotalOpenVolume);
        }

        // Insert all Finished orders
        foreach (var order in args.UpdatedOrders.Where(o => o.Data.Status == OrderStatus.Finished))
        {
            var orderData = order.Data;
            var symbol = GetSymbol(orderData);
            if (!ValidSymbols.Contains(symbol))
            {
                _logger.LogWarning("Skipping order with invalid symbol: {Symbol}, OrderId={OrderId}", symbol, orderData.UniqueId);
                continue;
            }
            var priceString = orderData.CurrentLimitPrice?.Value ?? "0";
            var price = long.TryParse(priceString, out var parsedPrice) ? parsedPrice : 0;
            var time = orderData.Time?.ProtobufTimestampToCST().ToString("O") ?? DateTime.UtcNow.ToString("O");

            _databaseHelper.InsertOrUpdateOrder(
                orderData.UniqueId,
                symbol,
                orderData.Status.ToString(),
                orderData.BuySell.ToString(),
                orderData.CurrentVolume,
                price,
                orderData.TotalFillVolume,
                time,
                orderData.StatusDetail,
                orderData.Change.ToString());
        }

        // Collect trades for Finished orders with fills
        var tradesToInsert = new List<(string buyOrderId, string sellOrderId, string symbol, int volume,
            long buyPrice, long sellPrice, string buyTime, string sellTime, long tradeDate)>();

        foreach (var order in args.UpdatedOrders.Where(o => o.Data.Status == OrderStatus.Finished && o.Data.TotalFillVolume > 0))
        {
            var orderData = order.Data;
            var orderId = orderData.UniqueId;
            var symbol = GetSymbol(orderData);
            if (!ValidSymbols.Contains(symbol))
            {
                _logger.LogWarning("Skipping trade for invalid symbol: {Symbol}, OrderId={OrderId}", symbol, orderId);
                continue;
            }
            var buySell = orderData.BuySell;
            var priceString = orderData.CurrentLimitPrice?.Value ?? "0";
            var price = long.TryParse(priceString, out var parsedPrice) ? parsedPrice : 0;
            var time = orderData.Time?.ProtobufTimestampToCST().ToString("O") ?? DateTime.UtcNow.ToString("O");
            var volume = orderData.TotalFillVolume;

            var tradeKey = orderId;
            if (_processedTradeIds.ContainsKey(tradeKey))
            {
                _logger.LogDebug("Skipping already processed trade: OrderId={OrderId}", orderId);
                continue;
            }

            _processedTradeIds.TryAdd(tradeKey, true);
            var tradeData = (orderId, price, time, volume);

            _logger.LogDebug("Adding trade to {BuySell} list: Symbol={Symbol}, OrderId={OrderId}, Volume={Volume}, Price={Price}",
                buySell, symbol, orderId, volume, price);

            if (buySell == BuySell.Buy)
            {
                var buyList = _pendingBuys.GetOrAdd(symbol, _ => new List<(string, long, string, int)>());
                lock (buyList)
                {
                    buyList.Add(tradeData);
                    buyList.Sort((a, b) => a.Time.CompareTo(b.Time));
                }
            }
            else if (buySell == BuySell.Sell)
            {
                var sellList = _pendingSells.GetOrAdd(symbol, _ => new List<(string, long, string, int)>());
                lock (sellList)
                {
                    sellList.Add(tradeData);
                    sellList.Sort((a, b) => a.Time.CompareTo(b.Time));
                }
            }

            if (_pendingBuys.TryGetValue(symbol, out var pendingBuys) && pendingBuys.Any() &&
                _pendingSells.TryGetValue(symbol, out var pendingSells) && pendingSells.Any())
            {
                lock (pendingBuys)
                lock (pendingSells)
                {
                    while (pendingBuys.Any() && pendingSells.Any())
                    {
                        var buy = pendingBuys.First();
                        var sell = pendingSells.First();
                        var matchedVolume = Math.Min(buy.Volume, sell.Volume);

                        if (matchedVolume > 0)
                        {
                            var tradeDate = orderData.Time?.ProtobufTimestampToCST().Ticks ?? DateTime.UtcNow.Ticks;
                            tradesToInsert.Add((
                                buy.OrderId, sell.OrderId, symbol, matchedVolume,
                                buy.Price, sell.Price, buy.Time, sell.Time, tradeDate));

                            if (buy.Volume > matchedVolume)
                                pendingBuys[0] = (buy.OrderId, buy.Price, buy.Time, buy.Volume - matchedVolume);
                            else
                                pendingBuys.RemoveAt(0);

                            if (sell.Volume > matchedVolume)
                                pendingSells[0] = (sell.OrderId, sell.Price, sell.Time, sell.Volume - matchedVolume);
                            else
                                pendingSells.RemoveAt(0);
                        }
                        else
                        {
                            _logger.LogWarning("No matching volume: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}, Symbol={Symbol}",
                                buy.OrderId, sell.OrderId, symbol);
                            break;
                        }
                    }
                }
            }
        }

        // Final pass to pair remaining orders for same instrument
        foreach (var symbol in ValidSymbols)
        {
            if (_pendingBuys.TryGetValue(symbol, out var pendingBuys) && pendingBuys.Any() &&
                _pendingSells.TryGetValue(symbol, out var pendingSells) && pendingSells.Any())
            {
                lock (pendingBuys)
                lock (pendingSells)
                {
                    while (pendingBuys.Any() && pendingSells.Any())
                    {
                        var buy = pendingBuys.First();
                        var sell = pendingSells.First();
                        var matchedVolume = Math.Min(buy.Volume, sell.Volume);
                        if (matchedVolume > 0)
                        {
                            var tradeDate = DateTime.Parse(buy.Time).Ticks > DateTime.Parse(sell.Time).Ticks
                                ? DateTime.Parse(buy.Time).Ticks
                                : DateTime.Parse(sell.Time).Ticks;
                            tradesToInsert.Add((
                                buy.OrderId, sell.OrderId, symbol, matchedVolume,
                                buy.Price, sell.Price, buy.Time, sell.Time, tradeDate));
                            _logger.LogInformation("Final pass paired: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}, Symbol={Symbol}, Volume={Volume}",
                                buy.OrderId, sell.OrderId, symbol, matchedVolume);
                            if (buy.Volume > matchedVolume)
                                pendingBuys[0] = (buy.OrderId, buy.Price, buy.Time, buy.Volume - matchedVolume);
                            else
                                pendingBuys.RemoveAt(0);
                            if (sell.Volume > matchedVolume)
                                pendingSells[0] = (sell.OrderId, sell.Price, sell.Time, sell.Volume - matchedVolume);
                            else
                                pendingSells.RemoveAt(0);
                        }
                        else break;
                    }
                }
            }
        }

        if (tradesToInsert.Any())
        {
            _databaseHelper.InsertOrUpdateTrades(tradesToInsert);
        }

        if (args.UpdatedOrders.Any() && args.UpdatedOrders.Count > 10)
        {
            _logger.LogInformation("Processing initial snapshot, inserting Finished orders");
        }

        _logger.LogInformation("Account Update: AccountId={AccountId}, Positions={PositionCount}, Orders={OrderCount}",
            args.Account?.Details.AccountId ?? "Unknown", args.UpdatedPositions.Count, args.UpdatedOrders.Count);
    }

    public override void Dispose()
    {
        _apiClient.OnConnectionStatusChanged -= OnConnectionStatusChanged;
        _apiClient.OnAccountUpdate -= OnAccountUpdate;
        _shutdownCts.Dispose();
        try
        {
            _apiClient.Dispose();
        }
        catch (System.Net.WebSockets.WebSocketException ex)
        {
            _logger.LogWarning(ex, "WebSocket error during T4APIClient disposal");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error disposing T4APIClient");
        }
        _logger.LogInformation("DemoClient resources released");
        base.Dispose();
    }
}