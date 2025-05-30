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

namespace T4APIDemo.DemoClient
{
    public class DemoClient : BackgroundService
    {
        private readonly T4APIClient _apiClient;
        private readonly DatabaseHelper _databaseHelper;
        private readonly IConfiguration _configuration;
        private readonly ILogger<DemoClient> _logger;
        private readonly CancellationTokenSource _shutdownCts = new();

        private readonly ConcurrentDictionary<string, int> _netQtyBySymbol = new();
        private readonly ConcurrentDictionary<string, List<(string OrderId, long Price, string Time, int RemainingVolume, int SequenceOrder)>> _pendingBuys = new();
        private readonly ConcurrentDictionary<string, List<(string OrderId, long Price, string Time, int RemainingVolume, int SequenceOrder)>> _pendingSells = new();

        private static readonly HashSet<string> ValidSymbols = new() { "XCME_Eq ES (M25)", "XCME_Eq MES (M25)", "XCME_NY CL (M25)" };

        public DemoClient(T4APIClient apiClient, DatabaseHelper databaseHelper, IConfiguration configuration, ILogger<DemoClient> logger)
        {
            _apiClient = apiClient ?? throw new ArgumentNullException(nameof(apiClient));
            _databaseHelper = databaseHelper ?? throw new ArgumentNullException(nameof(databaseHelper));
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _apiClient.OnReconnect += OnReconnect;
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

        private void OnReconnect(object? sender, EventArgs args)
        {
            _logger.LogInformation("Clearing pending orders on reconnect");
            _pendingBuys.Clear();
            _pendingSells.Clear();
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

            // Insert all orders
            foreach (var order in args.UpdatedOrders)
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

                _logger.LogDebug("Inserted order: OrderId={OrderId}, Status={Status}, Change={Change}, TotalFillVolume={TotalFillVolume}, TradesCount={TradesCount}",
                    orderData.UniqueId, orderData.Status, orderData.Change, orderData.TotalFillVolume, orderData.Trades.Count);
            }

            // Collect trades for orders with fills
            var tradesToInsert = new List<(string buyOrderId, string sellOrderId, string symbol, int volume,
                long buyPrice, long sellPrice, string buyTime, string sellTime, long tradeDate)>();

            foreach (var order in args.UpdatedOrders.Where(o => o.Data.TotalFillVolume > 0 || o.Data.Trades.Any()))
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

                foreach (var trade in orderData.Trades)
                {
                    if (_databaseHelper.IsFillProcessed(orderId, trade.SequenceOrder))
                    {
                        _logger.LogDebug("Skipping already processed fill: OrderId={OrderId}, SequenceOrder={SequenceOrder}, Volume={Volume}, Time={Time}",
                            orderId, trade.SequenceOrder, trade.Volume, trade.Time?.ToDateTime().ToString("O"));
                        continue;
                    }

                    var fillVolume = trade.Volume;
                    var fillTime = trade.Time?.ToDateTime().ToString("O") ?? time;
                    var fillPrice = long.TryParse(trade.Price?.Value ?? "0", out var tradePrice) ? tradePrice : price;
                    var tradeDate = trade.TradeDate > 0 ? trade.TradeDate : DateTime.UtcNow.Ticks;

                    var tradeData = (orderId, fillPrice, fillTime, fillVolume, trade.SequenceOrder);

                    _logger.LogDebug("Processing fill for {BuySell}: Symbol={Symbol}, OrderId={OrderId}, Volume={Volume}, Price={Price}, SequenceOrder={SequenceOrder}, FillTime={FillTime}",
                        buySell, symbol, orderId, fillVolume, fillPrice, trade.SequenceOrder, fillTime);

                    if (buySell == BuySell.Buy)
                    {
                        var buyList = _pendingBuys.GetOrAdd(symbol, _ => new List<(string, long, string, int, int)>());
                        lock (buyList)
                        {
                            buyList.Add(tradeData);
                            buyList.Sort((a, b) => a.Time.CompareTo(b.Time));
                        }
                    }
                    else if (buySell == BuySell.Sell)
                    {
                        var sellList = _pendingSells.GetOrAdd(symbol, _ => new List<(string, long, string, int, int)>());
                        lock (sellList)
                        {
                            sellList.Add(tradeData);
                            sellList.Sort((a, b) => a.Time.CompareTo(b.Time));
                        }
                    }
                }

                if (!orderData.Trades.Any() && orderData.TotalFillVolume > 0)
                {
                    _logger.LogWarning("Order has TotalFillVolume={TotalFillVolume} but no Trades: OrderId={OrderId}, Status={Status}",
                        orderData.TotalFillVolume, orderId, orderData.Status);
                }
            }

            // Log pending orders before matching
            foreach (var symbol in _pendingBuys.Keys)
            {
                if (_pendingBuys.TryGetValue(symbol, out var buys) && buys.Any())
                {
                    _logger.LogDebug("Before matching - Pending buy orders for {Symbol}: Count={Count}, Orders={Orders}",
                        symbol, buys.Count, string.Join("; ", buys.Select(b => $"Id={b.OrderId},Volume={b.RemainingVolume},Time={b.Time},SequenceOrder={b.SequenceOrder}")));
                }
            }
            foreach (var symbol in _pendingSells.Keys)
            {
                if (_pendingSells.TryGetValue(symbol, out var sells) && sells.Any())
                {
                    _logger.LogDebug("Before matching - Pending sell orders for {Symbol}: Count={Count}, Orders={Orders}",
                        symbol, sells.Count, string.Join("; ", sells.Select(s => $"Id={s.OrderId},Volume={s.RemainingVolume},Time={s.Time},SequenceOrder={s.SequenceOrder}")));
                }
            }

            // Attempt to match all pending orders
            foreach (var symbol in _pendingBuys.Keys.Concat(_pendingSells.Keys).Distinct())
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
                                var matchedVolume = Math.Min(buy.RemainingVolume, sell.RemainingVolume);

                                if (matchedVolume > 0)
                                {
                                    tradesToInsert.Add((
                                        buy.OrderId, sell.OrderId, symbol, matchedVolume,
                                        buy.Price, sell.Price, buy.Time, sell.Time, DateTime.UtcNow.Ticks));

                                    _databaseHelper.MarkFillProcessed(buy.OrderId, buy.SequenceOrder);
                                    _databaseHelper.MarkFillProcessed(sell.OrderId, sell.SequenceOrder);

                                    if (buy.RemainingVolume > matchedVolume)
                                        pendingBuys[0] = (buy.OrderId, buy.Price, buy.Time, buy.RemainingVolume - matchedVolume, buy.SequenceOrder);
                                    else
                                        pendingBuys.RemoveAt(0);

                                    if (sell.RemainingVolume > matchedVolume)
                                        pendingSells[0] = (sell.OrderId, sell.Price, sell.Time, sell.RemainingVolume - matchedVolume, sell.SequenceOrder);
                                    else
                                        pendingSells.RemoveAt(0);

                                    _logger.LogInformation("Matched trade: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}, Symbol={Symbol}, Volume={Volume}, BuyTime={BuyTime}, SellTime={SellTime}",
                                        buy.OrderId, sell.OrderId, symbol, matchedVolume, buy.Time, sell.Time);
                                }
                                else
                                {
                                    _logger.LogWarning("No matching volume: BuyOrderId={BuyOrderId}, SellOrderId={SellOrderId}, Symbol={Symbol}, BuyVolume={BuyVolume}, SellVolume={SellVolume}",
                                        buy.OrderId, sell.OrderId, symbol, buy.RemainingVolume, sell.RemainingVolume);
                                    break;
                                }
                            }
                        }
                }
            }

            // Log unmatched orders with total volume
            foreach (var symbol in _pendingBuys.Keys)
            {
                if (_pendingBuys.TryGetValue(symbol, out var buys) && buys.Any())
                {
                    var totalVolume = buys.Sum(b => b.RemainingVolume);
                    _logger.LogWarning("Unmatched buy orders for {Symbol}: Count={Count}, TotalVolume={TotalVolume}, Orders={Orders}",
                        symbol, buys.Count, totalVolume, string.Join("; ", buys.Select(b => $"Id={b.OrderId},Volume={b.RemainingVolume},Time={b.Time},SequenceOrder={b.SequenceOrder}")));
                }
            }
            foreach (var symbol in _pendingSells.Keys)
            {
                if (_pendingSells.TryGetValue(symbol, out var sells) && sells.Any())
                {
                    var totalVolume = sells.Sum(s => s.RemainingVolume);
                    _logger.LogWarning("Unmatched sell orders for {Symbol}: Count={Count}, TotalVolume={TotalVolume}, Orders={Orders}",
                        symbol, sells.Count, totalVolume, string.Join("; ", sells.Select(s => $"Id={s.OrderId},Volume={s.RemainingVolume},Time={s.Time},SequenceOrder={s.SequenceOrder}")));
                }
            }

            // Validate volume balance
            foreach (var symbol in _pendingBuys.Keys.Concat(_pendingSells.Keys).Distinct())
            {
                var buyVolume = _pendingBuys.TryGetValue(symbol, out var buys) ? buys.Sum(b => b.RemainingVolume) : 0;
                var sellVolume = _pendingSells.TryGetValue(symbol, out var sells) ? sells.Sum(s => s.RemainingVolume) : 0;
                if (buyVolume != sellVolume)
                {
                    _logger.LogWarning("Volume imbalance for {Symbol}: BuyVolume={BuyVolume}, SellVolume={SellVolume}",
                        symbol, buyVolume, sellVolume);
                }
            }

            if (tradesToInsert.Any())
            {
                _databaseHelper.InsertOrUpdateTrades(tradesToInsert);
            }
            else if (args.UpdatedOrders.Any(o => o.Data.TotalFillVolume > 0 || o.Data.Trades.Any()))
            {
                _logger.LogWarning("No trades inserted despite filled orders: FilledOrderCount={Count}, OrdersWithTrades={TradesCount}, OrderIds={OrderIds}",
                    args.UpdatedOrders.Count(o => o.Data.TotalFillVolume > 0),
                    args.UpdatedOrders.Count(o => o.Data.Trades.Any()),
                    string.Join(", ", args.UpdatedOrders.Where(o => o.Data.TotalFillVolume > 0 || o.Data.Trades.Any()).Select(o => o.Data.UniqueId)));
            }

            if (args.UpdatedOrders.Any())
            {
                _logger.LogInformation("Processed orders: Count={OrderCount}, Snapshot={IsSnapshot}",
                    args.UpdatedOrders.Count, args.UpdatedOrders.Count > 10 ? "Yes" : "No");
            }

            _logger.LogInformation("Account Update: AccountId={AccountId}, Positions={PositionCount}, Orders={OrderCount}",
                args.Account?.Details.AccountId ?? "Unknown", args.UpdatedPositions.Count, args.UpdatedOrders.Count);
        }

        public override void Dispose()
        {
            _apiClient.OnConnectionStatusChanged -= OnConnectionStatusChanged;
            _apiClient.OnAccountUpdate -= OnAccountUpdate;
            _apiClient.OnReconnect -= OnReconnect;
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
}



