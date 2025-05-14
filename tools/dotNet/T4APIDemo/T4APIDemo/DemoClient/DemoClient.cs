using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using T4APIDemo.T4;
using T4APIDemo.T4.Util;

namespace T4APIDemo.DemoClient;

public class DemoClient : BackgroundService
{
    private readonly ILogger<DemoClient> _logger;
    private readonly T4APIClient _apiClient;
    private readonly IConfiguration _configuration;

    public DemoClient(ILogger<DemoClient> logger, T4APIClient apiClient, IConfiguration configuration)
    {
        _logger = logger;
        _apiClient = apiClient;
        _configuration = configuration;

        _apiClient.OnConnectionStatusChanged += _apiClient_OnConnectionStatusChanged;
        _apiClient.OnAccountUpdate += _apiClient_OnAccountUpdate;
        _apiClient.OnMarketUpdate += _apiClient_OnMarketUpdate;
    }

    public override void Dispose()
    {
        // Unsubscribe from events to prevent memory leaks
        _apiClient.OnConnectionStatusChanged -= _apiClient_OnConnectionStatusChanged;
        _apiClient.OnAccountUpdate -= _apiClient_OnAccountUpdate;
        _apiClient.OnMarketUpdate -= _apiClient_OnMarketUpdate;

        _logger.LogInformation("DemoClient resources released");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DemoClient starting up...");

        try
        {
            // Start the API client
            await _apiClient.StartAsync();

            // Subscribe to all user accounts
            await _apiClient.SubscribeAllAccounts();

            //// Example: Subscribe to some markets
            //var markets = new (string, string, string)[]
            //{
            //    ("CME_C", "ZC", "XCME_C ZC (K25)"),
            //    ("CME_C", "ZS", "XCME_C ZS (K25)"),
            //    ("CME_Eq", "ES", "XCME_Eq ES (M25)"),
            //    ("CME_Eq", "NQ", "XCME_Eq NQ (M25)")
            //};

            //foreach (var market in markets)
            //{
            //    _logger.LogInformation($"Subscribing to market: {market.Item3}");
            //    await _apiClient.SubscribeMarket(market.Item1, market.Item2, market.Item3);
            //    _logger.LogInformation($"Subscribed market: {market.Item3}");
            //}

            // Keep the service running until cancellation is requested
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(5000, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in DemoClient");
        }
        finally
        {
            _logger.LogInformation("DemoClient shutting down...");
        }
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

        //_logger.LogInformation(
        //    "Account update: AccountId={AccountId}, Positions={PositionCount}, Orders={OrderCount}, Trades={TradeCount}",
        //    account.Details.AccountId,
        //    positions.Count,
        //    orders.Count,
        //    trades.Count);

        //// Log individual positions if any were updated
        //foreach (var position in positions)
        //{
        //    var posData = position.Data;
        //    _logger.LogInformation(
        //        "Position: Market={MarketId}, NetQty={NetQty}, AvgPrice={AvgPrice}",
        //        posData.MarketId,
        //        posData.Buys - posData.Sells,
        //        posData.AverageOpenPrice?.ToStringValue() ?? "N/A");
        //}

        //// Log individual orders if any were updated
        //foreach (var order in orders)
        //{
        //    var orderData = order.Data;
        //    _logger.LogInformation(
        //        "Order: ID={UniqueId}, Market={MarketId}, Status={Status}, BuySell={BuySell}, Qty={CurrentVolume}, Price={CurrentPrice}",
        //        orderData.UniqueId,
        //        orderData.MarketId,
        //        orderData.Status,
        //        orderData.BuySell,
        //        orderData.CurrentVolume,
        //        orderData.CurrentLimitPrice?.ToStringValue() ?? "Market");
        //}

        // Log individual trades if any were updated
        foreach (var trade in trades)
        {
            _logger.LogInformation(
                "Trade: ID={ExchangeTradeId}, Volume={Volume}, Price={Price}",
                trade.ExchangeTradeId,
                trade.Data.Volume,
                trade.Data.Price.ToStringValue());
        }
    }

    private void _apiClient_OnMarketUpdate(T4.MarketData.MarketDataSnapshot snapshot)
    {
        string marketId = snapshot.MarketID ?? "Unknown";

        // Log market depth updates
        if (snapshot.MarketDepth != null)
        {
            var depth = snapshot.MarketDepth;
            int bidLevels = depth.Bids.Count;
            int offerLevels = depth.Offers.Count;

            var bestBid = bidLevels > 0 ? depth.Bids[0] : null;
            var bestOffer = offerLevels > 0 ? depth.Offers[0] : null;

            string bestBidStr = bestBid != null ? $"{bestBid.Volume}@{bestBid.Price.ToStringValue()}" : "None";
            string bestOfferStr = bestOffer != null ? $"{bestOffer.Volume}@{bestOffer.Price.ToStringValue()}" : "None";

            _logger.LogInformation(
                "Market depth update: Market={MarketId}, Mode={Mode}, BestBid={BestBid}, BestOffer={BestOffer}",
                marketId,
                depth.Mode,
                bestBidStr,
                bestOfferStr);
        }

        // Log market by order updates
        if (snapshot.MarketByOrder != null)
        {
            var mbo = snapshot.MarketByOrder;
            _logger.LogInformation(
                "Market by order update: Market={MarketId}, Mode={Mode}, OrderCount={OrderCount}, BidLevels={BidLevels}, OfferLevels={OfferLevels}",
                marketId,
                mbo.MarketMode,
                mbo.OrderCount,
                mbo.Bids.Count,
                mbo.Offers.Count);
        }

        // Log settlement updates
        if (snapshot.MarketSettlement != null)
        {
            var settlement = snapshot.MarketSettlement;
            _logger.LogInformation(
                "Market settlement update: Market={MarketId}, SettlementPrice={SettlementPrice}",
                marketId,
                settlement.SettlementPrice?.ToStringValue() ?? "N/A");
        }

        // Log high/low updates
        if (snapshot.MarketHighLow != null)
        {
            var highLow = snapshot.MarketHighLow;
            _logger.LogInformation(
                "Market high/low update: Market={MarketId}, High={High}, Low={Low}",
                marketId,
                highLow.HighPrice?.ToStringValue() ?? "N/A",
                highLow.LowPrice?.ToStringValue() ?? "N/A");
        }
    }
}