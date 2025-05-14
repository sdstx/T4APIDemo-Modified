using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using T4Proto.V1.Orderrouting;
using T4Proto.V1.Service;

namespace T4APIDemo.T4.AccountData;

public class Order
{
    private volatile OrderUpdate _data;
    private readonly ConcurrentDictionary<string, Trade> _trades = new();
    private readonly ConcurrentDictionary<int, TradeLeg> _tradeLegs = new();

    private readonly Account _account;

    private readonly ILogger<Order> _logger;
    private readonly ILoggerFactory _loggerFactory;

    public Order(Account account, OrderUpdate orderUpdate, ILoggerFactory loggerFactory)
    {
        _account = account;
        _data = orderUpdate;

        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<Order>();
    }

    public OrderUpdate Data => _data;
    public Account Account => _account;
    public IReadOnlyDictionary<string, Trade> Trades => _trades;
    public IReadOnlyDictionary<int, TradeLeg> TradeLegs => _tradeLegs;

    public string UniqueId => _data.UniqueId;
    public string MarketId => _data.MarketId;

    public record OrderUpdateResult(Order Order, List<Trade> Trades);

    public OrderUpdateResult UpdateWithMessage(ServerMessage serverMessage)
    {
        var updatedTrades = new List<Trade>();

        switch (serverMessage.PayloadCase)
        {
            case ServerMessage.PayloadOneofCase.OrderUpdate:
                _data = serverMessage.OrderUpdate;
                ProcessOrderUpdateTrades(serverMessage.OrderUpdate);
                _logger.LogInformation("Order updated: {UniqueId}", _data.UniqueId);
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateTrade:
                var updatedTrade = AddOrUpdateTrade(serverMessage.OrderUpdateTrade);
                updatedTrades.Add(updatedTrade);
                _logger.LogInformation("Order trade update: {UniqueId}, Exchange Trade ID: {ExchangeTradeId}",
                    serverMessage.OrderUpdateTrade.UniqueId,
                    serverMessage.OrderUpdateTrade.ExchangeTradeId);
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateTradeLeg:
                AddOrUpdateTradeLeg(serverMessage.OrderUpdateTradeLeg);
                _logger.LogInformation("Order trade leg update: {UniqueId}, Leg Index: {LegIndex}",
                    serverMessage.OrderUpdateTradeLeg.UniqueId,
                    serverMessage.OrderUpdateTradeLeg.LegIndex);
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateStatus:
                UpdateStatusInformation(serverMessage.OrderUpdateStatus);
                _logger.LogInformation("Order status update: {UniqueId}, Status: {Status}",
                    serverMessage.OrderUpdateStatus.UniqueId,
                    serverMessage.OrderUpdateStatus.Status);
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateFailed:
                UpdateFailedStatus(serverMessage.OrderUpdateFailed);
                _logger.LogWarning("Order failed: {UniqueId}, Status: {Status}, Detail: {Detail}",
                    serverMessage.OrderUpdateFailed.UniqueId,
                    serverMessage.OrderUpdateFailed.Status,
                    serverMessage.OrderUpdateFailed.StatusDetail);
                break;
        }

        return new OrderUpdateResult(this, updatedTrades);
    }

    public OrderUpdateResult UpdateWithMultiMessage(OrderUpdateMultiMessage message)
    {
        var updatedTrades = new List<Trade>();

        switch (message.PayloadCase)
        {
            case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdate:
                {
                    _data = message.OrderUpdate;
                    var orderUpdatedTrades = ProcessOrderUpdateTrades(message.OrderUpdate);
                    updatedTrades.AddRange(orderUpdatedTrades);
                    _logger.LogInformation("Order multi update: {UniqueId}", _data.UniqueId);
                }
                break;

            case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateTrade:
                {
                    var updatedTrade = AddOrUpdateTrade(message.OrderUpdateTrade);
                    updatedTrades.Add(updatedTrade);
                    _logger.LogInformation("Order multi trade update: {UniqueId}, Exchange Trade ID: {ExchangeTradeId}",
                        message.OrderUpdateTrade.UniqueId,
                        message.OrderUpdateTrade.ExchangeTradeId);
                }
                break;

            case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateTradeLeg:
                AddOrUpdateTradeLeg(message.OrderUpdateTradeLeg);
                _logger.LogInformation("Order multi trade leg update: {UniqueId}, Leg Index: {LegIndex}",
                    message.OrderUpdateTradeLeg.UniqueId,
                    message.OrderUpdateTradeLeg.LegIndex);
                break;

            case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateStatus:
                UpdateStatusInformation(message.OrderUpdateStatus);
                _logger.LogInformation("Order multi status update: {UniqueId}, Status: {Status}",
                    message.OrderUpdateStatus.UniqueId,
                    message.OrderUpdateStatus.Status);
                break;

            case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateFailed:
                UpdateFailedStatus(message.OrderUpdateFailed);
                _logger.LogWarning("Order multi failed: {UniqueId}, Status: {Status}, Detail: {Detail}",
                    message.OrderUpdateFailed.UniqueId,
                    message.OrderUpdateFailed.Status,
                    message.OrderUpdateFailed.StatusDetail);
                break;
        }

        return new OrderUpdateResult(this, updatedTrades);
    }

    private List<Trade> ProcessOrderUpdateTrades(OrderUpdate update)
    {
        var updatedTrades = new List<Trade>();

        // Process any trades in the order update
        foreach (var tradeData in update.Trades)
        {
            var trade = new Trade(this, tradeData, _loggerFactory.CreateLogger<Trade>());
            var updatedTrade = _trades.AddOrUpdate(
                tradeData.ExchangeTradeId,
                trade,
                (_, _) => trade);

            updatedTrades.Add(updatedTrade);
        }

        // Process any trade legs in the order update
        foreach (var legData in update.TradeLegs)
        {
            var leg = new TradeLeg(this, legData, _loggerFactory.CreateLogger<TradeLeg>());
            _tradeLegs.AddOrUpdate(
                legData.LegIndex,
                leg,
                (_, _) => leg);
        }

        return updatedTrades;
    }

    private Trade AddOrUpdateTrade(OrderUpdateTrade tradeUpdate)
    {
        // Update the order data with trade information
        var updatedData = _data.Clone();
        updatedData.Change = tradeUpdate.Change;
        updatedData.ExchangeTime = tradeUpdate.ExchangeTime;
        updatedData.Status = tradeUpdate.Status;
        updatedData.ResponsePending = tradeUpdate.ResponsePending;
        updatedData.StatusDetail = tradeUpdate.StatusDetail;
        updatedData.Time = tradeUpdate.Time;
        updatedData.WorkingVolume = tradeUpdate.WorkingVolume;
        updatedData.TotalFillVolume = tradeUpdate.TotalFillVolume;

        // Check if this is a stop that has not triggered yet
        if (updatedData.PriceType == T4Proto.V1.Common.PriceType.StopLimit)
        {
            updatedData.PriceType = T4Proto.V1.Common.PriceType.Limit;
        }
        else if (updatedData.PriceType == T4Proto.V1.Common.PriceType.StopSameLimit)
        {
            updatedData.PriceType = T4Proto.V1.Common.PriceType.Limit;
            updatedData.CurrentLimitPrice = updatedData.CurrentStopPrice;
            updatedData.CurrentStopPrice = null;
        }
        else if (updatedData.PriceType == T4Proto.V1.Common.PriceType.StopMarket)
        {
            updatedData.PriceType = T4Proto.V1.Common.PriceType.Market;
        }

        _data = updatedData;

        // Create or update the Trade object
        var trade = _trades.AddOrUpdate(
            tradeUpdate.ExchangeTradeId,
            new Trade(this, tradeUpdate, _loggerFactory.CreateLogger<Trade>()),
            (_, existingTrade) =>
            {
                existingTrade.UpdateWithMessage(tradeUpdate);
                return existingTrade;
            });

        return trade;
    }

    private void AddOrUpdateTradeLeg(OrderUpdateTradeLeg legUpdate)
    {
        // Update the order data
        var updatedData = _data.Clone();
        _data = updatedData;

        // Create or update the TradeLeg object
        var tradeLeg = _tradeLegs.AddOrUpdate(
            legUpdate.LegIndex,
            new TradeLeg(this, legUpdate, _loggerFactory.CreateLogger<TradeLeg>()),
            (_, existingLeg) =>
            {
                existingLeg.UpdateWithMessage(legUpdate);
                return existingLeg;
            });
    }

    private void UpdateStatusInformation(OrderUpdateStatus statusUpdate)
    {
        var updatedData = _data.Clone();

        updatedData.Change = statusUpdate.Change;
        updatedData.ExchangeTime = statusUpdate.ExchangeTime;
        updatedData.Status = statusUpdate.Status;
        updatedData.ResponsePending = statusUpdate.ResponsePending;
        updatedData.StatusDetail = statusUpdate.StatusDetail;
        updatedData.Time = statusUpdate.Time;
        updatedData.CurrentVolume = statusUpdate.CurrentVolume;
        updatedData.CurrentLimitPrice = statusUpdate.CurrentLimitPrice;
        updatedData.CurrentStopPrice = statusUpdate.CurrentStopPrice;
        updatedData.PriceType = statusUpdate.PriceType;
        updatedData.TimeType = statusUpdate.TimeType;
        updatedData.ExchangeOrderId = statusUpdate.ExchangeOrderId;
        updatedData.WorkingVolume = statusUpdate.WorkingVolume;
        updatedData.ExecutingLoginId = statusUpdate.ExecutingLoginId;
        updatedData.UserId = statusUpdate.UserId;
        updatedData.UserName = statusUpdate.UserName;
        updatedData.RoutingUserId = statusUpdate.RoutingUserId;
        updatedData.RoutingUserName = statusUpdate.RoutingUserName;
        updatedData.UserAddress = statusUpdate.UserAddress;
        updatedData.SessionId = statusUpdate.SessionId;
        updatedData.AppId = statusUpdate.AppId;
        updatedData.AppName = statusUpdate.AppName;
        updatedData.ActivationType = statusUpdate.ActivationType;
        updatedData.ActivationDetails = statusUpdate.ActivationDetails;
        updatedData.TrailPrice = statusUpdate.TrailPrice;
        updatedData.CurrentMaxShow = statusUpdate.CurrentMaxShow;
        updatedData.NewVolume = statusUpdate.NewVolume;
        updatedData.NewLimitPrice = statusUpdate.NewLimitPrice;
        updatedData.NewStopPrice = statusUpdate.NewStopPrice;
        updatedData.NewMaxShow = statusUpdate.NewMaxShow;
        updatedData.Tag = statusUpdate.Tag;
        updatedData.TagClOrdId = statusUpdate.TagClOrdId;
        updatedData.TagOrigClOrdId = statusUpdate.TagOrigClOrdId;
        updatedData.SmpId = statusUpdate.SmpId;
        updatedData.ExchangeLoginId = statusUpdate.ExchangeLoginId;
        updatedData.ExchangeLocation = statusUpdate.ExchangeLocation;
        updatedData.AtsRegulatoryId = statusUpdate.AtsRegulatoryId;
        updatedData.MaxVolume = statusUpdate.MaxVolume;
        updatedData.SequenceOrder = statusUpdate.SequenceOrder;
        updatedData.AuthorizedTraderId = statusUpdate.AuthorizedTraderId;

        // Update instruction extra if available
        if (statusUpdate.InstructionExtra != null && statusUpdate.InstructionExtra.Count > 0)
        {
            foreach (var entry in statusUpdate.InstructionExtra)
            {
                updatedData.InstructionExtra[entry.Key] = entry.Value;
            }
        }

        updatedData.AppType = statusUpdate.AppType;

        // Assign the updated data
        _data = updatedData;
    }

    private void UpdateFailedStatus(OrderUpdateFailed failedUpdate)
    {
        var updatedData = _data.Clone();

        updatedData.Change = failedUpdate.Change;
        updatedData.ExchangeTime = failedUpdate.ExchangeTime;
        updatedData.Status = failedUpdate.Status;
        updatedData.ResponsePending = failedUpdate.ResponsePending;
        updatedData.StatusDetail = failedUpdate.StatusDetail;
        updatedData.Time = failedUpdate.Time;

        // Update these fields if they exist in the failed update
        if (!string.IsNullOrEmpty(failedUpdate.TagClOrdId))
        {
            updatedData.TagClOrdId = failedUpdate.TagClOrdId;
        }

        if (failedUpdate.SequenceOrder > 0)
        {
            updatedData.SequenceOrder = failedUpdate.SequenceOrder;
        }

        // For certain order changes, update additional fields based on the legacy VB code
        if (failedUpdate.Change == T4Proto.V1.Common.OrderChange.SubmissionFailed)
        {
            updatedData.Status = T4Proto.V1.Common.OrderStatus.Rejected;
            updatedData.WorkingVolume = 0;
            updatedData.CurrentVolume = updatedData.NewVolume;
            updatedData.CurrentLimitPrice = updatedData.NewLimitPrice;
            updatedData.CurrentStopPrice = updatedData.NewStopPrice;
            updatedData.CurrentMaxShow = updatedData.NewMaxShow;
            updatedData.SubmitTime = failedUpdate.Time;
        }
        else if (failedUpdate.Change == T4Proto.V1.Common.OrderChange.RevisionFailed)
        {
            updatedData.NewVolume = updatedData.CurrentVolume;
            updatedData.NewLimitPrice = updatedData.CurrentLimitPrice;
            updatedData.NewStopPrice = updatedData.CurrentStopPrice;
            updatedData.NewMaxShow = updatedData.CurrentMaxShow;
        }

        // Assign the updated data
        _data = updatedData;
    }
}