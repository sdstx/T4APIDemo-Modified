using Microsoft.Extensions.Logging;
using T4Proto.V1.Orderrouting;

namespace T4APIDemo.T4.AccountData;

public class Trade
{
    private volatile OrderUpdate.Types.Trade _data;
    private readonly Order _order;
    private readonly ILogger<Trade> _logger;

    public Trade(Order order, OrderUpdate.Types.Trade tradeData, ILogger<Trade> logger)
    {
        _order = order;
        _data = tradeData;
        _logger = logger;
    }

    public Trade(Order order, OrderUpdateTrade tradeUpdate, ILogger<Trade> logger)
    {
        _order = order;
        _logger = logger;

        // Convert OrderUpdateTrade to OrderUpdate.Types.Trade
        _data = new OrderUpdate.Types.Trade
        {
            SequenceOrder = tradeUpdate.SequenceOrder,
            Volume = tradeUpdate.Volume,
            Price = tradeUpdate.Price,
            ResidualVolume = tradeUpdate.ResidualVolume,
            Time = tradeUpdate.Time,
            ExchangeTradeId = tradeUpdate.ExchangeTradeId,
            ExchangeTime = tradeUpdate.ExchangeTime,
            ContraTrader = tradeUpdate.ContraTrader,
            ContraBroker = tradeUpdate.ContraBroker,
            TradeDate = tradeUpdate.TradeDate
        };
    }

    public OrderUpdate.Types.Trade Data => _data;
    public Order Order => _order;
    public string ExchangeTradeId => _data.ExchangeTradeId;

    public void UpdateWithMessage(OrderUpdateTrade tradeUpdate)
    {
        if (tradeUpdate.ExchangeTradeId != _data.ExchangeTradeId)
        {
            _logger.LogWarning("Received trade update with mismatched ExchangeTradeId. Expected: {Expected}, Received: {Received}",
                _data.ExchangeTradeId, tradeUpdate.ExchangeTradeId);
            return;
        }

        // Update trade data
        var updatedTrade = new OrderUpdate.Types.Trade
        {
            SequenceOrder = tradeUpdate.SequenceOrder,
            Volume = tradeUpdate.Volume,
            Price = tradeUpdate.Price,
            ResidualVolume = tradeUpdate.ResidualVolume,
            Time = tradeUpdate.Time,
            ExchangeTradeId = tradeUpdate.ExchangeTradeId,
            ExchangeTime = tradeUpdate.ExchangeTime,
            ContraTrader = tradeUpdate.ContraTrader,
            ContraBroker = tradeUpdate.ContraBroker,
            TradeDate = tradeUpdate.TradeDate
        };

        _data = updatedTrade;
        _logger.LogInformation("Trade updated: {ExchangeTradeId}", _data.ExchangeTradeId);
    }
}