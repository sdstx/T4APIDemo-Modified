using Microsoft.Extensions.Logging;
using T4Proto.V1.Orderrouting;

namespace T4APIDemo.T4.AccountData;

public class TradeLeg
{
    private volatile OrderUpdate.Types.TradeLeg _data;
    private readonly Order _order;
    private readonly ILogger<TradeLeg> _logger;

    public TradeLeg(Order order, OrderUpdate.Types.TradeLeg legData, ILogger<TradeLeg> logger)
    {
        _order = order;
        _data = legData;
        _logger = logger;
    }

    public TradeLeg(Order order, OrderUpdateTradeLeg legUpdate, ILogger<TradeLeg> logger)
    {
        _order = order;
        _logger = logger;

        // Convert OrderUpdateTradeLeg to OrderUpdate.Types.TradeLeg
        _data = new OrderUpdate.Types.TradeLeg
        {
            SequenceOrder = legUpdate.SequenceOrder,
            LegIndex = legUpdate.LegIndex,
            Volume = legUpdate.Volume,
            Price = legUpdate.Price,
            Time = legUpdate.Time,
            ExchangeTradeId = legUpdate.ExchangeTradeId,
            ExchangeTime = legUpdate.ExchangeTime,
            ContraTrader = legUpdate.ContraTrader,
            ContraBroker = legUpdate.ContraBroker,
            ResidualVolume = legUpdate.ResidualVolume,
            TradeDate = legUpdate.TradeDate
        };
    }

    public OrderUpdate.Types.TradeLeg Data => _data;
    public Order Order => _order;
    public int LegIndex => _data.LegIndex;

    public void UpdateWithMessage(OrderUpdateTradeLeg legUpdate)
    {
        if (legUpdate.LegIndex != _data.LegIndex)
        {
            _logger.LogWarning("Received trade leg update with mismatched LegIndex. Expected: {Expected}, Received: {Received}",
                _data.LegIndex, legUpdate.LegIndex);
            return;
        }

        // Update trade leg data
        var updatedLeg = new OrderUpdate.Types.TradeLeg
        {
            SequenceOrder = legUpdate.SequenceOrder,
            LegIndex = legUpdate.LegIndex,
            Volume = legUpdate.Volume,
            Price = legUpdate.Price,
            Time = legUpdate.Time,
            ExchangeTradeId = legUpdate.ExchangeTradeId,
            ExchangeTime = legUpdate.ExchangeTime,
            ContraTrader = legUpdate.ContraTrader,
            ContraBroker = legUpdate.ContraBroker,
            ResidualVolume = legUpdate.ResidualVolume,
            TradeDate = legUpdate.TradeDate
        };

        _data = updatedLeg;
        _logger.LogInformation("Trade leg updated: Index {LegIndex}", _data.LegIndex);
    }
}