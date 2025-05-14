using T4Proto.V1.Market;

namespace T4APIDemo.T4.MarketData;

public record MarketDataSnapshot
{
    public MarketDetails? MarketDetails { get; init; } = null;
    public MarketDepth? MarketDepth { get; init; } = null;
    public MarketSnapshot? MarketSnapshot { get; init; } = null;
    public MarketSettlement? MarketSettlement { get; init; } = null;
    public MarketHighLow? MarketHighLow { get; init; } = null;
    public MarketByOrder? MarketByOrder { get; init; } = null;

    public string? MarketID
    {
        get
        {
            return MarketDetails?.MarketId ?? MarketDepth?.MarketId ?? MarketSnapshot?.MarketId ?? MarketByOrder?.MarketId;
        }
    }

}
