using Google.Protobuf.WellKnownTypes;
using System.Collections;
using T4APIDemo.T4.Util;
using T4Proto.V1.Common;
using T4Proto.V1.Market;

namespace T4APIDemo.T4.MarketData;

public class MarketByOrder
{
    public class Order
    {
        private readonly MarketByOrderSnapshot.Types.Order _order;

        public Order(MarketByOrderSnapshot.Types.Order order)
        {
            _order = order;
        }

        public Order(MarketByOrderUpdate.Types.Update update)
        {
            _order = new MarketByOrderSnapshot.Types.Order
            {
                OrderId = update.OrderId,
                BidOffer = update.BidOffer,
                Price = update.Price,
                Volume = update.Volume,
                Priority = update.Priority
            };
        }

        public ulong OrderId => _order.OrderId;
        public BidOffer BidOffer => _order.BidOffer;
        public Price Price => _order.Price;
        public int Volume => _order.Volume;
        public ulong Priority => _order.Priority;
    }

    public class PriceLevel : IEnumerable<Order>
    {
        private readonly Price _price;
        private readonly List<Order> _orders;
        private readonly int _volume;

        public PriceLevel(Price price)
        {
            _price = price;
            _orders = new List<Order>();
            _volume = 0;
        }

        private PriceLevel(Price price, List<Order> orders)
        {
            _price = price;
            _orders = orders;
            _volume = orders.Sum(order => order.Volume);
        }

        public IEnumerator<Order> GetEnumerator() => _orders.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public Price Price => _price;
        public int Volume => _volume;
        public int OrderCount => _orders.Count;

        public PriceLevel AddOrder(Order order)
        {
            var orders = new List<Order>(_orders) { order };
            return new PriceLevel(_price, orders);
        }

        public PriceLevel RemoveOrder(Order order)
        {
            var orders = new List<Order>(_orders);
            orders.RemoveAll(o => o.OrderId == order.OrderId);
            return new PriceLevel(_price, orders);
        }
    }

    private readonly Timestamp _lastUpdateTime;
    private readonly MarketMode _mode;
    private readonly Dictionary<ulong, Order> _orders;
    private readonly SortedDictionary<decimal, PriceLevel> _bids;
    private readonly SortedDictionary<decimal, PriceLevel> _offers;
    private readonly string _marketId;

    public MarketByOrder()
    {
        _lastUpdateTime = new Timestamp();
        _mode = MarketMode.Undefined;
        _orders = new Dictionary<ulong, Order>();
        _bids = new SortedDictionary<decimal, PriceLevel>(Comparer<decimal>.Create((a, b) => b.CompareTo(a))); // Descending for bids
        _offers = new SortedDictionary<decimal, PriceLevel>(); // Ascending for offers
        _marketId = string.Empty;
    }

    private MarketByOrder(string marketId, Timestamp lastUpdateTime, MarketMode mode, Dictionary<ulong, Order> orders,
        SortedDictionary<decimal, PriceLevel> bids, SortedDictionary<decimal, PriceLevel> offers)
    {
        _marketId = marketId;
        _lastUpdateTime = lastUpdateTime;
        _mode = mode;
        _orders = orders;
        _bids = bids;
        _offers = offers;
    }

    public MarketByOrder ProcessSnapshot(MarketByOrderSnapshot msg)
    {
        var orders = new Dictionary<ulong, Order>();
        var bids = new SortedDictionary<decimal, PriceLevel>(Comparer<decimal>.Create((a, b) => b.CompareTo(a))); // Descending for bids
        var offers = new SortedDictionary<decimal, PriceLevel>(); // Ascending for offers

        foreach (var snapOrder in msg.Orders)
        {
            var order = new Order(snapOrder);
            orders[order.OrderId] = order;

            var priceValue = order.Price.ToDecimal()!.Value;

            if (order.BidOffer == BidOffer.Bid)
            {
                if (!bids.TryGetValue(priceValue, out var level))
                {
                    level = new PriceLevel(order.Price);
                }
                bids[priceValue] = level.AddOrder(order);
            }
            else if (order.BidOffer == BidOffer.Offer)
            {
                if (!offers.TryGetValue(priceValue, out var level))
                {
                    level = new PriceLevel(order.Price);
                }
                offers[priceValue] = level.AddOrder(order);
            }
        }

        return new MarketByOrder(msg.MarketId, msg.Time, msg.Mode, orders, bids, offers);
    }

    public MarketByOrder ProcessUpdate(MarketByOrderUpdate msg)
    {
        var orders = new Dictionary<ulong, Order>(_orders);
        var bids = new SortedDictionary<decimal, PriceLevel>(_bids, _bids.Comparer);
        var offers = new SortedDictionary<decimal, PriceLevel>(_offers, _offers.Comparer);

        foreach (var msgUpdate in msg.Updates)
        {
            switch (msgUpdate.UpdateType)
            {
                case MarketByOrderUpdate.Types.UpdateType.AddOrUpdate:
                    if (orders.TryGetValue(msgUpdate.OrderId, out var existingOrder))
                    {
                        // Price change, remove the existing order from the associated price level
                        decimal existingPriceValue = existingOrder.Price.ToDecimal()!.Value;

                        if (existingOrder.BidOffer == BidOffer.Bid)
                        {
                            if (bids.TryGetValue(existingPriceValue, out var existingLevel))
                            {
                                existingLevel = existingLevel.RemoveOrder(existingOrder);
                                if (existingLevel.OrderCount == 0)
                                {
                                    bids.Remove(existingPriceValue);
                                }
                                else
                                {
                                    bids[existingPriceValue] = existingLevel;
                                }
                            }
                        }
                        else if (existingOrder.BidOffer == BidOffer.Offer)
                        {
                            if (offers.TryGetValue(existingPriceValue, out var existingLevel))
                            {
                                existingLevel = existingLevel.RemoveOrder(existingOrder);
                                if (existingLevel.OrderCount == 0)
                                {
                                    offers.Remove(existingPriceValue);
                                }
                                else
                                {
                                    offers[existingPriceValue] = existingLevel;
                                }
                            }
                        }
                    }

                    // Add/update the order
                    var order = new Order(msgUpdate);
                    orders[order.OrderId] = order;

                    decimal priceValue = order.Price.ToDecimal()!.Value;

                    if (order.BidOffer == BidOffer.Bid)
                    {
                        if (!bids.TryGetValue(priceValue, out var level))
                        {
                            level = new PriceLevel(order.Price);
                        }
                        bids[priceValue] = level.AddOrder(order);
                    }
                    else if (order.BidOffer == BidOffer.Offer)
                    {
                        if (!offers.TryGetValue(priceValue, out var level))
                        {
                            level = new PriceLevel(order.Price);
                        }
                        offers[priceValue] = level.AddOrder(order);
                    }
                    break;

                case MarketByOrderUpdate.Types.UpdateType.Delete:
                    if (orders.TryGetValue(msgUpdate.OrderId, out var orderToDelete))
                    {
                        orders.Remove(msgUpdate.OrderId);

                        decimal deletePrice = orderToDelete.Price.ToDecimal()!.Value;

                        if (orderToDelete.BidOffer == BidOffer.Bid)
                        {
                            if (bids.TryGetValue(deletePrice, out var existingLevel))
                            {
                                existingLevel = existingLevel.RemoveOrder(orderToDelete);
                                if (existingLevel.OrderCount == 0)
                                {
                                    bids.Remove(deletePrice);
                                }
                                else
                                {
                                    bids[deletePrice] = existingLevel;
                                }
                            }
                        }
                        else if (orderToDelete.BidOffer == BidOffer.Offer)
                        {
                            if (offers.TryGetValue(deletePrice, out var existingLevel))
                            {
                                existingLevel = existingLevel.RemoveOrder(orderToDelete);
                                if (existingLevel.OrderCount == 0)
                                {
                                    offers.Remove(deletePrice);
                                }
                                else
                                {
                                    offers[deletePrice] = existingLevel;
                                }
                            }
                        }
                    }
                    break;

                case MarketByOrderUpdate.Types.UpdateType.Clear:
                    orders.Clear();
                    bids.Clear();
                    offers.Clear();
                    break;
            }
        }

        return new MarketByOrder(msg.MarketId, msg.Time, msg.Mode, orders, bids, offers);
    }

    public string MarketId => _marketId;
    public Timestamp LastUpdateTime => _lastUpdateTime;
    public MarketMode MarketMode => _mode;
    public int OrderCount => _orders.Count;
    public IReadOnlyDictionary<decimal, PriceLevel> Bids => _bids;
    public IReadOnlyDictionary<decimal, PriceLevel> Offers => _offers;
    public IReadOnlyDictionary<ulong, Order> Orders => _orders;
}