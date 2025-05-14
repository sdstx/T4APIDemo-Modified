using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using T4Proto.V1.Account;
using T4Proto.V1.Auth;
using T4Proto.V1.Orderrouting;
using T4Proto.V1.Service;

namespace T4APIDemo.T4.AccountData;

public class Account
{
    private volatile AccountDetails _details;
    private volatile AccountUpdate _update;

    private readonly ConcurrentDictionary<string, Position> _positions = new();
    private readonly ConcurrentDictionary<string, Order> _orders = new();

    private readonly ILogger<Account> _logger;
    private readonly ILoggerFactory _loggerFactory;

    public Account(LoginResponse.Types.Account account, ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        _logger = _loggerFactory.CreateLogger<Account>();

        _details = new AccountDetails()
        {
            AccountId = account.AccountId,
            AccountName = account.AccountName,
            Account = account.AccountName,
            DisplayName = account.DisplayName,
            Mode = account.Mode
        };

        _update = new AccountUpdate()
        {
            AccountId = account.AccountId,
            Status = T4Proto.V1.Common.AccountStatus.Unknown
        };
    }

    public AccountDetails Details
    {
        get => _details;
        set => _details = value;
    }

    public AccountUpdate LastUpdate
    {
        get => _update;
        set => _update = value;
    }

    public IReadOnlyDictionary<string, Position> Positions => _positions;
    public IReadOnlyDictionary<string, Order> Orders => _orders;

    public record AccountUpdateResult(Account Account, List<Position> Positions, List<Order> Orders, List<Trade> Trades);

    public AccountUpdateResult UpdateWithMessage(ServerMessage serverMessage)
    {
        var updatedPositions = new List<Position>();
        var updatedOrders = new List<Order>();
        var updatedTrades = new List<Trade>();

        switch (serverMessage.PayloadCase)
        {
            case ServerMessage.PayloadOneofCase.AccountDetails:
                _details = serverMessage.AccountDetails;
                _logger.LogInformation("=> AccountDetails: {AccountId}", serverMessage.AccountDetails);
                break;

            case ServerMessage.PayloadOneofCase.AccountUpdate:
                _update = serverMessage.AccountUpdate;
                break;

            case ServerMessage.PayloadOneofCase.AccountPosition:
                {
                    var position = _positions.AddOrUpdate(
                        serverMessage.AccountPosition.MarketId,
                        // Add new Position if key doesn't exist
                        _ => new Position(this, serverMessage.AccountPosition, _loggerFactory.CreateLogger<Position>()),
                        // Update existing Position
                        (_, existingPosition) => existingPosition);

                    if (position != null)
                    {
                        position.UpdateWithMessage(serverMessage);
                        updatedPositions.Add(position);
                    }

                    _logger.LogInformation("=> AccountPosition: {AccountId} / {MarketID}", serverMessage.AccountPosition.AccountId, serverMessage.AccountPosition.MarketId);
                }
                break;

            case ServerMessage.PayloadOneofCase.AccountSnapshot:
                {
                    foreach (var snapshotMessage in serverMessage.AccountSnapshot.Messages)
                    {
                        switch (snapshotMessage.PayloadCase)
                        {
                            case AccountSnapshotMessage.PayloadOneofCase.AccountDetails:
                                _details = snapshotMessage.AccountDetails;
                                break;

                            case AccountSnapshotMessage.PayloadOneofCase.AccountUpdate:
                                _update = snapshotMessage.AccountUpdate;
                                break;

                            case AccountSnapshotMessage.PayloadOneofCase.AccountPosition:
                                {
                                    var position = _positions.AddOrUpdate(
                                        snapshotMessage.AccountPosition.MarketId,
                                        // Add new Position if key doesn't exist
                                        _ => new Position(this, snapshotMessage.AccountPosition, _loggerFactory.CreateLogger<Position>()),
                                        // Update existing Position
                                        (_, existingPosition) => existingPosition);

                                    if (position != null)
                                    {
                                        position.UpdateWithMessage(snapshotMessage);
                                        updatedPositions.Add(position);
                                    }

                                    _logger.LogInformation("=> SNAPSHOT => AccountPosition: {AccountId} / {MarketID}", snapshotMessage.AccountPosition.AccountId, snapshotMessage.AccountPosition.MarketId);
                                }
                                break;

                            case AccountSnapshotMessage.PayloadOneofCase.OrderUpdateMulti:
                                var orderMultiUpdates = ProcessOrderUpdateMulti(snapshotMessage.OrderUpdateMulti);
                                updatedOrders.AddRange(orderMultiUpdates.Select(res => res.Order));
                                updatedTrades.AddRange(orderMultiUpdates.SelectMany(res => res.Trades));
                                break;

                            default:
                                _logger.LogInformation($" > Unhandled Snapshot Message: {snapshotMessage.PayloadCase}");
                                break;
                        }
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateMulti:
                {
                    var orderMultiUpdates = ProcessOrderUpdateMulti(serverMessage.OrderUpdateMulti);
                    updatedOrders.AddRange(orderMultiUpdates.Select(res => res.Order));
                    updatedTrades.AddRange(orderMultiUpdates.SelectMany(res => res.Trades));
                }
                break;
        }

        // De-duplicate updated positions, orders and trades.
        updatedPositions = updatedPositions.GroupBy(p => p.Data.MarketId).Select(g => g.First()).ToList();
        updatedOrders = updatedOrders.GroupBy(o => o.Data.UniqueId).Select(g => g.First()).ToList();
        updatedTrades = updatedTrades.GroupBy(t => t.Data.ExchangeTradeId).Select(g => g.First()).ToList();

        return new AccountUpdateResult(this, updatedPositions, updatedOrders, updatedTrades);
    }

    private List<Order.OrderUpdateResult> ProcessOrderUpdateMulti(OrderUpdateMulti multiUpdate)
    {
        if (multiUpdate.AccountId != _details.AccountId)
        {
            _logger.LogWarning("Received order multi update for wrong account ID. Expected: {Expected}, Received: {Received}", _details.AccountId, multiUpdate.AccountId);
            return [];
        }

        var orderUpdates = new List<Order.OrderUpdateResult>();

        foreach (var update in multiUpdate.Updates)
        {
            string uniqueId = "";

            // Extract the unique ID based on message type
            switch (update.PayloadCase)
            {
                case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdate:
                    uniqueId = update.OrderUpdate.UniqueId;
                    break;
                case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateStatus:
                    uniqueId = update.OrderUpdateStatus.UniqueId;
                    break;
                case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateTrade:
                    uniqueId = update.OrderUpdateTrade.UniqueId;
                    break;
                case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateTradeLeg:
                    uniqueId = update.OrderUpdateTradeLeg.UniqueId;
                    break;
                case OrderUpdateMultiMessage.PayloadOneofCase.OrderUpdateFailed:
                    uniqueId = update.OrderUpdateFailed.UniqueId;
                    break;
                default:
                    _logger.LogWarning("Unknown order update message type: {MessageType}", update.PayloadCase);
                    continue;
            }

            if (string.IsNullOrEmpty(uniqueId))
            {
                _logger.LogWarning("Received order update with empty UniqueId");
                continue;
            }

            // Create or update order
            var order = _orders.GetOrAdd(
                uniqueId,
                // Add new Order if key doesn't exist with minimal OrderUpdate data
                _ =>
                {
                    // Create a basic OrderUpdate with minimal fields
                    var orderUpdate = new OrderUpdate
                    {
                        UniqueId = uniqueId,
                        AccountId = multiUpdate.AccountId,
                        MarketId = multiUpdate.MarketId
                    };

                    return new Order(this, orderUpdate, _loggerFactory);
                });

            // Update the order with the message
            var orderUpdate = order.UpdateWithMultiMessage(update);
            orderUpdates.Add(orderUpdate);

            _logger.LogInformation("=> Order Update Multi: {AccountId} / {UniqueId} / Type: {UpdateType}",
                multiUpdate.AccountId, uniqueId, update.PayloadCase);
        }

        return orderUpdates;
    }
}