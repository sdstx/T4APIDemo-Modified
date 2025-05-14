using Microsoft.Extensions.Logging;
using T4Proto.V1.Account;
using T4Proto.V1.Service;

namespace T4APIDemo.T4.AccountData;

public class Position
{
    private volatile AccountPosition _data;
    private readonly Account _account;
    private readonly ILogger<Position> _logger;

    public Position(Account account, AccountPosition initialData, ILogger<Position> logger)
    {
        _account = account;
        _data = initialData;
        _logger = logger;
    }

    public AccountPosition Data
    {
        get => _data;
    }

    public Account Account => _account;

    public void UpdateWithMessage(ServerMessage serverMessage)
    {
        switch (serverMessage.PayloadCase)
        {
            case ServerMessage.PayloadOneofCase.AccountPosition:
                if (serverMessage.AccountPosition.MarketId == _data.MarketId &&
                    serverMessage.AccountPosition.AccountId == _account.Details.AccountId)
                {
                    _data = serverMessage.AccountPosition;
                }
                break;
        }
    }

    public void UpdateWithMessage(AccountSnapshotMessage snapshotMessage)
    {
        switch (snapshotMessage.PayloadCase)
        {
            case AccountSnapshotMessage.PayloadOneofCase.AccountPosition:
                if (snapshotMessage.AccountPosition.MarketId == _data.MarketId && snapshotMessage.AccountPosition.AccountId == _account.Details.AccountId)
                {
                    _data = snapshotMessage.AccountPosition;
                }
                break;
        }
    }
}