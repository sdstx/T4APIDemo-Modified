namespace T4APIDemo.T4.AccountData;

public class AccountUpdateEventArgs : EventArgs
{
    private Account.AccountUpdateResult _updateResult;

    public Account Account => _updateResult.Account;
    public List<Position> UpdatedPositions => _updateResult.Positions;
    public List<Order> UpdatedOrders => _updateResult.Orders;
    public List<Trade> UpdatedTrades => _updateResult.Trades;

    public AccountUpdateEventArgs(Account.AccountUpdateResult udpateResult)
    {
        _updateResult = udpateResult;
    }
}
