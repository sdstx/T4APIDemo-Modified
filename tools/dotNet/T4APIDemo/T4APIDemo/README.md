T4APIDemo-Modified
A modified version of the T4 API Demo project designed to capture all raw trade events in a SQLite database without pre-filtering, aligning with industry standards for trade execution reporting (e.g., FIX Protocol, CME’s iLink). The project processes snapshot trades from order.Data.Trades and post-login trades from OrderUpdateTrade messages, storing them in trades.db for auditing and reconciliation.
Features

Captures all trade events. 
Stores trade data in trades.db with columns: OrderUniqueId, Symbol, Price, TradeVolume, BuySell, Status, Timestamp, SequenceOrder.
Ensures uniqueness using OrderUniqueId and SequenceOrder, preventing duplicate entries.
Logs trade insertions and errors for traceability, using Microsoft.Extensions.Logging.
Supports XCME_Eq ES (M25) and other markets in the T4 Simulator.

Prerequisites

.NET 8.0 SDK
VS Code or another C# IDE
T4 Simulator credentials (e.g., Firm: CTS, Username: schwingdorf)
DB Browser for SQLite (optional, for viewing trades.db)

Setup

Clone the Repository:
git clone https://github.com/sdstx/T4APIDemo-Modified.git
cd T4APIDemo-Modified/t4-api-tools/tools/dotNet/T4APIDemo/T4APIDemo


Configure Credentials:

Edit appsettings.json with your T4 Simulator credentials:{
  "T4API": {
    "Firm": "",
    "Username": "your_username",
    "Password": "your_password"
  }
}




Build the Project:
dotnet build


Expect three warnings in T4APIClient.cs (CS8625, CS8602), which are benign.


Run the Application:
dotnet run


Outputs trade insertions to the console and stores them in trades.db.
Stop with Ctrl+C after capturing trades.



Usage

View Trades:

Open trades.db in DB Browser for SQLite to inspect trade records.
Example records:
OrderUniqueId=43164496-..., Price=592075, TradeVolume=1, Sell, Finished, 2025-05-28T11:21:24.2438731, SequenceOrder=0.
OrderUniqueId=113B3E52-..., Price=592275, TradeVolume=8, Buy, Finished, 2025-05-28T11:30:11.5541487, SequenceOrder=0.




Run SQL Queries:

Aggregate trades by order:SELECT 
    OrderUniqueId,
    BuySell,
    COUNT(*) AS TradeCount,
    SUM(TradeVolume) AS TotalTradeVolume,
    AVG(Price) AS AvgPrice,
    MIN(Timestamp) AS FirstTrade,
    MAX(Timestamp) AS LastTrade
FROM Trades
WHERE Status = 'Finished' AND TradeVolume > 0
GROUP BY OrderUniqueId, BuySell
ORDER BY FirstTrade;




Test New Trades:

Place trades in the T4 Simulator (e.g., buy/sell XCME_Eq ES (M25)).
Rerun dotnet run to capture new trades.
Verify in trades.db and simulator trade history.



Notes

Build Warnings: Three warnings in T4APIClient.cs (CS8625, CS8602) are harmless and stem from the T4 API library. Contact CTS Futures for updates if needed.
Uniqueness: Enforced by UNIQUE(OrderUniqueId, SequenceOrder) in DatabaseHelper.cs.
Dependencies: Relies on T4Proto.V1.Common, T4Proto.V1.Orderrouting, and Google.Protobuf.
Last Modified: May 28, 2025, capturing 76 trades (73 snapshot, 3 post-login).
License: Based on the original T4 API Demo; check CTS Futures’ terms for usage.

Contributing
Feel free to fork, submit issues, or create pull requests for enhancements (e.g., new queries, performance optimizations).
Contact
For support, contact the repository owner (sdstx) via GitHub issues or reach out to CTS Futures for T4 API assistance.
