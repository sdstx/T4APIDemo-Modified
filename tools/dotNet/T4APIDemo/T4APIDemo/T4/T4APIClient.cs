using Google.Protobuf;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Authentication;
using T4APIDemo.T4.AccountData;
using T4APIDemo.T4.CredentialProviders;
using T4APIDemo.T4.MarketData;
using T4APIDemo.T4.Util;
using T4Proto.V1.Account;
using T4Proto.V1.Auth;
using T4Proto.V1.Common;
using T4Proto.V1.Market;
using T4Proto.V1.Service;

namespace T4APIDemo.T4;

public class T4APIClient : IDisposable
{
    #region Events

    /// <summary>
    /// Event raised when an account is loaded and ready.
    /// </summary>
    public event EventHandler<AccountUpdateEventArgs>? OnAccountUpdate;

    /// <summary>
    /// Event raised on new real-time market data.
    /// </summary>
    public event Action<MarketDataSnapshot>? OnMarketUpdate;

    #endregion

    /// <summary>
    /// Event raised when the connection status changes.
    /// </summary>
    public event EventHandler<ConnectionStatusEventArgs>? OnConnectionStatusChanged;

    private const int HeartbeatIntervalMs = 20_000;
    private const int MessageTimeoutSeconds = HeartbeatIntervalMs * 3;

    private readonly PeriodicTimer _heartbeatTimer;
    private readonly ILogger<T4APIClient> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ICredentialProvider _credentialProvider;
    private readonly IHttpClientFactory _httpClientFactory;

    private ClientWebSocket _client;
    private LoginResponse? _loginResponse;

    // Connection tracking
    private readonly AsyncRetryPolicy _connectionPolicy;
    private readonly SemaphoreSlim _reconnectionLock = new(1, 1);
    private DateTime? _connectedSinceUTC;
    private int _disconnectionCount;

    private readonly Uri _webSocketUri;
    private readonly Uri _restUri;

    private bool _isDisposed;

    private readonly ConcurrentDictionary<string, MarketDataSnapshot> _marketSnapshots = new();
    private readonly List<(string ExchangeId, string ContractId, string MarketId)> _marketSubscriptions = new();
    private readonly List<(string ExchangeId, string ContractId, string MarketId)> _mboSubscriptions = new();

    /// <summary>
    /// Collection of user Accounts (AccountID -> Account).
    /// </summary>
    private readonly Dictionary<string, Account> _accounts = new();


    private DateTime _lastMessageReceived = DateTime.MinValue;


    // To manage the JWT needed for any REST calls.
    private AuthenticationToken? _authToken = null;
    private TaskCompletionSource<AuthenticationToken> _pendingTokenRequest = null;

    public T4APIClient(
        ICredentialProvider credentialProvider,
        ILogger<T4APIClient> logger,
        ILoggerFactory loggerFactory,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration)
    {
        _webSocketUri = new Uri(configuration["T4API:WebSocketUri"] ?? "");
        _restUri = new Uri(configuration["T4API:RESTUri"] ?? "");

        _credentialProvider = credentialProvider;
        _client = new ClientWebSocket();
        _logger = logger;
        _loggerFactory = loggerFactory;
        _httpClientFactory = httpClientFactory;

        _heartbeatTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(HeartbeatIntervalMs));

        var random = new Random();

        // Connection retry policy. Random jitter and backs off, but tries forever with a 5 second re-try interval.
        _connectionPolicy = Policy
            .Handle<Exception>(IsRecoverableException) // Only retry on recoverable exceptions
            .WaitAndRetryForeverAsync(
                retryAttempt => TimeSpan.FromMilliseconds(
                    Math.Min(5000, Math.Pow(2, retryAttempt) * 100) + random.Next(-500, 500)
                ),
                async (exception, retryCount, timeSpan) =>
                {
                    _logger.LogWarning(exception, "Connection attempt {Count} failed. Retrying in {Delay}ms", retryCount, timeSpan);
                    await Task.Yield();
                }
            );
    }

    public void Dispose()
    {
        _logger.LogInformation("Disposing T4APIClient");
        _isDisposed = true;
        _reconnectionLock.Dispose();
        _heartbeatTimer.Dispose();

        if (_client.State == WebSocketState.Open)
        {
            _client.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing", CancellationToken.None)
                  .GetAwaiter()
                  .GetResult();
        }

        _client.Dispose();
        _logger.LogInformation("T4APIClient disposed");
    }

    public async Task StartAsync()
    {
        _logger.LogInformation("Starting T4APIClient.");

        await ConnectAsync();

        _ = RunReceiveLoopAsync();
        _ = HeartbeatLoopAsync();

        _logger.LogInformation("T4APIClient startup complete.");
    }

    public async Task<HttpClient> GetHttpClientAsync()
    {
        // Get a new client from the factory
        var client = _httpClientFactory.CreateClient("T4API");

        if (_loginResponse != null)
        {
            // Use auth token if available, refreshing if needed
            var token = await GetAuthToken();

            if (token != null && token.HasToken)
            {
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
            }
            else
            {
                _logger.LogError("Failed to get auth token for HttpClient");
            }
        }

        return client;
    }

    public async Task<AuthenticationToken?> GetAuthToken()
    {
        // Check if current token is valid
        var tokenExpireTimeUTC = _authToken?.ExpireTime.ToDateTime() ?? DateTime.MinValue;
        var utcNow = DateTime.UtcNow;
        var remainingSeconds = tokenExpireTimeUTC.Subtract(utcNow).TotalSeconds;

        if (_authToken != null && remainingSeconds > 30)
        {
            return _authToken;
        }

        // Refresh the token

        // If we already have a pending request, wait for it
        if (_pendingTokenRequest != null)
        {
            return await _pendingTokenRequest.Task;
        }

        try
        {
            _pendingTokenRequest = new TaskCompletionSource<AuthenticationToken>();

            var tokenRequest = new AuthenticationTokenRequest
            {
                RequestId = Guid.NewGuid().ToString()
            };

            // Send the request
            await SendMessageAsync(tokenRequest);
            _logger.LogInformation($"Requested Authentication Token from API...");

            // Wait for the response (the response will be handled in HandleMessage and will complete the task)
            var token = await _pendingTokenRequest.Task;

            if (string.IsNullOrEmpty(token.Token) || token.FailMessage?.Length > 0)
            {
                _logger.LogError($"Authentication token request failed. Reason: {token.FailMessage}");
            }
            else
            {
                _logger.LogInformation($"Authentication token response received. Success");
            }

            // Cache the token.
            _authToken = token;

            return token;
        }
        finally
        {
            _pendingTokenRequest = null;
        }
    }

    #region Markets and Depth Updates

    public string ConnectedUserID => _loginResponse.UserId;

    public MarketDataSnapshot? GetLastSnapshot(string marketID)
    {
        _marketSnapshots.TryGetValue(marketID, out var snapshot);
        return snapshot;
    }

    #endregion

    #region Publishing

    private void PublishConnectionStatus(bool isConnected)
    {
        if (!isConnected)
        {
            _connectedSinceUTC = null;
        }

        var uptime = TimeSpan.Zero;
        if (_connectedSinceUTC.HasValue)
        {
            uptime = DateTime.UtcNow - _connectedSinceUTC.Value;
        }

        var args = new ConnectionStatusEventArgs(
            isConnected,
            uptime,
            _disconnectionCount
        );

        OnConnectionStatusChanged?.Invoke(this, args);
    }

    public void Republish()
    {
        // Republish connection status.
        PublishConnectionStatus(_client.State == WebSocketState.Open);

        // Republish accounts.
        foreach (var acct in _accounts.Values)
        {
            var accountUpdateResult = new Account.AccountUpdateResult(acct, acct.Positions.Values.ToList(), acct.Orders.Values.ToList(), acct.Orders.Values.SelectMany(o => o.Trades.Values).ToList());
            OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));
        }

        // Republish market depths.
        foreach (var depth in _marketSnapshots.Values)
        {
            OnMarketUpdate?.Invoke(depth);
        }
    }

    #endregion

    #region Connection Handling

    private async Task ConnectAsync()
    {
        await _connectionPolicy.ExecuteAsync(async () =>
        {
            if (_client.State != WebSocketState.None)
            {
                _client.Dispose();
                _client = new ClientWebSocket();

                // Clear existing state.
                _marketSnapshots.Clear();
                _accounts.Clear();
            }

            _logger.LogInformation("Connecting to {Uri}", _webSocketUri);
            await _client.ConnectAsync(_webSocketUri, CancellationToken.None);
            await AuthenticateAsync();
        });
    }

    private async Task AuthenticateAsync()
    {
        var loginRequest = await _credentialProvider.GetLoginRequestAsync();
        await SendMessageAsync(loginRequest);

        var serverMessage = await ReceiveMessageAsync();

        if (serverMessage.PayloadCase != T4Proto.V1.Service.ServerMessage.PayloadOneofCase.LoginResponse)
        {
            throw new AuthenticationException($"Expected auth response, got {serverMessage.PayloadCase}");
        }

        var loginResponse = serverMessage.LoginResponse;
        if (loginResponse.Result != T4Proto.V1.Common.LoginResult.Success)
        {
            throw new AuthenticationException($"Authentication failed: {loginResponse.ErrorMessage}");
        }

        // List the exchanges we have access to
        _logger.LogDebug($"User has access to {loginResponse.Exchanges.Count} exchanges:");
        foreach (var exchg in loginResponse.Exchanges)
        {
            _logger.LogDebug($"   Exchange: {exchg.ExchangeId}, Access: {exchg.MarketDataType}");
        }

        // Create accounts for the user.
        _logger.LogInformation($"User has access to {loginResponse.Accounts.Count} accounts:");
        foreach (var acct in loginResponse.Accounts)
        {
            _logger.LogInformation($"   Account: {acct.AccountName}, Mode: {acct.Mode}");
            _accounts.Add(acct.AccountId, new Account(acct, _loggerFactory));
        }

        _connectedSinceUTC = DateTime.UtcNow;
        _lastMessageReceived = DateTime.UtcNow;
        _loginResponse = loginResponse;

        if (_loginResponse.AuthenticationToken != null)
        {
            _authToken = _loginResponse.AuthenticationToken;
        }
    }

    private async Task RunReceiveLoopAsync()
    {
        bool wasConnected = false;

        while (!_isDisposed)
        {
            try
            {
                var isConnected = IsConnectionHealthy();

                // Handle connection state changes
                if (wasConnected && !isConnected)
                {
                    _disconnectionCount++;
                    _logger.LogWarning("Connection lost. Total disconnections: {Count}", _disconnectionCount);
                    PublishConnectionStatus(isConnected);
                }
                else if (!wasConnected && isConnected)
                {
                    _logger.LogInformation("Connection restored");
                    PublishConnectionStatus(isConnected);
                }

                wasConnected = isConnected;

                if (!isConnected)
                {
                    await ConnectAsync();
                    await ResubscribeMarketsAsync();
                    await Task.Delay(1000); // Prevent tight loop on connection attempts
                    continue;
                }

                // Process messages when connected
                var serverMessage = await ReceiveMessageAsync();
                ProcessServerMessage(serverMessage);
            }
            catch (Exception ex) when (!_isDisposed)
            {
                _logger.LogError(ex, "Error in receive/monitor loop");
                await Task.Delay(1000); // Prevent tight loop on errors
            }
        }
    }

    #endregion

    #region Market Subscription

    public async Task SubscribeMarket(string exchangeId, string contractId, string marketId)
    {
        // Add to subscribed markets list if not already present
        var marketTuple = (exchangeId, contractId, marketId);
        if (!_marketSubscriptions.Contains(marketTuple))
        {
            _marketSubscriptions.Add(marketTuple);
        }

        var message = new MarketDepthSubscribe
        {
            ExchangeId = exchangeId,
            ContractId = contractId,
            MarketId = marketId,
            Buffer = T4Proto.V1.Common.DepthBuffer.Smart,
            DepthLevels = T4Proto.V1.Common.DepthLevels.Normal
        };

        await SendMessageAsync(message);

        try
        {
            // Publish empty depth for immediate UI update
            var emptyDepth = new MarketDepth
            {
                MarketId = marketId,
                Mode = T4Proto.V1.Common.MarketMode.Undefined,
                Time = TimeUtil.CSTToProtobufTimestamp(DateTime.Now)
            };

            var snapshot = new MarketDataSnapshot { MarketDepth = emptyDepth };
            _marketSnapshots[marketId] = snapshot;
            OnMarketUpdate?.Invoke(snapshot);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while subscribing market: {MarketId}, Error: {Error}", marketId, ex);
        }
    }

    public async Task SubscribeMarketByOrder(string exchangeId, string contractId, string marketId)
    {
        // Add to subscribed markets list if not already present
        var marketTuple = (exchangeId, contractId, marketId);
        if (!_mboSubscriptions.Contains(marketTuple))
        {
            _mboSubscriptions.Add(marketTuple);
        }

        var message = new MarketByOrderSubscribe
        {
            ExchangeId = exchangeId,
            ContractId = contractId,
            MarketId = marketId,
            Subscribe = true
        };

        await SendMessageAsync(message);
        _logger.LogInformation($"Subscribed to MBO for: {marketId}");
    }

    public async Task UnsubscribeMarketByOrder(string exchangeId, string contractId, string marketId)
    {
        // Add to subscribed markets list if not already present
        var marketTuple = (exchangeId, contractId, marketId);
        if (_mboSubscriptions.Contains(marketTuple))
        {
            _mboSubscriptions.Remove(marketTuple);
        }

        var message = new MarketByOrderSubscribe
        {
            ExchangeId = exchangeId,
            ContractId = contractId,
            MarketId = marketId,
            Subscribe = false
        };

        await SendMessageAsync(message);
        _logger.LogInformation($"Unsubscribed to MBO for: {marketId}");
    }

    private async Task ResubscribeMarketsAsync()
    {
        foreach (var market in _marketSubscriptions)
        {
            await SubscribeMarket(market.ExchangeId, market.ContractId, market.MarketId);
        }
    }

    #endregion

    #region Account Management

    private List<LoginResponse.Types.Account> GetUserTradingAccounts()
    {
        if (_loginResponse?.Result != T4Proto.V1.Common.LoginResult.Success)
        {
            return [];
        }

        return _loginResponse.Accounts.ToList();
    }

    public async Task SubscribeAccounts(List<LoginResponse.Types.Account> accounts, AccountSubscribeType subscribe)
    {
        var message = new AccountSubscribe
        {
            Subscribe = subscribe,
            SubscribeAllAccounts = accounts.Count == 0,
            AccountId = { accounts.Select(a => a.AccountId) }
        };

        _logger.LogInformation($"Subscribing accounts: {string.Join(", ", accounts.Select(a => a.AccountId))}");
        await SendMessageAsync(message);
    }

    public async Task SubscribeAllAccounts()
    {
        var accounts = GetUserTradingAccounts();
        await SubscribeAccounts(accounts, AccountSubscribeType.AllUpdates);
    }

    #endregion

    #region Message Handling

    private async Task<T4Proto.V1.Service.ServerMessage> ReceiveMessageAsync()
    {
        using var messageStream = new MemoryStream();
        var buffer = new byte[4096];

        while (true)
        {
            var result = await _client.ReceiveAsync(buffer, CancellationToken.None);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                throw new Exception("Server closed connection");
            }

            // Write this chunk to our accumulating stream
            await messageStream.WriteAsync(buffer.AsMemory(0, result.Count));

            // If this is the end of the message, we can process it
            if (result.EndOfMessage)
            {
                var completeMessage = messageStream.ToArray();
                return T4Proto.V1.Service.ServerMessage.Parser.ParseFrom(completeMessage);
            }
        }
    }

    public async Task SendMessageAsync(IMessage message, ClientWebSocket? client = null)
    {
        client ??= _client;
        if (client.State != WebSocketState.Open)
        {
            throw new InvalidOperationException("WebSocket is not connected");
        }

        var outgoingMessage = ClientMessageHelper.CreateClientMessage(message);

        if (outgoingMessage == null)
        {
            throw new ArgumentException($"Unsupported message type: {message.GetType()}");
        }

        await client.SendAsync(outgoingMessage.ToByteArray(), WebSocketMessageType.Binary, true, CancellationToken.None);
    }

    private void ProcessServerMessage(T4Proto.V1.Service.ServerMessage serverMessage)
    {
        _lastMessageReceived = DateTime.UtcNow;  // Any message from server resets our timeout

        switch (serverMessage.PayloadCase)
        {
            case T4Proto.V1.Service.ServerMessage.PayloadOneofCase.Heartbeat:
                // _lastMessageReceived was updated above, nothing left to process.
                _logger.LogDebug("Received heartbeat with timestamp: {Timestamp}", serverMessage.Heartbeat.Timestamp);
                break;

            case ServerMessage.PayloadOneofCase.MarketDepthSubscribeReject:
                {
                    _logger.LogInformation($"Market depth subscription rejected: {serverMessage.MarketDepthSubscribeReject.MarketId} ({serverMessage.MarketDepthSubscribeReject.Mode})");
                    break;
                }

            case ServerMessage.PayloadOneofCase.MarketDepth:
                ProcessMarketDepth(serverMessage.MarketDepth);
                break;

            case ServerMessage.PayloadOneofCase.MarketByOrderSubscribeReject:
                ProcessMarketByOrderSubscribeReject(serverMessage.MarketByOrderSubscribeReject);
                break;

            case ServerMessage.PayloadOneofCase.MarketByOrderSnapshot:
                ProcessMarketByOrderSnapshot(serverMessage.MarketByOrderSnapshot);
                break;

            case ServerMessage.PayloadOneofCase.MarketByOrderUpdate:
                ProcessMarketByOrderUpdate(serverMessage.MarketByOrderUpdate);
                break;

            case ServerMessage.PayloadOneofCase.MarketSnapshot:
                {
                    _logger.LogInformation($"Received market snapshot: {serverMessage.MarketSnapshot.MarketId}");
                    foreach (var snapshotMessage in serverMessage.MarketSnapshot.Messages)
                    {
                        switch (snapshotMessage.PayloadCase)
                        {
                            case MarketSnapshotMessage.PayloadOneofCase.MarketDepth:
                                ProcessMarketDepth(snapshotMessage.MarketDepth);
                                break;
                        }
                    }
                    break;
                }

            case ServerMessage.PayloadOneofCase.MarketDetails:
                ProcessMarketDetails(serverMessage.MarketDetails);
                break;

            case ServerMessage.PayloadOneofCase.AccountSubscribeResponse:
                {
                    _logger.LogInformation($"Received account subscribe response. Success: {serverMessage.AccountSubscribeResponse.Success}");
                    serverMessage.AccountSubscribeResponse.Errors.ToList().ForEach(e => _logger.LogWarning(" * " + e));
                    break;
                }

            case ServerMessage.PayloadOneofCase.AccountDetails:
                {
                    if (_accounts.TryGetValue(serverMessage.AccountDetails.AccountId, out var account))
                    {
                        var accountUpdateResult = account.UpdateWithMessage(serverMessage);
                        OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));
                        _logger.LogInformation("Received account details: {AccountId}", serverMessage.AccountDetails.AccountId);
                    }
                    else
                    {
                        _logger.LogWarning("Received account details for unknown account: {AccountId}", serverMessage.AccountDetails.AccountId);
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.AccountUpdate:
                {
                    if (_accounts.TryGetValue(serverMessage.AccountUpdate.AccountId, out var account))
                    {
                        var accountUpdateResult = account.UpdateWithMessage(serverMessage);
                        OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));
                        _logger.LogInformation("Received account update: {AccountId}", serverMessage.AccountUpdate.AccountId);
                    }
                    else
                    {
                        _logger.LogWarning("Received account update for unknown account: {AccountId}", serverMessage.AccountUpdate.AccountId);
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.AccountPosition:
                {
                    if (_accounts.TryGetValue(serverMessage.AccountPosition.AccountId, out var account))
                    {
                        var accountUpdateResult = account.UpdateWithMessage(serverMessage);
                        OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));

                        _logger.LogInformation("Received account position: {AccountId}/{MarketID}", serverMessage.AccountPosition.AccountId, serverMessage.AccountPosition.MarketId);
                    }
                    else
                    {
                        _logger.LogWarning("Received account position for unknown account: {AccoAccountIduntName}, Market: {MarketID}", serverMessage.AccountPosition.AccountId, serverMessage.AccountPosition.MarketId);
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.AccountSnapshot:
                {
                    if (_accounts.TryGetValue(serverMessage.AccountSnapshot.AccountId, out var account))
                    {
                        var accountUpdateResult = account.UpdateWithMessage(serverMessage);
                        OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));
                        _logger.LogInformation("Received account snapshot: {AccountId}", serverMessage.AccountSnapshot.AccountId);
                    }
                    else
                    {
                        _logger.LogWarning("Received account snapshot for unknown account: {AccountId}", serverMessage.AccountSnapshot.AccountId);
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.OrderUpdateMulti:
                {
                    if (_accounts.TryGetValue(serverMessage.OrderUpdateMulti.AccountId, out var account))
                    {
                        var accountUpdateResult = account.UpdateWithMessage(serverMessage);
                        OnAccountUpdate?.Invoke(this, new AccountUpdateEventArgs(accountUpdateResult));
                        _logger.LogInformation("Received order update multi: {AccountId}", serverMessage.OrderUpdateMulti.AccountId);
                    }
                    else
                    {
                        _logger.LogWarning("Received order update multi for unknown account: {AccountId}", serverMessage.OrderUpdateMulti.AccountId);
                    }
                }
                break;

            case ServerMessage.PayloadOneofCase.MarketSettlement:
                ProcessMarketSettlement(serverMessage.MarketSettlement);
                break;

            case ServerMessage.PayloadOneofCase.MarketHighLow:
                ProcessMarketHighLow(serverMessage.MarketHighLow);
                break;

            case ServerMessage.PayloadOneofCase.AuthenticationToken:
                {
                    _authToken = serverMessage.AuthenticationToken;

                    // Complete the pending token request if any
                    if (_pendingTokenRequest != null && !_pendingTokenRequest.Task.IsCompleted)
                    {
                        _pendingTokenRequest.SetResult(_authToken);
                    }

                    _logger.LogInformation("Received authentication token. Expires at: {ExpireTime} CST", _authToken.ExpireTime.ProtobufTimestampToCST());
                    break;
                }

            default:
                _logger.LogInformation($"! Unhandled Server Message: {serverMessage.PayloadCase}");
                break;
        }
    }

    private void ProcessMarketByOrderSubscribeReject(MarketByOrderSubscribeReject marketByOrderSubscribeReject)
    {
        _logger.LogInformation($"Market by order subscription rejected: {marketByOrderSubscribeReject.MarketId} ({marketByOrderSubscribeReject.Mode})");
    }

    private void ProcessMarketDepth(MarketDepth marketDepth)
    {
        var updatedSnapshot = _marketSnapshots.AddOrUpdate(
            marketDepth.MarketId,
            new MarketDataSnapshot { MarketDepth = marketDepth },
            (_, sn) => sn with { MarketDepth = marketDepth });

        OnMarketUpdate?.Invoke(updatedSnapshot);
    }

    private void ProcessMarketByOrderSnapshot(MarketByOrderSnapshot marketByOrderSnapshot)
    {
        _logger.LogInformation($"Received market by order snapshot: {marketByOrderSnapshot.MarketId}");

        // Create a MarketByOrder and process the snapshot
        var marketByOrder = new MarketByOrder().ProcessSnapshot(marketByOrderSnapshot);

        var updatedSnapshot = _marketSnapshots.AddOrUpdate(
            marketByOrderSnapshot.MarketId,
            new MarketDataSnapshot { MarketByOrder = marketByOrder },
            (_, sn) => sn with { MarketByOrder = marketByOrder });

        OnMarketUpdate?.Invoke(updatedSnapshot);
    }

    private void ProcessMarketByOrderUpdate(MarketByOrderUpdate marketByOrderUpdate)
    {
        _logger.LogDebug($"Received market by order update: {marketByOrderUpdate.MarketId}");

        if (_marketSnapshots.TryGetValue(marketByOrderUpdate.MarketId, out var existingSnapshot))
        {
            if (existingSnapshot.MarketByOrder != null)
            {
                // Process the update with the existing market by order data
                var updatedMarketByOrder = existingSnapshot.MarketByOrder.ProcessUpdate(marketByOrderUpdate);

                var updatedSnapshot = existingSnapshot with { MarketByOrder = updatedMarketByOrder };
                _marketSnapshots[marketByOrderUpdate.MarketId] = updatedSnapshot;

                OnMarketUpdate?.Invoke(updatedSnapshot);
            }
            else
            {
                _logger.LogWarning($"Received market by order update for {marketByOrderUpdate.MarketId} but no existing snapshot data");
            }
        }
        else
        {
            _logger.LogWarning($"Received market by order update for unknown market: {marketByOrderUpdate.MarketId}");
        }
    }

    private void ProcessMarketDetails(MarketDetails marketDetails)
    {
        var updatedSnapshot = _marketSnapshots.AddOrUpdate(
            marketDetails.MarketId,
            new MarketDataSnapshot { MarketDetails = marketDetails },
            (_, sn) => sn with { MarketDetails = marketDetails });

        OnMarketUpdate?.Invoke(updatedSnapshot);
    }

    private void ProcessMarketSettlement(MarketSettlement marketSettlement)
    {
        var updatedSnapshot = _marketSnapshots.AddOrUpdate(
            marketSettlement.MarketId,
            new MarketDataSnapshot { MarketSettlement = marketSettlement },
            (_, sn) => sn with { MarketSettlement = marketSettlement });

        OnMarketUpdate?.Invoke(updatedSnapshot);
    }

    private void ProcessMarketHighLow(MarketHighLow marketHighLow)
    {
        var updatedSnapshot = _marketSnapshots.AddOrUpdate(
            marketHighLow.MarketId,
            new MarketDataSnapshot { MarketHighLow = marketHighLow },
            (_, sn) => sn with { MarketHighLow = marketHighLow });

        OnMarketUpdate?.Invoke(updatedSnapshot);
    }

    #endregion

    #region Heartbeat Handling

    private async Task HeartbeatLoopAsync()
    {
        _logger.LogInformation("Starting heartbeat loop");
        try
        {
            while (!_isDisposed && await _heartbeatTimer.WaitForNextTickAsync())
            {
                try
                {
                    await SendHeartbeatAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send heartbeat");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Heartbeat loop terminated unexpectedly");
        }
    }

    private async Task SendHeartbeatAsync()
    {
        if (_client.State != WebSocketState.Open)
        {
            _logger.LogWarning("Attempted to send heartbeat while connection is not open. Current state: {State}", _client.State);
            return;
        }

        var heartbeat = new T4Proto.V1.Service.Heartbeat
        {
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        await SendMessageAsync(heartbeat);
        _logger.LogInformation("Heartbeat sent at {Timestamp}", DateTime.UtcNow);
    }

    private bool IsConnectionHealthy()
    {
        if (_client.State != WebSocketState.Open)
        {
            _logger.LogWarning("Connection unhealthy: WebSocket state is {State}", _client.State);
            return false;
        }

        var timeSinceLastMessage = DateTime.UtcNow - _lastMessageReceived;
        var isHealthy = timeSinceLastMessage.TotalSeconds <= MessageTimeoutSeconds;

        if (!isHealthy)
        {
            _logger.LogWarning("Connection unhealthy: No messages received for {Seconds} seconds", timeSinceLastMessage.TotalSeconds);
        }

        return isHealthy;
    }

    #endregion

    #region Private Utility Methods

    private bool IsRecoverableException(Exception ex)
    {
        // Examples of recoverable exceptions
        return ex is SocketException ||
               ex is TimeoutException ||
               ex is TaskCanceledException ||
               (ex is HttpRequestException hrex && IsTransientStatusCode(hrex)) ||
               // Add other recoverable exception types
               // But exclude exceptions that indicate unrecoverable situations
               !(ex is AuthenticationException ||
                 ex is UnauthorizedAccessException ||
                 ex is InvalidOperationException);
    }

    private bool IsTransientStatusCode(HttpRequestException ex)
    {
        // If the exception has a status code
        if (ex.StatusCode.HasValue)
        {
            int code = (int)ex.StatusCode.Value;
            // Typically 5xx (server errors) and some 4xx codes are transient
            return code >= 500 || code == 408 || code == 429;
        }
        return true; // If no status code, assume it might be transient
    }

    #endregion
}