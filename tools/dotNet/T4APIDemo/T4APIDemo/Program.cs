using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using T4APIDemo;
using T4APIDemo.DemoClient;
using T4APIDemo.T4;
using T4APIDemo.T4.CredentialProviders;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureServices((hostContext, services) =>
{
    services.AddHttpClient();
    services.AddSingleton<ICredentialProvider>(sp =>
    {
        return new ConfigurationCredentialProvider(hostContext.Configuration);
    });
    services.AddSingleton<DatabaseHelper>(sp => 
        new DatabaseHelper("trades.db", sp.GetRequiredService<ILoggerFactory>().CreateLogger<DatabaseHelper>()));
    services.AddSingleton<T4APIClient>(sp =>
    {
        var config = hostContext.Configuration;
        var credentialProvider = sp.GetRequiredService<ICredentialProvider>();
        var logger = sp.GetRequiredService<ILogger<T4APIClient>>();
        var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
        var httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
        var databaseHelper = sp.GetRequiredService<DatabaseHelper>();
        return new T4APIClient(credentialProvider, logger, loggerFactory, httpClientFactory, config, databaseHelper);
    });
    services.AddHostedService<DemoClient>();
});

var host = builder.Build();
await host.RunAsync();
