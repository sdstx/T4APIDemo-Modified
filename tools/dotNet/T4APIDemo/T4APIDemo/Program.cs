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
    services.AddSingleton<T4APIClient>();
    services.AddSingleton<DatabaseHelper>(sp => 
        new DatabaseHelper("trades.db", sp.GetRequiredService<ILoggerFactory>()));
    services.AddHostedService<DemoClient>();
});

var host = builder.Build();
await host.RunAsync();


