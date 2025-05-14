using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using T4APIDemo.DemoClient;
using T4APIDemo.T4;
using T4APIDemo.T4.CredentialProviders;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureServices((hostContext, services) =>
{
    // Register HTTP client factory
    services.AddHttpClient();

    // Register credential provider
    services.AddSingleton<ICredentialProvider>(sp =>
    {
        return new ConfigurationCredentialProvider(hostContext.Configuration);
    });

    // Register T4APIClient
    services.AddSingleton<T4APIClient>();

    // Demo client use the the T4APIClient to talk to the T4 API.
    services.AddHostedService<DemoClient>();
});

// Build and run the DemoClient.
var host = builder.Build();
await host.RunAsync();