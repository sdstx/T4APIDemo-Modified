using Microsoft.Extensions.Configuration;

namespace T4APIDemo.T4.CredentialProviders;

public class ConfigurationCredentialProvider : ICredentialProvider
{
    private readonly IConfiguration _configuration;
    private readonly bool _useApiKey;

    public ConfigurationCredentialProvider(IConfiguration configuration)
    {
        _configuration = configuration;

        // Determine which authentication method to use
        _useApiKey = !string.IsNullOrEmpty(_configuration["T4API:ApiKey"]);

        // Validate configuration
        if (!_useApiKey && (string.IsNullOrEmpty(_configuration["T4API:Firm"]) ||
                           string.IsNullOrEmpty(_configuration["T4API:Username"]) ||
                           string.IsNullOrEmpty(_configuration["T4API:Password"]) ||
                           string.IsNullOrEmpty(_configuration["T4API:AppName"]) ||
                           string.IsNullOrEmpty(_configuration["T4API:AppLicense"])))
        {
            throw new ArgumentException("Either API key or complete credentials (firm, username, password, appName, appLicense) must be provided in configuration.");
        }
    }

    public Task<T4Proto.V1.Auth.LoginRequest> GetLoginRequestAsync()
    {
        var loginRequest = new T4Proto.V1.Auth.LoginRequest();

        if (_useApiKey)
        {
            loginRequest.ApiKey = _configuration["T4API:ApiKey"];
        }
        else
        {
            loginRequest.Firm = _configuration["T4API:Firm"];
            loginRequest.Username = _configuration["T4API:Username"];
            loginRequest.Password = _configuration["T4API:Password"];
            loginRequest.AppName = _configuration["T4API:AppName"];
            loginRequest.AppLicense = _configuration["T4API:AppLicense"];
        }

        return Task.FromResult(loginRequest);
    }
}