namespace T4APIDemo.T4.CredentialProviders;

public interface ICredentialProvider
{
    Task<T4Proto.V1.Auth.LoginRequest> GetLoginRequestAsync();
}