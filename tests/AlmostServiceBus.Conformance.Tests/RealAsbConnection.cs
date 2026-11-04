using Microsoft.Extensions.Configuration;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// The real Azure Service Bus connection string for the tests that compare against Azure: the
/// <c>ASB_CONNECTION_STRING</c> environment variable (CI, the documented command) if set, otherwise .NET user
/// secrets id <c>almostservicebus-conformance</c>, key <c>ASB_CONNECTION_STRING</c>
/// (<c>dotnet user-secrets set ASB_CONNECTION_STRING "..." --id almostservicebus-conformance</c>).
/// Never log the value.
/// </summary>
internal static class RealAsbConnection
{
    public const string Key = "ASB_CONNECTION_STRING";

    public const string NotConfigured =
        "ASB_CONNECTION_STRING is not set (environment variable or user secrets id almostservicebus-conformance)";

    private static readonly Lazy<string?> Value = new(() =>
    {
        var fromEnvironment = Environment.GetEnvironmentVariable(Key);
        if (!string.IsNullOrEmpty(fromEnvironment))
            return fromEnvironment;

        var fromSecrets = new ConfigurationBuilder()
            .AddUserSecrets(typeof(RealAsbConnection).Assembly, optional: true)
            .Build()[Key];
        return string.IsNullOrEmpty(fromSecrets) ? null : fromSecrets;
    });

    public static string? ConnectionString => Value.Value;
}
