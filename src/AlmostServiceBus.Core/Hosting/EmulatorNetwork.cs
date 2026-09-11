namespace AlmostServiceBus.Core.Hosting;

public static class EmulatorNetwork
{
    /// <summary>
    /// The single environment variable that names the host clients use to reach the emulator
    /// (for example a Docker service name). It is advertised as the public host and is treated
    /// as a default-namespace host, so <c>RootManageSharedAccessKey</c> maps to "default".
    /// </summary>
    public const string HostEnvironmentVariable = "ASB_HOST";

    public static string GetPublicHost()
    {
        var configured = Environment.GetEnvironmentVariable(HostEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(configured))
            return configured.Trim();

        // In a container fall back to the container's own hostname; locally use loopback.
        return IsRunningInContainer() ? Environment.MachineName : "localhost";
    }

    public static string GetBindHost()
    {
        var configured = Environment.GetEnvironmentVariable("ASB_BIND_HOST");
        if (!string.IsNullOrWhiteSpace(configured))
            return configured.Trim();

        return IsRunningInContainer() ? "0.0.0.0" : "localhost";
    }

    public static bool IsDefaultNamespaceHost(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
            return true;

        var candidate = host.Trim().TrimEnd('.');
        // Strip a "host:port" suffix, but leave bare IPv6 addresses (multiple colons) intact.
        if (candidate.Count(c => c == ':') == 1)
            candidate = candidate.Split(':', 2)[0];

        foreach (var alias in GetDefaultNamespaceHosts())
        {
            if (candidate.Equals(alias, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static IEnumerable<string> GetDefaultNamespaceHosts()
    {
        // Loopback interfaces.
        yield return "localhost";
        yield return "127.0.0.1";
        yield return "0.0.0.0";
        yield return "::1";

        // The container's own hostname (Docker sets this to the service/container name).
        yield return Environment.MachineName;

        // Docker Desktop's host gateway.
        yield return "host.docker.internal";

        // The configured public host, plus any extra comma/space-separated aliases.
        foreach (var alias in SplitAliases(Environment.GetEnvironmentVariable(HostEnvironmentVariable)))
            yield return alias;

        foreach (var alias in SplitAliases(Environment.GetEnvironmentVariable("ASB_DEFAULT_NAMESPACE_HOSTS")))
            yield return alias;
    }

    private static IEnumerable<string> SplitAliases(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            yield break;

        foreach (var part in value.Split(new[] { ',', ';', ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var trimmed = part.Trim();
            if (trimmed.Length > 0)
                yield return trimmed;
        }
    }

    private static bool IsRunningInContainer() =>
        string.Equals(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Environment.GetEnvironmentVariable("ASB_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase);
}
