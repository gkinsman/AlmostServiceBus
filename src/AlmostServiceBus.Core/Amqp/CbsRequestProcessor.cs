using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Handles CBS ($cbs) token authentication requests.
/// The emulator accepts all tokens unconditionally.
/// Extracts the SharedAccessKeyName from the SAS token and stores it
/// for namespace resolution by the link processor.
/// </summary>
public class CbsRequestProcessor : IRequestProcessor
{
    private static readonly TimeSpan DefaultTokenExpiration = TimeSpan.FromHours(1);

    private sealed class NamespaceHolder(string value)
    {
        public string Value { get; } = value;
    }

    // Maps each live AMQP connection instance → namespace name extracted from SAS key name.
    // Some AMQPNetLite callbacks can surface a different Connection wrapper for the same
    // logical socket, so also track the underlying transport identity and connection ids
    // when available.
    private static readonly ConditionalWeakTable<Connection, NamespaceHolder> _connectionNamespaces = new();
    private static readonly ConditionalWeakTable<ITransport, NamespaceHolder> _transportNamespaces = new();
    private static readonly ConcurrentDictionary<string, string> _connectionIdentityNamespaces = new(StringComparer.Ordinal);

    // CBS links are long-lived and can see bursts of token renewals across many
    // parallel clients, so keep the request credit comfortably above the default.
    public int Credit => 1000;

    public static string? GetNamespaceForConnection(Connection connection)
    {
        if (_connectionNamespaces.TryGetValue(connection, out var holder))
            return holder.Value;

        var transport = TryGetTransport(connection);
        if (transport is not null && _transportNamespaces.TryGetValue(transport, out holder))
            return holder.Value;

        var identityKey = TryGetConnectionIdentityKey(connection);
        return identityKey is not null && _connectionIdentityNamespaces.TryGetValue(identityKey, out var ns)
            ? ns
            : null;
    }

    internal static string? GetConnectionIdentityKey(Connection connection) =>
        TryGetConnectionIdentityKey(connection);

    public static void RemoveConnection(Connection connection)
    {
        _connectionNamespaces.Remove(connection);

        var transport = TryGetTransport(connection);
        if (transport is not null)
            _transportNamespaces.Remove(transport);

        var identityKey = TryGetConnectionIdentityKey(connection);
        if (identityKey is not null)
            _connectionIdentityNamespaces.TryRemove(identityKey, out _);
    }

    internal static void SetNamespaceForConnection(Connection connection, string? keyName)
    {
        RemoveConnection(connection);

        if (string.IsNullOrEmpty(keyName)
            || keyName.Equals("RootManageSharedAccessKey", StringComparison.OrdinalIgnoreCase))
            return;

        _connectionNamespaces.Add(connection, new NamespaceHolder(keyName));

        var transport = TryGetTransport(connection);
        if (transport is not null)
            _transportNamespaces.Add(transport, new NamespaceHolder(keyName));

        var identityKey = TryGetConnectionIdentityKey(connection);
        if (identityKey is not null)
            _connectionIdentityNamespaces[identityKey] = keyName;
    }

    public void Process(RequestContext requestContext)
    {
        TryExtractNamespace(requestContext);

        var response = new Message()
        {
            ApplicationProperties = new ApplicationProperties
            {
                ["status-code"] = 200,
                ["status-description"] = "OK",
                // Include an expiration timestamp so the Azure SDK knows when to renew
                // the token. Without this, some SDK versions may schedule immediate
                // renewal, flooding the CBS link with requests.
                ["expiration"] = DateTime.UtcNow.Add(DefaultTokenExpiration)
            },
            Properties = AmqpIdentifiers.CorrelateTo(requestContext.Message)
        };
        AmqpIdentifiers.CompleteRequest(requestContext, response);
    }

    private static void TryExtractNamespace(RequestContext requestContext)
    {
        try
        {
            string? token = null;
            if (requestContext.Message.Body is string s)
                token = s;
            else if (requestContext.Message.Body is byte[] bytes)
                token = Encoding.UTF8.GetString(bytes);

            if (token is null) return;

            var sknIdx = token.IndexOf("skn=", StringComparison.OrdinalIgnoreCase);
            if (sknIdx < 0) return;

            var start = sknIdx + 4;
            var end = token.IndexOf('&', start);
            var keyName = end >= 0 ? token[start..end] : token[start..];

            var connection = requestContext.Link.Session.Connection;
            SetNamespaceForConnection(connection, keyName);
        }
        catch { /* ignore parsing errors */ }
    }

    private static ITransport? TryGetTransport(Connection connection)
    {
        try
        {
            var transportProp = connection.GetType().GetProperty("Transport",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
            return transportProp?.GetValue(connection) as ITransport;
        }
        catch
        {
            return null;
        }
    }

    private static string? TryGetConnectionIdentityKey(Connection connection)
    {
        try
        {
            var type = connection.GetType();
            var remoteContainerId = type.GetProperty("RemoteContainerId",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)
                ?.GetValue(connection) as string;
            var containerId = type.GetProperty("ContainerId",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)
                ?.GetValue(connection) as string;

            if (string.IsNullOrWhiteSpace(remoteContainerId) && string.IsNullOrWhiteSpace(containerId))
                return null;

            return $"{remoteContainerId ?? ""}|{containerId ?? ""}";
        }
        catch
        {
            return null;
        }
    }
}
