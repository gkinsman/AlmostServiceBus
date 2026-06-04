namespace AlmostServiceBus.Core.Dashboard;

/// <summary>
/// Emulator connection details surfaced on the dashboard so users can copy the
/// connection string without hunting through console output. The connection string
/// is the default (<c>RootManageSharedAccessKey</c>) one, matching the startup banner.
/// </summary>
public record EmulatorInfo(
    string ConnectionString,
    int AmqpPort,
    int ManagementPort,
    int DashboardPort);

public record NamespaceInfo(string Name, int QueueCount, int TopicCount, DateTimeOffset LastActivityAt);

public record EntityOverview(
    List<QueueInfo> Queues,
    List<TopicInfo> Topics);

public record QueueInfo(
    string Name,
    int MessageCount,
    int DeadLetterCount,
    int TotalMessageCount,
    int ConsumedCount,
    int MaxDeliveryCount,
    string? ForwardTo);

public record TopicInfo(
    string Name,
    List<SubscriptionInfo> Subscriptions);

public record SubscriptionInfo(
    string Name,
    string? ForwardTo,
    int MessageCount,
    int RuleCount);

public record MessageInfo(
    string MessageId,
    long SequenceNumber,
    string? ContentType,
    string? CorrelationId,
    int DeliveryCount,
    DateTimeOffset EnqueuedTimeUtc,
    string? Subject,
    Dictionary<string, object>? ApplicationProperties,
    string? BodyText,
    Dictionary<string, object>? ScalarProperties,
    string State);
