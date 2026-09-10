using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Reads and writes the AMQP message identifier fields without assuming they hold a string.
/// AMQP 1.0 allows message-id and correlation-id to be a string, a ulong, a uuid or binary.
/// AMQPNetLite's <see cref="Properties.MessageId"/> and <see cref="Properties.CorrelationId"/>
/// properties cast to string, so reading either one throws InvalidCastException when the peer
/// sent another type. The Python azure-servicebus client sends a uuid, which made every request
/// from it fail with "Unable to cast object of type 'System.Guid' to type 'System.String'".
/// </summary>
internal static class AmqpIdentifiers
{
    /// <summary>
    /// Sends a request's response, keeping the type of the correlation id.
    /// </summary>
    /// <remarks>
    /// This replaces <see cref="RequestContext.Complete"/>, which does the same work but copies the
    /// request's message-id through the string-typed properties. A uuid message-id therefore threw
    /// inside AMQPNetLite before any response was sent, and the connection was closed with
    /// amqp:internal-error. Stringifying the id instead is not an answer: a client that matches a
    /// response to its request by message-id then never finds a match, and its request times out.
    /// Only the state field of the context is lost, which AMQPNetLite reads for nothing else.
    /// </remarks>
    public static void CompleteRequest(RequestContext requestContext, Message response)
    {
        response.Properties ??= new Properties();

        if (requestContext.Message.Properties?.GetMessageId() is { } messageId)
            response.Properties.SetCorrelationId(messageId);

        requestContext.ResponseLink.SendMessage(response);
        requestContext.Message.Dispose();
    }

    /// <summary>
    /// Builds the reply properties for a request, echoing the request's message-id as the
    /// response's correlation-id and keeping the type the peer used.
    /// </summary>
    public static Properties CorrelateTo(Message request)
    {
        var properties = new Properties();

        if (request.Properties?.GetMessageId() is { } messageId)
            properties.SetCorrelationId(messageId);

        return properties;
    }

    /// <summary>
    /// The message-id as text, whatever type it arrived as, or null when there is none.
    /// </summary>
    public static string? MessageIdAsText(Properties properties) =>
        properties.GetMessageId()?.ToString();

    /// <summary>
    /// The correlation-id as text, whatever type it arrived as, or null when there is none.
    /// </summary>
    public static string? CorrelationIdAsText(Properties properties) =>
        properties.GetCorrelationId()?.ToString();
}
