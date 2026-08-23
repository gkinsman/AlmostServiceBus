using Microsoft.AspNetCore.Http;

namespace AlmostServiceBus.Core.Management;

/// <summary>
/// Returns IResult responses with XML error bodies matching what the Azure Service Bus SDK expects.
/// </summary>
public static class ManagementApiErrors
{
    private const string ContentType = "application/xml;charset=utf-8";

    public static IResult EntityNotFound(string entityName)
    {
        var xml = $"<Error><Code>404</Code><Detail>Entity '{entityName}' could not be found.</Detail></Error>";
        return Results.Content(xml, ContentType, statusCode: StatusCodes.Status404NotFound);
    }

    public static IResult EntityAlreadyExists(string entityName)
    {
        var xml = $"<Error><Code>MessagingEntityAlreadyExists</Code><Detail>Entity '{entityName}' already exists.</Detail></Error>";
        return Results.Content(xml, ContentType, statusCode: StatusCodes.Status409Conflict);
    }
}
