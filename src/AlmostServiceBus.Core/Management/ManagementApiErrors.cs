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

    // Azure writes a CR inside its detail as &#xD; so that "\r\nParameter name: …" survives XML parsing.
    private static string EscapeDetail(string detail) =>
        System.Security.SecurityElement.Escape(detail).Replace("\r", "&#xD;", StringComparison.Ordinal);

    public static IResult BadRequest(string detail)
    {
        var xml = $"<Error><Code>400</Code><Detail>{EscapeDetail(detail)}</Detail></Error>";
        return Results.Content(xml, ContentType, statusCode: StatusCodes.Status400BadRequest);
    }

    /// <summary>409 with Azure's own detail text; the admin client raises MessagingEntityAlreadyExists.</summary>
    public static IResult Conflict(string detail)
    {
        var xml = $"<Error><Code>409</Code><Detail>{EscapeDetail(detail)}</Detail></Error>";
        return Results.Content(xml, ContentType, statusCode: StatusCodes.Status409Conflict);
    }

    public static IResult EntityAlreadyExists(string entityName)
    {
        var xml = $"<Error><Code>509</Code><Detail>Entity '{entityName}' already exists.</Detail></Error>";
        return Results.Content(xml, ContentType, statusCode: StatusCodes.Status409Conflict);
    }
}
