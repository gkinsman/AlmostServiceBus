using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace OrderFlowDemo.ServiceDefaults;

public static class Extensions
{
    public static IHostApplicationBuilder AddServiceDefaults(this IHostApplicationBuilder builder)
    {
        builder.ConfigureSerilog();
        return builder;
    }

    private static void ConfigureSerilog(this IHostApplicationBuilder builder)
    {
        var seqUrl = builder.Configuration["ConnectionStrings:seq"]
            ?? "http://localhost:9341";

        builder.Services.AddSerilog(lc => lc
            .ReadFrom.Configuration(builder.Configuration)
            .Enrich.FromLogContext()
            .Enrich.WithProperty("Application", builder.Environment.ApplicationName)
            .WriteTo.Console()
            .WriteTo.Seq(seqUrl));
    }
}
