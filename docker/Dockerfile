FROM mcr.microsoft.com/dotnet/aspnet:11.0
WORKDIR /app

# Expose AMQP (5672), HTTP Management (5300), and the Dashboard UI (15672)
EXPOSE 5300 5672 15672

COPY . ./

ENTRYPOINT ["dotnet", "AlmostServiceBus.Host.dll", "--Port", "5672", "--DashboardPort", "15672"]
