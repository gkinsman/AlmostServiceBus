# 1. Build Stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

RUN apt-get update && \
    apt-get install -y curl git && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs

WORKDIR /src

# Copy all source code
COPY . .

# Build and publish the Emulator
RUN dotnet publish src/AlmostServiceBus.Host/AlmostServiceBus.Host.csproj -c Release -o /app/publish

# 2. Runtime Stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

# Expose AMQP (5672), HTTP Management (5300), and the Dashboard UI (e.g., 15672)
EXPOSE 5300 5672 15672

# Copy both published apps from the build stage
COPY --from=build /app/publish .

# Start both apps
ENTRYPOINT ["dotnet", "AlmostServiceBus.Host.dll", "--Port", "5672", "--DashboardPort", "15672"]
