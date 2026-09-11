FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app

# Expose AMQP (5672), HTTP Management (5300), HTTPS Management (5301), and the Dashboard UI (15672)
EXPOSE 5300 5301 5672 15672

# Persist the generated admin TLS cert/CA so clients trust it once, across restarts.
VOLUME /certs

COPY . ./

ENTRYPOINT ["dotnet", "AlmostServiceBus.Host.dll", "--Port", "5672", "--DashboardPort", "15672", "--AdminTlsPort", "5301", "--AdminTlsCertDir", "/certs"]
