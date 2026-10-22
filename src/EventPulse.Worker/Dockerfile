FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Directory.Build.props .editorconfig ./
COPY src/EventPulse.Contracts/EventPulse.Contracts.csproj src/EventPulse.Contracts/
COPY src/EventPulse.Worker/EventPulse.Worker.csproj src/EventPulse.Worker/
RUN dotnet restore src/EventPulse.Worker/EventPulse.Worker.csproj
COPY src/EventPulse.Contracts/ src/EventPulse.Contracts/
COPY src/EventPulse.Worker/ src/EventPulse.Worker/
RUN dotnet publish src/EventPulse.Worker/EventPulse.Worker.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
ENV DOTNET_ENVIRONMENT=Docker
COPY --from=build /app .
ENTRYPOINT ["dotnet", "EventPulse.Worker.dll"]
