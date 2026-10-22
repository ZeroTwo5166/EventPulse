FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Directory.Build.props .editorconfig ./
COPY src/EventPulse.Contracts/EventPulse.Contracts.csproj src/EventPulse.Contracts/
COPY src/EventPulse.Api/EventPulse.Api.csproj src/EventPulse.Api/
RUN dotnet restore src/EventPulse.Api/EventPulse.Api.csproj
COPY src/EventPulse.Contracts/ src/EventPulse.Contracts/
COPY src/EventPulse.Api/ src/EventPulse.Api/
RUN dotnet publish src/EventPulse.Api/EventPulse.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
ENV DOTNET_ENVIRONMENT=Docker
COPY --from=build /app .
ENTRYPOINT ["dotnet", "EventPulse.Api.dll"]
