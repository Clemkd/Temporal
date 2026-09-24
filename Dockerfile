FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY TemporalPoc.sln ./
COPY src/TemporalPoc.Core/TemporalPoc.Core.csproj src/TemporalPoc.Core/
COPY src/TemporalPoc.Api/TemporalPoc.Api.csproj src/TemporalPoc.Api/
RUN dotnet restore src/TemporalPoc.Api/TemporalPoc.Api.csproj
COPY src/ src/
RUN dotnet publish src/TemporalPoc.Api/TemporalPoc.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "TemporalPoc.Api.dll"]
