FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env
WORKDIR /app

COPY ./*sln ./

COPY ./OpenFTTH.RelationalProjector/*.csproj ./OpenFTTH.RelationalProjector/

RUN dotnet restore --packages ./packages

COPY . ./
WORKDIR /app/OpenFTTH.RelationalProjector
RUN dotnet publish -c Release -o out --packages ./packages

# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:9.0
WORKDIR /app

COPY --from=build-env /app/OpenFTTH.RelationalProjector/out .
ENTRYPOINT ["dotnet", "OpenFTTH.RelationalProjector.dll"]
