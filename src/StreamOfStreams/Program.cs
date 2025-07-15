using Akka.Hosting;
using StreamOfStreams;
using Microsoft.Extensions.Hosting;

var hostBuilder = new HostBuilder();

hostBuilder.ConfigureServices((context, services) =>
{
    services.AddAkka("StreamOfStreams", (builder, sp) =>
    {
        
    });
});

var host = hostBuilder.Build();

await host.RunAsync();