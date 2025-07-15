using Akka.Hosting;
using StreamOfStreams;
using Microsoft.Extensions.Hosting;
using StreamOfStreams.Storage;

var hostBuilder = new HostBuilder();

hostBuilder.ConfigureServices((context, services) =>
{
    services.AddAkka("StreamOfStreams", (builder, sp) =>
    {
        builder
            .WithActors((actorSystem, registry) =>
            {
                var dataPointActor = actorSystem.ActorOf(Props.Create(() => new DataPointActor()), "DataPointActor");
                // Register the DataPointActor
                registry.Register<DataPointActor>(dataPointActor);
            })
            .WithActors((system, registry) =>
            {
                var writerActor = system.ActorOf(Props.Create(() => new DynamoDbWriterActor()), "DynamoDbWriterActor");
                // Register the DynamoDbWriterActor
                registry.Register<DynamoDbWriterActor>(writerActor);
                
                // S3 actor
                var s3WriterActor = system.ActorOf(Props.Create(() => new S3WriterActor()), "S3WriterActor");
                // Register the S3WriterActor
                registry.Register<S3WriterActor>(s3WriterActor);
            });
    });
});

var host = hostBuilder.Build();

await host.RunAsync();