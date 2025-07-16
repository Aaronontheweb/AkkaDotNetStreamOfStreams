using Akka.Hosting;
using StreamOfStreams;
using Microsoft.Extensions.Hosting;
using StreamOfStreams.Data;
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
            })
            .WithActors((system, registry) =>
            {
                var processorActorProps = ProcessorActor.PropsFor(system);
                var processorActor = system.ActorOf(processorActorProps, "ProcessorActor");
                // Register the ProcessorActor
                registry.Register<ProcessorActor>(processorActor);
            })
            .AddStartup(async (system, registry) =>
            {
                var processorActor = await registry.GetAsync<ProcessorActor>();
                await processorActor.Ask<ProcessorActor.ProcessingComplete>(new ProcessorActor.ProcessEntities(EntityData.Entities), TimeSpan.FromMinutes(1));
                system.Log.Info("Finished processing all entities.");
            });
    });
});

var host = hostBuilder.Build();

await host.RunAsync();