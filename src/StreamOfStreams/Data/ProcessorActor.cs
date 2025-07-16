using Akka.DependencyInjection;
using Akka.Hosting;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util.Internal;
using StreamOfStreams.Storage;

namespace StreamOfStreams.Data;

public sealed class ProcessorActor : UntypedActor
{
    private readonly IMaterializer _materializer = Context.Materializer();
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IRequiredActor<DataPointActor> _dataPointActor;
    private readonly IRequiredActor<DynamoDbWriterActor> _dynamoDbWriterActor;
    private readonly IRequiredActor<S3WriterActor> _s3WriterActor;

    /*
     * These could be different values, depending on the relative "heaviness" of each operation
     */
    private const int MaxDataPointsParallelism = 5;
    private const int MaxSubStreamParallelism = 5;

    public static Props PropsFor(ActorSystem system) => DependencyResolver.For(system).Props<ProcessorActor>();

    public sealed record ProcessEntities(IReadOnlyList<Entity> Entities);

    public sealed record ProcessingComplete(int Requested, int Completed, int Failed)
    {
        public static ProcessingComplete Empty => new(0, 0, 0);
    }

    public ProcessorActor(IRequiredActor<DataPointActor> dataPointActor,
        IRequiredActor<DynamoDbWriterActor> dynamoDbWriterActor,
        IRequiredActor<S3WriterActor> s3WriterActor)
    {
        _dataPointActor = dataPointActor;
        _dynamoDbWriterActor = dynamoDbWriterActor;
        _s3WriterActor = s3WriterActor;
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case ProcessEntities entities:
                RunTask(() => RunEntityProcessing(entities.Entities));
                
                break;
            
            default:
                Unhandled(message);
                break;
        }
    }

    private async Task RunEntityProcessing(IReadOnlyList<Entity> entities)
    {
        _log.Info($"Received request to process {entities.Count} entities.");

        if (entities.Count == 0)
            Sender.Tell(ProcessingComplete.Empty);

        var maxCount = entities.Count;
        var atomicCounter = new AtomicCounter(0);

        var initialGraph = Source.From(entities)
            .Ask<EntityWithDataPoints>(_dataPointActor.ActorRef, TimeSpan.FromSeconds(10), MaxDataPointsParallelism)
            .Log("EntityBeginProcessing", o => $"Received metadata for entity {o.EntityId}", _log,
                logLevel: LogLevel.InfoLevel)
            .SelectAsyncUnordered(MaxSubStreamParallelism, async points =>
            {
                var currentCount = atomicCounter.GetAndIncrement();
                _log.Info("Beginning processing on entity [{0}] {1}/{2}", points.EntityId, currentCount, maxCount);
                var (metadata, sourceRef) = points;

                // aggregate all the data points emitted by the SourceREf
                var aggregateStats = await sourceRef.Source.Aggregate(StatsAccumulator.Empty,
                        (acc, dp) => new StatsAccumulator(Sum: acc.Sum + dp.Value, Count: acc.Count + 1,
                            SumSquares: acc.SumSquares + (dp.Value * dp.Value),
                            LatestTimestamp: dp.Timestamp > acc.LatestTimestamp ? dp.Timestamp : acc.LatestTimestamp))
                    .Select(acc =>
                    {
                        // realistic looking-ish math - just for demo purposes
                        var newAverage = acc.Sum / acc.Count;
                        var newStdDev = Math.Sqrt((acc.SumSquares / acc.Count) - (newAverage * newAverage));

                        var blendedAverage =
                            (metadata.PreviousAverage * metadata.SomeScalar) +
                            (newAverage * (1 - metadata.SomeScalar));

                        var adjustedStdDev = newStdDev * (1 - metadata.SomeScalar);

                        return new ComputationResults(
                            Metadata: metadata,
                            Average: blendedAverage,
                            StandardDeviation: adjustedStdDev,
                            PreviousAverage: metadata.PreviousAverage,
                            Timestamp: acc.LatestTimestamp.UtcDateTime
                        );
                    })
                    .IdleTimeout(TimeSpan.FromMinutes(1)) // 60-second processing timeout
                    .RunWith(Sink.First<ComputationResults>(), _materializer);

                _log.Info("Finished computation for entity [{0}] {1}/{2} - saving to DynamoDb and S3", points.EntityId,
                    currentCount, maxCount);

                // ensure we get this data into S3 and DynamoDb before moving on
                var writeToDynamo = _dynamoDbWriterActor.ActorRef.Ask<IWriteResult>(
                    new DynamoDbWriterActor.WriteToDynamo(aggregateStats), TimeSpan.FromSeconds(5));

                var writeToS3 = _s3WriterActor.ActorRef.Ask<IWriteResult>(
                    new S3WriterActor.WriteToS3(aggregateStats), TimeSpan.FromSeconds(5));

                // wait for both to complete
                var r = await Task.WhenAll(writeToDynamo, writeToS3);

                _log.Info("Finished persistence for entity [{0}] {1}/{2}", points.EntityId, currentCount, maxCount);

                // TODO: retries, logging, etc if one of the two operations fails
                return r[0];
            })
            .RunWith(Sink.Ignore<IWriteResult>(), _materializer); // could do some totaling here to track failed requests

        // wait for execution to complete 
        await initialGraph;
        
        Sender.Tell(new ProcessingComplete(entities.Count, entities.Count, 0));;
    }

    internal readonly record struct StatsAccumulator(
        double Sum,
        int Count,
        double SumSquares,
        DateTimeOffset LatestTimestamp
    )
    {
        public static StatsAccumulator Empty => new(0, 0, 0, DateTimeOffset.MinValue);
    }
}