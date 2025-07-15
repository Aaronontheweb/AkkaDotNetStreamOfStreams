using Akka.Streams;
using Akka.Streams.Dsl;

namespace StreamOfStreams;

public sealed record EntityWithDataPoints(EntityMetadata Metadata, ISourceRef<DataPoint> DataPoints) : IWithEntityId
{
    public string EntityId => Metadata.Entity.EntityId;
}

/// <summary>
/// Gets a request for a stream of data points for a given entity.
/// </summary>
public sealed class DataPointActor : UntypedActor
{
    private readonly ILoggingAdapter _log = Context.GetLogger();
    private readonly IMaterializer _materializer = Context.Materializer();
    
    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case Entity entity:
                _log.Info($"Received request for data points for entity {entity.EntityId}");
                
                // need to generate metadata for the entity and a SourceRef<DataPoint>
                var metadata = EntityData.GenerateMetadata(entity);
                var dataPoints = EntityData.GenerateDataPoints(metadata).ToList();
                
                RunTask(async () =>
                {
                    var streamRef = StreamRefs.SourceRef<DataPoint>().Named($"entity-{entity.EntityId}-data-points");
                
                    var sourceRef = await Source.From(dataPoints)
                        .RunWith(streamRef, _materializer);
                    
                    var response = new EntityWithDataPoints(metadata, sourceRef);
                    _log.Info($"Sending response for entity {entity.EntityId} with {dataPoints.Count} data points.");
                    Sender.Tell(response);
                });
                
                break;
            
            default:
                Unhandled(message);
                break;
        }
    }
}