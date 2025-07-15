namespace StreamOfStreams.Storage;


/// <summary>
/// Simulate a bulk write to S3.
/// </summary>
public sealed class S3WriterActor : UntypedActor
{
    public sealed record WriteToS3(ComputationResults EntityWithDataPoints);
    
    public sealed record WriteResult(bool Success, string Message);
    
    private readonly ILoggingAdapter _log = Context.GetLogger();
    
    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case WriteToS3 writeToS3:
                _log.Info($"Received request to write data for entity {writeToS3.EntityWithDataPoints.EntityId} to S3.");
                
                // Simulate writing to S3
                RunTask(async () =>
                {
                    // Simulate some delay
                    await Task.Delay(Random.Shared.Next(1, 2000));
                    
                    // Simulate success
                    var result = new WriteResult(true, $"Successfully wrote data for entity {writeToS3.EntityWithDataPoints.EntityId} to S3.");
                    _log.Info(result.Message);
                    
                    Sender.Tell(result);
                });
                
                break;
            
            default:
                Unhandled(message);
                break;
        }
    }
}