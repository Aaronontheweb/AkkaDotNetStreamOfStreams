namespace StreamOfStreams;

public interface IWithEntityId
{
    string EntityId { get; }
}

/// <summary>
/// Some sort of entity that has an ID - not important.
/// </summary>
public sealed record Entity(string EntityId) : IWithEntityId;

/// <summary>
/// Some metadata for an entity.
/// </summary>
/// <param name="SomeScalar">An arbitrary scalar value</param>
public sealed record EntityMetadata(Entity Entity, double SomeScalar, double PreviousAverage);

public sealed record DataPoint(double Value, DateTimeOffset Timestamp);

public sealed record ComputationResults(
    EntityMetadata Metadata,
    double Average,
    double StandardDeviation,
    double PreviousAverage, DateTime Timestamp)
{
    public string EntityId => Metadata.Entity.EntityId;
}

public static class EntityData
{
    public static IEnumerable<Entity> Entities =>
        Enumerable.Range(1, 100)
            .Select(i => new Entity($"Entity-{i}"));
    
    public static EntityMetadata GenerateMetadata(Entity entity)
    {
        // Generate some metadata for the entity.
        // This is just a placeholder for whatever logic you want to implement.
        var random = Random.Shared;
        return new EntityMetadata(
            entity,
            SomeScalar: random.NextDouble() * 100,
            PreviousAverage: random.NextDouble() * 50);
    }

    public static IEnumerable<DataPoint> GenerateDataPoints(EntityMetadata entity)
    {
        // generate a random size of data points
        var random = Random.Shared;
        var count = random.Next(5, 50);
        
        // pick a deviation from the previous average
        var deviation = random.NextDouble() * 10 - 5; // between -5 and +5
        
        // generate data points based on the previous average and deviation
        return Enumerable.Range(1, count)
            .Select(i => new DataPoint(
                Value: entity.PreviousAverage + deviation + random.NextDouble() * 2 - 1, // small random variation
                Timestamp: DateTimeOffset.UtcNow.AddMinutes(-i)));
    }
}