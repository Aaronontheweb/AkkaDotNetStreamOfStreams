# StreamOfStreams - Akka.NET Streaming Demo

An [Akka.NET](https://getakka.net/) demonstration project showcasing advanced streaming patterns using Akka Streams and the Actor model. This project implements a "stream of streams" architecture where each entity produces its own stream of data points that are processed in parallel.

## Overview

This application demonstrates how to:
- Process multiple entity streams concurrently using Akka.NET actors and Akka Streams
- Use `SourceRef` to create distributed streams of data points
- Perform real-time statistical computations on streaming data
- Write results to multiple storage backends concurrently
- Integrate Akka actors with Akka Streams for complex data processing pipelines

## Architecture

The application consists of several key actors that work together to process entity data:

### Core Actors

- **DataPointActor** - Generates entity metadata and creates `SourceRef<DataPoint>` streams for each entity
- **ProcessorActor** - Main orchestrator that processes entities, consumes data streams, performs statistical calculations, and coordinates storage operations
- **DynamoDbWriterActor** - Simulates writing computation results to AWS DynamoDB
- **S3WriterActor** - Simulates writing computation results to AWS S3

### Data Flow

1. **Entity Processing**: The application processes 200 entities (Entity-10 through Entity-209)
2. **Stream Creation**: Each entity gets its own stream of 5-50 randomly generated data points
3. **Parallel Processing**: Multiple entity streams are processed concurrently with configurable parallelism
4. **Statistical Computation**: For each entity stream, the system calculates:
   - Average of data point values
   - Standard deviation
   - Blended average incorporating previous historical data
5. **Concurrent Storage**: Results are written simultaneously to both DynamoDB and S3
6. **Backpressure Management**: Akka Streams handles flow control and backpressure automatically

## Key Features

### Stream of Streams Pattern
Demonstrates how to manage multiple independent data streams where each entity produces its own stream of data points that are processed independently but orchestrated centrally.

### Parallelism Control
```csharp
private const int MaxDataPointsParallelism = 5;
private const int MaxSubStreamParallelism = 5;
```

### SourceRef Integration
Uses Akka Streams `SourceRef` to create materialized stream references that can be passed between actors:

```csharp
var sourceRef = await Source.From(dataPoints)
    .RunWith(streamRef, _materializer);
```

### Statistical Processing
Performs streaming aggregations using Akka Streams operators:
- `Aggregate` for accumulating statistics
- `Select` for transforming data
- `IdleTimeout` for handling processing timeouts

### Concurrent Storage Operations
Writes results to multiple storage systems concurrently using `Task.WhenAll`:

```csharp
var writeToDynamo = _dynamoDbWriterActor.ActorRef.Ask<IWriteResult>(...);
var writeToS3 = _s3WriterActor.ActorRef.Ask<IWriteResult>(...);
var results = await Task.WhenAll(writeToDynamo, writeToS3);
```

## Technology Stack

- **.NET 9.0** - Target framework
- **Akka.NET** - Actor framework and streaming library
- **Akka.Hosting** - Dependency injection integration
- **Microsoft.Extensions.Hosting** - Host builder and application lifetime management

## Getting Started

### Prerequisites
- .NET 9.0 SDK or later

### Running the Application

1. Clone the repository
2. Navigate to the project directory
3. Build the project:
   ```bash
   dotnet build
   ```
4. Run the application:
   ```bash
   dotnet run --project src/StreamOfStreams
   ```

The application will:y
1. Process 200 entities in parallel
2. Generate random data points for each entity
3. Perform statistical calculations on each entity's data stream
4. Write results to simulated DynamoDB and S3 storage
5. Log progress and completion status

### Expected Output

The application will log information about:
- Entity processing start and completion
- Data point generation for each entity
- Statistical computation results
- Storage operation success/failure
- Overall processing completion

## Use Cases

This pattern is useful for scenarios such as:
- **IoT Data Processing** - Processing sensor data streams from multiple devices
- **Financial Analytics** - Real-time analysis of trading data from multiple instruments
- **Log Processing** - Analyzing log streams from multiple services or applications
- **Machine Learning Pipelines** - Processing feature streams for multiple models
- **Event Sourcing** - Processing event streams from multiple aggregates

## Learning Objectives

This project demonstrates:
- Advanced Akka Streams patterns and operators
- Integration between Akka actors and Akka Streams
- Backpressure handling in streaming applications
- Concurrent processing with parallelism controls
- Actor-based dependency injection using Akka.Hosting
- Error handling and timeout management in streaming pipelines
