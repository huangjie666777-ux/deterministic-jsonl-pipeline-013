using System.Text;
using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class EngineTests
{
    private readonly string _dir = TestHarness.CreateWorkDir();

    private Task<PipelineResult> RunAsync(
        string input,
        string config,
        int workers = 4,
        string? outputPath = null,
        string? checkpointPath = null,
        bool resume = false)
    {
        string inputPath = TestHarness.WriteFile(_dir, $"input-{Guid.NewGuid():N}.jsonl", input);
        string configPath = TestHarness.WriteFile(_dir, $"config-{Guid.NewGuid():N}.json", config);
        outputPath ??= Path.Combine(_dir, $"output-{Guid.NewGuid():N}.jsonl");
        checkpointPath ??= Path.Combine(_dir, $"checkpoint-{Guid.NewGuid():N}.json");

        return new PipelineEngine().RunFileAsync(inputPath, outputPath, configPath, checkpointPath, workers, resume);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(32)]
    public async Task OutputIsIdenticalRegardlessOfWorkerCount(int workers)
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "validate", "field": "n", "type": "number" }""",
            """{ "op": "project", "fields": ["id", "n"] }""",
        });

        string input = TestHarness.InputFor(200);
        string output = Path.Combine(_dir, $"out-{workers}.jsonl");
        await RunAsync(input, config, workers, output, Path.Combine(_dir, $"cp-{workers}.json"));

        string expected = string.Concat(Enumerable.Range(0, 200).Select(i => $"{{\"id\":{i},\"n\":{i}}}\n"));
        Assert.Equal(expected, await File.ReadAllTextAsync(output));
    }

    [Fact]
    public async Task ProjectRenameAndFilterAreAppliedInOrder()
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "validate", "field": "n", "type": "number" }""",
            """{ "op": "filter", "field": "n", "min": 1, "max": 2 }""",
            """{ "op": "project", "fields": ["id", "n", "k"] }""",
            """{ "op": "rename", "field": "k", "to": "kind" }""",
        });

        string output = Path.Combine(_dir, "transform.jsonl");
        await RunAsync(TestHarness.InputFor(4), config, 4, output, Path.Combine(_dir, "transform-cp.json"));

        string[] lines = await File.ReadAllLinesAsync(output);
        Assert.Equal(
        [
            "{\"id\":1,\"kind\":\"v\",\"n\":1}",
            "{\"id\":2,\"kind\":\"v\",\"n\":2}",
        ], lines);
    }

    [Fact]
    public async Task ValidationFailureHasStableCodeAndMessage()
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "validate", "field": "n", "type": "number", "min": 10 }""",
        });

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0,\"n\":1}\n", config));
        Assert.Equal(PipelineErrorCodes.ValidationFailed, ex.Code);
        Assert.Contains("line 1", ex.Message);
    }

    [Fact]
    public async Task TypeErrorDuringFilterFailsDeterministically()
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "filter", "field": "n", "min": 0 }""",
        });

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0,\"n\":\"x\"}\n", config));
        Assert.Equal(PipelineErrorCodes.ValidationFailed, ex.Code);
    }

    [Fact]
    public async Task EmptyLineInvalidJsonDuplicateAndGappedSequencesAreRejected()
    {
        string config = TestHarness.ConfigFor(Array.Empty<string>());

        PipelineException empty = await Assert.ThrowsAsync<PipelineException>(() => RunAsync("\n", config));
        Assert.Equal(PipelineErrorCodes.EmptyLine, empty.Code);

        PipelineException invalid = await Assert.ThrowsAsync<PipelineException>(() => RunAsync("{not json}\n", config));
        Assert.Equal(PipelineErrorCodes.InvalidJson, invalid.Code);

        PipelineException duplicate = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0}\n{\"id\":0}\n", config));
        Assert.Equal(PipelineErrorCodes.DuplicateSequence, duplicate.Code);

        PipelineException gap = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0}\n{\"id\":2}\n", config));
        Assert.Equal(PipelineErrorCodes.InvalidSequence, gap.Code);
    }

    [Fact]
    public async Task UnknownOperationRejectedWithStableCode()
    {
        string config = """
{
  "sequenceField": "id",
  "maxInputLineBytes": 65536,
  "maxInFlight": 8,
  "maxOutputBytes": 1024,
  "maxOperations": 8,
  "operations": [ { "op": "explode" } ]
}
""";

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0}\n", config));
        Assert.Equal(PipelineErrorCodes.UnknownOperation, ex.Code);
    }

    [Fact]
    public async Task LimitsForLineBytesInFlightConfigAndOutputAreEnforced()
    {
        string lineLimitConfig = """
{
  "sequenceField": "id",
  "maxInputLineBytes": 8,
  "maxInFlight": 8,
  "maxOutputBytes": 1024,
  "maxOperations": 8,
  "operations": []
}
""";
        PipelineException lineEx = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0,\"x\":1234567890}\n", lineLimitConfig));
        Assert.Equal(PipelineErrorCodes.InputLineLimitExceeded, lineEx.Code);

        string outputLimitConfig = TestHarness.ConfigFor(Array.Empty<string>(), maxOutputBytes: 20);
        PipelineException outputEx = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync(TestHarness.InputFor(5), outputLimitConfig));
        Assert.Equal(PipelineErrorCodes.OutputLimitExceeded, outputEx.Code);

        string overCapConfig = """
{
  "sequenceField": "",
  "maxInputLineBytes": 8,
  "maxInFlight": 8,
  "maxOutputBytes": 1024,
  "maxOperations": 8,
  "operations": []
}
""";
        PipelineException configEx = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0}\n", overCapConfig));
        Assert.Equal(PipelineErrorCodes.InvalidConfig, configEx.Code);

        string overHardCapConfig = """
{
  "sequenceField": "id",
  "maxInputLineBytes": 8,
  "maxInFlight": 8,
  "maxOutputBytes": 1024,
  "maxOperations": 50000,
  "operations": []
}
""";
        PipelineException hardCapEx = await Assert.ThrowsAsync<PipelineException>(() =>
            RunAsync("{\"id\":0}\n", overHardCapConfig));
        Assert.Equal(PipelineErrorCodes.ConfigLimitExceeded, hardCapEx.Code);
    }

    [Fact]
    public async Task InFlightBoundIsHonoredWithSmallChannel()
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "validate", "field": "n", "type": "number" }""",
        }, maxInFlight: 2);

        string output = Path.Combine(_dir, "small-channel.jsonl");
        await RunAsync(TestHarness.InputFor(100), config, 16, output, Path.Combine(_dir, "small-channel-cp.json"));
        Assert.Equal(100, (await File.ReadAllLinesAsync(output)).Length);
    }

    [Fact]
    public async Task RepeatedRunsProduceIdenticalBytesAndStats()
    {
        string config = TestHarness.ConfigFor(new[]
        {
            """{ "op": "project", "fields": ["id", "n"] }""",
        });

        string input = TestHarness.InputFor(50);
        string inputPath = TestHarness.WriteFile(_dir, "stable-input.jsonl", input);
        string configPath = TestHarness.WriteFile(_dir, "stable-config.json", config);

        byte[] first;
        PipelineResult firstStats;
        await using (FileStream stream = File.OpenRead(inputPath))
        {
        }

        PipelineEngine engine = new();
        string out1 = Path.Combine(_dir, "stable-1.jsonl");
        string cp1 = Path.Combine(_dir, "stable-cp-1.json");
        firstStats = await engine.RunFileAsync(inputPath, out1, configPath, cp1, 8);
        first = await File.ReadAllBytesAsync(out1);

        string out2 = Path.Combine(_dir, "stable-2.jsonl");
        string cp2 = Path.Combine(_dir, "stable-cp-2.json");
        PipelineResult secondStats = await engine.RunFileAsync(inputPath, out2, configPath, cp2, 1);

        Assert.Equal(first, await File.ReadAllBytesAsync(out2));
        Assert.Equal(firstStats.Records, secondStats.Records);
        Assert.Equal(firstStats.OutputBytes, secondStats.OutputBytes);
    }
}
