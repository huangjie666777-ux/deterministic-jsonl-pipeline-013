using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class CompatibilityTests
{
    [Fact]
    public void NormalizeJsonProducesStablePropertyOrderForSimpleInput()
    {
        Assert.Equal("{\"value\":1}", PipelineEngine.NormalizeJson("{\"value\":1}"));
    }

    [Fact]
    public async Task ConcurrentWorkersProduceOrderedDeterministicOutput()
    {
        PipelineConfig config = PipelineConfig.Parse("{}"u8.ToArray());
        string input = string.Join(
            '\n',
            Enumerable.Range(0, 200).Select(i => $"{{\"seq\":{i},\"n\":{i}}}")) + "\n";
        byte[] inputBytes = System.Text.Encoding.UTF8.GetBytes(input);

        byte[] expected;
        using (MemoryStream inputOne = new(inputBytes))
        using (MemoryStream outputOne = new())
        {
            PipelineResult result = await new PipelineEngine(config, 1).RunAsync(inputOne, outputOne);
            expected = outputOne.ToArray();
            Assert.Equal(200, result.Records);
        }

        foreach (int workers in new[] { 1, 2, 4, 8, 16 })
        {
            using MemoryStream inputStream = new(inputBytes);
            using MemoryStream outputStream = new();
            PipelineResult result = await new PipelineEngine(config, workers)
                .RunAsync(inputStream, outputStream);
            Assert.Equal(expected, outputStream.ToArray());
            Assert.Equal(200, result.Records);
        }

        string[] lines = System.Text.Encoding.UTF8.GetString(expected).TrimEnd('\n').Split('\n');
        for (int index = 0; index < lines.Length; index++)
        {
            Assert.Equal($"{{\"seq\":{index},\"n\":{index}}}", lines[index]);
        }
    }

    [Fact]
    public async Task ProjectionRenameAndFilterAreAppliedInOrder()
    {
        const string configJson = """
        {
          "operations": [
            { "op": "validate", "field": "age", "type": "integer" },
            { "op": "filter", "field": "age", "operator": "ge", "value": 18 },
            { "op": "project", "fields": ["seq", "name", "age"] },
            { "op": "rename", "from": "name", "to": "customer" }
          ]
        }
        """;
        PipelineConfig config = PipelineConfig.Parse(System.Text.Encoding.UTF8.GetBytes(configJson));

        const string input = """
        {"seq":1,"name":"Alice","age":30,"extra":1}
        {"seq":2,"name":"Kid","age":10,"extra":2}
        {"seq":3,"name":"Bob","age":18,"extra":3}
        """;

        using MemoryStream inputStream = new(System.Text.Encoding.UTF8.GetBytes(input + "\n"));
        using MemoryStream outputStream = new();
        PipelineResult result = await new PipelineEngine(config, 4).RunAsync(inputStream, outputStream);

        const string expected = """
        {"seq":1,"customer":"Alice","age":30}
        {"seq":3,"customer":"Bob","age":18}
        """;
        Assert.Equal(expected + "\n", System.Text.Encoding.UTF8.GetString(outputStream.ToArray()));
        Assert.Equal(2, result.Records);
    }
}
