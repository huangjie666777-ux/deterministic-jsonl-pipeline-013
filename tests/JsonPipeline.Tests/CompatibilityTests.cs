using System.Text;
using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class CompatibilityTests
{
    [Fact]
    public void NormalizeJsonProducesStablePropertyOrderForSimpleInput()
    {
        Assert.Equal("{\"value\":1}", PipelineEngine.NormalizeJson("{ \"value\": 1 }"));
    }

    [Fact]
    public void NormalizeJsonSortsNestedPropertiesAndEscapesDeterministically()
    {
        Assert.Equal(
            "{\"a\":{\"x\":1,\"y\":2},\"b\":[3,4],\"c\":\"héllo\"}",
            PipelineEngine.NormalizeJson("{\"c\":\"héllo\",\"b\":[3,4],\"a\":{\"y\":2,\"x\":1}}"));
    }
}
