using JsonPipeline;

namespace JsonPipeline.Tests;

public sealed class CompatibilityTests
{
    [Fact]
    public void NormalizeJsonProducesStablePropertyOrderForSimpleInput()
    {
        Assert.Equal("{\"value\":1}", PipelineEngine.NormalizeJson("{\"value\":1}"));
    }
}
