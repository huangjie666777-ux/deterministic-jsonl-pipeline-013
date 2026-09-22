using System.Text;
using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class ErrorHandlingTests
{
    private static async Task<PipelineException> RunSingleAsync(string line, string configJson = "{}", int workers = 2)
    {
        PipelineConfig config = PipelineConfig.Parse(Encoding.UTF8.GetBytes(configJson));
        using MemoryStream input = new(Encoding.UTF8.GetBytes(line.EndsWith('\n') ? line : line + "\n"));
        using MemoryStream output = new();
        PipelineException exception = await Assert.ThrowsAsync<PipelineException>(
            () => new PipelineEngine(config, workers).RunAsync(input, output));
        Assert.Empty(output.ToArray());
        return exception;
    }

    [Fact]
    public async Task RejectsEmptyLineWithStableCode()
    {
        PipelineException ex = await RunSingleAsync("\n");
        Assert.Equal(PipelineErrorCode.EmptyLine, ex.Code);
        Assert.Equal("EMPTY_LINE", ex.StableCode);
    }

    [Fact]
    public async Task RejectsInvalidJsonWithStableCode()
    {
        PipelineException ex = await RunSingleAsync("{not json");
        Assert.Equal(PipelineErrorCode.InvalidJson, ex.Code);
        Assert.Equal("INVALID_JSON", ex.StableCode);
    }

    [Fact]
    public async Task RejectsMissingSequence()
    {
        PipelineException ex = await RunSingleAsync("{\"id\":1}");
        Assert.Equal(PipelineErrorCode.InvalidSequence, ex.Code);
    }

    [Fact]
    public async Task RejectsDuplicateAndOutOfOrderSequence()
    {
        PipelineConfig config = PipelineConfig.Parse("{}"u8.ToArray());
        byte[] input = Encoding.UTF8.GetBytes("{\"seq\":1}\n{\"seq\":1}\n");
        using MemoryStream inputStream = new(input);
        using MemoryStream outputStream = new();
        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => new PipelineEngine(config, 4).RunAsync(inputStream, outputStream));
        Assert.Equal(PipelineErrorCode.DuplicateSequence, ex.Code);
        Assert.Equal(2, ex.LineNumber);
    }

    [Fact]
    public void RejectsUnknownOperation()
    {
        PipelineException ex = Assert.Throws<PipelineException>(() => PipelineConfig.Parse(Encoding.UTF8.GetBytes(
            "{\"operations\":[{\"op\":\"teleport\"}]}")));
        Assert.Equal(PipelineErrorCode.UnknownOperation, ex.Code);
        Assert.Equal("UNKNOWN_OPERATION", ex.StableCode);
    }

    [Fact]
    public async Task RejectsTypeMismatchInValidation()
    {
        const string config = "{\"operations\":[{\"op\":\"validate\",\"field\":\"age\",\"type\":\"integer\"}]}";
        PipelineException ex = await RunSingleAsync("{\"seq\":1,\"age\":\"old\"}", config);
        Assert.Equal(PipelineErrorCode.TypeMismatch, ex.Code);
        Assert.Equal("TYPE_MISMATCH", ex.StableCode);
    }

    [Fact]
    public async Task RejectsMissingRequiredField()
    {
        const string config = "{\"operations\":[{\"op\":\"validate\",\"field\":\"id\",\"type\":\"string\"}]}";
        PipelineException ex = await RunSingleAsync("{\"seq\":1}", config);
        Assert.Equal(PipelineErrorCode.ValidationFailed, ex.Code);
        Assert.Equal("VALIDATION_FAILED", ex.StableCode);
    }

    [Fact]
    public async Task RejectsFilterTypeMismatch()
    {
        const string config = "{\"operations\":[{\"op\":\"filter\",\"field\":\"age\",\"operator\":\"gt\",\"value\":5}]}";
        PipelineException ex = await RunSingleAsync("{\"seq\":1,\"age\":\"x\"}", config);
        Assert.Equal(PipelineErrorCode.TypeMismatch, ex.Code);
    }

    [Fact]
    public async Task EnforcesMaxLineBytes()
    {
        const string config = "{\"limits\":{\"maxLineBytes\":8}}";
        PipelineException ex = await RunSingleAsync(
            "{\"seq\":1,\"pad\":\"" + new string('p', 64) + "\"}", config);
        Assert.Equal(PipelineErrorCode.LineTooLong, ex.Code);
        Assert.Equal("LINE_TOO_LONG", ex.StableCode);
    }

    [Fact]
    public async Task EnforcesMaxOutputBytes()
    {
        const string config = "{\"limits\":{\"maxOutputBytes\":5}}";
        PipelineException ex = await RunSingleAsync("{\"seq\":1}", config);
        Assert.Equal(PipelineErrorCode.OutputTooLarge, ex.Code);
        Assert.Equal("OUTPUT_TOO_LARGE", ex.StableCode);
    }

    [Fact]
    public void EnforcesMaxOperationsInConfig()
    {
        const string config = "{\"limits\":{\"maxOperations\":1},\"operations\":[{\"op\":\"filter\",\"field\":\"a\",\"operator\":\"eq\",\"value\":1},{\"op\":\"filter\",\"field\":\"b\",\"operator\":\"eq\",\"value\":2}]}";
        PipelineException ex = Assert.Throws<PipelineException>(() => PipelineConfig.Parse(Encoding.UTF8.GetBytes(config)));
        Assert.Equal(PipelineErrorCode.InvalidConfig, ex.Code);
    }

    [Fact]
    public async Task DoesNotDuplicateOrLoseLinesOnValidationFailure()
    {
        PipelineConfig config = PipelineConfig.Parse("{}"u8.ToArray());
        byte[] input = Encoding.UTF8.GetBytes("{\"seq\":1}\nnot-json\n{\"seq\":3}\n");
        using MemoryStream inputStream = new(input);
        using MemoryStream outputStream = new();
        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => new PipelineEngine(config, 8).RunAsync(inputStream, outputStream));
        Assert.Equal(2, ex.LineNumber);
        Assert.Equal("{\"seq\":1}\n", Encoding.UTF8.GetString(outputStream.ToArray()));
    }
}
