using System.Text;
using System.Text.Json;
using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class CheckpointTests : IDisposable
{
    private readonly string _directory;

    public CheckpointTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "jp-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try { Directory.Delete(_directory, recursive: true); } catch (IOException) { }
    }

    private string WriteFile(string name, string contents)
    {
        string path = Path.Combine(_directory, name);
        File.WriteAllText(path, contents);
        return path;
    }

    private const string ConfigJson = """
    {
      "sequenceField": "seq",
      "operations": [
        { "op": "validate", "field": "name", "type": "string" },
        { "op": "filter", "field": "keep", "operator": "eq", "value": true },
        { "op": "project", "fields": ["seq", "name"] },
        { "op": "rename", "from": "name", "to": "who" }
      ],
      "limits": { "maxLineBytes": 4096, "maxInFlight": 3, "maxOutputBytes": 1048576 }
    }
    """;

    private string PrepareInput(int records = 20)
    {
        StringBuilder builder = new();
        for (int i = 1; i <= records; i++)
        {
            bool keep = i % 3 != 0;
            builder.AppendLine($"{{\"seq\":{i},\"name\":\"n{i}\",\"keep\":{keep.ToString().ToLowerInvariant()}}}");
        }
        return WriteFile("input.jsonl", builder.ToString());
    }

    private PipelineRunOptions Options(string input, string config, long? crashAfter = null, bool resume = false) =>
        new(input, Path.Combine(_directory, "output.jsonl"), config, Path.Combine(_directory, "checkpoint.json"), 4, resume)
        {
            CrashAfterSequence = crashAfter,
        };

    [Fact]
    public async Task SuccessfulRunAtomicallyCommitsAndMarksComplete()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        string output = Path.Combine(_directory, "output.jsonl");
        string checkpoint = Path.Combine(_directory, "checkpoint.json");

        PipelineRunResult result = await FileRunner.RunAsync(Options(input, config));

        Assert.True(result.Completed);
        Assert.False(result.Resumed);
        Assert.Equal(20, result.InputRecords);
        Assert.True(File.Exists(output));
        Assert.False(File.Exists(output + ".tmp"));

        using JsonDocument state = JsonDocument.Parse(File.ReadAllText(checkpoint));
        Assert.Equal("complete", state.RootElement.GetProperty("status").GetString());
        Assert.Equal(20, state.RootElement.GetProperty("committedSequence").GetInt64());
        Assert.Equal(result.OutputBytes, state.RootElement.GetProperty("stats").GetProperty("outputBytes").GetInt64());
    }

    [Fact]
    public async Task ResumeContinuesAfterCrashAndProducesIdenticalBytes()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        string output = Path.Combine(_directory, "output.jsonl");
        string checkpoint = Path.Combine(_directory, "checkpoint.json");

        OperationCanceledException crash = await Assert.ThrowsAsync<OperationCanceledException>(
            () => FileRunner.RunAsync(Options(input, config, crashAfter: 7)));
        Assert.Contains("simulated crash", crash.Message);
        Assert.False(File.Exists(output));
        Assert.True(File.Exists(output + ".tmp"));

        using (JsonDocument state = JsonDocument.Parse(File.ReadAllText(checkpoint)))
        {
            Assert.Equal("running", state.RootElement.GetProperty("status").GetString());
            Assert.Equal(7, state.RootElement.GetProperty("committedSequence").GetInt64());
        }

        byte[] tempBeforeResume = await File.ReadAllBytesAsync(output + ".tmp");
        PipelineRunResult resumed = await FileRunner.RunAsync(Options(input, config, resume: true));
        Assert.True(resumed.Completed);
        Assert.True(resumed.Resumed);
        Assert.Equal(20, resumed.InputRecords);

        string input2 = PrepareInput();
        string config2 = WriteFile("config2.json", ConfigJson);
        string dir2 = Path.Combine(_directory, "clean");
        Directory.CreateDirectory(dir2);
        PipelineRunResult clean = await FileRunner.RunAsync(new PipelineRunOptions(
            input2, Path.Combine(dir2, "output.jsonl"), config2, Path.Combine(dir2, "checkpoint.json"), 4, false));

        byte[] resumedBytes = await File.ReadAllBytesAsync(output);
        byte[] cleanBytes = await File.ReadAllBytesAsync(Path.Combine(dir2, "output.jsonl"));
        Assert.Equal(cleanBytes, resumedBytes);
        Assert.Equal(clean.OutputBytes, resumed.OutputBytes);
        Assert.Equal(clean.CommittedRecords, resumed.CommittedRecords);
        Assert.Equal(clean.FilteredRecords, resumed.FilteredRecords);

        string[] lines = Encoding.UTF8.GetString(resumedBytes).TrimEnd('\n').Split('\n');
        Assert.DoesNotContain(lines, line => line.Length == 0);
        Assert.Equal((int)clean.CommittedRecords, lines.Length);
        bool isPrefix = resumedBytes.Length >= tempBeforeResume.Length
            && resumedBytes.AsSpan(0, tempBeforeResume.Length).SequenceEqual(tempBeforeResume);
        Assert.True(isPrefix);
    }

    [Fact]
    public async Task ResumeFailsOnInputHashMismatch()
    {
        string config = WriteFile("config.json", ConfigJson);
        string input = PrepareInput();
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => FileRunner.RunAsync(Options(input, config, crashAfter: 5)));

        File.WriteAllText(input, File.ReadAllText(input) + "{\"seq\":21,\"name\":\"x\",\"keep\":true}\n");

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => FileRunner.RunAsync(Options(input, config, resume: true)));
        Assert.Equal(PipelineErrorCode.InputHashMismatch, ex.Code);
        Assert.Equal("INPUT_HASH_MISMATCH", ex.StableCode);
    }

    [Fact]
    public async Task ResumeFailsOnConfigHashMismatch()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => FileRunner.RunAsync(Options(input, config, crashAfter: 5)));

        string changedConfig = ConfigJson.Replace("[\"seq\", \"name\"]", "[\"seq\", \"name\", \"keep\"]");
        File.WriteAllText(config, changedConfig);

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => FileRunner.RunAsync(Options(input, config, resume: true)));
        Assert.Equal(PipelineErrorCode.ConfigHashMismatch, ex.Code);
    }

    [Fact]
    public async Task ResumeFailsWhenTempOutputTampered()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        string output = Path.Combine(_directory, "output.jsonl");
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => FileRunner.RunAsync(Options(input, config, crashAfter: 6)));

        string temp = output + ".tmp";
        byte[] bytes = await File.ReadAllBytesAsync(temp);
        bytes[^1] = bytes[^1] == (byte)'\n' ? (byte)'X' : bytes[^1];
        await File.WriteAllBytesAsync(temp + ".alter", bytes);
        File.Delete(temp);
        File.Move(temp + ".alter", temp);

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => FileRunner.RunAsync(Options(input, config, resume: true)));
        Assert.Equal(PipelineErrorCode.TempOutputTampered, ex.Code);
        Assert.Equal("TEMP_OUTPUT_TAMPERED", ex.StableCode);
    }

    [Fact]
    public async Task FreshRunRefusesToOverwriteExistingCheckpoint()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        await FileRunner.RunAsync(Options(input, config));

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => FileRunner.RunAsync(Options(input, config)));
        Assert.Equal(PipelineErrorCode.InvalidResumeState, ex.Code);
    }

    [Fact]
    public async Task ResumeOfCompletedCheckpointFails()
    {
        string input = PrepareInput();
        string config = WriteFile("config.json", ConfigJson);
        await FileRunner.RunAsync(Options(input, config));

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(
            () => FileRunner.RunAsync(Options(input, config, resume: true)));
        Assert.Equal(PipelineErrorCode.InvalidResumeState, ex.Code);
    }


    [Fact]
    public async Task ResumeTruncatesTrailingBytesBeyondCheckpoint()
    {
        string input = PrepareInput(10);
        string config = WriteFile("config.json", ConfigJson);
        string output = Path.Combine(_directory, "output.jsonl");

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => FileRunner.RunAsync(Options(input, config, crashAfter: 4)));

        string temp = output + ".tmp";
        byte[] before = await File.ReadAllBytesAsync(temp);
        await File.WriteAllBytesAsync(temp + ".x", before.Concat(Encoding.UTF8.GetBytes("{\"seq\":99}\n")).ToArray());
        File.Delete(temp);
        File.Move(temp + ".x", temp);

        PipelineRunResult result = await FileRunner.RunAsync(Options(input, config, resume: true));
        Assert.True(result.Completed);
        Assert.True(result.Resumed);
        Assert.Equal(10, result.InputRecords);
        string[] lines = (await File.ReadAllLinesAsync(output));
        Assert.DoesNotContain(lines, line => line.Contains("99"));
    }
}
