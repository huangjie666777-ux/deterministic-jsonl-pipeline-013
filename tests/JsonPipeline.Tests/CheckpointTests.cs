using System.Text;
using System.Text.Json;
using JsonPipeline;
using Xunit;

namespace JsonPipeline.Tests;

public sealed class CheckpointTests
{
    private readonly string _dir = TestHarness.CreateWorkDir();

    private static string SimpleConfig() => TestHarness.ConfigFor(new[]
    {
        """{ "op": "project", "fields": ["id", "n"] }""",
    });

    [Fact]
    public async Task ResumeFailsWhenInputHashChanges()
    {
        string configPath = TestHarness.WriteFile(_dir, "config.json", SimpleConfig());
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(3));
        string output = Path.Combine(_dir, "out.jsonl");
        string checkpoint = Path.Combine(_dir, "cp.json");

        PipelineEngine engine = new();
        await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4);

        TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(4));
        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4, resume: true));
        Assert.Equal(PipelineErrorCodes.CheckpointInputMismatch, ex.Code);
    }

    [Fact]
    public async Task ResumeFailsWhenConfigHashChanges()
    {
        string configPath = TestHarness.WriteFile(_dir, "config.json", SimpleConfig());
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(3));
        string output = Path.Combine(_dir, "out.jsonl");
        string checkpoint = Path.Combine(_dir, "cp.json");

        PipelineEngine engine = new();
        await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4);

        string changedConfig = TestHarness.ConfigFor(new[]
        {
            """{ "op": "project", "fields": ["id"] }""",
        });
        TestHarness.WriteFile(_dir, "config.json", changedConfig);

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4, resume: true));
        Assert.Equal(PipelineErrorCodes.CheckpointConfigMismatch, ex.Code);
    }

    [Fact]
    public async Task NewRunRefusesToOverwriteExistingCheckpoint()
    {
        string configPath = TestHarness.WriteFile(_dir, "config.json", SimpleConfig());
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(2));
        string output = Path.Combine(_dir, "out.jsonl");
        string checkpoint = Path.Combine(_dir, "cp.json");

        PipelineEngine engine = new();
        await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4);

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4));
        Assert.Equal(PipelineErrorCodes.CheckpointExists, ex.Code);
    }

    [Fact]
    public async Task CompletedResumeIsIdempotentAndReturnsSameStats()
    {
        string configPath = TestHarness.WriteFile(_dir, "config.json", SimpleConfig());
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(3));
        string output = Path.Combine(_dir, "out.jsonl");
        string checkpoint = Path.Combine(_dir, "cp.json");

        PipelineEngine engine = new();
        PipelineResult first = await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4);
        DateTime mtime = File.GetLastWriteTimeUtc(output);
        await Task.Delay(20);

        PipelineResult again = await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4, resume: true);
        Assert.Equal(first.Records, again.Records);
        Assert.Equal(first.OutputBytes, again.OutputBytes);
        Assert.Equal(mtime, File.GetLastWriteTimeUtc(output));
    }

    [Fact]
    public async Task ResumeDetectsTamperedTempOutput()
    {
        string config = SimpleConfig();
        string configPath = TestHarness.WriteFile(_dir, "config.json", config);
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(4));
        string output = Path.Combine(_dir, "out.jsonl");
        string checkpoint = Path.Combine(_dir, "cp.json");

        PipelineEngine engine = new();
        await engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4);

        JsonDocument doc = JsonDocument.Parse(await File.ReadAllTextAsync(checkpoint));
        string tempPath = doc.RootElement.GetProperty("tempOutputPath").GetString()!;
        await File.AppendAllTextAsync(tempPath, "{\"id\":99,\"n\":99}\n");

        File.Delete(output);
        string checkpointText = await File.ReadAllTextAsync(checkpoint);
        File.WriteAllText(checkpoint, checkpointText.Replace("\"completed\": true", "\"completed\": false"));

        PipelineException ex = await Assert.ThrowsAsync<PipelineException>(() =>
            engine.RunFileAsync(inputPath, output, configPath, checkpoint, 4, resume: true));
        Assert.Equal(PipelineErrorCodes.CheckpointTempTampered, ex.Code);
    }

    [Fact]
    public async Task InterruptedRunResumesAndProducesSameOutputAsUninterruptedRun()
    {
        string config = SimpleConfig();
        string input = TestHarness.InputFor(20);

        string interruptedInput = TestHarness.WriteFile(_dir, "interrupted-input.jsonl", input);
        string interruptedConfig = TestHarness.WriteFile(_dir, "interrupted-config.json", config);
        string interruptedOutput = Path.Combine(_dir, "interrupted-out.jsonl");
        string interruptedCheckpoint = Path.Combine(_dir, "interrupted-cp.json");

        string prefixInput = TestHarness.WriteFile(_dir, "prefix-input.jsonl", TestHarness.InputFor(7));
        string prefixConfig = TestHarness.WriteFile(_dir, "prefix-config.json", config);
        string prefixOutput = Path.Combine(_dir, "prefix-out.jsonl");
        string prefixCheckpoint = Path.Combine(_dir, "prefix-cp.json");

        PipelineEngine engine = new();
        await engine.RunFileAsync(prefixInput, prefixOutput, prefixConfig, prefixCheckpoint, 2);

        CheckpointFile checkpointData = CheckpointStore.Read(prefixCheckpoint);
        string tempPath = checkpointData.TempOutputPath;

        CopyCheckpointState(prefixCheckpoint, interruptedCheckpoint, interruptedInput, interruptedConfig, tempPath);
        string interruptedTemp = Path.Combine(_dir, ".interrupted-out.jsonl.tmp");
        File.Copy(tempPath, interruptedTemp, overwrite: true);
        PatchTempPath(interruptedCheckpoint, interruptedTemp);

        PipelineResult resumed = await engine.RunFileAsync(
            interruptedInput, interruptedOutput, interruptedConfig, interruptedCheckpoint, 4, resume: true);

        string cleanOutput = Path.Combine(_dir, "clean-out.jsonl");
        string cleanCheckpoint = Path.Combine(_dir, "clean-cp.json");
        PipelineResult clean = await engine.RunFileAsync(
            TestHarness.WriteFile(_dir, "clean-input.jsonl", input),
            cleanOutput,
            TestHarness.WriteFile(_dir, "clean-config.json", config),
            cleanCheckpoint, 8);

        Assert.Equal(await File.ReadAllBytesAsync(cleanOutput), await File.ReadAllBytesAsync(interruptedOutput));
        Assert.Equal(clean.Records, resumed.Records);
        Assert.Equal(clean.OutputBytes, resumed.OutputBytes);
    }

    [Fact]
    public async Task AtomicCommitLeavesNoHalfRecordsAndCheckpointIsComplete()
    {
        string configPath = TestHarness.WriteFile(_dir, "config.json", SimpleConfig());
        string inputPath = TestHarness.WriteFile(_dir, "input.jsonl", TestHarness.InputFor(5));
        string output = Path.Combine(_dir, "atomic-out.jsonl");
        string checkpoint = Path.Combine(_dir, "atomic-cp.json");

        PipelineResult result = await new PipelineEngine().RunFileAsync(inputPath, output, configPath, checkpoint, 4);
        string[] lines = await File.ReadAllLinesAsync(output);
        Assert.Equal(5, lines.Length);
        Assert.All(lines, line => Assert.EndsWith("}", line));

        CheckpointFile data = CheckpointStore.Read(checkpoint);
        Assert.True(data.Completed);
        Assert.Equal(5, data.Stats.CommittedRecords);
        Assert.Equal(result.OutputBytes, data.Stats.OutputBytes);
        Assert.Equal(4, data.CommittedSequence);
        Assert.False(File.Exists(output + ".commit-tmp"));
    }

    private static void CopyCheckpointState(string source, string destination, string inputPath, string configPath, string tempPath)
    {
        string inputHash = HashOf.Sha256HexOfFile(inputPath);
        string configHash = HashOf.Sha256Hex(File.ReadAllBytes(configPath));
        string tempHash = HashOf.Sha256HexOfFile(tempPath);
        CheckpointFile sourceData = CheckpointStore.Read(source);

        CheckpointFile copy = new()
        {
            InputSha256 = inputHash,
            ConfigSha256 = configHash,
            CommittedSequence = sourceData.CommittedSequence,
            TempOutputPath = tempPath,
            TempOutputSha256 = tempHash,
            Stats = sourceData.Stats,
        };

        CheckpointStore.WriteAtomic(destination, copy);
    }

    private static void PatchTempPath(string checkpointPath, string tempPath)
    {
        CheckpointFile data = CheckpointStore.Read(checkpointPath);
        data.TempOutputPath = tempPath;
        CheckpointStore.WriteAtomic(checkpointPath, data);
    }

}
