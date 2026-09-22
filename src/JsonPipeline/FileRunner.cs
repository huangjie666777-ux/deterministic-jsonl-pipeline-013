using System.Security.Cryptography;
using System.Text.Json;

namespace JsonPipeline;

public static class FileRunner
{
    public static async Task<PipelineRunResult> RunAsync(
        PipelineRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.InputPath))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "--input path is required");
        }
        if (string.IsNullOrWhiteSpace(options.OutputPath))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "--output path is required");
        }
        if (string.IsNullOrWhiteSpace(options.ConfigPath))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "--config path is required");
        }
        if (string.IsNullOrWhiteSpace(options.CheckpointPath))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "--checkpoint path is required");
        }

        byte[] configBytes = await ReadAllBytesAsync(options.ConfigPath, cancellationToken).ConfigureAwait(false);
        PipelineConfig config = PipelineConfig.Parse(configBytes);
            if (configBytes.LongLength > config.Limits.MaxConfigBytes)
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidConfig,
                $"config is {configBytes.Length} bytes, exceeding maxConfigBytes {config.Limits.MaxConfigBytes}");
        }
        string configHash = CheckpointStore.Sha256Hex(configBytes);

        byte[] inputBytes = await ReadAllBytesAsync(options.InputPath, cancellationToken).ConfigureAwait(false);
        string inputHash = CheckpointStore.Sha256Hex(inputBytes);

        string tempPath = options.OutputPath + ".tmp";
        ResumeContext? resume = null;
        bool resumed = false;
        byte[]? resumeTempBytes = null;

        if (options.Resume)
        {
            CheckpointState checkpoint = CheckpointStore.Read(options.CheckpointPath);
            if (checkpoint.Status == "complete")
            {
                throw new PipelineException(
                    PipelineErrorCode.InvalidResumeState,
                    $"checkpoint {options.CheckpointPath} already reports completion; remove it to start a new run");
            }

            if (!CryptographicOperations.FixedTimeEquals(
                    HexToBytes(checkpoint.InputSha256), HexToBytes(inputHash)))
            {
                throw new PipelineException(
                    PipelineErrorCode.InputHashMismatch,
                    $"input hash {inputHash} does not match checkpoint hash {checkpoint.InputSha256}");
            }
            if (!CryptographicOperations.FixedTimeEquals(
                    HexToBytes(checkpoint.ConfigSha256), HexToBytes(configHash)))
            {
                throw new PipelineException(
                    PipelineErrorCode.ConfigHashMismatch,
                    $"config hash {configHash} does not match checkpoint hash {checkpoint.ConfigSha256}");
            }

            tempPath = checkpoint.TempOutputPath;
            byte[] tempBytes;
            try
            {
                tempBytes = await ReadAllBytesAsync(tempPath, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                throw new PipelineException(
                    PipelineErrorCode.InvalidResumeState,
                    $"temp output referenced by checkpoint is missing: {tempPath}",
                    ex);
            }

            if (tempBytes.Length < checkpoint.TempOutputBytes)
            {
                throw new PipelineException(
                    PipelineErrorCode.TempOutputTampered,
                    $"temp output {tempPath} is shorter ({tempBytes.Length} bytes) than checkpoint {checkpoint.TempOutputBytes} bytes");
            }

            if (tempBytes.Length > checkpoint.TempOutputBytes)
            {
                tempBytes = tempBytes[..(int)checkpoint.TempOutputBytes];
            }

            string existingTempHash = CheckpointStore.Sha256Hex(tempBytes);
            if (!CryptographicOperations.FixedTimeEquals(HexToBytes(existingTempHash), HexToBytes(checkpoint.TempOutputSha256)))
            {
                throw new PipelineException(
                    PipelineErrorCode.TempOutputTampered,
                    $"temp output {tempPath} prefix does not match checkpoint hash {checkpoint.TempOutputSha256}, got {existingTempHash}");
            }

            resumeTempBytes = tempBytes;
            resume = new ResumeContext(checkpoint.CommittedSequence, checkpoint.Stats.Clone(), checkpoint.Stats.InputRecords);
            resumed = true;
        }
        else if (File.Exists(options.CheckpointPath))
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidResumeState,
                $"checkpoint {options.CheckpointPath} already exists; use --resume or remove it first");
        }

        EnsureParentDirectory(tempPath);
        EnsureParentDirectory(options.OutputPath);
        EnsureParentDirectory(options.CheckpointPath);

        if (resume is not null && resumeTempBytes is not null)
        {
            FileStream? truncateStream = null;
            try
            {
                truncateStream = new FileStream(tempPath, FileMode.Open, FileAccess.Write, FileShare.None);
                truncateStream.SetLength(resumeTempBytes.Length);
            }
            catch (IOException ex)
            {
                throw new PipelineException(PipelineErrorCode.IoError, $"cannot truncate temp output {tempPath}: {ex.Message}", ex);
            }
            finally
            {
                truncateStream?.Dispose();
            }
        }

        CheckpointState state = new()
        {
            Status = "running",
            InputSha256 = inputHash,
            ConfigSha256 = configHash,
            CommittedSequence = resume?.CommittedSequence ?? long.MinValue,
            TempOutputPath = Path.GetFullPath(tempPath),
            TempOutputSha256 = CheckpointStore.Sha256Hex(Array.Empty<byte>()),
            TempOutputBytes = 0,
            Stats = resume?.Stats.Clone() ?? new PipelineStats(),
        };

        using IncrementalSha tempHash = new();
        if (resumeTempBytes is not null)
        {
            tempHash.Append(resumeTempBytes);
        }
        await using (MemoryStream inputStream = new(inputBytes, writable: false))
        await using (FileStream tempStream = new(
                         tempPath,
                         resume is null ? FileMode.Create : FileMode.Append,
                         FileAccess.Write,
                         FileShare.None))
        {
            PipelineOrchestrator orchestrator = new(config, options.Workers);
            Func<WorkItem, PipelineStats, ValueTask> onCommit = (item, stats) =>
            {
                tempStream.Flush(flushToDisk: true);
                if (!item.Dropped)
                {
                    tempHash.Append(item.Payload!);
                }

                state.CommittedSequence = item.Sequence;
                state.Stats = stats.Clone();
                state.TempOutputBytes = stats.OutputBytes;
                state.TempOutputSha256 = tempHash.CurrentHashHex();
                CheckpointStore.WriteAtomic(options.CheckpointPath, state);
                if (options.CrashAfterSequence is long crashSequence && item.Sequence >= crashSequence)
                {
                    throw new OperationCanceledException(
                        $"simulated crash after committing sequence {item.Sequence}");
                }
                return ValueTask.CompletedTask;
            };

            PipelineStats finalStats = await orchestrator
                .RunAsync(inputStream, tempStream, resume, onCommit, cancellationToken)
                .ConfigureAwait(false);

            tempStream.Flush(flushToDisk: true);
            state.Stats = finalStats.Clone();
            state.TempOutputBytes = finalStats.OutputBytes;
            state.TempOutputSha256 = tempHash.CurrentHashHex();
            CheckpointStore.WriteAtomic(options.CheckpointPath, state);
        }

        File.Move(tempPath, options.OutputPath, overwrite: true);

        state.Status = "complete";
        CheckpointStore.WriteAtomic(options.CheckpointPath, state);

        return new PipelineRunResult(
            state.Stats.InputRecords,
            state.Stats.CommittedRecords,
            state.Stats.FilteredRecords,
            state.Stats.OutputBytes,
            resumed,
            Completed: true);
    }

    private static void EnsureParentDirectory(string path)
    {
        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private static async Task<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            return await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            throw new PipelineException(PipelineErrorCode.IoError, $"file not found: {path}", ex);
        }
        catch (IOException ex)
        {
            throw new PipelineException(PipelineErrorCode.IoError, $"cannot read {path}: {ex.Message}", ex);
        }
    }

    private static byte[] HexToBytes(string hex)
    {
        try
        {
            return Convert.FromHexString(hex);
        }
        catch (FormatException ex)
        {
            throw new PipelineException(PipelineErrorCode.CheckpointCorrupt, $"invalid hash '{hex}' in checkpoint", ex);
        }
    }
}

internal static class StatsExtensions
{
    public static PipelineStats Clone(this PipelineStats stats) => new()
    {
        InputRecords = stats.InputRecords,
        CommittedRecords = stats.CommittedRecords,
        FilteredRecords = stats.FilteredRecords,
        OutputBytes = stats.OutputBytes,
    };
}

internal sealed class IncrementalSha : IDisposable
{
    private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

    public void Append(byte[] bytes) => _hash.AppendData(bytes);

    public string CurrentHashHex() => Convert.ToHexString(_hash.GetCurrentHash()).ToLowerInvariant();

    public void Dispose() => _hash.Dispose();
}
