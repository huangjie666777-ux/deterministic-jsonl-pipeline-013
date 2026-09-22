using System.Text;
using System.Threading.Channels;

namespace JsonPipeline;

public sealed class PipelineEngine
{
    public const int DefaultWorkers = 4;

    public static string NormalizeJson(string json) => CanonicalJson.Serialize(json);

    public Task<PipelineResult> RunAsync(
        Stream input,
        Stream output,
        CancellationToken cancellationToken = default)
    {
        PipelineConfig config = new();
        return RunAsync(input, output, config, DefaultWorkers, cancellationToken);
    }

    public async Task<PipelineResult> RunAsync(
        Stream input,
        Stream output,
        PipelineConfig config,
        int workers,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(config);
        if (workers <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(workers), "workers must be positive");
        }

        using MemoryStream bufferedInput = new();
        await input.CopyToAsync(bufferedInput, cancellationToken).ConfigureAwait(false);
        bufferedInput.Position = 0;
        string inputHash = HashOf.Sha256Hex(bufferedInput);
        bufferedInput.Position = 0;

        long total = await CountLinesAsync(bufferedInput, cancellationToken).ConfigureAwait(false);
        bufferedInput.Position = 0;

        InMemorySink sink = new(output);
        PipelineRunState state = new(inputHash, "", total, sink, null);
        PipelineStats stats = await PipelineRunner.RunAsync(bufferedInput, config, workers, state, cancellationToken).ConfigureAwait(false);
        await output.FlushAsync(cancellationToken).ConfigureAwait(false);
        return stats.ToResult();
    }

    public async Task<PipelineResult> RunFileAsync(
        string inputPath,
        string outputPath,
        string configPath,
        string checkpointPath,
        int workers = DefaultWorkers,
        bool resume = false,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(inputPath) || string.IsNullOrWhiteSpace(outputPath) ||
            string.IsNullOrWhiteSpace(configPath) || string.IsNullOrWhiteSpace(checkpointPath))
        {
            throw new PipelineException(PipelineErrorCodes.Usage, "input, output, config and checkpoint paths are required");
        }

        if (workers <= 0 || workers > 256)
        {
            throw new PipelineException(PipelineErrorCodes.Usage, "--workers must be in 1..256");
        }

        if (!File.Exists(inputPath))
        {
            throw new PipelineException(PipelineErrorCodes.Usage, $"input file not found: {inputPath}");
        }

        if (!File.Exists(configPath))
        {
            throw new PipelineException(PipelineErrorCodes.Usage, $"config file not found: {configPath}");
        }

        byte[] configBytes = await File.ReadAllBytesAsync(configPath, cancellationToken).ConfigureAwait(false);
        if (configBytes.Length > PipelineConfig.MaxConfigBytes)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"config size {configBytes.Length} exceeds limit {PipelineConfig.MaxConfigBytes} bytes");
        }

        PipelineConfig config = PipelineConfig.Parse(configBytes);
        string configHash = HashOf.Sha256Hex(configBytes);
        string inputHash = await HashFileAsync(inputPath, cancellationToken).ConfigureAwait(false);

        CheckpointFile checkpoint;
        FileStream? tempStream;

        if (resume)
        {
            (checkpoint, tempStream) = OpenForResume(checkpointPath, inputHash, configHash, outputPath);
            if (checkpoint.Completed)
            {
                tempStream?.Dispose();
                return new PipelineResult((int)checkpoint.Stats.CommittedRecords, checkpoint.Stats.OutputBytes);
            }

            if (tempStream is null)
            {
                throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, "resume state is inconsistent");
            }
        }
        else
        {
            if (File.Exists(checkpointPath))
            {
                throw new PipelineException(
                    PipelineErrorCodes.CheckpointExists,
                    $"checkpoint '{checkpointPath}' already exists; pass --resume to continue it");
            }

            string checkpointDirectory = Path.GetDirectoryName(Path.GetFullPath(checkpointPath)) ?? Directory.GetCurrentDirectory();
            Directory.CreateDirectory(checkpointDirectory);
            string tempPath = Path.Combine(
                checkpointDirectory,
                "." + Path.GetFileName(outputPath) + "." + HashOf.Sha256Hex(Encoding.UTF8.GetBytes(Path.GetFullPath(inputPath)))[..12] + ".tmp");

            tempStream = new FileStream(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 1 << 16,
                useAsync: true);

            checkpoint = new CheckpointFile
            {
                InputSha256 = inputHash,
                ConfigSha256 = configHash,
                CommittedSequence = -1,
                TempOutputPath = tempPath,
                TempOutputSha256 = HashOf.Empty,
                Stats = new PipelineStats(),
            };

            CheckpointStore.WriteAtomic(checkpointPath, checkpoint);
        }

        try
        {
            long total;
            await using (FileStream countStream = new(inputPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.Asynchronous))
            {
                total = await CountLinesAsync(countStream, cancellationToken).ConfigureAwait(false);
            }

            CheckpointStore store = new(checkpointPath, checkpoint);
            await using FileStream inputStream = new(inputPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, FileOptions.Asynchronous);
            checkpoint.Stats = checkpoint.Stats with { InputRecords = total };
            PipelineRunState state = new(inputHash, configHash, total, null!, checkpoint);
            FileSink sink = new(tempStream, store, state);
            state.SetSink(sink);

            PipelineStats stats = await PipelineRunner.RunAsync(inputStream, config, workers, state, cancellationToken).ConfigureAwait(false);
            await tempStream.FlushAsync(cancellationToken).ConfigureAwait(false);

            string finalHash = HashOf.Sha256HexOfFile(checkpoint.TempOutputPath);
            checkpoint.Stats = stats;
            checkpoint.TempOutputSha256 = finalHash;
            checkpoint.OutputSha256 = finalHash;
            checkpoint.CommittedSequence = state.LastCommittedSequence;
            checkpoint.Completed = true;
            store.WriteAtomic();

            await tempStream.DisposeAsync().ConfigureAwait(false);

            string outputDirectory = Path.GetDirectoryName(Path.GetFullPath(outputPath)) ?? Directory.GetCurrentDirectory();
            Directory.CreateDirectory(outputDirectory);
            string commitStaging = outputPath + ".commit-" + HashOf.Sha256Hex(Encoding.UTF8.GetBytes(Path.GetFullPath(checkpointPath)))[..8] + ".tmp";
            File.Copy(checkpoint.TempOutputPath, commitStaging, overwrite: true);
            File.Move(commitStaging, outputPath, overwrite: true);

            return stats.ToResult();
        }
        catch
        {
            await tempStream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static (CheckpointFile Checkpoint, FileStream? TempStream) OpenForResume(
        string checkpointPath,
        string inputHash,
        string configHash,
        string outputPath)
    {
        if (!File.Exists(checkpointPath))
        {
            throw new PipelineException(
                PipelineErrorCodes.CheckpointCorrupt,
                $"--resume requested but checkpoint '{checkpointPath}' does not exist");
        }

        CheckpointFile checkpoint = CheckpointStore.Read(checkpointPath);

        if (checkpoint.Version != 1)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, $"unsupported checkpoint version {checkpoint.Version}");
        }

        if (checkpoint.InputSha256 != inputHash)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointInputMismatch, "input SHA-256 does not match checkpoint; refusing to resume");
        }

        if (checkpoint.ConfigSha256 != configHash)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointConfigMismatch, "config SHA-256 does not match checkpoint; refusing to resume");
        }

        if (checkpoint.Completed)
        {
            if (!File.Exists(outputPath) || HashOf.Sha256HexOfFile(outputPath) != checkpoint.OutputSha256)
            {
                throw new PipelineException(
                    PipelineErrorCodes.CheckpointTempTampered,
                    "checkpoint is completed but final output is missing or modified; refusing to overwrite");
            }

            return (checkpoint, null);
        }

        if (string.IsNullOrWhiteSpace(checkpoint.TempOutputPath) || !File.Exists(checkpoint.TempOutputPath))
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointTempTampered, "temp output referenced by checkpoint is missing; cannot resume");
        }

        if (HashOf.Sha256HexOfFile(checkpoint.TempOutputPath) != checkpoint.TempOutputSha256)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointTempTampered, "temp output SHA-256 does not match checkpoint; refusing to resume");
        }

        FileStream tempStream = new(
            checkpoint.TempOutputPath,
            FileMode.Open,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 1 << 16,
            useAsync: true);
        tempStream.Seek(0, SeekOrigin.End);
        return (checkpoint, tempStream);
    }

    internal static async Task<string> HashFileAsync(string path, CancellationToken cancellationToken)
    {
        await using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 20, FileOptions.Asynchronous);
        return HashOf.Sha256Hex(stream);
    }

    internal static async Task<long> CountLinesAsync(Stream input, CancellationToken cancellationToken)
    {
        long position = input.CanSeek ? input.Position : 0;
        long count = 0;
        using (StreamReader reader = new(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, 1 << 16, leaveOpen: true))
        {
            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is not null)
            {
                count++;
            }
        }

        if (input.CanSeek)
        {
            input.Position = position;
        }

        return count;
    }
}
