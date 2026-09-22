using System.Text.Json;
using JsonPipeline;

Dictionary<string, string?> arguments;
try
{
    arguments = Parse(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    Environment.ExitCode = 2;
    return;
}

if (arguments.ContainsKey("help") || arguments.ContainsKey("h"))
{
    PrintUsage();
    return;
}

if (arguments.ContainsKey("demo"))
{
    Console.WriteLine(PipelineEngine.NormalizeJson("{\"status\":\"skeleton\"}"));
    return;
}

if (!arguments.TryGetValue("input", out string? input)
    || !arguments.TryGetValue("output", out string? output)
    || !arguments.TryGetValue("config", out string? configPath)
    || !arguments.TryGetValue("checkpoint", out string? checkpoint))
{
    Console.Error.WriteLine("error: --input, --output, --config and --checkpoint are required");
    PrintUsage(Console.Error);
    Environment.ExitCode = 2;
    return;
}

int workers = 2;
if (arguments.TryGetValue("workers", out string? workersText))
{
    if (!int.TryParse(workersText, out workers) || workers < 1)
    {
        Console.Error.WriteLine($"error: invalid --workers value '{workersText}'");
        Environment.ExitCode = 2;
        return;
    }
}

bool resume = arguments.ContainsKey("resume");

try
{
    using CancellationTokenSource cts = new();
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        cts.Cancel();
    };

    PipelineRunResult result = await FileRunner.RunAsync(
        new PipelineRunOptions(input, output, configPath, checkpoint, workers, resume),
        cts.Token);

    Console.WriteLine(JsonSerializer.Serialize(new
    {
        code = "OK",
        result.InputRecords,
        result.CommittedRecords,
        result.FilteredRecords,
        result.OutputBytes,
        result.Resumed,
        result.Completed,
    }));
}
catch (PipelineException ex)
{
    Console.Error.WriteLine($"{ex.StableCode}: {ex.Message}");
    Environment.ExitCode = ex.Code switch
    {
        PipelineErrorCode.EmptyLine
            or PipelineErrorCode.InvalidJson
            or PipelineErrorCode.InvalidSequence
            or PipelineErrorCode.DuplicateSequence
            or PipelineErrorCode.UnknownOperation
            or PipelineErrorCode.TypeMismatch
            or PipelineErrorCode.ValidationFailed
            or PipelineErrorCode.LineTooLong
            or PipelineErrorCode.OutputTooLarge => 3,
        PipelineErrorCode.InvalidConfig => 4,
        PipelineErrorCode.CheckpointCorrupt
            or PipelineErrorCode.InputHashMismatch
            or PipelineErrorCode.ConfigHashMismatch
            or PipelineErrorCode.TempOutputTampered
            or PipelineErrorCode.InvalidResumeState => 5,
        _ => 6,
    };
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("CANCELLED: interrupted before atomic commit; rerun with --resume");
    Environment.ExitCode = 7;
}

static Dictionary<string, string?> Parse(string[] args)
{
    Dictionary<string, string?> result = new(StringComparer.Ordinal);
    for (int index = 0; index < args.Length; index++)
    {
        string argument = args[index];
        if (argument is "--resume" or "--demo")
        {
            result[argument[2..]] = "true";
            continue;
        }

        if (!argument.StartsWith("--", StringComparison.Ordinal) || argument.Length <= 2)
        {
            throw new ArgumentException($"unrecognized argument '{argument}'");
        }

        string body = argument[2..];
        int equals = body.IndexOf('=');
        string key;
        string? value;
        if (equals >= 0)
        {
            key = body[..equals];
            value = body[(equals + 1)..];
        }
        else
        {
            key = body;
            if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException($"argument '--{key}' requires a value");
            }
            value = args[++index];
        }
        result[key] = value;
    }
    return result;
}

static void PrintUsage(TextWriter? writer = null)
{
    writer ??= Console.Out;
    writer.WriteLine("usage: JsonPipeline.Cli --input <input.jsonl> --output <output.jsonl> --config <config.json> --checkpoint <checkpoint.json> [--workers <n>] [--resume]");
}
