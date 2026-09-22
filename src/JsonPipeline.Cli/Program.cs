using System.Text.Json;
using JsonPipeline;

Dictionary<string, string> options = new(StringComparer.Ordinal);
bool resume = false;
bool demo = false;

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--resume":
            resume = true;
            break;
        case "--demo":
            demo = true;
            break;
        case "--input":
        case "--output":
        case "--config":
        case "--workers":
        case "--checkpoint":
            if (i + 1 >= args.Length)
            {
                Fail("USAGE", $"missing value for {args[i]}");
                return 2;
            }

            options[args[i]] = args[++i];
            break;
        default:
            Fail("USAGE", $"unknown argument '{args[i]}'");
            return 2;
    }
}

if (demo)
{
    string? root = FindRepositoryRoot(AppContext.BaseDirectory);
    if (root is null)
    {
        Fail("USAGE", "cannot locate examples/demo directory");
        return 2;
    }

    string demoDirectory = Path.Combine(root, "examples", "demo");
    options["--input"] = Path.Combine(demoDirectory, "input.jsonl");
    options["--config"] = Path.Combine(demoDirectory, "pipeline.config.json");
    options["--output"] = Path.Combine(demoDirectory, "output.jsonl");
    options["--checkpoint"] = Path.Combine(demoDirectory, ".checkpoint.json");
    options.TryAdd("--workers", Environment.ProcessorCount.ToString());
}

string[] required = { "--input", "--output", "--config", "--checkpoint" };
foreach (string key in required)
{
    if (!options.TryGetValue(key, out string? value) || string.IsNullOrWhiteSpace(value))
    {
        Console.Error.WriteLine("usage: JsonPipeline.Cli --input <file.jsonl> --output <file.jsonl> --config <file.json> --checkpoint <file.json> [--workers n] [--resume]");
        Fail("USAGE", $"missing required argument {key}");
        return 2;
    }
}

int workers = PipelineEngine.DefaultWorkers;
if (options.TryGetValue("--workers", out string? workersText))
{
    if (!int.TryParse(workersText, out workers) || workers <= 0 || workers > 256)
    {
        Fail("USAGE", "--workers must be an integer in 1..256");
        return 2;
    }
}

try
{
    PipelineEngine engine = new();
    PipelineResult result = await engine.RunFileAsync(
        options["--input"],
        options["--output"],
        options["--config"],
        options["--checkpoint"],
        workers,
        resume);

    Console.WriteLine(JsonSerializer.Serialize(new
    {
        output = Path.GetFullPath(options["--output"]),
        records = result.Records,
        outputBytes = result.OutputBytes,
        resumed = resume,
        workers,
    }));
    return 0;
}
catch (PipelineException ex)
{
    Console.Error.WriteLine(ex.Message);
    return ExitCodeFor(ex.Code);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"INTERNAL_ERROR: {ex.Message}");
    return 1;
}

static void Fail(string code, string message)
{
    Console.Error.WriteLine($"{code}: {message}");
}

static int ExitCodeFor(string code) => code == PipelineErrorCodes.Usage ? 2 : 1;

static string? FindRepositoryRoot(string start)
{
    DirectoryInfo? directory = new(start);
    while (directory is not null)
    {
        if (File.Exists(Path.Combine(directory.FullName, "JsonPipeline.sln")))
        {
            return directory.FullName;
        }

        directory = directory.Parent;
    }

    return null;
}
