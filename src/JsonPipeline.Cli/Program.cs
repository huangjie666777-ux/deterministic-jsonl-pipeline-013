using JsonPipeline;

if (args.Length == 1 && args[0] == "--demo")
{
    Console.WriteLine(PipelineEngine.NormalizeJson("{\"status\":\"skeleton\"}"));
    return;
}

Console.Error.WriteLine("usage: JsonPipeline.Cli --demo");
Environment.ExitCode = 2;
