using System.Text;

namespace JsonPipeline.Tests;

internal static class TestHarness
{
    public static string CreateWorkDir()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jsonpipeline-tests", Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        return directory;
    }

    public static string WriteFile(string directory, string name, string content)
    {
        string path = Path.Combine(directory, name);
        File.WriteAllText(path, content);
        return path;
    }

    public static string ConfigFor(IEnumerable<string> operations, int maxInFlight = 128, int maxOutputBytes = 4 << 20)
    {
        string body = string.Join(",\n    ", operations);
        return $$"""
{
  "sequenceField": "id",
  "maxInputLineBytes": 65536,
  "maxInFlight": {{maxInFlight}},
  "maxOutputBytes": {{maxOutputBytes}},
  "maxOperations": 64,
  "operations": [
    {{body}}
  ]
}
""";
    }

    public static string InputFor(int count, string extra = "")
    {
        StringBuilder builder = new();
        for (int i = 0; i < count; i++)
        {
            builder.Append($"{{\"id\":{i},\"n\":{i},{extra}\"k\":\"v\"}}\n");
        }

        return builder.ToString();
    }
}
