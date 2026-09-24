namespace TemporalPoc.Core.Storage;

/// <summary>
/// Life cycle of a file in the bucket:
/// incoming/ -> processing/ -> processed/ | invalid/  (+ converted/ for the normalized output).
/// A file is "consumed" as soon as it leaves incoming/.
/// </summary>
public static class FileLayout
{
    public const string Incoming = "incoming/";
    public const string Processing = "processing/";
    public const string Processed = "processed/";
    public const string Invalid = "invalid/";
    public const string Converted = "converted/";

    public static readonly string[] AllPrefixes = [Incoming, Processing, Processed, Invalid, Converted];

    /// <summary>Returns the path relative to the life-cycle prefix ("incoming/a/b.csv" -> "a/b.csv").</summary>
    public static string Relative(string key)
    {
        foreach (var prefix in AllPrefixes)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                return key[prefix.Length..];
            }
        }
        return key;
    }

    public static string IncomingKey(string relative) => Incoming + relative;
    public static string ProcessingKey(string relative) => Processing + relative;
    public static string ProcessedKey(string relative) => Processed + relative;
    public static string InvalidKey(string relative) => Invalid + relative;
    public static string ConvertedKey(string relative) => Converted + relative + ".ndjson";
    public static string ErrorReportKey(string relative) => Invalid + relative + ".error.json";

    /// <summary>Deterministic workflow id of the ingestion of a file: one running workflow per file.</summary>
    public static string IngestionWorkflowId(string relative) => "ingest:" + relative;
}
