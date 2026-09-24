using System.Text.Json;

namespace TemporalPoc.Core.Storage;

/// <summary>
/// File system implementation for local runs without S3. Tags are stored in a ".tags" side tree.
/// Writes go through a temp file + atomic rename so a crash never leaves a partial object.
/// </summary>
public sealed class FileSystemObjectStore(string rootPath) : IObjectStore
{
    private const string TagsDir = ".tags";
    private readonly string _root = Path.GetFullPath(rootPath);

    public Task EnsureReadyAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(_root);
        return Task.CompletedTask;
    }

    public Task<ObjectListing> ListAsync(string prefix, int maxKeys, string? continuationToken, CancellationToken ct = default)
    {
        var dir = Path.Combine(_root, prefix.TrimEnd('/'));
        if (!Directory.Exists(dir))
        {
            return Task.FromResult(new ObjectListing([], null));
        }

        var keys = Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories)
            .Where(p => !Path.GetFileName(p).StartsWith(".tmp-", StringComparison.Ordinal))
            .Select(ToKey)
            .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
            .Where(k => continuationToken is null || string.CompareOrdinal(k, continuationToken) > 0)
            .Order(StringComparer.Ordinal)
            .Take(maxKeys + 1)
            .ToList();

        var page = keys.Take(maxKeys).Select(k =>
        {
            var info = new FileInfo(ToPath(k));
            return new ObjectInfo(k, info.Exists ? info.Length : 0, info.Exists ? info.LastWriteTimeUtc : DateTimeOffset.UtcNow);
        }).ToList();

        return Task.FromResult(new ObjectListing(page, keys.Count > maxKeys ? page[^1].Key : null));
    }

    public Task<ObjectInfo?> StatAsync(string key, CancellationToken ct = default)
    {
        var info = new FileInfo(ToPath(key));
        return Task.FromResult(info.Exists ? new ObjectInfo(key, info.Length, info.LastWriteTimeUtc) : null);
    }

    public Task<Stream> OpenReadAsync(string key, CancellationToken ct = default)
    {
        var path = ToPath(key);
        if (!File.Exists(path))
        {
            throw new ObjectNotFoundException(key);
        }
        return Task.FromResult<Stream>(File.OpenRead(path));
    }

    public async Task PutAsync(string key, Stream content, string contentType, IReadOnlyDictionary<string, string>? tags = null, CancellationToken ct = default)
    {
        var path = ToPath(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var tmp = Path.Combine(Path.GetDirectoryName(path)!, ".tmp-" + Guid.NewGuid().ToString("N"));
        await using (var file = File.Create(tmp))
        {
            await content.CopyToAsync(file, ct);
        }
        File.Move(tmp, path, overwrite: true);
        if (tags is not null)
        {
            await SetTagsAsync(key, tags, ct);
        }
    }

    public async Task CopyAsync(string sourceKey, string destinationKey, CancellationToken ct = default)
    {
        await using var source = await OpenReadAsync(sourceKey, ct);
        await PutAsync(destinationKey, source, "application/octet-stream", null, ct);
    }

    public Task DeleteAsync(string key, CancellationToken ct = default)
    {
        foreach (var path in new[] { ToPath(key), TagsPath(key) })
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        return Task.CompletedTask;
    }

    public async Task SetTagsAsync(string key, IReadOnlyDictionary<string, string> tags, CancellationToken ct = default)
    {
        var path = TagsPath(key);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        await File.WriteAllTextAsync(path, JsonSerializer.Serialize(tags), ct);
    }

    public async Task<IReadOnlyDictionary<string, string>> GetTagsAsync(string key, CancellationToken ct = default)
    {
        var path = TagsPath(key);
        if (!File.Exists(path))
        {
            return new Dictionary<string, string>();
        }
        return JsonSerializer.Deserialize<Dictionary<string, string>>(await File.ReadAllTextAsync(path, ct)) ?? [];
    }

    private string ToPath(string key) => Path.Combine(_root, key.Replace('/', Path.DirectorySeparatorChar));

    private string TagsPath(string key) => Path.Combine(_root, TagsDir, key.Replace('/', Path.DirectorySeparatorChar) + ".json");

    private string ToKey(string path) => Path.GetRelativePath(_root, path).Replace(Path.DirectorySeparatorChar, '/');
}
