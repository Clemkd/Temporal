namespace TemporalPoc.Core.Storage;

public sealed record ObjectInfo(string Key, long Size, DateTimeOffset LastModified);

public sealed record ObjectListing(IReadOnlyList<ObjectInfo> Objects, string? ContinuationToken);

public sealed class ObjectNotFoundException(string key) : Exception($"Object '{key}' not found")
{
    public string Key { get; } = key;
}

/// <summary>Minimal S3-like abstraction. Every mutating operation must be idempotent-friendly.</summary>
public interface IObjectStore
{
    Task EnsureReadyAsync(CancellationToken ct = default);

    Task<ObjectListing> ListAsync(string prefix, int maxKeys, string? continuationToken, CancellationToken ct = default);

    Task<ObjectInfo?> StatAsync(string key, CancellationToken ct = default);

    /// <summary>Opens an object for reading. Throws <see cref="ObjectNotFoundException"/>.</summary>
    Task<Stream> OpenReadAsync(string key, CancellationToken ct = default);

    Task PutAsync(string key, Stream content, string contentType, IReadOnlyDictionary<string, string>? tags = null, CancellationToken ct = default);

    /// <summary>Server side copy. Throws <see cref="ObjectNotFoundException"/> if the source is missing.</summary>
    Task CopyAsync(string sourceKey, string destinationKey, CancellationToken ct = default);

    /// <summary>Deletes an object, no-op when missing.</summary>
    Task DeleteAsync(string key, CancellationToken ct = default);

    Task SetTagsAsync(string key, IReadOnlyDictionary<string, string> tags, CancellationToken ct = default);

    Task<IReadOnlyDictionary<string, string>> GetTagsAsync(string key, CancellationToken ct = default);
}

public static class ObjectStoreExtensions
{
    /// <summary>
    /// Idempotent move: safe to call again after a crash between the copy and the delete.
    /// Returns false when neither the source nor the destination exist.
    /// </summary>
    public static async Task<bool> MoveAsync(this IObjectStore store, string sourceKey, string destinationKey,
        IReadOnlyDictionary<string, string>? tags = null, CancellationToken ct = default)
    {
        if (await store.StatAsync(sourceKey, ct) is not null)
        {
            await store.CopyAsync(sourceKey, destinationKey, ct);
            if (tags is not null)
            {
                await store.SetTagsAsync(destinationKey, tags, ct);
            }
            await store.DeleteAsync(sourceKey, ct);
            return true;
        }

        if (await store.StatAsync(destinationKey, ct) is not null)
        {
            // Already moved by a previous (crashed) attempt: make sure tags are applied.
            if (tags is not null)
            {
                await store.SetTagsAsync(destinationKey, tags, ct);
            }
            return true;
        }

        return false;
    }

    public static async Task PutBytesAsync(this IObjectStore store, string key, byte[] content, string contentType,
        IReadOnlyDictionary<string, string>? tags = null, CancellationToken ct = default)
    {
        using var stream = new MemoryStream(content, writable: false);
        await store.PutAsync(key, stream, contentType, tags, ct);
    }

    public static async Task<int> CountAsync(this IObjectStore store, string prefix, Func<ObjectInfo, bool>? filter = null, CancellationToken ct = default)
    {
        var count = 0;
        string? token = null;
        do
        {
            var page = await store.ListAsync(prefix, 1000, token, ct);
            count += filter is null ? page.Objects.Count : page.Objects.Count(filter);
            token = page.ContinuationToken;
        }
        while (token is not null);
        return count;
    }
}
