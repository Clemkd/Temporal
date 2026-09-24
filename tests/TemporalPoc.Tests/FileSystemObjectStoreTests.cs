using TemporalPoc.Core.Storage;

namespace TemporalPoc.Tests;

public sealed class FileSystemObjectStoreTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "poc-store-" + Guid.NewGuid().ToString("N"));
    private readonly FileSystemObjectStore _store;

    public FileSystemObjectStoreTests() => _store = new FileSystemObjectStore(_root);

    public void Dispose() => Directory.Delete(_root, recursive: true);

    [Fact]
    public async Task Move_is_idempotent_and_applies_tags()
    {
        await _store.PutBytesAsync("incoming/a/b.csv", "x"u8.ToArray(), "text/csv");
        var tags = new Dictionary<string, string> { ["status"] = "processing" };

        Assert.True(await _store.MoveAsync("incoming/a/b.csv", "processing/a/b.csv", tags));
        // Replay after a crash: source already gone, destination present -> still a success.
        Assert.True(await _store.MoveAsync("incoming/a/b.csv", "processing/a/b.csv", tags));
        Assert.False(await _store.MoveAsync("incoming/zzz.csv", "processing/zzz.csv"));

        Assert.Null(await _store.StatAsync("incoming/a/b.csv"));
        Assert.Equal("processing", (await _store.GetTagsAsync("processing/a/b.csv"))["status"]);
    }

    [Fact]
    public async Task Listing_is_paginated_and_ordered()
    {
        for (var i = 0; i < 25; i++)
        {
            await _store.PutBytesAsync($"incoming/f{i:D2}.csv", [1], "text/csv");
        }

        var keys = new List<string>();
        string? token = null;
        do
        {
            var page = await _store.ListAsync("incoming/", 10, token);
            keys.AddRange(page.Objects.Select(o => o.Key));
            token = page.ContinuationToken;
        }
        while (token is not null);

        Assert.Equal(25, keys.Count);
        Assert.Equal(keys.Order(StringComparer.Ordinal), keys);
        Assert.Equal(25, await _store.CountAsync("incoming/"));
    }
}
