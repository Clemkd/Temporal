using System.Net;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using TemporalPoc.Core.Configuration;

namespace TemporalPoc.Core.Storage;

/// <summary>S3 implementation (AWS, MinIO, SeaweedFS, RustFS...).</summary>
public sealed class S3ObjectStore : IObjectStore, IDisposable
{
    private readonly IAmazonS3 _s3;
    private readonly string _bucket;

    public S3ObjectStore(StorageSettings settings)
    {
        _bucket = settings.Bucket;
        var config = new AmazonS3Config
        {
            ServiceURL = settings.ServiceUrl,
            ForcePathStyle = true,
            AuthenticationRegion = settings.Region,
            // S3-compatible servers do not always support the new default checksums of the v4 SDK.
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED,
            MaxErrorRetry = 3,
        };
        _s3 = new AmazonS3Client(new BasicAWSCredentials(settings.AccessKey, settings.SecretKey), config);
    }

    public async Task EnsureReadyAsync(CancellationToken ct = default)
    {
        try
        {
            await _s3.PutBucketAsync(new PutBucketRequest { BucketName = _bucket }, ct);
        }
        catch (AmazonS3Exception e) when (e.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
        {
        }
    }

    public async Task<ObjectListing> ListAsync(string prefix, int maxKeys, string? continuationToken, CancellationToken ct = default)
    {
        var response = await _s3.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = _bucket,
            Prefix = prefix,
            MaxKeys = maxKeys,
            ContinuationToken = continuationToken,
        }, ct);

        var objects = (response.S3Objects ?? [])
            .Select(o => new ObjectInfo(o.Key, o.Size ?? 0, o.LastModified is { } d ? new DateTimeOffset(d.ToUniversalTime()) : DateTimeOffset.UtcNow))
            .ToList();
        return new ObjectListing(objects, response.IsTruncated == true ? response.NextContinuationToken : null);
    }

    public async Task<ObjectInfo?> StatAsync(string key, CancellationToken ct = default)
    {
        try
        {
            var meta = await _s3.GetObjectMetadataAsync(_bucket, key, ct);
            return new ObjectInfo(key, meta.ContentLength, meta.LastModified is { } d ? new DateTimeOffset(d.ToUniversalTime()) : DateTimeOffset.UtcNow);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<Stream> OpenReadAsync(string key, CancellationToken ct = default)
    {
        try
        {
            using var response = await _s3.GetObjectAsync(_bucket, key, ct);
            // Buffer: files are small (a few MB) and it frees the HTTP connection immediately.
            var buffer = new MemoryStream();
            await response.ResponseStream.CopyToAsync(buffer, ct);
            buffer.Position = 0;
            return buffer;
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ObjectNotFoundException(key);
        }
    }

    public async Task PutAsync(string key, Stream content, string contentType, IReadOnlyDictionary<string, string>? tags = null, CancellationToken ct = default)
    {
        await _s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucket,
            Key = key,
            InputStream = content,
            ContentType = contentType,
            AutoCloseStream = false,
        }, ct);
        if (tags is not null)
        {
            await SetTagsAsync(key, tags, ct);
        }
    }

    public async Task CopyAsync(string sourceKey, string destinationKey, CancellationToken ct = default)
    {
        try
        {
            await _s3.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = _bucket,
                SourceKey = sourceKey,
                DestinationBucket = _bucket,
                DestinationKey = destinationKey,
            }, ct);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ObjectNotFoundException(sourceKey);
        }
    }

    public async Task DeleteAsync(string key, CancellationToken ct = default)
    {
        try
        {
            await _s3.DeleteObjectAsync(_bucket, key, ct);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
        }
    }

    public async Task SetTagsAsync(string key, IReadOnlyDictionary<string, string> tags, CancellationToken ct = default)
    {
        await _s3.PutObjectTaggingAsync(new PutObjectTaggingRequest
        {
            BucketName = _bucket,
            Key = key,
            Tagging = new Tagging
            {
                // S3 limits: 10 tags, 128 chars key, 256 chars value.
                TagSet = tags.Take(10).Select(t => new Tag { Key = Truncate(t.Key, 128), Value = Truncate(Sanitize(t.Value), 256) }).ToList(),
            },
        }, ct);
    }

    public async Task<IReadOnlyDictionary<string, string>> GetTagsAsync(string key, CancellationToken ct = default)
    {
        try
        {
            var response = await _s3.GetObjectTaggingAsync(new GetObjectTaggingRequest { BucketName = _bucket, Key = key }, ct);
            return (response.Tagging ?? []).ToDictionary(t => t.Key, t => t.Value);
        }
        catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            return new Dictionary<string, string>();
        }
    }

    public void Dispose() => _s3.Dispose();

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];

    // Tag values only accept letters, digits, spaces and + - = . _ : / @
    private static string Sanitize(string value) =>
        new(value.Select(c => char.IsLetterOrDigit(c) || " +-=._:/@".Contains(c) ? c : '_').ToArray());
}
