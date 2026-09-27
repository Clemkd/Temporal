using System.Net.Sockets;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using TemporalPoc.Core.Chaos;

namespace TemporalPoc.Core.Vehicles;

/// <summary>
/// Decides whether a failure is transient (worth retrying) or not. Unknown exceptions are NOT transient:
/// a bug or a functional error would fail the same way on every attempt.
/// </summary>
public static class FailureClassifier
{
    public static bool IsTransient(Exception e) => e switch
    {
        TimeoutException or IOException or SocketException or HttpRequestException => true,
        TransientChaosException => true,
        NpgsqlException npgsql => npgsql.IsTransient,
        DbUpdateConcurrencyException => true,
        DbUpdateException { InnerException: { } inner } => IsTransient(inner),
        InvalidOperationException { InnerException: { } inner } => IsTransient(inner),   // EF wraps provider errors
        AggregateException aggregate => aggregate.InnerExceptions.Count > 0 && aggregate.InnerExceptions.All(IsTransient),
        _ => false,
    };
}
