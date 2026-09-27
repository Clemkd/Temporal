using Microsoft.EntityFrameworkCore;

namespace TemporalPoc.Core.Data;

public sealed class PocDbContext(DbContextOptions<PocDbContext> options) : DbContext(options)
{
    public DbSet<IngestedFile> Files => Set<IngestedFile>();
    public DbSet<Measurement> Measurements => Set<Measurement>();
    public DbSet<CategorizationStaging> CategorizationStaging => Set<CategorizationStaging>();
    public DbSet<ProcessingResult> ProcessingResults => Set<ProcessingResult>();
    public DbSet<ProcessingJob> ProcessingJobs => Set<ProcessingJob>();
    public DbSet<PipelineDefinitionRecord> Pipelines => Set<PipelineDefinitionRecord>();
    public DbSet<VehicleDayRun> VehicleDayRuns => Set<VehicleDayRun>();
    public DbSet<VehicleDayResult> VehicleDayResults => Set<VehicleDayResult>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<IngestedFile>(e =>
        {
            e.ToTable("files");
            e.HasKey(x => x.Key);
            e.HasIndex(x => x.Status);
        });

        model.Entity<Measurement>(e =>
        {
            e.ToTable("measurements");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).UseIdentityAlwaysColumn();
            e.HasIndex(x => new { x.FileKey, x.LineNumber }).IsUnique();
            e.HasIndex(x => new { x.SensorType, x.Id });
        });

        model.Entity<CategorizationStaging>(e =>
        {
            e.ToTable("categorization_staging");
            e.HasKey(x => new { x.JobId, x.MeasurementId });
            e.HasIndex(x => new { x.JobId, x.BatchNumber });
        });

        model.Entity<ProcessingResult>(e =>
        {
            e.ToTable("processing_results");
            e.HasKey(x => x.Id);
            e.HasIndex(x => new { x.JobId, x.BatchNumber, x.SensorId }).IsUnique();
        });

        model.Entity<ProcessingJob>(e =>
        {
            e.ToTable("processing_jobs");
            e.HasKey(x => x.JobId);
        });

        model.Entity<VehicleDayRun>(e =>
        {
            e.ToTable("vehicle_day_runs");
            e.HasKey(x => new { x.VehicleId, x.Day });
            e.HasIndex(x => x.Status);
        });

        model.Entity<VehicleDayResult>(e =>
        {
            e.ToTable("vehicle_day_results");
            e.HasKey(x => new { x.VehicleId, x.Day });
        });

        model.Entity<PipelineDefinitionRecord>(e =>
        {
            e.ToTable("pipelines");
            e.HasKey(x => new { x.Name, x.Version });
            e.Property(x => x.DefinitionJson).HasColumnType("jsonb");
        });
    }
}
