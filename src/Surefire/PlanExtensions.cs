namespace Surefire;

public static class PlanExtensions
{
    public static async Task<JobRun?> GetStepRunAsync(
        this IJobClient client, string planRunId, string stepName, CancellationToken ct = default)
    {
        var result = await client.GetRunsAsync(new RunFilter
        {
            PlanRunId = planRunId,
            PlanStepName = stepName,
            Take = 1
        }, ct);
        return result.Items.FirstOrDefault();
    }

    public static async Task<IReadOnlyList<JobRun>> GetStepRunsAsync(
        this IJobClient client, string planRunId, string stepName, CancellationToken ct = default)
    {
        var result = await client.GetRunsAsync(new RunFilter
        {
            PlanRunId = planRunId,
            PlanStepName = stepName
        }, ct);
        return result.Items;
    }
}
