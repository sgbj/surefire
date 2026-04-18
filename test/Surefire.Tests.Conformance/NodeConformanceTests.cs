namespace Surefire.Tests.Conformance;

public abstract class NodeConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task HeartbeatNode_CreatesOnFirstCall()
    {
        var ct = TestContext.Current.CancellationToken;
        var nodeName = $"node-{Guid.CreateVersion7():N}";

        await Store.HeartbeatAsync(nodeName, ["JobA"], ["default"], [], ct);

        var nodes = await Store.GetNodesAsync(ct);
        var node = Assert.Single(nodes, n => n.Name == nodeName);
        Assert.Contains("JobA", node.RegisteredJobNames);
        Assert.Contains("default", node.RegisteredQueueNames);
    }

    [Fact]
    public async Task HeartbeatNode_UpdatesHeartbeat()
    {
        var ct = TestContext.Current.CancellationToken;
        var nodeName = $"node-{Guid.CreateVersion7():N}";

        await Store.HeartbeatAsync(nodeName, ["JobA"], ["default"], [], ct);
        var nodes1 = await Store.GetNodesAsync(ct);
        var hb1 = Assert.Single(nodes1, n => n.Name == nodeName).LastHeartbeatAt;

        await Task.Delay(10, ct);
        await Store.HeartbeatAsync(nodeName, ["JobA"], ["default"], [], ct);
        var nodes2 = await Store.GetNodesAsync(ct);
        var hb2 = Assert.Single(nodes2, n => n.Name == nodeName).LastHeartbeatAt;

        Assert.True(hb2 > hb1);
    }

    [Fact]
    public async Task HeartbeatNode_UpdatesRegisteredJobs()
    {
        var ct = TestContext.Current.CancellationToken;
        var nodeName = $"node-{Guid.CreateVersion7():N}";

        await Store.HeartbeatAsync(nodeName, ["JobA", "JobB"], ["default"], [], ct);
        var nodes1 = await Store.GetNodesAsync(ct);
        var node1 = Assert.Single(nodes1, n => n.Name == nodeName);
        Assert.Equal(2, node1.RegisteredJobNames.Count);

        await Store.HeartbeatAsync(nodeName, ["JobC"], ["q1", "q2"], [], ct);
        var nodes2 = await Store.GetNodesAsync(ct);
        var node2 = Assert.Single(nodes2, n => n.Name == nodeName);
        Assert.Single(node2.RegisteredJobNames);
        Assert.Contains("JobC", node2.RegisteredJobNames);
        Assert.Equal(2, node2.RegisteredQueueNames.Count);
    }

    [Fact]
    public async Task GetNodes_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var names = Enumerable.Range(0, 3)
            .Select(_ => $"node-{Guid.CreateVersion7():N}")
            .ToList();

        foreach (var name in names)
        {
            await Store.HeartbeatAsync(name, ["Job"], ["default"], [], ct);
        }

        var nodes = await Store.GetNodesAsync(ct);

        foreach (var name in names)
        {
            Assert.Contains(nodes, n => n.Name == name);
        }
    }
}