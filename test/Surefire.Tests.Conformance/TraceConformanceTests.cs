namespace Surefire.Tests.Conformance;

/// <summary>
///     Conformance for the tree-aware trace primitives
///     (<see cref="IJobStore.GetDirectChildrenAsync" />,
///     <see cref="IJobStore.GetAncestorChainAsync" />). Used by the dashboard's
///     <c>/runs/{id}/trace</c> endpoint; every store must satisfy these contracts.
/// </summary>
public abstract class TraceConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetDirectChildren_ReturnsOnlyDirectChildren_OrderedByCreatedAtThenId()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Trace_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        // Three direct children, interleaved with a grandchild that must be excluded.
        var child1 = CreateRun(jobName) with { ParentRunId = parent.Id, RootRunId = parent.Id };
        var child2 = CreateRun(jobName) with { ParentRunId = parent.Id, RootRunId = parent.Id };
        var child3 = CreateRun(jobName) with { ParentRunId = parent.Id, RootRunId = parent.Id };
        var grandchild = CreateRun(jobName) with { ParentRunId = child1.Id, RootRunId = parent.Id };
        await Store.CreateRunsAsync([child1, child2, child3, grandchild], cancellationToken: ct);

        var page = await Store.GetDirectChildrenAsync(parent.Id, cancellationToken: ct);
        Assert.Equal(3, page.Items.Count);
        Assert.DoesNotContain(page.Items, r => r.Id == grandchild.Id);

        // Monotonic by (CreatedAt, Id).
        for (var i = 1; i < page.Items.Count; i++)
        {
            var prev = page.Items[i - 1];
            var curr = page.Items[i];
            Assert.True(prev.CreatedAt <= curr.CreatedAt);
            if (prev.CreatedAt == curr.CreatedAt)
            {
                Assert.True(string.CompareOrdinal(prev.Id, curr.Id) < 0);
            }
        }
    }

    [Fact]
    public async Task GetDirectChildren_CursorPaginates_AndNextCursorIsNullAtEnd()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TracePage_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        var children = new List<JobRun>();
        for (var i = 0; i < 10; i++)
        {
            var child = CreateRun(jobName) with { ParentRunId = parent.Id, RootRunId = parent.Id };
            children.Add(child);
            await Store.CreateRunsAsync([child], cancellationToken: ct);
        }

        var allFetched = new List<JobRun>();
        string? cursor = null;
        var pages = 0;
        while (true)
        {
            var page = await Store.GetDirectChildrenAsync(parent.Id, cursor, take: 3, cancellationToken: ct);
            pages++;
            allFetched.AddRange(page.Items);
            if (page.NextCursor is null)
            {
                break;
            }

            cursor = page.NextCursor;
            Assert.InRange(pages, 1, 10); // guard against infinite loop
        }

        Assert.Equal(10, allFetched.Count);

        // Pagination preserves global ordering: concatenated pages must equal a single full fetch.
        var full = await Store.GetDirectChildrenAsync(parent.Id, take: 1000, cancellationToken: ct);
        Assert.Equal(full.Items.Select(r => r.Id), allFetched.Select(r => r.Id));
    }

    [Theory]
    [InlineData(3, 3)] // exactly 1 page (count == take)
    [InlineData(6, 3)] // exactly 2 full pages (count == 2 * take)
    [InlineData(9, 3)] // exactly 3 full pages (count == 3 * take)
    public async Task GetDirectChildren_ExactMultipleBoundary_NextCursorIsNullOnFinalPage(
        int totalChildren, int take)
    {
        // NextCursor contract: when the remaining row count is an exact multiple of take, the
        // final page must have NextCursor == null without an extra empty fetch.
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceBoundary_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        for (var i = 0; i < totalChildren; i++)
        {
            var child = CreateRun(jobName) with { ParentRunId = parent.Id, RootRunId = parent.Id };
            await Store.CreateRunsAsync([child], cancellationToken: ct);
        }

        var fetched = new List<JobRun>();
        string? cursor = null;
        var pages = 0;
        while (true)
        {
            var page = await Store.GetDirectChildrenAsync(parent.Id, cursor, take: take, cancellationToken: ct);
            pages++;
            fetched.AddRange(page.Items);
            if (page.NextCursor is null)
            {
                break;
            }

            cursor = page.NextCursor;
            Assert.InRange(pages, 1, totalChildren); // guard against infinite loop
        }

        Assert.Equal(totalChildren, fetched.Count);
        Assert.Equal(totalChildren / take, pages);
    }

    [Fact]
    public async Task GetDirectChildren_CursorPaginates_WithSameCreatedAt_DoesNotSkipOrDuplicate()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceTieAfter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var children = new List<JobRun>();
        for (var i = 0; i < 12; i++)
        {
            var child = CreateRun(jobName) with
            {
                ParentRunId = parent.Id,
                RootRunId = parent.Id,
                CreatedAt = baseTime,
                NotBefore = baseTime
            };
            children.Add(child);
            await Store.CreateRunsAsync([child], cancellationToken: ct);
        }

        var expectedIds = children
            .OrderBy(c => c.Id, StringComparer.Ordinal)
            .Select(c => c.Id)
            .ToList();

        var fetchedIds = new List<string>();
        string? cursor = null;
        while (true)
        {
            var page = await Store.GetDirectChildrenAsync(parent.Id, cursor, take: 4, cancellationToken: ct);
            fetchedIds.AddRange(page.Items.Select(i => i.Id));
            if (page.NextCursor is null)
            {
                break;
            }

            cursor = page.NextCursor;
        }

        Assert.Equal(expectedIds.Count, fetchedIds.Count);
        Assert.Equal(expectedIds.Count, fetchedIds.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(expectedIds, fetchedIds);
    }

    [Fact]
    public async Task GetDirectChildren_BeforeCursor_WithSameCreatedAt_PaginatesStrictlyBackward()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceTieBefore_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var children = new List<JobRun>();
        for (var i = 0; i < 10; i++)
        {
            var child = CreateRun(jobName) with
            {
                ParentRunId = parent.Id,
                RootRunId = parent.Id,
                CreatedAt = baseTime,
                NotBefore = baseTime
            };
            children.Add(child);
            await Store.CreateRunsAsync([child], cancellationToken: ct);
        }

        var ordered = children
            .OrderBy(c => c.Id, StringComparer.Ordinal)
            .ToList();

        var focus = ordered[7];
        var expected = ordered.Take(7).Select(c => c.Id).Reverse().ToList();

        var fetched = new List<string>();
        var beforeCursor = DirectChildrenPage.EncodeCursor(focus.CreatedAt, focus.Id);
        while (true)
        {
            var page = await Store.GetDirectChildrenAsync(parent.Id,
                beforeCursor: beforeCursor,
                take: 3,
                cancellationToken: ct);

            fetched.AddRange(page.Items.Select(i => i.Id));
            if (page.NextCursor is null)
            {
                break;
            }

            beforeCursor = page.NextCursor;
        }

        Assert.Equal(expected, fetched);
    }

    [Fact]
    public async Task GetDirectChildren_BeforeCursor_ReturnsReverseKeysetInDescOrder()
    {
        // Reverse-keyset direction: used by the trace endpoint to fetch siblings that
        // precede the focus in a single query (no walk, no cap).
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceBefore_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        // Distinct createdAt per child keeps ordering deterministic regardless of id lex order.
        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var children = new List<JobRun>();
        for (var i = 0; i < 6; i++)
        {
            var child = CreateRun(jobName) with
            {
                ParentRunId = parent.Id,
                RootRunId = parent.Id,
                CreatedAt = baseTime.AddMilliseconds(i),
                NotBefore = baseTime.AddMilliseconds(i)
            };
            children.Add(child);
            await Store.CreateRunsAsync([child], cancellationToken: ct);
        }

        // Use child #4 as the focus anchor; 4 children come before it (0..3).
        var focus = children[4];
        var focusCursor = DirectChildrenPage.EncodeCursor(focus.CreatedAt, focus.Id);

        var before = await Store.GetDirectChildrenAsync(
            parent.Id, null, focusCursor, 3, ct);

        // DESC order: most recent first. Take=3 captures the 3 closest predecessors.
        Assert.Equal(3, before.Items.Count);
        Assert.Equal(children[3].Id, before.Items[0].Id);
        Assert.Equal(children[2].Id, before.Items[1].Id);
        Assert.Equal(children[1].Id, before.Items[2].Id);
        Assert.NotNull(before.NextCursor);

        var beforeMore = await Store.GetDirectChildrenAsync(
            parent.Id, null, before.NextCursor, 3, ct);
        Assert.Single(beforeMore.Items);
        Assert.Equal(children[0].Id, beforeMore.Items[0].Id);
        Assert.Null(beforeMore.NextCursor);
    }

    [Fact]
    public async Task GetDirectChildren_BothCursorsSupplied_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceCursors_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var parent = CreateRun(jobName);
        await Store.CreateRunsAsync([parent], cancellationToken: ct);

        var someCursor = DirectChildrenPage.EncodeCursor(DateTimeOffset.UtcNow, "x");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            Store.GetDirectChildrenAsync(parent.Id, someCursor, someCursor, 10, ct));
    }

    [Fact]
    public async Task GetDirectChildren_NoChildren_ReturnsEmptyWithNullCursor()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceEmpty_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var lonely = CreateRun(jobName);
        await Store.CreateRunsAsync([lonely], cancellationToken: ct);

        var page = await Store.GetDirectChildrenAsync(lonely.Id, cancellationToken: ct);
        Assert.Empty(page.Items);
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public async Task GetAncestorChain_ReturnsRootToImmediateParent_ExcludingSelf()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceChain_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        // Build a 4-level chain: root, a, b, c.
        var root = CreateRun(jobName);
        var a = CreateRun(jobName) with { ParentRunId = root.Id, RootRunId = root.Id };
        var b = CreateRun(jobName) with { ParentRunId = a.Id, RootRunId = root.Id };
        var c = CreateRun(jobName) with { ParentRunId = b.Id, RootRunId = root.Id };
        await Store.CreateRunsAsync([root, a, b, c], cancellationToken: ct);

        var chain = await Store.GetAncestorChainAsync(c.Id, ct);

        Assert.Equal(3, chain.Count);
        // Root-to-parent order.
        Assert.Equal(root.Id, chain[0].Id);
        Assert.Equal(a.Id, chain[1].Id);
        Assert.Equal(b.Id, chain[2].Id);
    }

    [Fact]
    public async Task GetAncestorChain_RootRun_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TraceRoot_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var root = CreateRun(jobName);
        await Store.CreateRunsAsync([root], cancellationToken: ct);

        var chain = await Store.GetAncestorChainAsync(root.Id, ct);
        Assert.Empty(chain);
    }

    [Fact]
    public async Task GetAncestorChain_UnknownRun_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var chain = await Store.GetAncestorChainAsync(Guid.CreateVersion7().ToString("N"), ct);
        Assert.Empty(chain);
    }
}
