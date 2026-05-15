---
title: AOT and trimming
description: Use Surefire with Native AOT and trimming.
---

Surefire is designed to work with trimming and Native AOT when your app uses source-generated JSON metadata for the types it passes through jobs.

The main thing you need to provide is a `JsonSerializerContext` for your own argument and result types.

## Register your JSON context

Add your app's `JsonSerializerContext` to `SurefireOptions.SerializerOptions` when you call `AddSurefire`:

```csharp
using System.Text.Json.Serialization;

builder.Services.AddSurefire(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonContext.Default);
});

[JsonSerializable(typeof(AddArgs))]
[JsonSerializable(typeof(AddResult))]
[JsonSerializable(typeof(List<AddResult>))]
internal partial class AppJsonContext : JsonSerializerContext;
```

Include the types used for:

- Job arguments
- Job results
- Stream item types
- Types passed to or returned from `IJobClient`
- Collections you deserialize, such as `List<T>` or `T[]`

## Use the normal APIs

Use the normal Surefire APIs for jobs, callbacks, and client calls:

```csharp
app.AddJob("Add", (int a, int b) => new AddResult(a + b));

var result = await client.RunAsync<AddResult>("Add", new { a = 1, b = 2 });
```

Surefire's source generator handles the supported call shapes it can see at build time. If the build reports IL2026 or IL3050 warnings from Surefire APIs, first make sure the argument and result types are in your `JsonSerializerContext`. If the warning remains, simplify the call shape or treat it as a current generator limitation.
