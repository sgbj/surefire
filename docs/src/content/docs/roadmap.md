---
title: Roadmap
description: Ideas I'm thinking about for future versions of Surefire.
---

Besides bug fixes and other improvements, this is a short list of ideas I've been looking into. If any
of these seem useful, or the wrong direction, please let me
know by opening an issue or discussion on
[GitHub](https://github.com/sgbj/surefire).

## Better dashboard auth

Right now the dashboard is unauthenticated by default, so you have to call 
`.RequireAuthorization(...)` to lock it down. I'd like to make it safe by default, 
possibly with something like the login token used by the Aspire dashboard, and add more customization options.


## Durable jobs

A `JobBuilder.Durable()` method that lets a job act as a long-running
orchestrator. Calls to `IJobClient` from inside it would be recorded
and replayed, so the orchestrator can wait on child runs without holding a
concurrency slot.
