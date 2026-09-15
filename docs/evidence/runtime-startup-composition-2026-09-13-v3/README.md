# Linux race and runtime composition validation v3

This run used the current checkout to build race-instrumented Linux amd64
`nodeagent`, runtime command and model-runtime binaries, plus the source-matched
observer fixtures. They ran in a disposable Docker namespace with no host
mounts, network or business workload access.

The selected native Node groups passed with no skips, failures or race reports:

```text
TestRuntimeStartupAuthority*
TestPrepareRemoteStartupOrchestration*
TestRuntimeStartupOrchestration*
TestRuntimeStartupServer*
TestRuntimeStartupCoordinator*
TestRuntimeJournalObservation*
TestRuntimeStartupGrantActivation*
TestRuntimeObserverCustody
TestRuntimeBootstrapPublication*
```

This validates the current pidfd, observer custody, journal, grant and
orchestration lifetimes under the race detector. It remains validation-only and
does not provide production Fleet, issuer, image or Launch Receipt evidence.
