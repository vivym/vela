# Pinned Argo CD sources

Version: [v3.5.3](https://github.com/argoproj/argo-cd/releases/tag/v3.5.3).
Retrieved from the official argoproj/argo-cd repository; Git blob SHA and complete
file SHA256 were checked. `../render.py` verifies these SHA256 values before use.

| Source path at v3.5.3 | SHA256 |
| --- | --- |
| manifests/namespace-install.yaml | df727dfc83666dbcc78dceef969505cf9c1659af774c64f9c08a204b09bd7dba |
| manifests/crds/application-crd.yaml | 5dde0e229249b6b707beb98674c1deae3949d5c319a6c45b9f5a80c99618e40c |
| manifests/crds/applicationset-crd.yaml | 7d282054f41ca2b71a22bab04a8caf0000e07e53a62432e6ea292dae5ab4f07d |
| manifests/crds/appproject-crd.yaml | ab225266944322750136f1198d93786e4a79a0e43c8d149abfba44da30a3eac8 |

The installation uses namespace-scoped upstream RBAC, fixed image digests, CPU
placement, restricted tenant writers and the existing Keycloak. The optional
ApplicationSet CRD is installed for Argo discovery; its controller is not deployed.
The upstream manifests remain unedited; `install.yaml` is the derived artifact.
