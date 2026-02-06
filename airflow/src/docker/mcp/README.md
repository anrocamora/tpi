# Airflow MCP server container image

This directory contains a Dockerfile that assembles the upstream
[`airflow-mcp-server`](https://github.com/abhishekbhakat/airflow-mcp-server)
project into a container image that can be published to the private Telekom
registry (`mtr.devops.telekom.de`).  The resulting image is designed to work
with the Helm chart under `src/main/helm/charts/airflow`.

## Build arguments

| Argument | Default | Purpose |
| --- | --- | --- |
| `AIRFLOW_MCP_REPO` | `https://github.com/abhishekbhakat/airflow-mcp-server.git` | Git repository cloned during the build. |
| `AIRFLOW_MCP_REF` | `0.9.1` | Tag or branch from the repository to embed in the image. |

Override the arguments to pin the image to a different upstream release:

```bash
docker build \
  -t mtr.devops.telekom.de/genomica/airflow-mcp-server:0.9.1 \
  --build-arg AIRFLOW_MCP_REF=0.9.1 \
  airflow/src/main/docker/mcp
```

## Runtime

The container runs the server in HTTP mode on port `3000` so that it is
reachable through the Kubernetes `Service` created by the chart.  The
application expects the following environment variables to be populated by the
Helm chart (via the MCP `ConfigMap`):

- `AIRFLOW_BASE_URL`: Base URL of the target Airflow control plane.
- `AUTH_TOKEN`: JWT token that authenticates the MCP server against Airflow.
- `AIRFLOW_MCP_RESOURCES_DIR` *(optional)*: Path with markdown guides to publish
  as MCP resources.

You can override the default entrypoint arguments if a different transport is
required:

```bash
kubectl set env deployment/airflow-mcp \ 
  --namespace tpi \ 
  AIRFLOW_BASE_URL=https://airflow.example.com
```
