# Boomerang Flow Controller Service

This service handles and translates the requests that go to Kubernetes.

It uses the Kubernetes Java Client to interact with Kubernetes.

When writing new controller integrations, it is recommended to look through the Docs to find the exact Client method to use and then look at the API code to see how it works for advance configurations such as the Watcher API.

## Development

When running the service locally you need access to a kubernetes API endpoint

## References

### Kubernetes Java Client

- Client: https://github.com/kubernetes-client/java
- Examples: https://github.com/kubernetes-client/java/blob/master/examples/src/main/java/io/kubernetes/client/examples
- API: https://github.com/kubernetes-client/java/tree/master/kubernetes/src/main/java/io/kubernetes/client/apis
- Docs: https://github.com/kubernetes-client/java/tree/master/kubernetes/docs

### Kubernetes ConfigMap

We currently use projected volumes however subpath was considered.

- Projected Volumes: https://stackoverflow.com/questions/49287078/how-to-merge-two-configmaps-using-volume-mount-in-kubernetes
- SubPath: https://blog.sebastian-daschner.com/entries/multiple-kubernetes-volumes-directory

