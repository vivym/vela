# Node exporter unavailable

1. Inspect the target in Prometheus and the corresponding `monitoring-prometheus-node-exporter` Pod.
2. Check the node condition, kubelet, RKE2 agent, disk and network from the management node.
3. For a protected host (`10.1.201.44`, `.56`, `.57`, `.66`), perform read-only checks only; never reboot, power-cycle or reload a driver.
4. Restore the exporter or cordon the node through the approved maintenance procedure, then verify `up{job="node-exporter"}` for 15 minutes.
