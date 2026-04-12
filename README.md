# faasd - a lightweight and portable version of OpenFaaS

This is a fork of faasd used for research purposes. The original faasd project can be found at [faasd GitHub repository](https://github.com/openfaas/faasd).

faasd is [OpenFaaS](https://github.com/openfaas/) reimagined, but without the cost and complexity of Kubernetes. It runs on a single host with very modest requirements, making it fast and easy to manage. Under the hood it uses [containerd](https://containerd.io/) and [Container Networking Interface (CNI)](https://github.com/containernetworking/cni) along with the same core OpenFaaS components from the main project.
