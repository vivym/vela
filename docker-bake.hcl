variable "RELEASE_REVISION" {
  default = ""
}

variable "RELEASE_IMAGE_PREFIX" {
  default = ""
}

variable "H3_BACKEND_CONTEXT" {
  default = ""
}

variable "H3_BACKEND_SHA256" {
  default = ""
}

group "vela-all" {
  targets = [
    "vela-control",
    "vela-fleet-controller",
    "vela-h3-runner",
    "vela-worker-agent",
  ]
}

target "_common" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
  args = {
    RELEASE_REVISION = RELEASE_REVISION
    GO_BASE           = "docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
    PYTHON_BASE       = "docker.io/library/python:3.13.11-slim-bookworm@sha256:20080e807bfc404f8450b185cf0fc95d553462673598549613735f70a5b4d5d0"
    DEBIAN_BASE       = "docker.io/library/debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171"
    UV_BASE           = "ghcr.io/astral-sh/uv:0.8.22@sha256:9874eb7afe5ca16c363fe80b294fe700e460df29a55532bbfea234a0f12eddb1"
  }
}

target "vela-control" {
  inherits = ["_common"]
  target   = "vela-control"
  tags     = ["${RELEASE_IMAGE_PREFIX}/vela-control:${RELEASE_REVISION}"]
}

target "vela-fleet-controller" {
  inherits = ["_common"]
  target   = "vela-fleet-controller"
  tags     = ["${RELEASE_IMAGE_PREFIX}/vela-fleet-controller:${RELEASE_REVISION}"]
}

target "vela-worker-agent" {
  inherits = ["_common"]
  target   = "vela-worker-agent"
  tags     = ["${RELEASE_IMAGE_PREFIX}/vela-worker-agent:${RELEASE_REVISION}"]
}

target "vela-h3-runner" {
  inherits = ["_common"]
  target   = "vela-h3-runner"
  tags     = ["${RELEASE_IMAGE_PREFIX}/vela-h3-runner:${RELEASE_REVISION}"]
  contexts = {
    h3_backend = H3_BACKEND_CONTEXT
  }
  args = {
    H3_BACKEND_SHA256 = H3_BACKEND_SHA256
  }
}
