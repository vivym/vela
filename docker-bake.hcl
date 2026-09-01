variable "RELEASE_REVISION" {
  default = ""
}

variable "RELEASE_IMAGE_PREFIX" {
  default = ""
}

group "vela-all" {
  targets = [
    "vela-control",
    "vela-fleet-controller",
    "vela-stage-worker-agent",
  ]
}

target "_common" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
  args = {
    RELEASE_REVISION = RELEASE_REVISION
    GO_BASE           = "docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
    DEBIAN_BASE       = "docker.io/library/debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171"
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

target "vela-stage-worker-agent" {
  inherits = ["_common"]
  target   = "vela-stage-worker-agent"
  tags     = ["${RELEASE_IMAGE_PREFIX}/vela-stage-worker-agent:${RELEASE_REVISION}"]
}
