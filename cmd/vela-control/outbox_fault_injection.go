package main

const (
	labOutboxFaultPhaseEnv          = "VELA_LAB_OUTBOX_FAULT_PHASE"
	labOutboxFaultMarkerPath        = "/tmp/vela-lab-outbox-fault/marker.json"
	labOutboxFaultMarkerSchema      = "vela-lab-outbox-fault-marker-v1"
	labOutboxFaultReadMarkerArg     = "--lab-read-outbox-fault-marker"
	publisherPrePubAckCrash         = "publisher-pre-puback-crash"
	publisherPostPubAckPreMarkCrash = "publisher-post-puback-pre-mark-crash"
)
