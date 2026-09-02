package h3request

import (
	"encoding/json"
	"testing"
)

func TestFreezeBuildsExactFastH3ParametersAndRootInputs(t *testing.T) {
	seed := int64(17)
	request := Request{
		Task: "ref2va",
		Seed: &seed,
		Conditions: []Condition{{
			Role: "reference", Type: "image",
			URI:         "vela://uploads/reference-frame",
			DownloadURL: "https://objects.example.test/reference?signature=secret",
			SHA256:      "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
			SizeBytes:   4096,
		}},
		Target: Target{ShortEdge: 768, AspectRatio: "16:9", DurationSeconds: float64Pointer(5)},
		Sampling: Sampling{
			NumInferenceSteps: 30,
			Quality:           "lossless",
		},
	}
	frozen, err := Freeze("a reference animation", "balanced", "job-key", request)
	if err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	if frozen.Parameters.SchemaRevision != 1 || frozen.Parameters.CanonicalRequest.Schema != "minimax_h3.request/v1" {
		t.Fatalf("frozen parameters = %#v", frozen.Parameters)
	}
	if frozen.Parameters.CanonicalRequest.Seed != seed ||
		frozen.Parameters.CanonicalRequest.Conditions[0].URI != request.Conditions[0].URI {
		t.Fatalf("canonical request = %#v", frozen.Parameters.CanonicalRequest)
	}
	if len(frozen.RootInputs) != 1 || frozen.RootInputs[0].ConditionIndex != 0 ||
		frozen.RootInputs[0].DownloadURL != request.Conditions[0].DownloadURL {
		t.Fatalf("root inputs = %#v", frozen.RootInputs)
	}
	encoded, err := json.Marshal(frozen.Parameters)
	if err != nil {
		t.Fatalf("marshal parameters: %v", err)
	}
	const want = `{"schema_revision":1,"canonical_request":{"schema":"minimax_h3.request/v1","task":"ref2va","prompt":"a reference animation","conditions":[{"role":"reference","type":"image","uri":"vela://uploads/reference-frame"}],"target":{"short_edge":768,"aspect_ratio":"16:9","duration_seconds":5},"seed":17},"sampling":{"num_inference_steps":30,"quality":"lossless","imgvid_cond_noise_aug_for_inference":null,"audio_cond_noise_aug_for_inference":null}}`
	if string(encoded) != want {
		t.Fatalf("parameters = %s\nwant       = %s", encoded, want)
	}
}

func TestFreezeDefaultsTextRequestAndDerivesStableSeed(t *testing.T) {
	first, err := Freeze("text only", "fast", "same-key", Request{})
	if err != nil {
		t.Fatalf("Freeze first: %v", err)
	}
	second, err := Freeze("text only", "fast", "same-key", Request{})
	if err != nil {
		t.Fatalf("Freeze second: %v", err)
	}
	if first.Parameters.CanonicalRequest.Seed != second.Parameters.CanonicalRequest.Seed ||
		first.Parameters.CanonicalRequest.Seed < 0 {
		t.Fatalf("derived seeds = %d/%d", first.Parameters.CanonicalRequest.Seed, second.Parameters.CanonicalRequest.Seed)
	}
	if first.Parameters.CanonicalRequest.Task != "t2va" ||
		len(first.Parameters.CanonicalRequest.Conditions) != 0 || len(first.RootInputs) != 0 {
		t.Fatalf("default request = %#v", first)
	}
	if first.Parameters.Sampling.NumInferenceSteps != 20 || first.Parameters.Sampling.Quality != "high" {
		t.Fatalf("default sampling = %#v", first.Parameters.Sampling)
	}
	different, err := Freeze("text only", "fast", "different-key", Request{})
	if err != nil {
		t.Fatalf("Freeze different: %v", err)
	}
	if different.Parameters.CanonicalRequest.Seed == first.Parameters.CanonicalRequest.Seed {
		t.Fatal("different idempotency keys derived the same seed")
	}
}

func TestFreezeRejectsUnmaterializableOrAmbiguousRootInputs(t *testing.T) {
	cases := []struct {
		name      string
		condition Condition
	}{
		{name: "non HTTPS fetch", condition: validCondition("http://objects.example.test/input")},
		{name: "credential in authority", condition: validCondition("https://user:password@objects.example.test/input")},
		{name: "invalid digest", condition: func() Condition {
			value := validCondition("https://objects.example.test/input")
			value.SHA256 = "ABC"
			return value
		}()},
		{name: "empty material", condition: func() Condition {
			value := validCondition("https://objects.example.test/input")
			value.SizeBytes = 0
			return value
		}()},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := Freeze("conditioned", "balanced", "key", Request{
				Task: "ref2va", Conditions: []Condition{test.condition},
			})
			if err == nil {
				t.Fatal("Freeze accepted invalid root input")
			}
		})
	}
}

func validCondition(downloadURL string) Condition {
	return Condition{
		Role: "reference", Type: "image", URI: "vela://uploads/reference",
		DownloadURL: downloadURL,
		SHA256:      "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
		SizeBytes:   4096,
	}
}

func float64Pointer(value float64) *float64 { return &value }
