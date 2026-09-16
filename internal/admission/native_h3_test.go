package admission

import (
	"github.com/vivym/vela/internal/h3request"
	"testing"
)

func TestNativeH3RejectsUnsupportedSamplingBeforeWorkerAssignment(t *testing.T) {
	for _, test := range []struct {
		name     string
		sampling h3request.Sampling
		valid    bool
	}{
		{"implicit high", h3request.Sampling{}, false},
		{"native release", h3request.Sampling{NumInferenceSteps: 20, Quality: "lossless"}, true},
		{"wrong steps", h3request.Sampling{NumInferenceSteps: 30, Quality: "lossless"}, false},
		{"high", h3request.Sampling{NumInferenceSteps: 20, Quality: "high"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := Request{Model: "minimax-h3-live-validation", GenerationPreset: "fast", ServiceClass: "standard", OutputSpec: "h3-native-av-1344x768-5s-24fps", GenerationCount: 1, Prompt: "lake", H3: &h3request.Request{Sampling: test.sampling}}
			content, _, err := canonicalRequest(request, "native-sampling-test")
			if err != nil {
				t.Fatal(err)
			}
			err = validateNativeH3Request(request, content)
			if (err == nil) != test.valid {
				t.Fatalf("validation=%v, want valid=%v", err, test.valid)
			}
		})
	}
}
