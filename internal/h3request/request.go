package h3request

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"strings"
	"unicode/utf8"
)

const (
	CanonicalRequestSchema  = "minimax_h3.request/v1"
	StageParametersRevision = 1
	maxRootInputBytes       = int64(32 * 1024 * 1024 * 1024)
	maxInferenceSteps       = 10_000
)

type Request struct {
	Task       string      `json:"task,omitempty"`
	Seed       *int64      `json:"seed,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
	Target     Target      `json:"target,omitempty"`
	Sampling   Sampling    `json:"sampling,omitempty"`
}

type Condition struct {
	Role             string   `json:"role"`
	Type             string   `json:"type"`
	URI              string   `json:"uri"`
	DownloadURL      string   `json:"download_url"`
	SHA256           string   `json:"sha256"`
	SizeBytes        int64    `json:"size_bytes"`
	FrameIndex       *int32   `json:"frame_index,omitempty"`
	StartTimeSeconds *float64 `json:"start_time_seconds,omitempty"`
}

type Target struct {
	ShortEdge       int      `json:"short_edge,omitempty"`
	AspectRatio     string   `json:"aspect_ratio,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
}

type Sampling struct {
	NumInferenceSteps                    int      `json:"num_inference_steps,omitempty"`
	Quality                              string   `json:"quality,omitempty"`
	ImageVideoConditionNoiseAugmentation *float64 `json:"imgvid_cond_noise_aug_for_inference,omitempty"`
	AudioConditionNoiseAugmentation      *float64 `json:"audio_cond_noise_aug_for_inference,omitempty"`
}

type FrozenRequest struct {
	Parameters Parameters  `json:"parameters"`
	RootInputs []RootInput `json:"root_inputs"`
}

type Parameters struct {
	SchemaRevision   int              `json:"schema_revision"`
	CanonicalRequest CanonicalRequest `json:"canonical_request"`
	Sampling         FrozenSampling   `json:"sampling"`
}

type CanonicalRequest struct {
	Schema         string               `json:"schema"`
	Task           string               `json:"task"`
	Prompt         string               `json:"prompt"`
	Conditions     []CanonicalCondition `json:"conditions"`
	Target         FrozenTarget         `json:"target"`
	Seed           int64                `json:"seed"`
	FlowShift      *float64             `json:"flow_shift,omitempty"`
	AudioFlowShift *float64             `json:"audio_flow_shift,omitempty"`
}

type CanonicalCondition struct {
	Role             string   `json:"role"`
	Type             string   `json:"type"`
	URI              string   `json:"uri"`
	FrameIndex       *int32   `json:"frame_index,omitempty"`
	StartTimeSeconds *float64 `json:"start_time_seconds,omitempty"`
}

type FrozenTarget struct {
	ShortEdge       int      `json:"short_edge"`
	AspectRatio     string   `json:"aspect_ratio"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
}

type FrozenSampling struct {
	NumInferenceSteps                    int      `json:"num_inference_steps"`
	Quality                              string   `json:"quality"`
	ImageVideoConditionNoiseAugmentation *float64 `json:"imgvid_cond_noise_aug_for_inference"`
	AudioConditionNoiseAugmentation      *float64 `json:"audio_cond_noise_aug_for_inference"`
}

type RootInput struct {
	ConditionIndex int    `json:"condition_index"`
	URI            string `json:"uri"`
	DownloadURL    string `json:"download_url"`
	SHA256         string `json:"sha256"`
	SizeBytes      int64  `json:"size_bytes"`
}

func Freeze(prompt, generationPreset, idempotencyKey string, request Request) (FrozenRequest, error) {
	if !validText(prompt, 80_000) || !validText(idempotencyKey, 128) {
		return FrozenRequest{}, errors.New("H3 request identity is invalid")
	}
	task := request.Task
	if task == "" {
		task = "t2va"
	}
	if task != "t2va" && task != "ref2va" && task != "fl2va" {
		return FrozenRequest{}, errors.New("H3 task is unsupported")
	}

	target, err := freezeTarget(request.Target)
	if err != nil {
		return FrozenRequest{}, err
	}
	sampling, err := freezeSampling(generationPreset, request.Sampling)
	if err != nil {
		return FrozenRequest{}, err
	}
	conditions := make([]CanonicalCondition, 0, len(request.Conditions))
	rootInputs := make([]RootInput, 0, len(request.Conditions))
	for index, condition := range request.Conditions {
		canonical, root, conditionErr := freezeCondition(index, condition)
		if conditionErr != nil {
			return FrozenRequest{}, conditionErr
		}
		conditions = append(conditions, canonical)
		rootInputs = append(rootInputs, root)
	}
	if task == "t2va" && len(conditions) != 0 {
		return FrozenRequest{}, errors.New("H3 t2va request cannot contain conditions")
	}
	if task != "t2va" && len(conditions) == 0 {
		return FrozenRequest{}, errors.New("conditioned H3 request requires root inputs")
	}

	canonical := CanonicalRequest{
		Schema: CanonicalRequestSchema, Task: task, Prompt: prompt,
		Conditions: conditions, Target: target,
	}
	if request.Seed != nil {
		if *request.Seed < 0 {
			return FrozenRequest{}, errors.New("H3 seed is invalid")
		}
		canonical.Seed = *request.Seed
	} else {
		canonical.Seed, err = deriveSeed(idempotencyKey, canonical, sampling, rootInputs)
		if err != nil {
			return FrozenRequest{}, err
		}
	}
	return FrozenRequest{
		Parameters: Parameters{
			SchemaRevision: StageParametersRevision, CanonicalRequest: canonical,
			Sampling: sampling,
		},
		RootInputs: rootInputs,
	}, nil
}

func freezeCondition(index int, condition Condition) (CanonicalCondition, RootInput, error) {
	if index < 0 || !validText(condition.Role, 100) || !validText(condition.Type, 100) ||
		!validText(condition.URI, 16*1024) || !validHTTPSDownloadURL(condition.DownloadURL) ||
		condition.SizeBytes <= 0 || condition.SizeBytes > maxRootInputBytes ||
		!validSHA256(condition.SHA256) {
		return CanonicalCondition{}, RootInput{}, errors.New("H3 condition root input is invalid")
	}
	if condition.FrameIndex != nil && *condition.FrameIndex < 0 {
		return CanonicalCondition{}, RootInput{}, errors.New("H3 condition frame index is invalid")
	}
	if condition.StartTimeSeconds != nil &&
		(!finite(*condition.StartTimeSeconds) || *condition.StartTimeSeconds < 0) {
		return CanonicalCondition{}, RootInput{}, errors.New("H3 condition start time is invalid")
	}
	return CanonicalCondition{
			Role: condition.Role, Type: condition.Type, URI: condition.URI,
			FrameIndex: condition.FrameIndex, StartTimeSeconds: condition.StartTimeSeconds,
		}, RootInput{
			ConditionIndex: index, URI: condition.URI, DownloadURL: condition.DownloadURL,
			SHA256: condition.SHA256, SizeBytes: condition.SizeBytes,
		}, nil
}

func freezeTarget(target Target) (FrozenTarget, error) {
	if target.ShortEdge == 0 {
		target.ShortEdge = 768
	}
	if target.AspectRatio == "" {
		target.AspectRatio = "16:9"
	}
	if target.DurationSeconds == nil {
		value := 5.0
		target.DurationSeconds = &value
	}
	if target.ShortEdge != 768 || !validText(target.AspectRatio, 100) ||
		!finite(*target.DurationSeconds) || *target.DurationSeconds <= 0 {
		return FrozenTarget{}, errors.New("H3 target is invalid")
	}
	return FrozenTarget(target), nil
}

func freezeSampling(generationPreset string, sampling Sampling) (FrozenSampling, error) {
	defaultSteps, defaultQuality := 0, ""
	switch generationPreset {
	case "quality":
		defaultSteps, defaultQuality = 50, "lossless"
	case "balanced":
		defaultSteps, defaultQuality = 30, "high"
	case "fast":
		defaultSteps, defaultQuality = 20, "high"
	default:
		return FrozenSampling{}, errors.New("H3 generation preset is invalid")
	}
	if sampling.NumInferenceSteps == 0 {
		sampling.NumInferenceSteps = defaultSteps
	}
	if sampling.Quality == "" {
		sampling.Quality = defaultQuality
	}
	if sampling.NumInferenceSteps <= 0 || sampling.NumInferenceSteps > maxInferenceSteps ||
		(sampling.Quality != "high" && sampling.Quality != "lossless") ||
		!optionalUnitFloat(sampling.ImageVideoConditionNoiseAugmentation) ||
		!optionalUnitFloat(sampling.AudioConditionNoiseAugmentation) {
		return FrozenSampling{}, errors.New("H3 sampling is invalid")
	}
	return FrozenSampling(sampling), nil
}

func deriveSeed(
	idempotencyKey string,
	canonical CanonicalRequest,
	sampling FrozenSampling,
	rootInputs []RootInput,
) (int64, error) {
	type seedRootInput struct {
		ConditionIndex int    `json:"condition_index"`
		URI            string `json:"uri"`
		SHA256         string `json:"sha256"`
		SizeBytes      int64  `json:"size_bytes"`
	}
	root := make([]seedRootInput, 0, len(rootInputs))
	for _, input := range rootInputs {
		root = append(root, seedRootInput{
			ConditionIndex: input.ConditionIndex, URI: input.URI,
			SHA256: input.SHA256, SizeBytes: input.SizeBytes,
		})
	}
	payload, err := json.Marshal(struct {
		IdempotencyKey string           `json:"idempotency_key"`
		Canonical      CanonicalRequest `json:"canonical_request"`
		Sampling       FrozenSampling   `json:"sampling"`
		RootInputs     []seedRootInput  `json:"root_inputs"`
	}{idempotencyKey, canonical, sampling, root})
	if err != nil {
		return 0, errors.New("encode H3 seed material")
	}
	digest := sha256.Sum256(append([]byte("vela/minimax-h3/seed/v1\x00"), payload...))
	return int64(binary.BigEndian.Uint64(digest[:8]) & math.MaxInt64), nil
}

func validHTTPSDownloadURL(value string) bool {
	if !validText(value, 16*1024) {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" &&
		parsed.User == nil && parsed.Fragment == ""
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func optionalUnitFloat(value *float64) bool {
	return value == nil || finite(*value) && *value >= 0 && *value <= 1
}

func finite(value float64) bool { return !math.IsInf(value, 0) && !math.IsNaN(value) }

func validText(value string, maximum int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= maximum &&
		utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}
