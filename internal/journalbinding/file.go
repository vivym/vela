package journalbinding

import (
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func ReadVerifierFile(path string) (*Verifier, error) {
	keys, err := stageauthority.ReadVerifierKeyringFile(path)
	if err != nil {
		return nil, err
	}
	defer stageauthority.ClearKeyring(keys)
	return NewVerifier(keys)
}

func LoadFile(path string, verifier *Verifier) (*velav1.WorkerBootstrapBinding, error) {
	wire, err := securefile.Read(path, MaximumBytes, true)
	if err != nil {
		return nil, err
	}
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return nil, err
	}
	value := &velav1.WorkerBootstrapBinding{}
	if err := protojson.Unmarshal(wire, value); err != nil {
		return nil, err
	}
	return verifier.Verify(value)
}

func Encode(value *velav1.WorkerBootstrapBinding) ([]byte, error) {
	if err := validate(value, true); err != nil {
		return nil, err
	}
	wire, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(value)
	if err != nil || len(wire) > MaximumBytes {
		return nil, ErrInvalid
	}
	return wire, nil
}
