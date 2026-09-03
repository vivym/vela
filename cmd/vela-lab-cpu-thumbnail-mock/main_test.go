package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunBindsCPUThumbnailProtocolIdentity(t *testing.T) {
	var output bytes.Buffer
	err := run(
		context.Background(), nil, "stdio-json-v1",
		bytes.NewBufferString(`{"schema_version":1,"request_id":1,"operation":"shutdown"}`+"\n"),
		&output,
	)
	if err != nil {
		t.Fatalf("run CPU thumbnail mock: %v", err)
	}
	if !strings.Contains(output.String(), `"acknowledged":true`) {
		t.Fatalf("shutdown response = %q", output.String())
	}
}

func TestRunRejectsArgumentsAndWrongProtocol(t *testing.T) {
	for _, testCase := range []struct {
		arguments []string
		protocol  string
	}{
		{arguments: []string{"unexpected"}, protocol: "stdio-json-v1"},
		{protocol: "unknown"},
	} {
		if err := run(
			context.Background(), testCase.arguments, testCase.protocol,
			bytes.NewBuffer(nil), &bytes.Buffer{},
		); err == nil {
			t.Fatalf("run accepted arguments=%v protocol=%q", testCase.arguments, testCase.protocol)
		}
	}
}
