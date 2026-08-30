package inbox

import "testing"

func TestNewJetStreamConsumerWithPostCommitHookRequiresHook(t *testing.T) {
	consumer, err := NewJetStreamConsumerWithPostCommitHook(nil, nil)
	if err == nil || consumer != nil {
		t.Fatalf("consumer = %#v, error = %v", consumer, err)
	}
}
