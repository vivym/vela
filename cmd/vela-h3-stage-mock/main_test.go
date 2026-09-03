package main

import "testing"

func TestComponentFromExecutable(t *testing.T) {
	for name, want := range map[string]string{
		"h3-encoder": "ENCODER", "h3-dit": "DIT", "h3-vae-decoder": "VAE_DECODER",
	} {
		got, err := componentFromExecutable(name)
		if err != nil || got != want {
			t.Fatalf("componentFromExecutable(%q)=%q error=%v want=%q", name, got, err, want)
		}
	}
	if _, err := componentFromExecutable("vela-h3-stage-mock"); err == nil {
		t.Fatal("componentFromExecutable accepted an unbound executable")
	}
}
