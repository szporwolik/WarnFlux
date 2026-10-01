package i18n

import "testing"

func TestCatalogParity(t *testing.T) {
	enKeys := map[string]bool{}
	plKeys := map[string]bool{}
	for k := range en {
		enKeys[k] = true
	}
	for k := range pl {
		plKeys[k] = true
	}
	for k := range enKeys {
		if !plKeys[k] {
			t.Errorf("key %q missing in PL", k)
		}
	}
	for k := range plKeys {
		if !enKeys[k] {
			t.Errorf("key %q missing in EN", k)
		}
	}
	for k, v := range en {
		if v == "" {
			t.Errorf("EN key %q is empty", k)
		}
	}
	for k, v := range pl {
		if v == "" {
			t.Errorf("PL key %q is empty", k)
		}
	}
	t.Logf("en=%d pl=%d", len(en), len(pl))
}
