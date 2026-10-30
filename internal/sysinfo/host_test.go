package sysinfo

import (
	"os"
	"strings"
	"testing"
	"time"
)

// TestParseOSRelease covers the shapes the operators' hosts actually ship: a
// quoted pretty name, a bare one, comments and an unparseable line.
func TestParseOSRelease(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"debian", "PRETTY_NAME=\"Debian GNU/Linux 13 (trixie)\"\nNAME=\"Debian GNU/Linux\"\n", "Debian GNU/Linux 13 (trixie)"},
		{"no pretty name", "NAME=\"Alpine Linux\"\nVERSION_ID=3.20\n", "Alpine Linux"},
		{"comments and junk", "# a comment\nnot-a-pair\nPRETTY_NAME='Raspberry Pi OS'\n", "Raspberry Pi OS"},
		{"nothing usable", "ID=linux\n", ""},
	}
	for _, c := range cases {
		if got := parseOSRelease(c.body); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// TestParseCPUModel pins what the panel shows for the machines the station is
// deployed on: an x86 box names its CPU, a Raspberry Pi names its board, and an
// ARM kernel that only reports the part number is translated.
func TestParseCPUModel(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"x86", "processor\t: 0\nmodel name\t: Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz\n", "Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz"},
		{"board model", "processor\t: 0\nModel\t\t: Raspberry Pi 4 Model B Rev 1.5\n", "Raspberry Pi 4 Model B Rev 1.5"},
		{"arm part only", "processor\t: 0\nCPU implementer\t: 0x41\nCPU part\t: 0xd08\nCPU revision\t: 3\n", "Cortex-A72"},
		{"unknown part", "processor\t: 0\nCPU part\t: 0xfff\n", ""},
		{"empty", "", ""},
	}
	for _, c := range cases {
		if got := parseCPUModel(c.body); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// TestCountProcessors documents that the core count comes from the kernel's own
// list, so a container with a CPU limit still shows the host's processors.
func TestCountProcessors(t *testing.T) {
	body := "processor\t: 0\nmodel name\t: A\n\nprocessor\t: 1\nmodel name\t: A\n\nprocessor\t: 2\n"
	if got := countProcessors(body); got != 3 {
		t.Fatalf("processors = %d, want 3", got)
	}
	if got := countProcessors("model name: A\n"); got != 0 {
		t.Fatalf("processors = %d, want 0 without the key", got)
	}
}

// TestReadLoadAndUptime pins the two dynamic values: the load is shown as the
// kernel prints it, the uptime as a duration.
func TestReadLoadAndUptime(t *testing.T) {
	dir := t.TempDir()
	load := dir + "/loadavg"
	if err := writeFile(load, "0.41 0.28 0.20 1/394 18066\n"); err != nil {
		t.Fatal(err)
	}
	if got, ok := readLoad(load); !ok || got != "0.41 0.28 0.20" {
		t.Fatalf("load = %q (%v), want the three averages", got, ok)
	}

	up := dir + "/uptime"
	if err := writeFile(up, "10839.68 41268.29\n"); err != nil {
		t.Fatal(err)
	}
	got, ok := readUptime(up)
	if !ok {
		t.Fatal("uptime not parsed")
	}
	if want := time.Duration(10839.68 * float64(time.Second)); got != want {
		t.Fatalf("uptime = %s, want %s", got, want)
	}

	if _, ok := readLoad(dir + "/absent"); ok {
		t.Fatal("a missing file must report ok=false")
	}
	if _, ok := readUptime(dir + "/absent"); ok {
		t.Fatal("a missing file must report ok=false")
	}
}

// TestHostReportsThisMachine is the live check: on Linux the sampler must fill
// in the kernel, architecture and core count it is asked to display.
func TestHostReportsThisMachine(t *testing.T) {
	h := New().Host()
	if h.Kernel == "" {
		t.Error("kernel release is empty on Linux")
	}
	if h.Arch == "" {
		t.Error("architecture is empty")
	}
	if h.Cores < 1 {
		t.Errorf("cores = %d, want at least 1", h.Cores)
	}
	if h.Uptime <= 0 {
		t.Error("host uptime must be positive")
	}
	if !strings.Contains(h.Load, " ") {
		t.Errorf("load = %q, want three averages", h.Load)
	}
}

// writeFile writes a fixture file.
func writeFile(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o600)
}
