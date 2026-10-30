package sysinfo

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Host describes the machine the station runs on. Everything here comes from
// the kernel, so it describes the HOST even inside a container (/proc is the
// host's): the kernel release, the architecture, the CPU model, the core count,
// the host's uptime and its load average.
//
// The distribution name is the one exception — it lives in /etc/os-release,
// which inside a container belongs to the image. The station therefore reads
// the host's copy when it is mounted for it (see hostOSReleasePaths) and leaves
// the row out when no distribution is known, instead of showing the image's.
type Host struct {
	// OS is the host distribution ("Debian GNU/Linux 13 (trixie)"), empty
	// when the host's os-release is not readable.
	OS string
	// Kernel is the running kernel release ("6.12.47+rpt-rpi-v8").
	Kernel string
	// Arch is the machine architecture ("aarch64", "x86_64").
	Arch string
	// CPU is the processor model. On x86 it is the "model name"; on a
	// Raspberry Pi the board model, which is what an operator recognises.
	CPU string
	// Cores is the number of processors the kernel reports.
	Cores int
	// Uptime is how long the HOST has been up (not the station process).
	Uptime time.Duration
	// Load is the host load average ("0.41 0.28 0.20"), empty when the
	// kernel does not report it.
	Load string
}

// hostOSReleasePaths are tried in order. Inside a container /etc/os-release is
// the image's, so a deployment that wants the distro row mounts the host's file
// at /host/os-release (deploy/README.md); a native install reads it directly.
var hostOSReleasePaths = []string{
	"/host/os-release",
	"/etc/os-release",
}

// Host returns the machine description: the static parts detected once at
// construction, the uptime and load read on every poll.
func (s *Sampler) Host() Host {
	h := s.host
	if d, ok := readUptime("/proc/uptime"); ok {
		h.Uptime = d
	}
	if load, ok := readLoad("/proc/loadavg"); ok {
		h.Load = load
	}
	return h
}

// detectHost reads the static machine description once.
func detectHost() Host {
	h := Host{
		Kernel: readFirstLine("/proc/sys/kernel/osrelease"),
		Arch:   readMachine(),
	}
	for _, path := range hostOSReleasePaths {
		if body, err := os.ReadFile(path); err == nil {
			if name := parseOSRelease(string(body)); name != "" {
				h.OS = name
				break
			}
		}
	}
	cpuinfo, _ := os.ReadFile("/proc/cpuinfo")
	h.CPU = parseCPUModel(string(cpuinfo))
	if h.CPU == "" {
		// No CPU description in cpuinfo (aarch64 kernels often carry none):
		// the device tree names the board, which is what an operator knows.
		h.CPU = strings.Trim(string(mustReadFile("/proc/device-tree/model")), "\x00\n ")
	}
	h.Cores = countProcessors(string(cpuinfo))
	if h.Cores == 0 {
		h.Cores = runtime.NumCPU()
	}
	return h
}

// parseOSRelease returns the human-readable distribution name.
func parseOSRelease(body string) string {
	fields := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		fields[strings.TrimSpace(key)] = strings.Trim(strings.TrimSpace(value), `"'`)
	}
	if name := fields["PRETTY_NAME"]; name != "" {
		return name
	}
	if name := fields["NAME"]; name != "" {
		return name
	}
	return ""
}

// parseCPUModel finds the processor description in /proc/cpuinfo. x86 hosts
// carry "model name"; ARM kernels are inconsistent, so the ARM part number is
// translated to the core name people recognise ("Cortex-A72").
func parseCPUModel(cpuinfo string) string {
	keys := []string{"model name", "Model", "Hardware", "cpu model", "Processor"}
	part := ""
	sc := bufio.NewScanner(strings.NewReader(cpuinfo))
	for sc.Scan() {
		key, value, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		for _, want := range keys {
			if key == want && value != "" {
				return value
			}
		}
		if key == "CPU part" && part == "" {
			part = value
		}
	}
	if name := armCoreName(part); name != "" {
		return name
	}
	return ""
}

// armCoreNames maps the ARM "CPU part" register to the marketing name, so a
// Raspberry Pi does not have to report "0xd08" to its operator.
var armCoreNames = map[string]string{
	"0xd03": "Cortex-A53",
	"0xd04": "Cortex-A35",
	"0xd05": "Cortex-A55",
	"0xd07": "Cortex-A57",
	"0xd08": "Cortex-A72",
	"0xd0b": "Cortex-A76",
	"0xd0c": "Neoverse-N1",
	"0xd40": "Neoverse-V1",
	"0xd49": "Neoverse-N2",
}

// armCoreName translates an ARM part number; unknown parts stay unnamed
// instead of guessing.
func armCoreName(part string) string {
	return armCoreNames[strings.ToLower(strings.TrimSpace(part))]
}

// countProcessors counts the logical processors the kernel reports.
func countProcessors(cpuinfo string) int {
	n := 0
	sc := bufio.NewScanner(strings.NewReader(cpuinfo))
	for sc.Scan() {
		if key, _, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(key) == "processor" {
			n++
		}
	}
	return n
}

// readMachine reports the machine architecture from uname.
func readMachine() string {
	var uts unix.Utsname
	if err := unix.Uname(&uts); err != nil {
		return runtime.GOARCH
	}
	return string(byteSlice(uts.Machine[:]))
}

// byteSlice trims a uname character array.
func byteSlice(b []byte) []byte {
	for i, c := range b {
		if c == 0 {
			return b[:i]
		}
	}
	return b
}

// readUptime reports how long the host has been up.
func readUptime(path string) (time.Duration, bool) {
	body, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	field, _, _ := strings.Cut(strings.TrimSpace(string(body)), " ")
	secs, err := strconv.ParseFloat(field, 64)
	if err != nil || secs <= 0 {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// readLoad reports the host load average as the kernel prints it.
func readLoad(path string) (string, bool) {
	body, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	fields := strings.Fields(string(body))
	if len(fields) < 3 {
		return "", false
	}
	return strings.Join(fields[:3], " "), true
}

// readFirstLine reads a single-line file (kernel release), trimmed.
func readFirstLine(path string) string {
	body, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	line, _, _ := strings.Cut(string(body), "\n")
	return strings.TrimSpace(line)
}

// mustReadFile reads a file that may legitimately be absent.
func mustReadFile(path string) []byte {
	body, _ := os.ReadFile(path)
	return body
}
