package main

import (
	"fmt"
	"regexp"
	"strings"
)

// Devicetree codename -> name. Only models listed in nativeABI get installed: the daemon drives
// model-specific hardware (mic array + VQE preset, LEDs, clock, inputs).
func modelName(code string) string {
	switch code {
	case "cucumber":
		return "Yandex Station Midi"
	case "plum":
		return "Yandex Station Lite 2"
	}
	return ""
}

// nativeABI is the userspace the known firmware of this model ships. "" = not supported.
// The installed binary is build/<arch>/pulsar for that ABI when its dynamic loader is present.
func nativeABI(code string) string {
	switch code {
	case "cucumber":
		return "aarch64"
	case "plum":
		return "armhf"
	}
	return ""
}

// archOf picks build/<arch>/ from the loaders actually on the station. uname and the codename are
// not enough: some Midi images run a 32-bit userspace on the 64-bit kernel (#13) and then need
// the armhf build (its ALSA ABI is the one in audio/alsa_abi.h, #14). Lite 2 stays armhf-only.
// A 64-bit plum image has not been seen, and the aarch64 binary needs glibc 2.34, so installing
// it on a miss would crash-loop back to the Yandex stack (no .prev on a first install).
func archOf(code string, aarch64, armhf bool) string {
	switch nativeABI(code) {
	case "aarch64":
		if aarch64 {
			return "aarch64"
		}
		if code == "cucumber" && armhf {
			return "armhf"
		}
	case "armhf":
		if armhf {
			return "armhf"
		}
	}
	return ""
}

// hasZigbee: the model has the Zigbee radio the daemon can run as a router (radio/), so the router
// firmware image is installed next to the daemon.
func hasZigbee(code string) bool { return code == "cucumber" }

type station struct {
	serial, arch string
	zigbee       bool
}

// found: installable stations, in the order printed by probe.
var found []station

var codename = regexp.MustCompile(`yandex,([a-z0-9_]*)`)

// probe prints one status line for s and records it in found if it is installable. A device we
// connected to ourselves is disconnected again when it is not.
func probe(s string, ours bool) {
	skip := func(msg string) {
		fmt.Printf("  %s: %s\n", s, msg)
		if ours {
			disconnect(s)
		}
	}
	compat, err := sh(s, `tr '\0' ' ' </proc/device-tree/compatible`)
	if err != nil {
		skip("adb not usable (unauthorized or offline)")
		return
	}
	m := codename.FindStringSubmatch(compat)
	if m == nil || m[1] == "" {
		skip("not a Yandex Station")
		return
	}
	code := m[1]
	name := modelName(code)
	if name == "" {
		name = "unknown Yandex device"
	}
	name += " (" + code + ")"
	if nativeABI(code) == "" {
		skip(name + " — not supported yet, skipped")
		return
	}
	// `; true`: a missing loader is not an error. Without it the last [ -x ] exits 1 on a
	// normal Midi (no armhf loader) and sh() reports a failure that is not an adb failure.
	loaders, err := sh(s, `[ -x /lib/ld-linux-aarch64.so.1 ] && echo aarch64; [ -x /lib/ld-linux-armhf.so.3 ] && echo armhf; true`)
	if err != nil {
		skip("adb not usable (unauthorized or offline)")
		return
	}
	arch := archOf(code, strings.Contains(loaders, "aarch64"), strings.Contains(loaders, "armhf"))
	if arch == "" {
		skip(name + " — unsupported userland ABI, skipped")
		return
	}
	if uid, _ := sh(s, "id -u"); uid != "0" {
		skip(name + " — adb is not root, skipped")
		return
	}
	var state string
	if _, err := sh(s, "pidof pulsar >/dev/null"); err == nil {
		state = "satellite running"
	} else if _, err := sh(s, "[ -x "+dataDir+"/pulsar ]"); err == nil {
		state = "satellite installed, not running"
	} else {
		state = "stock Yandex"
	}
	found = append(found, station{s, arch, hasZigbee(code)})
	note := ""
	if arch != nativeABI(code) {
		note = ", " + arch + " build"
	}
	fmt.Printf("  [%d] %s: %s — %s%s\n", len(found), s, name, state, note)
}
