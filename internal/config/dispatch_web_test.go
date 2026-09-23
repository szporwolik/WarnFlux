package config

import (
	"strings"
	"testing"
	"time"
)

const dispatchExample = `
app:
  log_level: info
storage:
  driver: sqlite
  path: /data/warnflux.db
sources: []
outputs: []
dispatch:
  queue_size: 2048
  mqtt_receivers:
    - id: local
      enabled: true
      broker: tcp://mosquitto:1883
      client_id: warnflux-dispatch-local
      connect_timeout: 5s
      keep_alive: 45s
      warnflux:
        enabled: true
        topic_prefix: warnflux
      subscriptions:
        - topic: "home/#"
          qos: 2
        - topic: "alarm/+/state"
    - id: remote-club
      enabled: false
      broker: tcp://10.10.10.10:1883
      client_id: warnflux-dispatch-club
      warnflux:
        enabled: false
      subscriptions:
        - topic: "club/#"
          qos: 1
web:
  enabled: true
  listen: ":8080"
  title: "WarnFlux"
  name: "WarnFlux Ops"
  header1: "SPOK"
  header2: "Społeczna Platforma Ostrzegania i Komunikacji"
  tagline: "od społeczności • dla mieszkańców • w trosce o bezpieczeństwo"
  domain: "spok.example.com"
  auth:
    username: admin
    password: change-me
    secure_cookie: false
actions:
  - id: logger-action
    type: logger
    enabled: false
    runtime:
      queue_size: 64
      call_timeout: 3s
      shutdown_timeout: 3s
    config:
      level: warn
`

func TestLoadDispatchWebActions(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, dispatchExample))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Dispatch.QueueSize != 2048 {
		t.Errorf("queue_size = %d", cfg.Dispatch.QueueSize)
	}
	if len(cfg.Dispatch.Receivers) != 2 {
		t.Fatalf("receivers = %d", len(cfg.Dispatch.Receivers))
	}
	r := cfg.Dispatch.Receivers[0]
	if r.ID != "local" || !r.Enabled || r.Broker != "tcp://mosquitto:1883" {
		t.Errorf("receiver = %+v", r)
	}
	if r.ClientID != "warnflux-dispatch-local" {
		t.Errorf("client_id = %q", r.ClientID)
	}
	if r.ConnectTimeout != 5*time.Second || r.KeepAlive != 45*time.Second {
		t.Errorf("timeouts = %s %s", r.ConnectTimeout, r.KeepAlive)
	}
	if !r.WF.Enabled || r.WF.TopicPrefix != "warnflux" {
		t.Errorf("warnflux mode = %+v", r.WF)
	}
	if len(r.Subscriptions) != 2 || r.Subscriptions[0].QoS != 2 || r.Subscriptions[1].QoS != 1 {
		t.Errorf("subscriptions = %+v", r.Subscriptions)
	}
	remote := cfg.Dispatch.Receivers[1]
	if remote.Enabled || remote.WF.Enabled {
		t.Errorf("remote receiver unexpectedly enabled: %+v", remote)
	}
	if !cfg.Web.Enabled || cfg.Web.Listen != ":8080" || cfg.Web.Title != "WarnFlux" || cfg.Web.Name != "WarnFlux Ops" {
		t.Errorf("web = %+v", cfg.Web)
	}
	if cfg.Web.Header1 != "SPOK" || cfg.Web.Header2 != "Społeczna Platforma Ostrzegania i Komunikacji" {
		t.Errorf("web headers = %q / %q", cfg.Web.Header1, cfg.Web.Header2)
	}
	if cfg.Web.Tagline != "od społeczności • dla mieszkańców • w trosce o bezpieczeństwo" {
		t.Errorf("web tagline = %q", cfg.Web.Tagline)
	}
	if cfg.Web.Domain != "spok.example.com" {
		t.Errorf("web domain = %q, want spok.example.com", cfg.Web.Domain)
	}
	if cfg.Web.Auth.Username != "admin" || cfg.Web.Auth.Password != "change-me" {
		t.Errorf("web auth = %+v", cfg.Web.Auth)
	}
	if len(cfg.Actions) != 1 {
		t.Fatalf("actions = %d", len(cfg.Actions))
	}
	a := cfg.Actions[0]
	if a.ID != "logger-action" || a.Type != "logger" || a.Enabled {
		t.Errorf("action = %+v", a)
	}
	if a.Runtime.QueueSize != 64 || a.Runtime.CallTimeout != 3*time.Second || a.Runtime.ShutdownTimeout != 3*time.Second {
		t.Errorf("action runtime = %+v", a.Runtime)
	}
	if a.Config == nil {
		t.Error("action config node missing")
	}
}

func TestDispatchDefaults(t *testing.T) {
	cfg, err := Load(writeTempConfig(t, `
app:
  log_level: info
storage:
  driver: sqlite
dispatch:
  mqtt_receivers:
    - id: local
      enabled: true
      broker: tcp://mosquitto:1883
      client_id: warnflux-dispatch-local
      warnflux: {}
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Dispatch.QueueSize != 1024 {
		t.Errorf("default queue_size = %d", cfg.Dispatch.QueueSize)
	}
	r := cfg.Dispatch.Receivers[0]
	if r.ConnectTimeout != 10*time.Second || r.KeepAlive != 30*time.Second {
		t.Errorf("default receiver timeouts = %s %s", r.ConnectTimeout, r.KeepAlive)
	}
	if !r.WF.Enabled || r.WF.TopicPrefix != "warnflux" {
		t.Errorf("default warnflux mode = %+v", r.WF)
	}
}

func TestStrictDecodingRejectsTypos(t *testing.T) {
	for name, doc := range map[string]string{
		"mqtt_recivers": `dispatch:
  mqtt_recivers: []`,
		"clientid": `dispatch:
  mqtt_receivers:
    - id: local
      enabled: true
      clientid: x`,
		"que_size": `dispatch:
  que_size: 10`,
		"web_typo": `web:
  enabeld: true`,
		"action_runtime_typo": `actions:
  - id: a
    type: logger
    runtime:
      que_size: 10`,
	} {
		body := "app:\n  log_level: info\nstorage:\n  driver: sqlite\n" + doc
		if _, err := Load(writeTempConfig(t, body)); err == nil {
			t.Errorf("%s: typo accepted, want rejection", name)
		}
	}
}

func TestReceiverValidation(t *testing.T) {
	base := func(receiver string) string {
		return "app:\n  log_level: info\nstorage:\n  driver: sqlite\ndispatch:\n  mqtt_receivers:\n" + receiver
	}
	cases := map[string]string{
		"duplicate ids": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
    - id: local
      enabled: true
      broker: tcp://b:1883
      client_id: c2`,
		"bad id": `    - id: "Upper"
      enabled: true
      broker: tcp://a:1883
      client_id: c1`,
		"missing broker": `    - id: local
      enabled: true
      client_id: c1`,
		"missing client_id": `    - id: local
      enabled: true
      broker: tcp://a:1883`,
		"password xor file": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      password: p
      password_file: /f`,
		"bad prefix wildcard": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      warnflux:
        topic_prefix: "warnflux/#"`,
		"bad qos": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      subscriptions:
        - topic: "a/#"
          qos: 3`,
		"empty filter": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      subscriptions:
        - topic: ""`,
		"hash not last": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      subscriptions:
        - topic: "a/#/b"`,
		"no input at all": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1`,
		"negative connect timeout": `    - id: local
      enabled: true
      broker: tcp://a:1883
      client_id: c1
      connect_timeout: -1s`,
	}
	for name, receiver := range cases {
		if _, err := Load(writeTempConfig(t, base(receiver))); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

func TestWebAuthValidation(t *testing.T) {
	cases := map[string]string{
		"missing username": `web:
  enabled: true
  auth:
    password: p`,
		"missing password": `web:
  enabled: true
  auth:
    username: u`,
		"password xor file": `web:
  enabled: true
  auth:
    username: u
    password: p
    password_file: /f`,
	}
	for name, doc := range cases {
		body := "app:\n  log_level: info\nstorage:\n  driver: sqlite\n" + doc
		if _, err := Load(writeTempConfig(t, body)); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
	// Disabled web needs no credentials.
	if _, err := Load(writeTempConfig(t, "app:\n  log_level: info\nstorage:\n  driver: sqlite\nweb:\n  enabled: false\n")); err != nil {
		t.Errorf("disabled web rejected: %v", err)
	}
}

func TestActionValidation(t *testing.T) {
	// One ID namespace across sources, outputs and actions.
	body := "app:\n  log_level: info\nstorage:\n  driver: sqlite\n" + `
sources:
  - id: logger-action
    type: imgw
actions:
  - id: logger-action
    type: logger
`
	if _, err := Load(writeTempConfig(t, body)); err == nil || !strings.Contains(err.Error(), "duplicate plugin id") {
		t.Errorf("duplicate id across namespaces accepted: %v", err)
	}
	// Bad queue size.
	body2 := "app:\n  log_level: info\nstorage:\n  driver: sqlite\n" + `
actions:
  - id: a
    type: logger
    runtime:
      queue_size: 0
`
	if _, err := Load(writeTempConfig(t, body2)); err == nil {
		t.Error("queue_size 0 accepted, want rejection")
	}
	// Bounds.
	body3 := "app:\n  log_level: info\nstorage:\n  driver: sqlite\n" + `
actions:
  - id: a
    type: logger
    runtime:
      queue_size: 100001
`
	if _, err := Load(writeTempConfig(t, body3)); err == nil {
		t.Error("oversized queue accepted, want rejection")
	}
}

func TestDispatchQueueSizeBounds(t *testing.T) {
	for _, size := range []string{"0", "100001"} {
		body := "app:\n  log_level: info\nstorage:\n  driver: sqlite\ndispatch:\n  queue_size: " + size + "\n"
		if _, err := Load(writeTempConfig(t, body)); err == nil {
			t.Errorf("queue_size %s accepted, want rejection", size)
		}
	}
}
