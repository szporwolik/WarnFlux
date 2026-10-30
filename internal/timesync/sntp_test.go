package timesync

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// TestQueryNTPRoundTrip talks to the station's own server: the offset the
// client computes must be the offset the server's clock has, because that is
// exactly what the LAN relies on while off-grid.
func TestQueryNTPRoundTrip(t *testing.T) {
	const skew = 3 * time.Second
	addr := freeUDPAddr(t)
	srv := &Server{
		Stratum: func() uint8 { return 1 },
		Now:     func() time.Time { return time.Now().Add(skew) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx, addr, ready) }()
	<-ready

	offset, rtt, err := QueryNTP(ctx, addr, 2*time.Second, nil)
	if err != nil {
		t.Fatalf("QueryNTP: %v", err)
	}
	if diff := offset - skew; absDuration(diff) > 200*time.Millisecond {
		t.Fatalf("offset = %s, want about %s", offset, skew)
	}
	if rtt < 0 || rtt > time.Second {
		t.Fatalf("rtt = %s, want a small non-negative value", rtt)
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Serve returned %v", err)
	}
}

// TestServerAnswer pins the packet a client parses: mode 4, the request's
// transmit timestamp echoed as "originate", and the server's own clock.
func TestServerAnswer(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 500000000, time.UTC)
	srv := &Server{Stratum: func() uint8 { return 2 }, Now: func() time.Time { return now }}
	req := make([]byte, sntpPacketSize)
	req[0] = 0b00_011_011 // LI 0, version 3, mode 3 (client)
	putTimestamp(req[40:], time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC))

	resp := srv.answer(req)
	if mode := resp[0] & 0b111; mode != 4 {
		t.Fatalf("mode = %d, want 4 (server)", mode)
	}
	if version := (resp[0] >> 3) & 0b111; version != 3 {
		t.Fatalf("version = %d, want the request's 3", version)
	}
	if resp[1] != 2 {
		t.Fatalf("stratum = %d, want 2", resp[1])
	}
	if got := readTimestamp(resp[24:]); !got.Equal(time.Date(2026, 10, 29, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("originate = %s, want the client's transmit timestamp", got)
	}
	if got := readTimestamp(resp[40:]); !got.Equal(now) {
		t.Fatalf("transmit = %s, want %s", got, now)
	}
	if got := readTimestamp(resp[32:]); !got.Equal(now) {
		t.Fatalf("receive = %s, want %s", got, now)
	}
	if string(resp[12:16]) != "WFLX" {
		t.Fatalf("reference id = %q, want WFLX", resp[12:16])
	}
}

// TestQueryNTPRejectsKissOfDeath keeps a server that refuses to answer from
// being read as "offset zero": a kiss-o'-death (stratum 0) is an error.
func TestQueryNTPRejectsKissOfDeath(t *testing.T) {
	addr := fakeNTPServer(t, func(resp []byte) {
		resp[0] = 0b00_100_100
		resp[1] = 0
		putTimestamp(resp[32:], time.Now())
		putTimestamp(resp[40:], time.Now())
	})
	if _, _, err := QueryNTP(context.Background(), addr, 2*time.Second, nil); err == nil {
		t.Fatal("kiss-o'-death must be an error")
	}
}

// TestQueryNTPTimeout proves the probe cannot hang the discipline pass.
func TestQueryNTPTimeout(t *testing.T) {
	addr := freeUDPAddr(t) // nobody listens here
	start := time.Now()
	_, _, err := QueryNTP(context.Background(), addr, 200*time.Millisecond, nil)
	if err == nil {
		t.Fatal("a silent server must time out")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("probe took %s, want about the timeout", elapsed)
	}
}

// freeUDPAddr reserves a localhost port the test can reuse.
func freeUDPAddr(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := conn.LocalAddr().String()
	conn.Close()
	return addr
}

// fakeNTPServer answers one request with the bytes the test writes.
func fakeNTPServer(t *testing.T, fill func(resp []byte)) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	go func() {
		buf := make([]byte, 512)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil || n < sntpPacketSize {
				return
			}
			resp := make([]byte, sntpPacketSize)
			copy(resp[24:32], buf[40:48])
			fill(resp)
			binary.BigEndian.PutUint32(resp[16:20], uint32(time.Now().Unix()+ntpEpochOffset))
			if _, err := conn.WriteToUDP(resp, from); err != nil {
				return
			}
		}
	}()
	return conn.LocalAddr().String()
}
