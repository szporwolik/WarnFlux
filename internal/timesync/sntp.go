package timesync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

// NTP epoch (1900-01-01) offset from the Unix epoch in seconds.
const ntpEpochOffset = 2208988800

// sntpPacketSize is the size of a v3/v4 client request; responses are the same.
const sntpPacketSize = 48

// QueryNTP asks one server for its time over plain SNTP (RFC 4330) and returns
// the offset this station's clock needs (server − local) together with the
// round-trip time. It never blocks longer than timeout.
func QueryNTP(ctx context.Context, server string, timeout time.Duration, dial func(ctx context.Context, network, addr string) (net.Conn, error)) (offset, rtt time.Duration, err error) {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if dial == nil {
		d := &net.Dialer{}
		dial = d.DialContext
	}
	conn, err := dial(ctx, "udp", server)
	if err != nil {
		return 0, 0, fmt.Errorf("dial ntp %s: %w", server, err)
	}
	defer conn.Close()
	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return 0, 0, fmt.Errorf("ntp %s: %w", server, err)
	}

	req := make([]byte, sntpPacketSize)
	req[0] = 0b00_100_011 // LI 0, version 3, mode 3 (client)
	t1 := time.Now()
	putTimestamp(req[40:], t1)
	if _, err := conn.Write(req); err != nil {
		return 0, 0, fmt.Errorf("ntp %s: write: %w", server, err)
	}

	resp := make([]byte, sntpPacketSize)
	n, err := conn.Read(resp)
	if err != nil {
		return 0, 0, fmt.Errorf("ntp %s: read: %w", server, err)
	}
	t4 := time.Now()
	if n < sntpPacketSize {
		return 0, 0, fmt.Errorf("ntp %s: short response (%d bytes)", server, n)
	}
	mode := resp[0] & 0b111
	if mode != 4 {
		return 0, 0, fmt.Errorf("ntp %s: unexpected mode %d", server, mode)
	}
	if stratum := resp[1]; stratum == 0 {
		return 0, 0, fmt.Errorf("ntp %s: kiss-o'-death (stratum 0)", server)
	}

	t2 := readTimestamp(resp[32:]) // server receive
	t3 := readTimestamp(resp[40:]) // server transmit
	if t2.IsZero() || t3.IsZero() {
		return 0, 0, fmt.Errorf("ntp %s: empty timestamps", server)
	}
	// RFC 4330: offset = ((T2 − T1) + (T3 − T4)) / 2, delay = (T4 − T1) − (T3 − T2).
	offset = (t2.Sub(t1) + t3.Sub(t4)) / 2
	rtt = t4.Sub(t1) - t3.Sub(t2)
	return offset, rtt, nil
}

// putTimestamp writes t as an NTP 64-bit timestamp.
func putTimestamp(b []byte, t time.Time) {
	sec := uint32(t.Unix() + ntpEpochOffset)
	frac := uint32(uint64(t.Nanosecond()) * (1 << 32) / 1e9)
	binary.BigEndian.PutUint32(b[0:4], sec)
	binary.BigEndian.PutUint32(b[4:8], frac)
}

// readTimestamp reads an NTP 64-bit timestamp; the zero value means "unset".
func readTimestamp(b []byte) time.Time {
	sec := binary.BigEndian.Uint32(b[0:4])
	frac := binary.BigEndian.Uint32(b[4:8])
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	return time.Unix(int64(sec)-ntpEpochOffset, int64(uint64(frac)*1e9>>32)).UTC()
}

// Server is the station's own SNTP server: while the station runs off-grid it
// is the LAN's only time source, so the answer is always the disciplined clock.
type Server struct {
	// Stratum reported to clients: 1 when the clock came from GPS or a
	// reference NTP server, 3 otherwise.
	Stratum func() uint8
	Now     func() time.Time
	Logger  Logger
}

// Logger is the subset of slog the daemon uses.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Debug(msg string, args ...any)
}

// Serve answers SNTP requests until ctx is cancelled.
func (s *Server) Serve(ctx context.Context, listen string, ready chan<- struct{}) error {
	addr, err := net.ResolveUDPAddr("udp", listen)
	if err != nil {
		return fmt.Errorf("sntp server %q: %w", listen, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("sntp server %q: %w", listen, err)
	}
	defer conn.Close()
	if ready != nil {
		close(ready)
	}
	if s.Logger != nil {
		s.Logger.Info("sntp server listening", "listen", conn.LocalAddr().String())
	}

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	buf := make([]byte, 512)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			// A malformed datagram must not stop the server.
			if s.Logger != nil {
				s.Logger.Debug("sntp server: read failed", "error", err)
			}
			continue
		}
		if n < sntpPacketSize {
			continue
		}
		resp := s.answer(buf[:n])
		if _, err := conn.WriteToUDP(resp, from); err != nil && s.Logger != nil {
			s.Logger.Debug("sntp server: write failed", "error", err, "client", from.String())
		}
	}
}

// answer builds the response to one request.
func (s *Server) answer(req []byte) []byte {
	resp := make([]byte, sntpPacketSize)
	stratum := uint8(3)
	if s.Stratum != nil {
		if st := s.Stratum(); st != 0 {
			stratum = st
		}
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	// LI 0, version copied from the request (3 when it says nothing), mode 4.
	version := (req[0] >> 3) & 0b111
	if version == 0 {
		version = 3
	}
	resp[0] = version<<3 | 0b100
	resp[1] = stratum
	resp[2] = 6    // poll
	resp[3] = 0xEC // precision 2^-20
	copy(resp[12:16], []byte("WFLX"))
	copy(resp[24:32], req[40:48])                                              // originate = client transmit
	binary.BigEndian.PutUint32(resp[16:20], uint32(now.Unix()+ntpEpochOffset)) // reference
	putTimestamp(resp[32:], now)                                               // receive
	putTimestamp(resp[40:], now)                                               // transmit
	return resp
}
