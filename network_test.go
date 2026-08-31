package natpmp

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockUDPServer runs a temporary UDP server on loopback for testing.
type mockUDPServer struct {
	conn      *net.UDPConn
	port      int
	t         *testing.T
	handler   func(req []byte, src *net.UDPAddr) []byte
	lastSrcIP net.IP
	lastSrcMu sync.Mutex
	closeOnce sync.Once
	stopChan  chan struct{}
}

func startMockServer(t *testing.T, handler func(req []byte, src *net.UDPAddr) []byte) *mockUDPServer {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("failed to start mock UDP server: %v", err)
	}
	s := &mockUDPServer{
		conn:     conn,
		port:     conn.LocalAddr().(*net.UDPAddr).Port,
		t:        t,
		handler:  handler,
		stopChan: make(chan struct{}),
	}
	go s.serve()
	return s
}

func (s *mockUDPServer) serve() {
	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-s.stopChan:
				return
			default:
				return
			}
		}
		s.lastSrcMu.Lock()
		s.lastSrcIP = remoteAddr.IP
		s.lastSrcMu.Unlock()

		reqCopy := make([]byte, n)
		copy(reqCopy, buf[:n])

		resp := s.handler(reqCopy, remoteAddr)
		if resp != nil {
			_, _ = s.conn.WriteToUDP(resp, remoteAddr)
		}
	}
}

func (s *mockUDPServer) getLastSrcIP() net.IP {
	s.lastSrcMu.Lock()
	defer s.lastSrcMu.Unlock()
	return s.lastSrcIP
}

func (s *mockUDPServer) close() {
	s.closeOnce.Do(func() {
		close(s.stopChan)
		_ = s.conn.Close()
	})
}

func TestNetworkGetExternalAddress(t *testing.T) {
	expectedExternalIP := [4]byte{203, 0, 113, 42}
	expectedEpoch := uint32(123456)

	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		if len(req) != 2 || req[0] != 0 || req[1] != 0 {
			return nil
		}
		resp := make([]byte, 12)
		resp[0] = 0                           // Vers 0
		resp[1] = 0x80                        // Op 0 + 128
		writeNetworkOrderUint16(resp[2:4], 0) // Result code 0
		writeNetworkOrderUint32(resp[4:8], expectedEpoch)
		copy(resp[8:12], expectedExternalIP[:])
		return resp
	})
	defer server.close()

	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), port: server.port},
		timeout: 2 * time.Second,
	}

	res, err := client.GetExternalAddress()
	if err != nil {
		t.Fatalf("GetExternalAddress failed: %v", err)
	}
	if res.SecondsSinceStartOfEpoc != expectedEpoch {
		t.Errorf("expected epoch %d, got %d", expectedEpoch, res.SecondsSinceStartOfEpoc)
	}
	if res.ExternalIPAddress != expectedExternalIP {
		t.Errorf("expected IP %v, got %v", expectedExternalIP, res.ExternalIPAddress)
	}
}

func TestNetworkAddPortMapping(t *testing.T) {
	expectedInternalPort := uint16(8080)
	expectedMappedPort := uint16(9090)
	expectedLifetime := uint32(3600)
	expectedEpoch := uint32(654321)

	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		if len(req) != 12 || req[0] != 0 || req[1] != 2 {
			return nil
		}
		resp := make([]byte, 16)
		resp[0] = 0                           // Vers 0
		resp[1] = 0x82                        // Op 2 + 128 (TCP)
		writeNetworkOrderUint16(resp[2:4], 0) // Result code 0
		writeNetworkOrderUint32(resp[4:8], expectedEpoch)
		writeNetworkOrderUint16(resp[8:10], expectedInternalPort)
		writeNetworkOrderUint16(resp[10:12], expectedMappedPort)
		writeNetworkOrderUint32(resp[12:16], expectedLifetime)
		return resp
	})
	defer server.close()

	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), port: server.port},
		timeout: 2 * time.Second,
	}

	res, err := client.AddPortMapping("tcp", int(expectedInternalPort), int(expectedMappedPort), int(expectedLifetime))
	if err != nil {
		t.Fatalf("AddPortMapping failed: %v", err)
	}
	if res.InternalPort != expectedInternalPort {
		t.Errorf("expected internal port %d, got %d", expectedInternalPort, res.InternalPort)
	}
	if res.MappedExternalPort != expectedMappedPort {
		t.Errorf("expected mapped external port %d, got %d", expectedMappedPort, res.MappedExternalPort)
	}
	if res.PortMappingLifetimeInSeconds != expectedLifetime {
		t.Errorf("expected lifetime %d, got %d", expectedLifetime, res.PortMappingLifetimeInSeconds)
	}
	if res.SecondsSinceStartOfEpoc != expectedEpoch {
		t.Errorf("expected epoch %d, got %d", expectedEpoch, res.SecondsSinceStartOfEpoc)
	}
}

func TestNetworkLocalIPBinding(t *testing.T) {
	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		resp := make([]byte, 12)
		resp[0] = 0
		resp[1] = 0x80
		return resp
	})
	defer server.close()

	localIP := net.IPv4(127, 0, 0, 1)
	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), local: localIP, port: server.port},
		timeout: 2 * time.Second,
	}

	_, err := client.GetExternalAddress()
	if err != nil {
		t.Fatalf("GetExternalAddress failed: %v", err)
	}

	srcIP := server.getLastSrcIP()
	if !srcIP.Equal(localIP) {
		t.Errorf("expected mock server to observe src IP %v, got %v", localIP, srcIP)
	}
}

func TestNetworkTimeoutAndRetry(t *testing.T) {
	var attempts int32

	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		current := atomic.AddInt32(&attempts, 1)
		// Drop the first 2 requests, respond on the 3rd attempt
		if current < 3 {
			return nil
		}
		resp := make([]byte, 12)
		resp[0] = 0
		resp[1] = 0x80
		return resp
	})
	defer server.close()

	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), port: server.port},
		timeout: 3 * time.Second,
	}

	_, err := client.GetExternalAddress()
	if err != nil {
		t.Fatalf("expected successful retry, got error: %v", err)
	}
	if total := atomic.LoadInt32(&attempts); total < 3 {
		t.Errorf("expected at least 3 attempts, got %d", total)
	}
}

func TestNetworkServerErrors(t *testing.T) {
	// Server responds with Result Code 3: Network Failure
	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		resp := make([]byte, 12)
		resp[0] = 0
		resp[1] = 0x80
		writeNetworkOrderUint16(resp[2:4], 3)
		return resp
	})
	defer server.close()

	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), port: server.port},
		timeout: 2 * time.Second,
	}

	_, err := client.GetExternalAddress()
	if err == nil {
		t.Fatalf("expected error from server, got nil")
	}
	expectedSubstring := "Non-zero result code 3 (Network Failure (box has no DHCP lease or external IP))"
	if err.Error() != expectedSubstring {
		t.Errorf("expected error %q, got %q", expectedSubstring, err.Error())
	}
}

func TestNetworkClientConstructors(t *testing.T) {
	gw := net.IPv4(192, 168, 1, 1)
	local := net.IPv4(192, 168, 1, 100)
	to := 5 * time.Second

	c1 := NewClient(gw)
	n1 := c1.caller.(*network)
	if !n1.gateway.Equal(gw) || n1.local != nil || c1.timeout != 0 {
		t.Errorf("NewClient failed to initialize fields correctly")
	}

	c2 := NewClientWithTimeout(gw, to)
	n2 := c2.caller.(*network)
	if !n2.gateway.Equal(gw) || n2.local != nil || c2.timeout != to {
		t.Errorf("NewClientWithTimeout failed to initialize fields correctly")
	}

	c3 := NewClientWithLocal(gw, local)
	n3 := c3.caller.(*network)
	if !n3.gateway.Equal(gw) || !n3.local.Equal(local) || c3.timeout != 0 {
		t.Errorf("NewClientWithLocal failed to initialize fields correctly")
	}

	c4 := NewClientWithLocalAndTimeout(gw, local, to)
	n4 := c4.caller.(*network)
	if !n4.gateway.Equal(gw) || !n4.local.Equal(local) || c4.timeout != to {
		t.Errorf("NewClientWithLocalAndTimeout failed to initialize fields correctly")
	}
}

func TestNetworkConcurrency(t *testing.T) {
	server := startMockServer(t, func(req []byte, src *net.UDPAddr) []byte {
		resp := make([]byte, 12)
		resp[0] = 0
		resp[1] = 0x80
		writeNetworkOrderUint16(resp[2:4], 0)
		copy(resp[8:12], []byte{1, 2, 3, 4})
		return resp
	})
	defer server.close()

	client := &Client{
		caller:  &network{gateway: net.IPv4(127, 0, 0, 1), port: server.port},
		timeout: 3 * time.Second,
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := client.GetExternalAddress()
			if err != nil {
				t.Errorf("concurrent call failed: %v", err)
				return
			}
			if !bytes.Equal(res.ExternalIPAddress[:], []byte{1, 2, 3, 4}) {
				t.Errorf("unexpected IP address %v", res.ExternalIPAddress)
			}
		}()
	}
	wg.Wait()
}
