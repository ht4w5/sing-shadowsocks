package shadowaead_2022_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"sync"
	"testing"

	shadowsocks "github.com/ht4w5/sing-shadowsocks"
	"github.com/ht4w5/sing-shadowsocks/shadowaead_2022"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func TestMultiService(t *testing.T) {
	t.Parallel()
	method := "2022-blake3-aes-128-gcm"
	var iPSK [16]byte
	rand.Reader.Read(iPSK[:])

	var wg sync.WaitGroup

	multiService, err := shadowaead_2022.NewMultiService[string](method, iPSK[:], 500, &multiHandler{t, &wg}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var uPSK [16]byte
	rand.Reader.Read(uPSK[:])
	multiService.UpdateUsers([]string{"my user"}, [][]byte{uPSK[:]})

	client, err := shadowaead_2022.New(method, [][]byte{iPSK[:], uPSK[:]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)

	serverConn, clientConn := net.Pipe()
	defer common.Close(serverConn, clientConn)
	go func() {
		err := multiService.NewConnection(context.Background(), serverConn, M.Metadata{})
		if err != nil {
			serverConn.Close()
			t.Error(E.Cause(err, "server"))
			return
		}
	}()
	_, err = client.DialConn(clientConn, M.ParseSocksaddr("test.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

type multiHandler struct {
	t  *testing.T
	wg *sync.WaitGroup
}

func (h *multiHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	if metadata.Destination.String() != "test.com:443" {
		h.t.Error("bad destination")
	}
	h.wg.Done()
	return nil
}

func (h *multiHandler) NewPacketConnection(ctx context.Context, conn N.PacketConn, metadata M.Metadata) error {
	return nil
}

func (h *multiHandler) NewError(ctx context.Context, err error) {
	h.t.Error(ctx, err)
}

func TestMultiServiceAddUsers(t *testing.T) {
	t.Parallel()
	method := "2022-blake3-aes-128-gcm"
	var iPSK [16]byte
	rand.Reader.Read(iPSK[:])

	var wg sync.WaitGroup

	multiService, err := shadowaead_2022.NewMultiService[string](method, iPSK[:], 500, &multiHandler{t, &wg}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Add user via AddUsers
	var uPSK [16]byte
	rand.Reader.Read(uPSK[:])
	err = multiService.AddUsers([]shadowsocks.UserKey[string]{{User: "test-user", Key: uPSK[:]}})
	if err != nil {
		t.Fatal(err)
	}

	// Verify user can connect
	client, err := shadowaead_2022.New(method, [][]byte{iPSK[:], uPSK[:]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)

	serverConn, clientConn := net.Pipe()
	defer common.Close(serverConn, clientConn)
	go func() {
		err := multiService.NewConnection(context.Background(), serverConn, M.Metadata{})
		if err != nil {
			serverConn.Close()
			t.Error(E.Cause(err, "server"))
			return
		}
	}()
	_, err = client.DialConn(clientConn, M.ParseSocksaddr("test.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestMultiServiceAddUsersWithPasswords(t *testing.T) {
	t.Parallel()
	method := "2022-blake3-aes-128-gcm"
	var iPSK [16]byte
	rand.Reader.Read(iPSK[:])

	var wg sync.WaitGroup

	multiService, err := shadowaead_2022.NewMultiService[string](method, iPSK[:], 500, &multiHandler{t, &wg}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Add user via AddUsersWithPasswords
	var uPSK [16]byte
	rand.Reader.Read(uPSK[:])
	password := base64Encode(uPSK[:])
	err = multiService.AddUsersWithPasswords([]shadowsocks.UserPassword[string]{{User: "test-user", Password: password}})
	if err != nil {
		t.Fatal(err)
	}

	// Verify user can connect
	client, err := shadowaead_2022.New(method, [][]byte{iPSK[:], uPSK[:]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)

	serverConn, clientConn := net.Pipe()
	defer common.Close(serverConn, clientConn)
	go func() {
		err := multiService.NewConnection(context.Background(), serverConn, M.Metadata{})
		if err != nil {
			serverConn.Close()
			t.Error(E.Cause(err, "server"))
			return
		}
	}()
	_, err = client.DialConn(clientConn, M.ParseSocksaddr("test.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestMultiServiceRemoveUsers(t *testing.T) {
	t.Parallel()
	method := "2022-blake3-aes-128-gcm"
	var iPSK [16]byte
	rand.Reader.Read(iPSK[:])

	var wg sync.WaitGroup

	multiService, err := shadowaead_2022.NewMultiService[string](method, iPSK[:], 500, &multiHandler{t, &wg}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Add user
	var uPSK [16]byte
	rand.Reader.Read(uPSK[:])
	err = multiService.UpdateUsers([]string{"test-user"}, [][]byte{uPSK[:]})
	if err != nil {
		t.Fatal(err)
	}

	// Remove user
	multiService.RemoveUsers([]string{"test-user"})

	// Verify user cannot connect
	client, err := shadowaead_2022.New(method, [][]byte{iPSK[:], uPSK[:]}, nil)
	if err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := net.Pipe()
	defer common.Close(serverConn, clientConn)
	sErrCh := make(chan error, 1)
	go func() {
		err := multiService.NewConnection(context.Background(), serverConn, M.Metadata{})
		serverConn.Close()
		sErrCh <- err
	}()
	_, err = client.DialConn(clientConn, M.ParseSocksaddr("test.com:443"))
	if err == nil {
		t.Fatal("expected error for removed user")
	}
	// Ensure server also got an error
	if sErr := <-sErrCh; sErr == nil {
		t.Fatal("expected server error for removed user")
	}
}

func TestMultiServiceConcurrentAccess(t *testing.T) {
	t.Parallel()
	method := "2022-blake3-aes-128-gcm"
	var iPSK [16]byte
	rand.Reader.Read(iPSK[:])

	multiService, err := shadowaead_2022.NewMultiService[string](method, iPSK[:], 500, &multiHandler{t, &sync.WaitGroup{}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Test concurrent AddUsers and RemoveUsers
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			var key [16]byte
			rand.Reader.Read(key[:])
			multiService.AddUsers([]shadowsocks.UserKey[string]{{User: fmt.Sprintf("user-%d", i), Key: key[:]}})
		}(i)
		go func(i int) {
			defer wg.Done()
			multiService.RemoveUsers([]string{fmt.Sprintf("user-%d", i)})
		}(i)
	}
	wg.Wait()
}

func base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}
