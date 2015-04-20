package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"runtime"
	"sync"
	"time"
)

var (
	listen           = flag.String("l", ":8888", "port to accept requests")
	targetProduction = flag.String("a", "localhost:8080", "where production traffic goes. localhost:8080")
	altTarget        = flag.String("b", "localhost:8081", "where testing traffic goes. response are skipped. localhost:8081")
	linger           = flag.Duration("linger", 200*time.Millisecond, "time to finish reading from b before terminating connection")
	debug            = flag.Bool("debug", false, "debug logging")
	timeout          = flag.Duration("timeout", 1*time.Second, "total request timeout")
	deadline         = flag.Duration("deadline", 100*time.Millisecond, "deadline to establish connections to b")
	logThreshold     = flag.Duration("log-threshold", 500*time.Millisecond, "request duration before logging")
)

var ErrTimeout = errors.New("timeout")

// a garbage buffer to accept all reads into from b
var garbage = make([]byte, 64*1024)

type Tee struct {
	a io.ReadWriteCloser
	b io.ReadWriteCloser

	// buffer writes coming in so they don't need to block
	buf *bufio.Writer

	closed bool
}

// Provide the io.Writer interface to Tee
func (t *Tee) Write(p []byte) (n int, err error) {
	t.buf.Write(p)
	go t.buf.Flush()
	return t.a.Write(p)
}

// Provider the io.Reader interface to Tee
func (t *Tee) Read(p []byte) (n int, err error) {
	go t.b.Read(garbage[0:len(p)])
	return t.a.Read(p)
}

// Provide the io.Closer interface to Tee
func (t *Tee) Close() error {
	// Allow to only be closed once
	if t.closed {
		return nil
	}
	t.closed = true

	// At this point, we need to lazily close the "b" connection
	// Meaning, we want to make a best effort to drain it's reads
	// before hard closing. This will prevent the "b" side of the pipe
	// from getting pipes closed unexpectedly
	// BUT we also don't want to wait around forever
	go func() {
		debugLog("[DEBUG] lingering for b to disconnect")

		// Start the final drain of the socket
		c := make(chan struct{}, 1)
		go func() {
			io.Copy(ioutil.Discard, t.b)
			c <- struct{}{}
		}()
		select {
		case <-c:
			// Drain finished, and the backend closed the socket
			debugLog("[DEBUG] b closed connection")
		case <-time.After(*linger):
			// We waited too long, forcibly close this shit
			debugLog("[DEBUG] forcing b closed")
		}
		debugLog("[DEBUG] finished draining tee")

		// Close the socket
		t.b.Close()
		// Release Tee back to it's pool for reuse
		teePool.Put(t)
	}()
	// Forcibly close our "a" pipe when client disconnects
	return t.a.Close()
}

var teePool = &sync.Pool{
	New: func() interface{} {
		var t Tee
		return &t
	},
}

func NewTee(a, b io.ReadWriteCloser) io.ReadWriteCloser {
	t := teePool.Get().(*Tee)
	t.closed = false
	t.a = a
	t.b = b
	t.buf = bufio.NewWriter(b)
	return t
}

func HandleTCP(conn, out io.ReadWriteCloser) {
	// read data from client and write into server(s)
	debugLog("[DEBUG] proxying teh bytes")

	var wg sync.WaitGroup
	// Copy bytes from client and write to server(s)
	wg.Add(2)

	c := make(chan struct{}, 0)
	go func() {
		go func() {
			io.Copy(out, conn)
			// client got an error or EOF, so disconnect
			conn.Close()
			out.Close()
			debugLog("[DEBUG] client disconnected")
			wg.Done()
		}()

		// Copy bytes from server(s) and write to client
		go func() {
			io.Copy(conn, out)
			// server got an error or EOF, so disconnect
			out.Close()
			conn.Close()
			debugLog("[DEBUG] server disconnected")
			wg.Done()
		}()

		wg.Wait()
		c <- struct{}{}
	}()

	// Don't let the request take forever
	select {
	case <-c:
		// finished on time
	case <-time.After(*timeout):
		log.Println("[ERROR] Connection timeout!")
	}

	// Assert that we're cleaned up
	conn.Close()
	out.Close()
}

func debugLog(a ...interface{}) {
	if !*debug {
		return
	}
	log.Println(a...)
}

func TeeConnectTimeout(conn net.Conn, targetAddr, altAddr string) (out io.ReadWriteCloser, err error) {
	start := time.Now()
	defer func() {
		debugLog("[DEBUG] connect time", time.Now().Sub(start))
	}()

	starta := time.Now()
	// Establish our connection to "a" socket
	out, err = net.DialTimeout("tcp", targetAddr, 500*time.Second)
	debugLog("[DEBUG] connect to a", time.Now().Sub(starta))
	if err != nil {
		log.Println("[ERROR] Could not connect to 'a', closing.", err)
		// if we can't even connect to a, there's no point in continuing
		return
	}

	var b net.Conn
	startb := time.Now()
	b, err = net.DialTimeout("tcp", altAddr, *deadline)
	debugLog("[DEBUG] connect to b", time.Now().Sub(startb))
	if err == nil {
		out = NewTee(out, b)
	} else {
		log.Println("[ERROR] Could not connect to 'b', ignoring.", err)
		// we can't connect to b, but it doesn't really matter
		err = nil
	}
	return
}

func tee(conn net.Conn, targetAddr, altAddr string) {
	// log.Println(fmt.Sprintf("[INFO] new connection %s", conn.RemoteAddr()))
	start := time.Now()

	starta := time.Now()
	out, err := TeeConnectTimeout(conn, targetAddr, altAddr)
	enda := time.Now()
	if err != nil {
		log.Println("[ERROR] Couldn't establish connection to upstream!", err)
		conn.Close()
		return
	}

	startb := time.Now()
	HandleTCP(conn, out)
	endb := time.Now()

	end := time.Now()

	// Log any request that took longer than our threshold
	if end.Sub(start) > *logThreshold {
		log.Println(fmt.Sprintf("[INFO] total: %s, conn: %s, read: %s", end.Sub(start), enda.Sub(starta), endb.Sub(startb)))
	}
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("teeproxy")
	fmt.Println("listen:", *listen)
	fmt.Println("a:", *targetProduction)
	fmt.Println("b:", *altTarget)
	fmt.Println("deadline:", *deadline)
	fmt.Println("linger:", *linger)
	fmt.Println("log-threshold:", *logThreshold)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}

	// dat concurrent Accept
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					log.Println(err)
					continue
				}
				go tee(conn, *targetProduction, *altTarget)
			}
		}()
	}

	select {}
}
