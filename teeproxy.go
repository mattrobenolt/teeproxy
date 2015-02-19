package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime"
)

var (
	listen           = flag.String("l", ":8888", "port to accept requests")
	targetProduction = flag.String("a", "localhost:8080", "where production traffic goes. localhost:8080")
	altTarget        = flag.String("b", "localhost:8081", "where testing traffic goes. response are skipped. localhost:8081")
)

type DubbleReverseProxy struct {
	Target      *httputil.ReverseProxy
	Alternative *httputil.ReverseProxy
}

func (dp *DubbleReverseProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	outreq := new(http.Request)
	*outreq = *req // includes shallow copies of maps, but okay
	go dp.Alternative.ServeHTTP(&NoopResponseWriter{make(http.Header)}, outreq)
	dp.Target.ServeHTTP(rw, req)
}

func NewDubbleProxy(target, alternative *url.URL) *DubbleReverseProxy {
	targetProxy := httputil.NewSingleHostReverseProxy(target)
	altProxy := httputil.NewSingleHostReverseProxy(alternative)
	return &DubbleReverseProxy{targetProxy, altProxy}
}

type NoopResponseWriter struct {
	header http.Header
}

func (r *NoopResponseWriter) Header() http.Header {
	return r.header
}
func (*NoopResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}
func (*NoopResponseWriter) WriteHeader(int) {}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	targetURL, _ := url.Parse("http://" + *targetProduction)
	altURL, _ := url.Parse("http://" + *altTarget)

	s := &http.Server{
		Addr:    *listen,
		Handler: NewDubbleProxy(targetURL, altURL),
	}
	log.Fatal(s.ListenAndServe())
}
