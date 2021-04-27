package streams

import (
    "net/http"
    "log"
    "time"
    "regexp"
    "io/ioutil"
    "strings"
    "encoding/json"
    "crypto/md5"
    "encoding/hex"
    "github.com/influxdata/line-protocol"
    "github.com/ltkh/relay-server/internal/monitor"
    "github.com/prometheus/client_golang/prometheus"
)

type Write struct {
    Listen       string
    Locations    []struct {
        Urls         []string
        Cache        bool
        Regexp       []struct {
            Match        string
            Replace      string
        }
    }
    Timeout      time.Duration
    DelayTime    time.Duration
    Repeat       int
    CacheDir     string
}

type Query struct {
    Urls         []string
    Auth         string
    Query        string
    Body         []byte
}

func readUserIP(r *http.Request) string {
    IPAddress := r.Header.Get("X-Real-Ip")
    if IPAddress == "" {
        IPAddress = r.Header.Get("X-Forwarded-For")
    }
    return IPAddress
}

func (m *Write) ServeHTTP(w http.ResponseWriter, r *http.Request) {

    if r.URL.Path == "/ping" {
        w.WriteHeader(204)
        return
    }
  
    if r.URL.Path == "/write" {

        //reading request body
        body, err := ioutil.ReadAll(r.Body)
        if err != nil {
            log.Printf("[error] %v - %s", err, r.URL.Path)
        }
        defer r.Body.Close()

        lines := strings.Split(string(body), "\n")

        handler := protocol.NewMetricHandler()
        parser := protocol.NewParser(handler)

        rhost := readUserIP(r)

        //parsing request body
        for _, line := range lines {
            if line != "" {
                _, err := parser.Parse([]byte(line))
                if err != nil {
                    monitor.ErrCounter.With(prometheus.Labels{"rhost":rhost,"uri":r.RequestURI}).Inc()
                    log.Printf("[error] %v (%s)", err, rhost)
                }
                monitor.PntCounter.With(prometheus.Labels{"rhost":rhost,"uri":r.RequestURI}).Inc()
            }
        }

        monitor.ReqCounter.With(prometheus.Labels{"listen":m.Listen}).Inc()

        for _, locat := range m.Locations {

            for _, rexp := range locat.Regexp {
                re := regexp.MustCompile(rexp.Match)
                for _, line := range lines {
                    line = re.ReplaceAllString(line, rexp.Replace)
                }
            }

            query := &Query{
                Urls:   locat.Urls,
                Auth:   r.Header.Get("Authorization"),
                Query:  r.URL.Query().Encode(),
                Body:   []byte(strings.Join(lines, "\n")),
            }

            go Sender(query, m.Repeat, m.Timeout, m.DelayTime, locat.Cache, m.CacheDir)

        }

        w.WriteHeader(204)
        return
    }

    w.WriteHeader(404)
}

func Sender(query *Query, repeat int, timeout time.Duration, delay time.Duration, cache bool, cacheDir string){
    for i := 0; i <= repeat; i++ {
        for _, url := range query.Urls {
            _, code := request("POST", url, query.Query, query.Body, query.Auth, timeout)
            if code < 500 { 
                return
            } else {
                monitor.DrpCounter.With(prometheus.Labels{"url":url}).Inc()
            }
        }
        time.Sleep(delay * time.Second)
    }
    if cache {
        if err := cacheWrite(query, cacheDir); err != nil {
            log.Printf("[error] %v", err)
        }
    }
}

func request(method string, url string, query string, rbody []byte, auth string, timeout time.Duration) ([]byte, int) {
  
    client := &http.Client{ Timeout: time.Duration(timeout * time.Second) }

    req, err := http.NewRequest(method, url, strings.NewReader(string(rbody)))
    if err != nil {
        log.Printf("[error] %v %d", err, http.StatusServiceUnavailable)
        return []byte(err.Error()), http.StatusServiceUnavailable
    }

    req.URL.RawQuery = query
    if auth != "" {
        req.Header.Set("Authorization", auth)
    }
    req.Header.Add("Accept-Encoding", "identity")

    resp, err := client.Do(req)
    if err != nil {
        log.Printf("[error] %v %d", err, http.StatusServiceUnavailable)
        return []byte(err.Error()), http.StatusServiceUnavailable
    }
    defer resp.Body.Close() 

    //reading request body
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Printf("[error] %v %d", err, http.StatusServiceUnavailable)
        return []byte(err.Error()), http.StatusServiceUnavailable
    }
  
    return body, resp.StatusCode

}

func cacheWrite(query *Query, dir string) error {
    out, err := json.MarshalIndent(query, "", "")
    if err != nil {
        return err
    }

    hasher := md5.New()
    hasher.Write(out)

    path := dir+"/"+hex.EncodeToString(hasher.Sum(nil))

    if err := ioutil.WriteFile(path, out, 0644); err != nil {
        return err
    }

    return nil
}
