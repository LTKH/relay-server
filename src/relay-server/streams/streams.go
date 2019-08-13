package streams

import (
  "net/http"
  "log"
  "time"
  "io/ioutil"
  //"bytes"
  "strings"
  //"crypto/sha1"
  //"encoding/json"
  "relay-server/config"
  //"github.com/gadelkareem/cachita"
  //"github.com/gokyle/filecache"
  //"github.com/peterbourgon/diskv"
  //"github.com/allegro/bigcache"
  //"cmd/go/internal/cache"
)

var (
  req_chan = make(map[string](chan *Query))
  job_chan = make(map[string](chan int))
)

type Write struct {
  Location   []string
  Timeout    time.Duration
}

type Read struct {
  Location   []string
  Timeout    time.Duration
  Max_queue  int
}

type Query struct {
  Method     string
  Url        string
  Auth       string
  Query      string
  Body       string
}

type Batch struct {
  Auth       string
  Body       []string
}

func GetReqChan() map[string](chan *Query) {
  return req_chan
}

func GetJobChan() map[string](chan int) {
  return job_chan
}

func (m *Write) ServeHTTP(w http.ResponseWriter, r *http.Request) {

  if r.URL.Path == "/ping" {
    w.Header().Add("X-InfluxDB-Version", "relay-server")
    w.WriteHeader(204)
    return
  }

  if r.URL.Path == "/write" {

    //reading request body
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      log.Printf("[error] %v - %s", err, r.URL.Path)
    }

    for _, url := range m.Location {
      select {
        case req_chan[url] <- &Query{
          Method: "POST",
          Url:    url+r.URL.Path,
          Auth:   r.Header.Get("Authorization"),
          Query:  r.URL.Query().Encode(),
          Body:   string(body),
        }:
        default:
          log.Printf("[error] cahhel is not ready - %s", url)
      }
    }
    w.WriteHeader(204)
    return
  }

  w.WriteHeader(400)
}

func (m *Read) ServeHTTP(w http.ResponseWriter, r *http.Request) {

  if r.URL.Path == "/query" || r.URL.Path == "/ping" {

    //reading request body
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      log.Printf("[error] %v - %s", err, r.URL.Path)
      return
    }

    for _, url := range m.Location {
      if req_chan[url] != nil && len(req_chan[url]) > m.Max_queue {
        log.Printf("[error] max queue exceeded - %s %d", url, len(req_chan[url]))
        continue
      }

      answBody, answCode := request(
        &Query{
          r.Method,
          url+r.URL.Path,
          r.Header.Get("Authorization") ,
          r.URL.Query().Encode(),
          string(body),
        },
        m.Timeout,
      )

      if answCode >= 400 {
        log.Printf("[error] answer code - %s %d", url, answCode)
        continue
      }

      w.WriteHeader(answCode)
      w.Header().Add("Content-Type", "application/json")
      w.Write(answBody)
      return
    }

    w.WriteHeader(500)
    return
  }

  w.WriteHeader(400)
  return
}

func request(query *Query, tout time.Duration) ([]byte, int) {
  client := &http.Client{ Timeout: time.Duration(tout * time.Second) }

  req, err := http.NewRequest(query.Method, query.Url, strings.NewReader(query.Body))
  if err != nil {
    log.Printf("[error] %v %d", err, http.StatusServiceUnavailable)
    return []byte(err.Error()), http.StatusServiceUnavailable
  }

  req.URL.RawQuery = query.Query
  if query.Auth != "" {
    req.Header.Set("Authorization", query.Auth)
  }

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

/*
func cachePut(query *Query) bool {
  out, err := json.Marshal(query)
  if err != nil {
      log.Printf("[error] %v", err)
      return false
  }
  sh := sha1.New()
  sh.Write([]byte(out))
  key := sh.Sum(nil)

  //cache, err := cachita.File()
  cache, err := cachita.NewFileCache("/Users/dmitry/Documents/relay-server/cache", 1, 0)
  if err != nil {
    log.Printf("[error] %v", err)
    return false
  }

  err = cache.Put(string(key), query, 60*time.Minute)
  if err != nil {
    log.Printf("[error] %v", err)
    return false
  }

  log.Printf("[info] save cache - %x", key)
  return true

  //log.Printf("[info] %v", cache.readData())

  //var qu Query
  //err = cache.Get(string(key), &qu)
  //if err != nil && err != cachita.ErrNotFound {
  //    panic(err)
  //}
  //log.Printf("%v", qu) //prints "some data"

  cache := filecache.NewDefaultCache()
  cache.MaxSize = 128 * filecache.Megabyte
  cache.Start()

  buf := bytes.NewBufferString(string(out))
  err = cache.WriteFile(buf, "/Users/dmitry/Documents/relay-server/cache/123")
  if err != nil {
      log.Printf("[error] %v", err)
  }

  // Initialize a new diskv store, rooted at "my-data-dir", with a 1MB cache.
  d := diskv.New(diskv.Options{
    BasePath:     "/Users/dmitry/Documents/relay-server/cache",
    Transform:    func(s string) []string { return []string{} },
    CacheSizeMax: 1024 * 1024,
  })

  // Write three bytes to the key "alpha".
  err = d.Write(string(key), []byte(out))
  if err != nil {
      log.Printf("[error] %v", err)
  }


  //cache, _ := bigcache.NewBigCache(bigcache.DefaultConfig(10 * time.Minute))
  //cache.Set(string(key), []byte(string(out)))

}
*/

func Sender(url string, cfg config.Config) {
  for {
    if len(job_chan) < cfg.Write.Threads {

      batch := map[string]*Batch{}

      for ok := true; ok; ok = (len(req_chan[url]) > 0) {
        select {
          case r := <- req_chan[url]:
            if batch[r.Query] == nil {
              batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
            }
            for _, bd := range strings.Split(r.Body, "\n") {
              if bd != "" {
                batch[r.Query].Body = append(batch[r.Query].Body, bd)
              }
            }
            if len(batch[r.Query].Body) > cfg.Batch.Size {
              break
            }
        }
      }

      for q, r := range batch{
        job_chan[url] <- 1
        go func(q string, url string, r *Batch, cfg config.Config){
          body := strings.Join(r.Body, "\n")

          for i := 1; i <= cfg.Batch.Repeat; i++ {

            query := &Query{ "POST", url+"/write", r.Auth, q, body }
            _, code := request(query, cfg.Write.Timeout)

            if code == 200 || code == 204 {
              break
            }

            if i == cfg.Batch.Repeat {
              //cachePut(query)
            }

            time.Sleep(cfg.Batch.Delay_time * time.Second)
          }
          <- job_chan[url]
        }(q, url, r, cfg)
      }
    }

    time.Sleep(cfg.Batch.Max_wait * time.Second)
  }
}
