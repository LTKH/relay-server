package streams

import (
  "net/http"
  "log"
  "time"
  "io/ioutil"
  //"bytes"
  "strings"
  //"crypto/sha1"
  "encoding/json"
  "crypto/md5"
  "encoding/hex"
  "relay-server/config"
  //"github.com/gadelkareem/cachita"
  //"github.com/gokyle/filecache"
  "github.com/peterbourgon/diskv"
  //"github.com/allegro/bigcache"
  //"cmd/go/internal/cache"
  //"gopkg.in/stash.v1"
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
  Locat      string
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

  if r.URL.Path == "/write" {

    //reading request body
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      log.Printf("[error] %v - %s", err, r.URL.Path)
    }

    for _, locat := range m.Location {
      select {
        case req_chan[locat] <- &Query{
          Method: "POST",
          Url:    locat+r.URL.Path,
          Auth:   r.Header.Get("Authorization"),
          Query:  r.URL.Query().Encode(),
          Body:   string(body),
          Locat:  locat,
        }:
        default:
          log.Printf("[error] channel is not ready - %s", locat)
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

    for _, locat := range m.Location {
      if req_chan[locat] != nil && len(req_chan[locat]) > m.Max_queue {
        log.Printf("[error] max queue exceeded - %s %d", locat, len(req_chan[locat]))
        continue
      }

      answBody, answCode := request(
        &Query{
          Method: r.Method,
          Url:    locat+r.URL.Path,
          Auth:   r.Header.Get("Authorization") ,
          Query:  r.URL.Query().Encode(),
          Body:   string(body),
          Locat:  locat,
        },
        m.Timeout,
      )

      if answCode >= 400 {
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

func Sender(locat string, cfg config.Config) {

  for {
    if len(job_chan) < cfg.Write.Threads {

      batch := map[string]*Batch{}

      for i := 0; i < cfg.Batch.Size; i++ {
        select {
          case r := <- req_chan[locat]:
            if batch[r.Query] == nil {
              batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
            }
            for _, bd := range strings.Split(r.Body, "\n") {
              if bd != "" {
                batch[r.Query].Body = append(batch[r.Query].Body, bd)
              }
            }
            if len(batch[r.Query].Body) > cfg.Batch.Size {
              goto End
            }
          default:
            continue
        }
      }

      End:

      for q, r := range batch{
        job_chan[locat] <- 1
        go func(q string, locat string, r *Batch, cfg config.Config){
          body := strings.Join(r.Body, "\n")

          for i := 1; i <= cfg.Write.Repeat; i++ {

            query := &Query{ "POST", locat+"/write", r.Auth, q, body, locat }
            _, code := request(query, cfg.Write.Timeout)

            if code < 500 {
              break
            }

            if i == cfg.Write.Repeat {

              d := diskv.New(diskv.Options{
            		BasePath:     cfg.Batch.Cache_dir,
            		CacheSizeMax: cfg.Batch.Cache_size,
            	})

              out, err := json.Marshal(query)
              if err != nil {
                log.Printf("[error] %v", err)
                break
              }

              data := []byte(out)
              hasher := md5.New()
              hasher.Write(data)

              err = d.Write(hex.EncodeToString(hasher.Sum(nil)), data)
              if err != nil {
                log.Printf("[error] %v", err)
                break
              }
              log.Printf("[info] added request to cache - %s (%d)", query.Url, code)

            }

            time.Sleep(cfg.Write.Delay_time * time.Second)
          }
          <- job_chan[locat]
        }(q, locat, r, cfg)
      }
    }

    time.Sleep(cfg.Batch.Max_wait * time.Second)
  }
}
