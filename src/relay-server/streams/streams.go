package streams

import (
  "net/http"
  "log"
  "time"
  "io/ioutil"
  //"bytes"
  "strings"
  "regexp"
  //"crypto/sha1"
  //"sync/atomic"
  "sync"
  "encoding/json"
  "crypto/md5"
  "encoding/hex"
  "relay-server/config"
)

var (
  Req_chan = make(map[string](chan *Query))
  Job_chan = make(map[string](chan int))
  //Stt_rexp = make(map[string](*Limits))
  //Stt_stat = make(map[string](sync.Map))
  Stt_stat = make(map[string](*Limits))
)

type Limits struct {
  Stat         sync.Map
  Regexp       *regexp.Regexp
  Replace      string
  Limit        int
}

type Write struct {
  Location     []string
  Timeout      time.Duration
}

type Read struct {
  Location     []string
  Timeout      time.Duration
  Max_threads  int
}

type Query struct {
  Method       string
  Url          string
  Auth         string
  Query        string
  Body         string
  Locat        string
}

type Batch struct {
  Auth         string
  Body         []string
}

func (m *Write) ServeHTTP(w http.ResponseWriter, r *http.Request) {

  if r.URL.Path == "/write" {

    //reading request body
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      log.Printf("[error] %v - %s", err, r.URL.Path)
    }

    //parsing request body
    for _, line := range strings.Split(string(body), "\n") {
      for key, limit := range Stt_stat {
        if !limit.Regexp.MatchString(line){
          continue
        }
        tag := limit.Regexp.ReplaceAllString(line, limit.Replace)

        v, ok := limit.Stat.Load(tag)
        if ok {
          val := v.(int)
          if limit.Limit > 0 && val > limit.Limit {
            w.WriteHeader(503)
            return
          }
          Stt_stat[key].Stat.Store(tag, val + 1)
        } else {
          Stt_stat[key].Stat.Store(tag, 1)
        }
      }
    }

    for _, locat := range m.Location {
      select {
        case Req_chan[locat] <- &Query{
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
      if Job_chan[locat] != nil && len(Job_chan[locat]) > m.Max_threads {
        log.Printf("[error] max threads exceeded - %s %d", locat, len(Req_chan[locat]))
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

func Repeat(query *Query, cfg config.Config) {

  Job_chan[query.Locat] <- 1

  for i := 1; i <= cfg.Write.Repeat; i++ {

    _, code := request(query, cfg.Write.Timeout)
    if code < 500 { 
      break
    }
    
    if i == cfg.Write.Repeat && cfg.Cache.Enabled {
      
      out, err := json.MarshalIndent(query, "", "")
      if err != nil {
        log.Printf("[error] %v", err)
        break
      }

      hasher := md5.New()
      hasher.Write(out)

      path := cfg.Cache.Directory+"/"+hex.EncodeToString(hasher.Sum(nil))

      if err := ioutil.WriteFile(path, out, 0644); err != nil {
        log.Printf("[error] creating cache file: %v", err)
      } else {
        log.Printf("[info] added request to cache - %s (%d)", query.Url, code)
      }

    }

    time.Sleep(cfg.Write.Delay_time * time.Second)
  }

  <- Job_chan[query.Locat]
}

func Sender(locat string, cfg config.Config) {

  for {
    if len(Job_chan) < cfg.Write.Threads {

      batch := map[string]*Batch{}

      for i := 0; i < cfg.Batch.Size; i++ {
        select {
          case r := <- Req_chan[locat]:
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
        
        query := &Query{ "POST", locat+"/write", r.Auth, q, strings.Join(r.Body, "\n"), locat }
        go Repeat(query, cfg) 

      }
    }

    time.Sleep(cfg.Batch.Max_wait * time.Second)
  }
}
