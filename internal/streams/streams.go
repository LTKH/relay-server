package streams

import (
  //"net"
  "net/http"
  _ "net/http/pprof"
  "log"
  "time"
  "io/ioutil"
  "strings"
  "regexp"
  "sync"
  "encoding/json"
  "crypto/md5"
  "encoding/hex"
  "github.com/ltkh/relay-server/internal/config"
)

var (
  Req_chan = make(map[string](chan *Query))
  Job_chan = make(map[string](chan int))
  Stt_stat = make(map[string](*Limits))
  Enabled  = make(chan int)
)

type Limits struct {
  Stat         sync.Map
  Regexp       *regexp.Regexp
  Replace      string
  Limit        int
  Drop         bool
}

type Write struct {
  Location     []string
  Timeout      time.Duration
}

type Request struct {
  *http.Request
  Body         []byte
}

type Query struct {
  Locat        string
  Auth         string
  Query        string
  Body         []byte
}

type Batch struct {
  Auth         string
  Body         []string
}

func (m *Write) ServeHTTP(w http.ResponseWriter, r *http.Request) {

  if r.URL.Path == "/ping" {
    if len(Enabled) == 0 {
      w.WriteHeader(204)
      return
    }
    w.WriteHeader(503)
    return
  }

  //reading request body
  body, err := ioutil.ReadAll(r.Body)
  if err != nil {
    log.Printf("[error] %v - %s", err, r.URL.Path)
  }
  defer r.Body.Close()

  
  if r.URL.Path == "/write" {

    //parsing request body
    for _, line := range strings.Split(string(body), "\n") {
      if line != "" {
        for key, limit := range Stt_stat {
          if !limit.Regexp.MatchString(line){
            log.Printf("[warning] limit not match (%s): %s", r.RemoteAddr, line)
            if limit.Drop {
              w.WriteHeader(400)
              return
            }
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
    }

    for _, locat := range m.Location {
      select {
        case Req_chan[locat] <- &Query{
          Locat:  locat, 
          Auth:   r.Header.Get("Authorization"),
          Query:  r.URL.Query().Encode(),
          Body:   body,
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

func Repeat(query *Query, cfg *config.Config) {

  Job_chan[query.Locat] <- 1

  for i := 1; i <= cfg.Write.Repeat; i++ {

    _, code := request("POST", query.Locat+"/write", query.Query, query.Body, query.Auth, cfg.Write.Timeout)

    if code < 500 { 
      break
    }
    
    if (i == cfg.Write.Repeat || len(Enabled) > 0) && cfg.Cache.Enabled {
      if err := cacheWrite(query, cfg.Cache.Directory); err != nil {
        log.Printf("[error] creating cache file: %v", err)
      } else {
        log.Printf("[info] added request to cache - %s (%d)", query.Locat+"/write", code)
      }
      break
    }

    time.Sleep(cfg.Write.Delay_time * time.Second)
  }

  <- Job_chan[query.Locat]
}


func Sender(locat string, cfg *config.Config) {

  for {

    //go func(locat string, cfg *config.Config){

      batch := map[string]*Batch{}

      for i := 0; i < len(Req_chan[locat]); i++ {
        select {
          case r := <- Req_chan[locat]:

            if batch[r.Query] == nil {
              batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
            }
            for _, bd := range strings.Split(string(r.Body), "\n") {
              if bd != "" {
                batch[r.Query].Body = append(batch[r.Query].Body, bd)
              }
              if len(batch[r.Query].Body) >= cfg.Batch.Size {
                body := strings.Join(batch[r.Query].Body, "\n")
                go Repeat(&Query{ locat, batch[r.Query].Auth, r.Query, []byte(body) }, cfg)
                batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
              }
            }
          default:
            continue
        }
      }
    
      for q, r := range batch{  
        if len(r.Body) > 0 {
          body := strings.Join(r.Body, "\n")
          go Repeat(&Query{ locat, r.Auth, q, []byte(body) }, cfg)
        }
      }

    //}(locat, cfg)

    time.Sleep(cfg.Batch.Max_wait * time.Second)

  }

}

/*
func Sender(locat string, cfg *config.Config) {
  for {
    
    if len(Job_chan[locat]) < cfg.Write.Threads {
      batch := map[string]*Batch{}
      for i:=0; i < cfg.Batch.Size; i++ {
        select {
          case r := <- Req_chan[locat]:
            if batch[r.Query] == nil {
              batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
            }
            for _, bd := range strings.Split(string(r.Body), "\n") {
              if bd != "" {
                batch[r.Query].Body = append(batch[r.Query].Body, bd)
              }
              if len(batch[r.Query].Body) >= cfg.Batch.Size {
                //go Repeat(&Query{ "POST", locat, "/write", batch[r.Query].Auth, r.Query, []byte(strings.Join(batch[r.Query].Body, "\n")) }, cfg)
                //batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
                goto End
              }
            }
          default:
            continue
        }
      }

      End:

      for query, r := range batch{  
        Job_chan[locat] <- 1

        go func(query string, url string, r *Batch, cfg *config.Config){

          body := strings.Join(r.Body, "\n")

          for i:=0; i < cfg.Write.Repeat; i++ {
            _, code := request("POST", locat+"/write", query, []byte(body), r.Auth, cfg.Write.Timeout)
            //method string, url string, query string, rbody []byte, auth string, timeout time.Duration

            if code < 500 {
              break
            }

            time.Sleep(cfg.Write.Delay_time * time.Second)
          }

        }(query, locat, r, cfg)
      }


    }


    time.Sleep(cfg.Batch.Max_wait * time.Second)
  }
}
*/