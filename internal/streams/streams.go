package streams

import (
	"net/http"
	_ "net/http/pprof"
	"log"
	"time"
	"io/ioutil"
	"strings"
	"regexp"
	"encoding/json"
	"crypto/md5"
	"encoding/hex"
	"github.com/influxdata/line-protocol"
	"github.com/ltkh/relay-server/internal/config"
)

var (
	Req_chan = make(map[string](chan *Query))
	Job_chan = make(map[string](chan int))
	Stt_chan = make(map[string](chan *Stats))
	Stt_regx = make(map[string](*Limits))
)

type Stats struct {
    Limit        string
	Key          string
}

type Limits struct {
	Regexp       *regexp.Regexp
	Replace      string
}

type Write struct {
	Location  []struct {
		Addr     string
		Cache    bool
	}
	Timeout      time.Duration
}

type Request struct {
	*http.Request
	Body         []byte
}

type Query struct {
	Addr         string
	Auth         string
	Query        string
	Body         []string
}

type Batch struct {
	Auth         string
	Body         []string
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

		go func(body []byte, r *http.Request){

			var lines []string

			handler := protocol.NewMetricHandler()
			parser := protocol.NewParser(handler)

			//parsing request body
			for _, line := range strings.Split(string(body), "\n") {
				if line != "" {
					_, err := parser.Parse([]byte(line))
					if err != nil {
						log.Printf("[error] %v (%s)", err, r.RemoteAddr)
						continue
					}

					for key, _ := range Stt_chan {
						if Stt_regx[key].Regexp.MatchString(line){
							tag := Stt_regx[key].Regexp.ReplaceAllString(line, Stt_regx[key].Replace)
							select {
								case Stt_chan[key] <- &Stats{
									Limit:   key, 
									Key:     tag,
								}:
								default:
									log.Printf("[error] statistics channel is full - %s (%s)", key, r.RemoteAddr)
									break
							}
						}
					}

					lines = append(lines, line)
				}
			}

			if len(lines) == 0 {
				return
			}

			for _, locat := range m.Location {
				select {
					case Req_chan[locat.Addr] <- &Query{
						Addr:   locat.Addr, 
						Auth:   r.Header.Get("Authorization"),
						Query:  r.URL.Query().Encode(),
						Body:   lines,
					}:
					default:
						log.Printf("[error] channel is not ready - %s", locat.Addr)
				}
			}

		}(body, r)

		w.WriteHeader(204)
		return
  	}

  	w.WriteHeader(404)
}

func request(method string, url string, query string, rbody []string, auth string, timeout time.Duration) ([]byte, int) {
  
	client := &http.Client{ Timeout: time.Duration(timeout * time.Second) }

	req, err := http.NewRequest(method, url, strings.NewReader(strings.Join(rbody, "\n")))
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

func Repeat(query *Query, cache bool, cfg *config.Config) {

	Job_chan[query.Addr] <- 1

	for i := 1; i <= cfg.Write.Repeat; i++ {

		_, code := request("POST", query.Addr, query.Query, query.Body, query.Auth, cfg.Write.Timeout)

		if code < 500 { 
			break
		}
		
		if i == cfg.Write.Repeat && cache {
			if err := cacheWrite(query, cfg.Cache.Directory); err != nil {
				log.Printf("[error] creating cache file: %v", err)
			} else {
				log.Printf("[info] added request to cache - %s (%d)", query.Addr, code)
			}
			break
		}

		time.Sleep(cfg.Write.Delay_time * time.Second)
	}

	<- Job_chan[query.Addr]
}

func Sender(addr string, cache bool, cfg *config.Config) {

	for {

		batch := map[string]*Batch{}

		for i := 0; i < len(Req_chan[addr]); i++ {
			select {
				case r := <- Req_chan[addr]:

					if batch[r.Query] == nil {
						batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
					}
					for _, bd := range r.Body {
						if bd != "" {
							batch[r.Query].Body = append(batch[r.Query].Body, bd)
						}
						if len(batch[r.Query].Body) >= cfg.Batch.Size {
							go Repeat(&Query{ addr, batch[r.Query].Auth, r.Query, batch[r.Query].Body }, cache, cfg)
							batch[r.Query] = &Batch{ Auth: r.Auth, Body: []string{} }
						}
					}
				default:
					continue
			}
		}
		
		for q, r := range batch{  
			if len(r.Body) > 0 {
				go Repeat(&Query{ addr, r.Auth, q, r.Body }, cache, cfg)
			}
		}

		time.Sleep(cfg.Batch.Max_wait * time.Second)

	}

}