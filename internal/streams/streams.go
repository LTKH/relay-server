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
	"github.com/influxdata/line-protocol"
	"github.com/ltkh/relay-server/internal/config"
)

var (
	Req_chan = make(map[string](chan *Query))
	Job_chan = make(map[string](chan int))
	Stt_stat = make(map[string](*Limits))
)

type Limits struct {
	Stat         sync.Map
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

func statMatch(line string) bool {
    for key, limit := range Stt_stat {
		
		if !limit.Regexp.MatchString(line){
			log.Printf("[warning] limit not matched: %s", line)
			return false
		} else {
            tag := limit.Regexp.ReplaceAllString(line, limit.Replace)
			v, ok := limit.Stat.Load(tag)
			if ok {
				val := v.(int)
				Stt_stat[key].Stat.Store(tag, val + 1)
			} else {
				Stt_stat[key].Stat.Store(tag, 1)
			}
		}
	}

	return true
}

func (m *Write) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.URL.Path == "/ping" {
		w.WriteHeader(204)
		return
	}

	//reading request body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("[error] %v - %s", err, r.URL.Path)
	}
	defer r.Body.Close()

  
	if r.URL.Path == "/write" {

		var lines []string
		var errors []string

		handler := protocol.NewMetricHandler()
		parser := protocol.NewParser(handler)

        //parsing request body
		for _, line := range strings.Split(string(body), "\n") {

			if line != "" {
				_, err := parser.Parse([]byte(line))
				if err != nil {
					log.Printf("[error] %v", err)
					errors = append(errors, err.Error())
					continue
				}
			
				statMatch(line)
				lines = append(lines, line)
			}
		}

		if len(lines) == 0 {
			w.WriteHeader(400)
			w.Write([]byte(strings.Join(errors, "\n")))
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

		if len(errors) > 0 {
			w.WriteHeader(400)
			w.Write([]byte(strings.Join(errors, "\n")))
			return
		}

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

		queryes := map[string]*Query{}

		for i := len(Req_chan[addr]); i > 0; i-- {
			select {
				case query := <- Req_chan[addr]:
	
					if queryes[query.Query] == nil {
						queryes[query.Query] = query
					} else {
						for _, bd := range query.Body {
                            queryes[query.Query].Body = append(queryes[query.Query].Body, bd)
						}
					}
					
			}

			for k, query := range queryes{  
				if i == 1 || len(query.Body) >= cfg.Batch.Size {
					go Repeat(query, cache, cfg)
					delete(queryes, k)
				}
			}
		}

		time.Sleep(cfg.Batch.Max_wait * time.Second)

	}

}
