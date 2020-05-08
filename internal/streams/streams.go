package streams

import (
	//"net"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"github.com/ltkh/relay-server/internal/config"
	"strings"
	"sync"
	"time"
)

var (
	Req_chan = make(map[string](chan *Query))
	Job_chan = make(map[string](chan int))
	Stt_stat = make(map[string](*Limits))
	Enabled  = make(chan int)
	rex_infl = regexp.MustCompile(`^[^,\s"]+(?:,[^,\s"]+=(?:"[^"]*"|[^,\s]+))*\s+(?:,?[^,\s"]+=(?:"[^"]*"|[^,\s]+))+(\s+[0-9]{19,19})?$`)
)

type Limits struct {
	Stat    sync.Map
	Regexp  *regexp.Regexp
	Replace string
	Limit   int
	Drop    bool
}

type Write struct {
	Location []string
	Timeout  time.Duration
}

type Request struct {
	*http.Request
	Body []byte
}

type Query struct {
	Locat string
	Auth  string
	Query string
	Body  []string
}

type Batch struct {
	Auth string
	Body []string
}

func checkMatch(line string) bool {
	line = strings.ReplaceAll(line, `\ `, `%5C%20`)
	line = strings.ReplaceAll(line, `\,`, `%5C%2C`)
	line = strings.ReplaceAll(line, `\"`, `%5C%22`)
	if !rex_infl.MatchString(line){
		return false
	}

	return true
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
		var lines []string

		//parsing request body
		for _, line := range strings.Split(string(body), "\n") {
			if line != "" {
				if !checkMatch(line){
					log.Printf("[warning] line not match (%s): %s", r.RemoteAddr, line)
					continue
				}

				lines = append(lines, line)

				for key, limit := range Stt_stat {
					if limit.Regexp.MatchString(line) {
						tag := limit.Regexp.ReplaceAllString(line, limit.Replace)

						v, ok := limit.Stat.Load(tag)
						if ok {
							val := v.(int)
							if limit.Limit > 0 && val > limit.Limit {
								w.WriteHeader(503)
								return
							}
							Stt_stat[key].Stat.Store(tag, val+1)
						} else {
							Stt_stat[key].Stat.Store(tag, 1)
						}

					}
				}
			}
		}

		if len(lines) == 0 {
            w.WriteHeader(400)
			return
		}

		for _, locat := range m.Location {
			select {
			case Req_chan[locat] <- &Query{
				Locat: locat,
				Auth:  r.Header.Get("Authorization"),
				Query: r.URL.Query().Encode(),
				Body:  lines,
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

func request(method string, url string, query string, rbody []string, auth string, timeout time.Duration) ([]byte, int) {

	client := &http.Client{Timeout: time.Duration(timeout * time.Second)}

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

	path := dir + "/" + hex.EncodeToString(hasher.Sum(nil))

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

	<-Job_chan[query.Locat]
}

func Sender(locat string, cfg *config.Config) {

	for {

		batch := map[string]*Batch{}

		for i := 0; i < len(Req_chan[locat]); i++ {
			select {
			case r := <-Req_chan[locat]:
				if batch[r.Query] == nil {
					batch[r.Query] = &Batch{Auth: r.Auth, Body: []string{}}
				}
				for _, bd := range r.Body {
					if bd != "" {
						batch[r.Query].Body = append(batch[r.Query].Body, bd)
					}
					if len(batch[r.Query].Body) >= cfg.Batch.Size {
						//body := strings.Join(batch[r.Query].Body, "\n")
						go Repeat(&Query{locat, batch[r.Query].Auth, r.Query, batch[r.Query].Body}, cfg)
						batch[r.Query] = &Batch{Auth: r.Auth, Body: []string{}}
					}
				}
			default:
				continue
			}
		}

		for q, r := range batch {
			if len(r.Body) > 0 {
				//body := strings.Join(r.Body, "\n")
				go Repeat(&Query{locat, r.Auth, q, r.Body}, cfg)
			}
		}

		time.Sleep(cfg.Batch.Max_wait * time.Second)

	}

}
