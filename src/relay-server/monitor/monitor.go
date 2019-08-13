package monitor

import (
  "net"
  "regexp"
  "strings"
  "net/http"
  "encoding/json"
  "relay-server/config"
  "relay-server/streams"
)

type Monitor struct {
  Config    config.Config
}

type Health struct {
  Location  string `json:"location"`
  Req_count int    `json:"req_count"`
  Job_count int    `json:"job_count"`
}

func (m *Monitor) ServeHTTP(w http.ResponseWriter, r *http.Request){
  req_chan := streams.GetReqChan()
  job_chan := streams.GetJobChan()

  mapD := []*Health{}

  for _, stream := range m.Config.Write.Streams{
    for _, url := range stream.Location{
      location := url
      re := regexp.MustCompile("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
      location = re.ReplaceAllStringFunc(url, func (a string) string{
        host, err := net.LookupAddr(a)
        if err != nil {
          return a
        }
        return strings.Join(host, "")
      })
      mapD = append(mapD, &Health{
        Location:  location,
        Req_count: len(req_chan[url]),
        Job_count: len(job_chan[url]),
      })
    }
  }

  mapB, _ := json.Marshal(mapD)
  w.Write(mapB)
}
