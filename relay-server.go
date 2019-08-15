package main

import (
  "net/http"
  "log"
  "time"
  "flag"
  "runtime"
  "os"
  "os/signal"
  "syscall"
  "encoding/json"
  "relay-server/config"
  "relay-server/monitor"
  "relay-server/streams"
  "github.com/peterbourgon/diskv"
)

var (
  server = make(map[string](*http.Server))
)

func main(){

  //limits the number of operating system threads
  runtime.GOMAXPROCS(runtime.NumCPU())

  //command-line flag parsing
  cfFile := flag.String("config", "", "config file")
  flag.Parse()

  //loading configuration file
  cfg, err := config.LoadConfigFile(*cfFile)
  if err != nil {
    log.Fatalf("[error] %v", err)
  }

  req_chan := streams.GetReqChan()
  job_chan := streams.GetJobChan()

  //opening port for monitoring
  go http.ListenAndServe(cfg.Monit.Listen, &(monitor.Monitor{ Config: cfg }))

  //opening write ports
  for _, stream := range cfg.Write.Streams{
    server[stream.Listen] = &http.Server{
      Addr: stream.Listen,
      Handler: &(streams.Write{
        Location:  stream.Location,
        Timeout:   cfg.Write.Timeout,
      }),
    }
    go server[stream.Listen].ListenAndServe()
    log.Printf("[info] listen write port - %s", stream.Listen)

    for _, locat := range stream.Location{
      req_chan[locat] = make(chan *streams.Query, cfg.Batch.Buffer_size)
      job_chan[locat] = make(chan int, cfg.Batch.Job_count)

      for i := 0; i < cfg.Write.Threads; i++ {
        go streams.Sender(locat, cfg)
        time.Sleep(1000000)
      }
    }
  }

  //opening read ports
  for _, stream := range cfg.Read.Streams{
    server[stream.Listen] = &http.Server{
      Addr: stream.Listen,
      Handler: &(streams.Read{
        Location:  stream.Location,
        Timeout:   cfg.Read.Timeout,
        Max_queue: cfg.Read.Max_queue,
      }),
    }
    go server[stream.Listen].ListenAndServe()
    log.Printf("[info] listen read port - %s", stream.Listen)
  }

  log.Print("[info] relay-server started o_O")

  //program completion signal processing
  c := make(chan os.Signal, 2)
  signal.Notify(c, os.Interrupt, syscall.SIGTERM)
  go func() {
    <- c
    log.Print("[info] relay-server stopped")
    os.Exit(0)
  }()

  //daemon mode
  for {
    d := diskv.New(diskv.Options{
  		BasePath:     cfg.Batch.Cache_dir,
  		CacheSizeMax: cfg.Batch.Cache_size,
  	})

    for key := range d.Keys(nil) {
  		val, err := d.Read(key)
  		if err != nil {
  			log.Printf("[error] %v", err)
        continue
  		}

      var query streams.Query
      if err := json.Unmarshal(val, &query); err != nil {
        log.Printf("[error] %v", err)
        continue
      }

      if len(req_chan[query.Locat]) < cfg.Batch.Buffer_size {
        select {
        case req_chan[query.Locat] <- &query:
            d.Erase(key)
            log.Printf("[info] added request to channel from cache - %s", query.Url)
          default:
            log.Printf("[error] channel is not ready - %s", query.Locat)
        }
      }

  	}


    time.Sleep(60 * time.Second)
  }

}
