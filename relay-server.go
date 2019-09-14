package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	//"reflect"
  "regexp"
  "runtime"
	"syscall"
  "time"
  "io/ioutil"
  "encoding/json"
	"relay-server/config"
	"relay-server/monitor"
	"relay-server/streams"
)

var (
	server = make(map[string](*http.Server))
)

func openPorts(conf config.Config) error {

	//opening read ports
	for _, stream := range conf.Read.Streams {
    server[stream.Listen] = &http.Server{
			Addr: stream.Listen,
      Handler: &(streams.Read{
        Location:    stream.Location,
        Timeout:     conf.Read.Timeout,
        Max_threads: conf.Read.Max_threads,
      }),
    }
    go func(listen string) { 
      if err := server[listen].ListenAndServe(); err != nil {
        log.Printf("[info] opening read ports: %v", err)
      }
    }(stream.Listen)
	}
  
	//opening write ports
	for _, stream := range conf.Write.Streams {
		server[stream.Listen] = &http.Server{
			Addr: stream.Listen,
			Handler: &(streams.Write{
				Location: stream.Location,
				Timeout:  conf.Write.Timeout,
			}),
    }
    go func(listen string) { 
      if err := server[listen].ListenAndServe(); err != nil {
        log.Printf("[info] opening write ports: %v", err)
      }
    }(stream.Listen)

		for _, locat := range stream.Location {
			streams.Req_chan[locat] = make(chan *streams.Query, conf.Batch.Buffer_size)
			streams.Job_chan[locat] = make(chan int, conf.Write.Threads)

			for i := 0; i < conf.Write.Threads; i++ {
				go streams.Sender(locat, conf)
				time.Sleep(1000000)
			}
		}
	}

	return nil
}

func closePorts(conf config.Config) error {

	//closing read ports
	for _, stream := range conf.Read.Streams {
		if err := server[stream.Listen].Close(); err != nil {
			return err
    }
    time.Sleep(1000000)
	}

	//closing write ports
	for _, stream := range conf.Write.Streams {
		if err := server[stream.Listen].Close(); err != nil {
			return err
    }
    time.Sleep(1000000)
	}

	return nil
}

func loadLimits(conf config.Config) error {
	streams.Stt_stat = make(map[string](*streams.Limits))

	for key, limit := range conf.Limits {
		if limit.Enabled {
			res, err := regexp.Compile(limit.Regexp)
			if err != nil {
				return err
			}
			streams.Stt_stat[key] = &streams.Limits{
				Regexp:  res,
				Replace: limit.Replace,
				Limit:   limit.Limit,
			}
		}
	}
	return nil
}

func main() {

	//limits the number of operating system threads
	runtime.GOMAXPROCS(runtime.NumCPU())

	//command-line flag parsing
	cfFile := flag.String("config", "", "config file")
	flag.Parse()

	//loading configuration file
	conf, err := config.LoadConfigFile(*cfFile)
	if err != nil {
		log.Fatalf("[error] loading configuration file: %v", err)
  }
  
  //opening monitoring port
  monitor.Start(conf.Monit.Listen)

	//opening read/write ports
	if err := openPorts(conf); err != nil {
		log.Fatalf("[error] opening read/write ports: %v", err)
	}

	//compile expressions
	if err := loadLimits(conf); err != nil {
		log.Fatalf("[error] compile expressions: %v", err)
	}

  log.Print("[info] relay-server started o_O")

  /*
	//reloading configuration file
	go func(file string) {
		for {
			//loading configuration file
			cfg, err := config.LoadConfigFile(file)
			if err != nil {
				log.Printf("[error] loading configuration file: %v", err)
				continue
			}
			if !reflect.DeepEqual(cfg, conf) {
				log.Printf("[info] loaded configuration file: %v", *cfFile)

				//compile expressions
				if err := loadLimits(conf); err != nil {
					log.Printf("[error] compile expressions: %v", err)
				}

				//saving new config
				conf = cfg
			}
			time.Sleep(10 * time.Second)
		}
  }(*cfFile)
  */

	//program completion signal processing
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		//closing read/write ports
		if err := closePorts(conf); err != nil {
			log.Printf("[error] closing read/write ports: %v", err)
    }
    for i := 0; i < 60; i++ {
      count := 0
      for _, stream := range conf.Write.Streams {
        for _, locat := range stream.Location {
          count = count + len(streams.Req_chan[locat])
          count = count + len(streams.Job_chan[locat])
        }
      }
      if count == 0 { break }
      time.Sleep(1 * time.Second)
    }
		log.Print("[info] relay-server stopped")
		os.Exit(0)
	}()

	//daemon mode
	for {

		if conf.Cache.Enabled {

      files, err := ioutil.ReadDir(conf.Cache.Directory)
      if err != nil {
        log.Printf("[error] reading cache directory: %v", err)
      }

      unix_time := time.Now().Unix()

      for _, file := range files {
        
        if unix_time - file.ModTime().Unix() < 300 {
          continue
        }

        path := conf.Cache.Directory+"/"+file.Name()

        data, err := ioutil.ReadFile(path)
        if err != nil {
          log.Printf("[error] reading cache file: %s", file.Name())
          continue
        }

        var query *streams.Query
        if err := json.Unmarshal(data, &query); err != nil {
          log.Printf("[error] reading cache file: %v", err)
          continue
        }

        if len(streams.Job_chan[query.Locat]) < conf.Write.Threads/2 {

          go streams.Repeat(query, conf)

          log.Printf("[info] readed request from cache - %s", query.Url)

          if err := os.Remove(path); err != nil {
            log.Printf("[error] deleting cache file: %v", err)
          }
          
        }
      }
		}

		time.Sleep(60 * time.Second)
	}

}
