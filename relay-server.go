package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"syscall"
	"time"
	"io/ioutil"
	"encoding/json"
	"github.com/ltkh/relay-server/internal/config"
	"github.com/ltkh/relay-server/internal/monitor"
	"github.com/ltkh/relay-server/internal/streams"
)

var (
	server = make(map[string](*http.Server))
)

func openPorts(conf config.Config) error {

	//opening write ports
	for _, stream := range conf.Write.Streams {
		server[stream.Listen] = &http.Server{
			Addr: stream.Listen,
			Handler: &streams.Write{
				Location:      stream.Location,
				Timeout:       conf.Write.Timeout,
			},
    	}
		go func(listen string) { 
			if err := server[listen].ListenAndServe(); err != nil {
				log.Printf("[info] opening write ports: (%s) %v", listen, err)
			}
		}(stream.Listen)

		for _, locat := range stream.Location {
			streams.Req_chan[locat.Addr] = make(chan *streams.Query, conf.Batch.Buffer_size)
			streams.Job_chan[locat.Addr] = make(chan int, 1000000)
		}

	}

	return nil
}

func closePorts(conf config.Config) error {

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
				Drop:    limit.Drop,
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
	  
	//starting sender
	for _, stream := range conf.Write.Streams {
		for _, locat := range stream.Location {
			go streams.Sender(locat.Addr, locat.Cache, &conf)
			time.Sleep(1000000)
		}
	}

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
		//disabled streams
		closePorts(conf)

		//waiting for processing to complete
		for i := 0; i < 60; i++ {
			count := 0
			for _, stream := range conf.Write.Streams {
				for _, locat := range stream.Location {
					count = count + len(streams.Req_chan[locat.Addr])
					count = count + len(streams.Job_chan[locat.Addr])
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

			cnt := 0

			for _, file := range files { 
				
				cnt++

				if cnt > conf.Cache.Batch_cnt {
					break
				}

				path := conf.Cache.Directory+"/"+file.Name()

				data, err := ioutil.ReadFile(path)
				if err != nil {
					log.Printf("[error] reading cache file: %s", file.Name())
					if err := os.Remove(path); err != nil {
						log.Printf("[error] deleting cache file: %v", err)
					}
					continue
				}

				var query *streams.Query
				if err := json.Unmarshal(data, &query); err != nil {
					log.Printf("[error] reading cache file: %v", err)
					if err := os.Remove(path); err != nil {
						log.Printf("[error] deleting cache file: %v", err)
					}
					continue
				}

				if len(streams.Job_chan[query.Addr]) < conf.Write.Threads {

					go streams.Repeat(query, true, &conf)

					log.Printf("[info] readed request from cache - %s", query.Addr+"/write")

					if err := os.Remove(path); err != nil {
						log.Printf("[error] deleting cache file: %v", err)
					}
				
				}
			}
		}

		time.Sleep(conf.Cache.Wait * time.Second)
	}

}
