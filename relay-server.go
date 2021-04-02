package main

import (
    "flag"
    "log"
    "net/http"
    "os"
    "os/signal"
    "runtime"
    "syscall"
    "time"
    "io/ioutil"
    "encoding/json"
    "gopkg.in/natefinch/lumberjack.v2"
    "github.com/ltkh/relay-server/internal/config"
    "github.com/ltkh/relay-server/internal/monitor"
    "github.com/ltkh/relay-server/internal/streams"
)

var (
    server = make(map[string](*http.Server))
)

func openPorts(conf *config.Config) error {

    //opening write ports
    for _, stream := range conf.Write.Streams {
        server[stream.Listen] = &http.Server{ 
            Addr: stream.Listen,
            Handler: &streams.Write{
                Listen:        stream.Listen,
                Locations:     stream.Locations,
                Timeout:       conf.Write.Timeout,
                Repeat:        conf.Write.Repeat,
                DelayTime:     conf.Write.Delay_time,
                CacheDir:      conf.Cache.Directory,
            },
        }
        go func(listen string) { 
            if err := server[listen].ListenAndServe(); err != nil {
                log.Printf("[info] opening write ports: (%s) %v", listen, err)
            }
        }(stream.Listen)
    }

    return nil
}

func closePorts(conf *config.Config) error {

    //closing write ports
    for _, stream := range conf.Write.Streams {
        if err := server[stream.Listen].Close(); err != nil {
            return err
        }
        time.Sleep(1000000)
    }

    return nil
}

func main() {

    //limits the number of operating system threads
    runtime.GOMAXPROCS(runtime.NumCPU())

    //command-line flag parsing
    cfFile          := flag.String("config", "", "config file")
    lgFile          := flag.String("logfile", "", "log file")
    logMaxSize      := flag.Int("log.max-size", 1, "log max size") 
    logMaxBackups   := flag.Int("log.max-backups", 3, "log max backups")
    logMaxAge       := flag.Int("log.max-age", 10, "log max age")
    logCompress     := flag.Bool("log.compress", true, "log compress")
    flag.Parse()

    //loading configuration file
    cfg, err := config.LoadConfigFile(*cfFile)
    if err != nil {
        log.Fatalf("[error] loading configuration file: %v", err)
    }
  
    //opening monitoring port
    monitor.Start(cfg.Monit.Listen)

    //opening read/write ports
    if err := openPorts(cfg); err != nil {
        log.Fatalf("[error] opening read/write ports: %v", err)
    }

    //logging settings
    if *lgFile != "" {
        log.SetOutput(&lumberjack.Logger{
            Filename:   *lgFile,
            MaxSize:    *logMaxSize,    // megabytes after which new file is created
            MaxBackups: *logMaxBackups, // number of backups
            MaxAge:     *logMaxAge,     // days
            Compress:   *logCompress,   // using gzip
        })
    }

    log.Print("[info] relay-server started o_O")

    //program completion signal processing
    c := make(chan os.Signal, 2)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        //disabled streams
        closePorts(cfg)

        log.Print("[info] relay-server stopped")
        os.Exit(0)
    }()

    //daemon mode
    for {

          if cfg.Cache.Enabled {

            files, err := ioutil.ReadDir(cfg.Cache.Directory)
            if err != nil {
                log.Printf("[error] reading cache directory: %v", err)
            }

            cnt := 0

            for _, file := range files { 
                
                cnt++

                if cnt > cfg.Cache.Batch_cnt {
                    break
                }

                path := cfg.Cache.Directory+"/"+file.Name()

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

                go streams.Sender(query, 0, 0, 0, true, cfg.Cache.Directory)

                if err := os.Remove(path); err != nil {
                    log.Printf("[error] deleting cache file: %v", err)
                }
            }
        }

        time.Sleep(cfg.Cache.Wait * time.Second)
    }

}