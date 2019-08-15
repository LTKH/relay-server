package config

import (
  "time"
  "os"
  "github.com/naoina/toml"
)

type Config struct {
  Batch struct {
    Size         int
    Max_wait     time.Duration
    Buffer_size  int
    Job_count    int
    Cache_dir    string
    Cache_size   uint64
  }
  Read struct {
    Timeout      time.Duration
    Max_queue    int
    Streams []struct {
      Listen     string
      Location   []string
    }
  }
  Write struct {
    Timeout      time.Duration
    Threads      int
    Repeat       int
    Delay_time   time.Duration
    Streams []struct {
      Listen     string
      Location   []string
    }
  }
  Monit struct {
    Listen       string
  }
}

func LoadConfigFile(filename string) (cfg Config, err error) {
  f, err := os.Open(filename)
  if err != nil {
    return cfg, err
  }
  defer f.Close()

  return cfg, toml.NewDecoder(f).Decode(&cfg)
}
