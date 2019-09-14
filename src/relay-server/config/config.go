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
  }
  Cache struct {
    Enabled      bool
    Directory    string
  }
  Read struct {
    Timeout      time.Duration
    Max_threads  int
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
  Limits  map[string]Limitations
}

type Limitations struct {
  Enabled      bool
  Regexp       string
  Replace      string
  Limit        int
}

func LoadConfigFile(filename string) (cfg Config, err error) {
  f, err := os.Open(filename)
  if err != nil {
    return cfg, err
  }
  defer f.Close()

  return cfg, toml.NewDecoder(f).Decode(&cfg)
}
