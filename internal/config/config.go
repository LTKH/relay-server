package config

import (
	"github.com/naoina/toml"
	"os"
	"time"
)

type Config struct {
	Batch struct {
		Size        int
		Max_wait    time.Duration
		Buffer_size int
	}
	Cache struct {
		Enabled     bool
		Directory   string
		Batch_size  int
		Wait        time.Duration
	}
	Write struct {
		Timeout    time.Duration
		Threads    int
		Repeat     int
		Delay_time time.Duration
		Streams    []struct {
			Listen   string
			Location []string
		}
	}
	Monit struct {
		Listen string
	}
	Limits map[string]Limitations
}

type Limitations struct {
	Enabled bool
	Regexp  string
	Replace string
	Limit   int
}

func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	return cfg, toml.NewDecoder(f).Decode(&cfg)
}
