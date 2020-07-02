package config

import (
	"time"
	"os"
	"github.com/naoina/toml"
)

type Config struct {
	Server struct {
		Log_max_size     int
		Log_max_backups  int
		Log_max_age      int
		Log_compress     bool
	}
	Batch struct {
		Size             int
		Max_wait         time.Duration
		Buffer_size      int
	}
	Cache struct {
		Enabled          bool
		Directory        string
		Wait             time.Duration
		Batch_cnt        int
	}
	Write struct {
		Timeout          time.Duration
		Threads          int
		Repeat           int
		Delay_time       time.Duration
		Streams []struct {
			Listen       string
			Location  []struct {
				Addr     string
				Cache    bool
			}
		}
	}
	Monit struct {
		Listen           string
	}
	Limits map[string]Limitations
}

type Limitations struct {
	Enabled              bool
	Regexp               string
	Replace              string
}

func LoadConfigFile(filename string) (cfg Config, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	return cfg, toml.NewDecoder(f).Decode(&cfg)
}
