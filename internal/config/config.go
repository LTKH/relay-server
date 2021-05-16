package config

import (
    "time"
    //"log"
    "io/ioutil"
    "regexp"
    "gopkg.in/yaml.v2"
)

type Config struct {
    Cache struct {
        Enabled          bool
        Directory        string
        Wait             time.Duration
        Batch_cnt        int
    }
    Write struct {
        Timeout          time.Duration
        Repeat           int
        Delay_time       time.Duration
        Streams []struct {
            Listen       string
            Locations    []Location
        }
    }
    Monit struct {
        Listen           string
    }
}

type Location struct {
    Urls         []string
    Cache        bool
    Regexp       []struct {
        Match        string
        Replace      string
    }
}

func LoadConfigFile(filename string) (*Config, error) {
    cfg := &Config{}

    content, err := ioutil.ReadFile(filename)
    if err != nil {
       return cfg, err
    }

    if err := yaml.UnmarshalStrict(content, cfg); err != nil {
        return cfg, err
    }

    for _, stream := range cfg.Write.Streams {
        for _, locat := range stream.Locations {
            for _, rexp := range locat.Regexp {
                _, err = regexp.Compile(rexp.Match)
                if err != nil {
                    return cfg, err
                }
            }
        }
    }

    return cfg, nil
}
