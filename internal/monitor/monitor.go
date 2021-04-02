package monitor

import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (

    ReqCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "relay_server",
            Name:      "req_count",
            Help:      "",
        },
        []string{"listen"},
    )

    PntCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "relay_server",
            Name:      "pnt_counter",
            Help:      "",
        },
        []string{"rhost","uri"},
    )

    DrpCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "relay_server",
            Name:      "req_dropped",
            Help:      "",
        },
        []string{"url"},
    )

    ErrCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "relay_server",
            Name:      "err_count",
            Help:      "",
        },
        []string{"rhost","uri"},
    )
)

func Start(listen string){

    http.Handle("/metrics", promhttp.Handler())

    prometheus.MustRegister(ReqCounter)
    prometheus.MustRegister(PntCounter)
    prometheus.MustRegister(DrpCounter)
    prometheus.MustRegister(ErrCounter)

    go http.ListenAndServe(listen, nil)
}
