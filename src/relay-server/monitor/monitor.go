package monitor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"relay-server/streams"
	"time"
)

var (
	reqGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name:      "req_count",
			Help:      "",
		},
		[]string{"location"},
	)

	jobGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name:      "job_count",
			Help:      "",
		},
		[]string{"location"},
	)

	sttGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name:      "stt_stats",
			Help:      "",
		},
		[]string{"limit", "key"},
	)
)

func Start(listen string) {

	http.Handle("/metrics", promhttp.Handler())

	prometheus.MustRegister(reqGauge)
	prometheus.MustRegister(jobGauge)
	prometheus.MustRegister(sttGauge)

	go func() {
		for {
			for key, _ := range streams.Req_chan {
				reqGauge.With(prometheus.Labels{"location": key}).Set(float64(len(streams.Req_chan[key])))
			}
			for key, _ := range streams.Job_chan {
				jobGauge.With(prometheus.Labels{"location": key}).Set(float64(len(streams.Job_chan[key])))
			}
			for key, limit := range streams.Stt_stat {
				limit.Stat.Range(func(k, v interface{}) bool {
					sttGauge.With(prometheus.Labels{"limit": key, "key": k.(string)}).Set(float64(v.(int)))
					streams.Stt_stat[key].Stat.Store(k, 0)
					return true
				})
			}
			time.Sleep(10 * time.Second)
		}
	}()

	go http.ListenAndServe(listen, nil)
}
