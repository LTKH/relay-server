package monitor

import (
	"time"
	"net/http"
	"github.com/ltkh/relay-server/internal/streams"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (

	reqGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name: "req_count",
			Help: "",
		},
		[]string{"location"},
	)

	jobGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name: "job_count",
			Help: "",
		},
		[]string{"location"},
	)

	sttGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "relay_server",
			Name: "stt_stats",
			Help: "",
		},
		[]string{"limit","key"},
	)
)

func Start(listen string){

	http.Handle("/metrics", promhttp.Handler())

	prometheus.MustRegister(reqGauge)
	prometheus.MustRegister(jobGauge)
	prometheus.MustRegister(sttGauge)

	go func() {
		for {
			for key, _ := range streams.Req_chan {
				reqGauge.With(prometheus.Labels{"location":key}).Set(float64(len(streams.Req_chan[key])))
			}
			for key, _ := range streams.Job_chan {
				jobGauge.With(prometheus.Labels{"location":key}).Set(float64(len(streams.Job_chan[key])))
			}
			
			for key, _ := range streams.Stt_chan {
				stats := make(map[string]int)
				for i := 0; i < len(streams.Stt_chan[key]); i++ {
					select {
						case r := <- streams.Stt_chan[key]:
							stats[r.Key]++
					}
				}
				for k, v := range stats {
					sttGauge.With(prometheus.Labels{"limit":key,"key":k}).Set(float64(v))
				}
			}
			
			time.Sleep(10 * time.Second)
		}
	}()

	go http.ListenAndServe(listen, nil)
}
