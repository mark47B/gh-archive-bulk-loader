package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	DownloadDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "gharchive_download_duration_seconds",
		Help: "Время скачивания одного архива",
	})
	WriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "mongodb_write_duration_seconds",
		Help: "Время вставки батча в MongoDB",
	})
	RecordsInserted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mongodb_inserted_records_total",
		Help: "Количество добавленных документов",
	})
	ErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "gharchive_errors_total",
		Help: "Ошибки по типам",
	}, []string{"type"})
	DownloadToWriteRatio = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gharchive_download_to_write_ratio",
		Help: "Отношение времени скачивания к времени записи",
	})
	RecordsPerSecond = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "records_processed_per_second",
		Help: "Количество обработанных записей в секунду",
	})
	CacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "redis_cache_hits_total",
		Help: "Количество попаданий в кеш",
	})
	CacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "redis_cache_misses_total",
		Help: "Количество промахов кеша",
	})
)

func init() {
	prometheus.MustRegister(
		DownloadDuration,
		WriteDuration,
		RecordsInserted,
		ErrorsTotal,
		DownloadToWriteRatio,
		RecordsPerSecond,
		CacheHits,
		CacheMisses,
	)
}

func StartMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
