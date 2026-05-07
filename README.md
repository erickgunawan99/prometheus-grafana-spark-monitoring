* docker compose up -d
* copy run_spark_job.txt and paste it into terminal
* paste all the prometheus_query.txt into grafana dashboard (localhost:3000)
* explore more on :
  - prometheus UI (localhost:9090)
  - spark master metrics complete list (localhost:8080/metrics/master/prometheus/)
  - spark worker 1 (localhost:/8081/metrics/worker/prometehus/)
  - spark worker 2 (localhost:/8082/metrics/worker/prometehus/)
  - spark driver (localhost:4040/metrics/prometheus/)
  - spark executors (localhost:4040/metrics/executors/prometheus/)
  - spark app (localhost:8080/metrics/applications/prometheus/)

