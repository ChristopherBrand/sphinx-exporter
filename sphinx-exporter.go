package main

import (
  "database/sql"
  "log"
  "net/http"
  "os"
  "strconv"
  "time"
  "flag"
  _ "github.com/go-sql-driver/mysql"
  "github.com/prometheus/client_golang/prometheus"
  "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
  gauges = make(map[string]*prometheus.GaugeVec)
  indexCount = prometheus.NewGauge(prometheus.GaugeOpts{
    Name: "sphinx_index_count",
    Help: "Number of indexes",
  })

  previousIndexes = make(map[string]bool)

  sphinxIp = flag.String("sphinx-address", "127.0.0.1", "Sphinx MySQL address")
  sphinxPort = flag.String("sphinx-port", "9306", "Sphinx MySQL port")
  listenPort = flag.String("listen-port", "9247", "Port for sphinx exporter to listen on")

  logger = log.New(os.Stderr, "", 0)
)

func newGaugeVec(gaugeKey string, gaugeName string, gaugeHelp string) {
  gauges[gaugeKey] = prometheus.NewGaugeVec(
    prometheus.GaugeOpts{
      Name: gaugeName,
      Help: gaugeHelp,
    },
    []string{"index"},
  )
  prometheus.MustRegister(gauges[gaugeKey])
}

func getSphinxConnection() *sql.DB {
  db, err := sql.Open("mysql", "@tcp(" + *sphinxIp + ":" + *sphinxPort + ")/")
  if err != nil {
    logger.Println("%s", err);
    return nil
  }
  err = db.Ping()
  if err != nil {
    logger.Println("%s", err);
    return nil
  }
  
  return db
}

func zeroMissedIndexes(currentIndexes map[string]bool) {
  for indexName, _ := range previousIndexes {
    if currentIndexes[indexName] != true {
      for key, _ := range gauges { 
        gauges[key].With(prometheus.Labels{"index":indexName}).Set(0)
      }
    }
  }

  previousIndexes = currentIndexes
}

func setStats() {
  //Schedule next run
  time.Sleep(5 * time.Second)
  go setStats()

  // Map to track indexes that were stat'd this iteration
  currentIndexes := make(map[string]bool)

  db := getSphinxConnection()

  // Init index counter
  totalIndexes := float64(0)

  if db != nil {

    // Get indexes
    indexes, err := db.Query("SHOW TABLES")
    if err != nil {
      logger.Println(err)
      return
    }
    defer indexes.Close()

    for indexes.Next() {
      indexName := ""
      indexType := ""

      // Get next index
  	  err := indexes.Scan(&indexName, &indexType)
      if err != nil {
        logger.Println(err)
        return
      }

      // Retrieve stats for `indexName`
      stats, err := db.Query("SHOW INDEX " + indexName + " STATUS")
      if err != nil {
        logger.Println(err)
        return
      }

      currentIndexes[indexName] = true

      defer stats.Close()

      // Set gauge for each stat for `indexName`
      for stats.Next() {
        statName := ""
        statValue := ""

        // Fetch stat
        err := stats.Scan(&statName, &statValue)
        if err != nil {
          logger.Println(err)
          return
        }

        // Set gauge for `statName` with `indexName`
        if gauges[statName] != nil {
          count, err := strconv.ParseFloat(statValue, 64)
          if err != nil {
            logger.Println(err)
            return
          }
          gauges[statName].With(prometheus.Labels{"index":indexName}).Set(count)
        }
      }

      // Count the index
      totalIndexes++;
    }

    db.Close()
  }

  zeroMissedIndexes(currentIndexes)

  indexCount.Set(totalIndexes);
}

func init() {
  newGaugeVec("indexed_documents", "sphinx_indexed_documents", "Number of documents indexed")
  newGaugeVec("indexed_bytes", "sphinx_indexed_bytes", "Indexed Bytes")
  newGaugeVec("field_tokens_title", "sphinx_field_tokens_title", "Sums of per-field length titles over the entire index")
  newGaugeVec("field_tokens_body", "sphinx_field_tokens_body", "Sums of per-field length bodies over the entire index")
  newGaugeVec("total_tokens", "sphinx_total_tokens", "Total tokens")
  newGaugeVec("ram_bytes", "sphinx_ram_bytes", "total size (in bytes) of the RAM-resident index portion")
  newGaugeVec("disk_bytes", "sphinx_disk_bytes", "total size (in bytes) of the disk index")
  newGaugeVec("mem_limit", "sphinx_mem_limit", "Memory limit")

  prometheus.MustRegister(indexCount);
}

func main() {
  flag.Parse()
  setStats()
  http.Handle("/metrics", promhttp.Handler())
  log.Fatal(http.ListenAndServe(":" + *listenPort, nil))
}