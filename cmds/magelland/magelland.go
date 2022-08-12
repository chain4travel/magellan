// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/chain4travel/caminogo/utils/logging"
	"github.com/chain4travel/magellan/api"
	"github.com/chain4travel/magellan/balance"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/db"
	"github.com/chain4travel/magellan/models"
	oreliusRpc "github.com/chain4travel/magellan/rpc"
	"github.com/chain4travel/magellan/services/rewards"
	"github.com/chain4travel/magellan/servicesctrl"
	"github.com/chain4travel/magellan/stream"
	"github.com/chain4travel/magellan/stream/consumers"
	"github.com/chain4travel/magellan/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	rootCmdUse  = "magelland [command]"
	rootCmdDesc = "Daemons for Magellan."

	apiCmdUse  = "api"
	apiCmdDesc = "Runs the API daemon"

	streamCmdUse  = "stream"
	streamCmdDesc = "Runs stream commands"

	streamIndexerCmdUse  = "indexer"
	streamIndexerCmdDesc = "Runs the stream indexer daemon"

	envCmdUse  = "env"
	envCmdDesc = "Displays information about the Magellan environment"

	defaultReplayQueueSize    = int(2000)
	defaultReplayQueueThreads = int(4)

	mysqlMigrationFlag          = "run-mysql-migration"
	mysqlMigrationFlagShorthand = "m"
	mysqlMigrationFlagDesc      = "Executes migration scripts for the configured mysql database. This might change the schema, use with caution. For more info on migration have a look at the migrations folder in services/db/migrations/ and check the latest commits"

	mysqlMigrationPathFlag          = "mysql-migration-path"
	mysqlMigrationPathFlagShorthand = "p"
	mysqlMigrationPathDefault       = "services/db/migrations"
)

func main() {
	//init cache key values
	//initCacheStorage()

	if err := execute(); err != nil {
		log.Fatalln("Failed to run:", err.Error())
	}
}

// Get the root command for magellan
func execute() error {
	rand.Seed(time.Now().UnixNano())

	var (
		runErr             error
		config             = &cfg.Config{}
		serviceControl     = &servicesctrl.Control{}
		runMysqlMigration  = func() *bool { s := false; return &s }()
		mysqlMigrationPath = func() *string { s := mysqlMigrationPathDefault; return &s }()
		configFile         = func() *string { s := ""; return &s }()
		replayqueuesize    = func() *int { i := defaultReplayQueueSize; return &i }()
		replayqueuethreads = func() *int { i := defaultReplayQueueThreads; return &i }()
		cmd                = &cobra.Command{Use: rootCmdUse, Short: rootCmdDesc, Long: rootCmdDesc,
			PersistentPreRun: func(cmd *cobra.Command, args []string) {
				c, err := cfg.NewFromFile(*configFile)
				if err != nil {
					log.Fatalln("Failed to read config file", *configFile, ":", err.Error())
				}
				lf := logging.NewFactory(c.Logging)
				alog, err := lf.Make("magellan")
				if err != nil {
					log.Fatalln("Failed to create log", c.Logging.Directory, ":", err.Error())
				}

				mysqllogger := &MysqlLogger{
					Log: alog,
				}
				_ = mysql.SetLogger(mysqllogger)

				if *runMysqlMigration {
					dbConfig := c.Services.DB
					migrationErr := migrateMysql(fmt.Sprintf("%s://%s", dbConfig.Driver, dbConfig.DSN), *mysqlMigrationPath)
					if migrationErr != nil {
						log.Fatalf("Failed to run migration: %v", migrationErr)
					}
				}

				models.SetBech32HRP(c.NetworkID)

				serviceControl.Log = alog
				serviceControl.Services = c.Services
				serviceControl.ServicesCfg = *c
				serviceControl.Chains = c.Chains
				serviceControl.Persist = db.NewPersist()
				serviceControl.Features = c.Features
				persist := db.NewPersist()
				serviceControl.BalanceManager = balance.NewManager(persist, serviceControl)
				err = serviceControl.Init(c.NetworkID)
				if err != nil {
					log.Fatalln("Failed to create service control", ":", err.Error())
				}

				*config = *c

				if config.MetricsListenAddr != "" {
					sm := http.NewServeMux()
					sm.Handle("/metrics", promhttp.Handler())
					go func() {
						err = http.ListenAndServe(config.MetricsListenAddr, sm)
						if err != nil {
							log.Fatalln("Failed to start metrics listener", err.Error())
						}
					}()
					alog.Info("Starting metrics handler on %s", config.MetricsListenAddr)
				}
				if config.AdminListenAddr != "" {
					rpcServer := rpc.NewServer()
					codec := json2.NewCodec()
					rpcServer.RegisterCodec(codec, "application/json")
					rpcServer.RegisterCodec(codec, "application/json;charset=UTF-8")
					api := oreliusRpc.NewAPI(alog)
					if err := rpcServer.RegisterService(api, "api"); err != nil {
						log.Fatalln("Failed to start admin listener", err.Error())
					}
					sm := http.NewServeMux()
					sm.Handle("/api", rpcServer)
					go func() {
						err = http.ListenAndServe(config.AdminListenAddr, sm)
						if err != nil {
							log.Fatalln("Failed to start metrics listener", err.Error())
						}
					}()
				}
			},
		}
	)

	// Add flags and commands
	cmd.PersistentFlags().StringVarP(configFile, "config", "c", "config.json", "config file")
	cmd.PersistentFlags().IntVarP(replayqueuesize, "replayqueuesize", "", defaultReplayQueueSize, fmt.Sprintf("replay queue size default %d", defaultReplayQueueSize))
	cmd.PersistentFlags().IntVarP(replayqueuethreads, "replayqueuethreads", "", defaultReplayQueueThreads, fmt.Sprintf("replay queue size threads default %d", defaultReplayQueueThreads))

	// migration specific flags
	cmd.PersistentFlags().BoolVarP(runMysqlMigration, mysqlMigrationFlag, mysqlMigrationFlagShorthand, false, mysqlMigrationFlagDesc)
	cmd.PersistentFlags().StringVarP(mysqlMigrationPath, mysqlMigrationPathFlag, mysqlMigrationPathFlagShorthand, mysqlMigrationPathDefault, "path for mysql migrations")

	cmd.AddCommand(
		createStreamCmds(serviceControl, config, &runErr),
		createAPICmds(serviceControl, config, &runErr),
		createEnvCmds(config, &runErr))

	//init cache scheduler
	go initCacheScheduler(config)
	// Execute the command and return the runErr to the caller
	if err := cmd.Execute(); err != nil {
		return err
	}

	return runErr
}

func createAPICmds(sc *servicesctrl.Control, config *cfg.Config, runErr *error) *cobra.Command {
	return &cobra.Command{
		Use:   apiCmdUse,
		Short: apiCmdDesc,
		Long:  apiCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			lc, err := api.NewServer(sc, *config)
			if err != nil {
				*runErr = err
				return
			}
			runListenCloser(lc)
		},
	}
}

func createStreamCmds(sc *servicesctrl.Control, config *cfg.Config, runErr *error) *cobra.Command {
	streamCmd := &cobra.Command{
		Use:   streamCmdUse,
		Short: streamCmdDesc,
		Long:  streamCmdDesc,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
			os.Exit(0)
		},
	}

	// Add sub commands
	streamCmd.AddCommand(&cobra.Command{
		Use:   streamIndexerCmdUse,
		Short: streamIndexerCmdDesc,
		Long:  streamIndexerCmdDesc,
		Run: func(cmd *cobra.Command, arg []string) {
			runStreamProcessorManagers(
				sc,
				config,
				runErr,
				producerFactories(sc, config),
				[]consumers.ConsumerFactory{
					consumers.IndexerConsumer,
				},
				[]stream.ProcessorFactoryChainDB{
					consumers.IndexerDB,
					consumers.IndexerConsensusDB,
				},
				[]stream.ProcessorFactoryInstDB{},
			)(cmd, arg)
		},
	})

	return streamCmd
}

func producerFactories(sc *servicesctrl.Control, cfg *cfg.Config) []utils.ListenCloser {
	var factories []utils.ListenCloser
	for _, v := range cfg.Chains {
		switch v.VMType {
		case models.AVMName:
			p, err := stream.NewProducerChain(sc, *cfg, v.ID, stream.EventTypeDecisions, stream.IndexTypeTransactions, stream.IndexXChain)
			if err != nil {
				panic(err)
			}
			factories = append(factories, p)
			p, err = stream.NewProducerChain(sc, *cfg, v.ID, stream.EventTypeConsensus, stream.IndexTypeVertices, stream.IndexXChain)
			if err != nil {
				panic(err)
			}
			factories = append(factories, p)
		case models.PVMName:
			p, err := stream.NewProducerChain(sc, *cfg, v.ID, stream.EventTypeDecisions, stream.IndexTypeBlocks, stream.IndexPChain)
			if err != nil {
				panic(err)
			}
			factories = append(factories, p)
		case models.CVMName:
			p, err := stream.NewProducerChain(sc, *cfg, v.ID, stream.EventTypeDecisions, stream.IndexTypeBlocks, stream.IndexCChain)
			if err != nil {
				panic(err)
			}
			factories = append(factories, p)
		}
	}
	return factories
}

func createEnvCmds(config *cfg.Config, runErr *error) *cobra.Command {
	return &cobra.Command{
		Use:   envCmdUse,
		Short: envCmdDesc,
		Long:  envCmdDesc,
		Run: func(_ *cobra.Command, _ []string) {
			configBytes, err := json.MarshalIndent(config, "", "    ")
			if err != nil {
				*runErr = err
				return
			}

			fmt.Println(string(configBytes))
		},
	}
}

// runListenCloser runs the ListenCloser until signaled to stop
func runListenCloser(lc utils.ListenCloser) {
	// Start listening in the background
	go func() {
		if err := lc.Listen(); err != nil {
			log.Fatalln("Daemon listen error:", err.Error())
		}
	}()

	// Wait for exit signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-sigCh

	// Stop server
	if err := lc.Close(); err != nil {
		log.Fatalln("Daemon shutdown error:", err.Error())
	}
}

// runStreamProcessorManagers returns a cobra command that instantiates and runs
// a set of stream process managers
func runStreamProcessorManagers(
	sc *servicesctrl.Control,
	config *cfg.Config,
	runError *error,
	listenCloseFactories []utils.ListenCloser,
	consumerFactories []consumers.ConsumerFactory,
	factoriesChainDB []stream.ProcessorFactoryChainDB,
	factoriesInstDB []stream.ProcessorFactoryInstDB,
) func(_ *cobra.Command, _ []string) {
	return func(_ *cobra.Command, arg []string) {
		wg := &sync.WaitGroup{}

		bm, _ := sc.BalanceManager.(*balance.Manager)
		err := bm.Start(sc.IsAccumulateBalanceIndexer)
		if err != nil {
			*runError = err
			return
		}
		defer func() {
			bm.Close()
		}()

		rh := &rewards.Handler{}
		err = rh.Start(sc)
		if err != nil {
			*runError = err
			return
		}
		defer func() {
			rh.Close()
		}()

		err = consumers.Bootstrap(sc, config.NetworkID, config, consumerFactories)
		if err != nil {
			*runError = err
			return
		}

		sc.BalanceManager.Exec()

		runningControl := utils.NewRunning()

		err = consumers.IndexerFactories(sc, config, factoriesChainDB, factoriesInstDB, wg, runningControl)
		if err != nil {
			*runError = err
			return
		}

		if len(arg) > 0 && arg[0] == "api" {
			lc, err := api.NewServer(sc, *config)
			if err != nil {
				log.Fatalln("API listen error:", err.Error())
			} else {
				listenCloseFactories = append(listenCloseFactories, lc)
			}
		}

		for _, listenCloseFactory := range listenCloseFactories {
			wg.Add(1)
			go func(lc utils.ListenCloser) {
				wg.Done()
				if err := lc.Listen(); err != nil {
					log.Fatalln("Daemon listen error:", err.Error())
				}
			}(listenCloseFactory)
		}

		// Wait for exit signal
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		<-sigCh

		for _, listenCloseFactory := range listenCloseFactories {
			if err := listenCloseFactory.Close(); err != nil {
				log.Println("Daemon shutdown error:", err.Error())
			}
		}

		runningControl.Close()

		wg.Wait()
	}
}

type MysqlLogger struct {
	Log logging.Logger
}

func (m *MysqlLogger) Print(v ...interface{}) {
	s := fmt.Sprint(v...)
	m.Log.Warn("[mysql]: %s", s)
}

func migrateMysql(mysqlDSN, migrationsPath string) error {
	migrationSource := fmt.Sprintf("file://%v", migrationsPath)
	migrater, migraterErr := migrate.New(migrationSource, mysqlDSN)
	if migraterErr != nil {
		return migraterErr
	}

	if err := migrater.Up(); err != nil {
		if err != migrate.ErrNoChange {
			return err
		}
		log.Println("[mysql]: no new migrations to execute")
	}

	return nil
}

//initialize scheduler-timer for cache
func initCacheScheduler(config *cfg.Config) {
	//we give a threshold of 10 seconds in order for the api server to fireup (since the caching mechanism is running as a separate thread)
	time.Sleep(10 * time.Second)
	toDate := time.Now()
	yesterdayDateTime := time.Now().AddDate(0, 0, -1)
	prevWeekDateTime := time.Now().AddDate(0, 0, -7)
	prevMonthDateTime := time.Now().AddDate(0, -1, 0)

	//convert to specific date format
	toDateStr := convertToMagellanDateFormat(toDate)
	yesterdayDateTimeStr := convertToMagellanDateFormat(yesterdayDateTime)
	prevWeekDateTimeStr := convertToMagellanDateFormat(prevWeekDateTime)
	prevMonthDateTimeStr := convertToMagellanDateFormat(prevMonthDateTime)

	initCacheStorage(config.Chains)

	MyTimer := time.NewTimer(3 * time.Second)

	for _ = range MyTimer.C {
		MyTimer.Stop()

		//update cache for all chains
		for id := range config.Chains {
			//fmt.Println(id)
			//previous day aggregate number
			getAggregatesAndUpdate(id, yesterdayDateTimeStr, toDateStr, "day")
			getAggregatesFeesAndUpdate(id, yesterdayDateTimeStr, toDateStr, "day")
			//previous week aggregate number
			getAggregatesAndUpdate(id, prevWeekDateTimeStr, toDateStr, "week")
			getAggregatesFeesAndUpdate(id, prevWeekDateTimeStr, toDateStr, "week")
			//previous month aggregate number
			getAggregatesAndUpdate(id, prevMonthDateTimeStr, toDateStr, "month")
			getAggregatesFeesAndUpdate(id, prevMonthDateTimeStr, toDateStr, "month")
		}

		MyTimer.Reset(3 * time.Second)
	}
}

func initCacheStorage(pchains cfg.Chains) {
	var aggregateTransMap = cfg.GetAggregateTransactionsMap()
	var aggregateFeesMap = cfg.GetAggregateFeesMap()
	for id := range pchains {
		aggregateTransMap[id] = map[string]uint64{}
		aggregateFeesMap[id] = map[string]uint64{}

		//we initialize the value for all 3 chains
		aggregateTransMap[id]["day"] = 0
		aggregateTransMap[id]["week"] = 0
		aggregateTransMap[id]["month"] = 0

		//we initialize the value for all 3 chains also here
		aggregateFeesMap[id]["day"] = 0
		aggregateFeesMap[id]["week"] = 0
		aggregateFeesMap[id]["month"] = 0
	}
}

func getAggregatesAndUpdate(chainid string, startTime string, endTime string, rangeKeyType string) string {
	serverPort := 8080
	requestURL := fmt.Sprintf("http://localhost:%d/v2/aggregates?cacheUpd=true&chainID="+chainid+"&startTime="+startTime+"&endTime="+endTime, serverPort)

	res, err := http.Get(requestURL)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
	}

	if res != nil {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("client: could not read response body: %s\n", err)
		}
		var aggregatesMain cfg.AggregatesMain
		aggregatesMainJson := resBody
		json.Unmarshal([]byte(aggregatesMainJson), &aggregatesMain)
		//based on the rangeKeyType we update the relevant part of our map - cache (we could imply that from the diff endTime - startTime but for simplicity we added this switch)
		cfg.GetAggregateTransactionsMap()[chainid][rangeKeyType] = aggregatesMain.Aggregates.TransactionCount
		return strconv.FormatUint(aggregatesMain.Aggregates.TransactionCount, 10)
	}
	return ""
}

func getAggregatesFeesAndUpdate(chainid string, startTime string, endTime string, rangeKeyType string) string {
	serverPort := 8080
	requestURL := fmt.Sprintf("http://localhost:%d/v2/txfeeAggregates?cacheUpd=true&chainID="+chainid+"&startTime="+startTime+"&endTime="+endTime, serverPort)

	res, err := http.Get(requestURL)
	if err != nil {
		fmt.Printf("error making http request: %s\n", err)
	}

	if res != nil {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("client: could not read response body: %s\n", err)
		}
		var aggregatesFeesMain cfg.AggregatesFeesMain
		aggregatesFeesMainJson := resBody
		json.Unmarshal([]byte(aggregatesFeesMainJson), &aggregatesFeesMain)
		//based on the rangeKeyType we update the relevant part of our map - cache (we could imply that from the diff endTime - startTime but for simplicity we added this switch)
		cfg.GetAggregateFeesMap()[chainid][rangeKeyType] = aggregatesFeesMain.Aggregates.Txfee
		return strconv.FormatUint(aggregatesFeesMain.Aggregates.Txfee, 10)
	}
	return ""
}

func convertToMagellanDateFormat(pDateTime time.Time) string {
	//we need to convert to format e.g 2022-07-01T10:21:16.808Z
	return strconv.Itoa(pDateTime.Year()) + "-" + lpadDatePart(int(pDateTime.Month())) + "-" + lpadDatePart(pDateTime.Day()) + "T" + lpadDatePart(pDateTime.Hour()) + ":" + lpadDatePart(pDateTime.Minute()) + ":" + lpadDatePart(pDateTime.Second()) + "." + "000Z"
}

func lpadDatePart(datepart int) string {
	return fmt.Sprintf("%02d", datepart)
}
