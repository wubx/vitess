package tabletserver

import (
	"flag"
	"io/ioutil"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")
	qsConfigFile    = flag.String("queryserver-config-file", "", "config file name for the query service")
	customRules     = flag.String("customrules", "", "custom query rules file")
)

// DefaultQSConfig is the default value for the query service config.
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = Config{
	PoolSize:           16,
	StreamPoolSize:     750,
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	RowCache:           nil,
}

// InitQueryService registers the query service, after loading any
// necessary config files. It also starts any relevant streaming logs.
func InitQueryService() {
	SqlQueryLogger.ServeLogs(*queryLogHandler)
	TxLogger.ServeLogs(*txLogHandler)

	qsConfig := DefaultQsConfig
	if *qsConfigFile != "" {
		if err := jscfg.ReadJson(*qsConfigFile, &qsConfig); err != nil {
			log.Fatalf("cannot load qsconfig file: %v", err)
		}
	}

	RegisterQueryService(qsConfig)
}

// LoadCustomRules returns custom rules as specified by the command
// line flags.
func LoadCustomRules() (qrs *QueryRules) {
	if *customRules == "" {
		return NewQueryRules()
	}

	data, err := ioutil.ReadFile(*customRules)
	if err != nil {
		log.Fatalf("Error reading file %v: %v", *customRules, err)
	}

	qrs = NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Fatalf("Error unmarshaling query rules %v", err)
	}
	return qrs
}
