package main

import (
	"bytes"
	"fmt"
	"mrInspector/lokiclient"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var parseTs time.Time
var parseLoc uint64
var reSplit = regexp.MustCompile("\n\nseqNum: ")
var reFinder = regexp.MustCompile("0x(\\w+)\nTime: (.*)\n\nCode: 0x(\\w+)\nClass: (-?\\d+)\nLocale: 0x(\\w+)\nEvent Description: (.*)\nEvent Data:\n===========\n([\\s\\S]+)?")

func init() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetLevel(log.InfoLevel)
}

type Entry struct {
	SeqNum           uint64
	Time             time.Time
	Code             uint64
	Class            int64
	Locale           uint64
	EventDescription string
	EventData        string
}

func (e *Entry) Serialize() map[string]string {
	result := make(map[string]string)
	result["seq_num"] = strconv.FormatUint(e.SeqNum, 10)
	result["unix_nano"] = strconv.FormatInt(e.Time.UnixNano(), 10)
	result["code"] = strconv.FormatUint(e.Code, 10)
	result["class"] = strconv.FormatInt(e.Class, 10)
	result["locale"] = strconv.FormatUint(e.Locale, 10)
	return result
}

func (e *Entry) FormatLine() string {
	var builder strings.Builder
	builder.Grow(len(e.EventDescription) + len(e.EventData) + 1)

	builder.WriteString(e.EventDescription)
	builder.WriteString("\n")
	builder.WriteString(e.EventData)
	return builder.String()
}

func getTimestamp() (time.Time, uint64) {
	bS, err := os.ReadFile("./.pos.dat")
	if err != nil {
		return time.Time{}, 0
	}
	par := strings.Split(string(bS), "\n")
	if len(par) != 2 {
		panic("invalid .pos.dat format")
	}
	ts, err := time.Parse(time.RFC3339Nano, par[0])
	if err != nil {
		return time.Time{}, 0
	}
	ui, err := strconv.ParseUint(par[1], 10, 64)
	if err != nil {
		return time.Time{}, 0
	}
	return ts, ui
}

func setTimestamp() error {
	buf := bytes.NewBufferString(parseTs.Format(time.RFC3339Nano))
	buf.WriteString("\n")
	buf.WriteString(strconv.FormatUint(parseLoc, 10))
	return os.WriteFile("./.pos.dat", buf.Bytes(), 0644)
}

func handleLogs(path string) ([]*Entry, error) {
	bS, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	result := make([]*Entry, 0)
	partial := reSplit.Split(string(bS), -1)
	for _, s := range partial {
		pRes := reFinder.FindAllSubmatch([]byte(s), -1)
		if len(pRes) > 0 && len(pRes[0]) == 8 {
			seqNum_ := pRes[0][1]
			parseSeqNum_, errParse := strconv.ParseUint(string(seqNum_), 16, 64)
			if errParse != nil {
				return nil, errParse
			}
			time_ := pRes[0][2]
			parseTime_, errParse := time.Parse(time.ANSIC, string(time_))
			if errParse != nil {
				return nil, errParse
			}
			if parseTs.After(parseTime_) && parseSeqNum_ <= parseLoc {
				continue // skip
			}
			code_ := pRes[0][3]
			parseCode_, errParse := strconv.ParseUint(string(code_), 16, 64)
			if errParse != nil {
				return nil, errParse
			}
			class_ := pRes[0][4]
			parseClass_, errParse := strconv.ParseInt(string(class_), 10, 64)
			if errParse != nil {
				return nil, errParse
			}
			locale_ := pRes[0][5]
			parseLocale_, errParse := strconv.ParseUint(string(locale_), 16, 64)
			if errParse != nil {
				return nil, errParse
			}
			evDesc_ := pRes[0][6]
			evData_ := pRes[0][7]
			entry := &Entry{
				SeqNum:           parseSeqNum_,
				Time:             parseTime_,
				Code:             parseCode_,
				Class:            parseClass_,
				Locale:           parseLocale_,
				EventDescription: string(evDesc_),
				EventData:        string(evData_),
			}
			result = append(result, entry)
		}
	}
	if len(result) > 0 {
		parseTs = result[len(result)-1].Time // update
		parseLoc = result[len(result)-1].SeqNum
	}
	return result, nil
}

type scrapeConf struct {
	path     string
	interval time.Duration
	doneChan chan bool
}

func Worker(conf *scrapeConf) error {
	ticker := time.NewTicker(conf.interval)
	for {
		select {
		case <-ticker.C:
			entries, err := handleLogs(conf.path)
			if err != nil {
				log.Error(err)
				return err
			}
			if len(entries) > 0 {
				for _, entry := range entries {
					lokiclient.GetClient().Logf(entry.FormatLine(), entry.Serialize())
				}
			}
		case <-conf.doneChan:
			return nil
		}
	}
}

func main() {
	// calc TimeZone shift
	now := time.Now()
	_, shift := now.Zone()
	shift /= 3600

	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
	var interval, path_, startTm, logPath string
	var itv time.Duration

	if logPath = viper.GetString("service.log_dir"); logPath == "" {
		logPath = "./mr-watcher.log"
	} else {
		logPath = path.Join(logPath, "mr-watcher.log")
	}
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = file.Close()
	}()
	log.SetOutput(file)

	if path_ = viper.GetString("log_scrape.log_path"); path_ == "" {
		panic("unconfigured log_scrape.log_path")
	}
	if interval = viper.GetString("log_scrape.interval"); interval == "" {
		panic("unconfigured log_scrape.interval")
	}
	itv, err = time.ParseDuration(interval)
	if err != nil {
		panic(err)
	}
	if startTm = viper.GetString("log_scrape.start_time"); startTm == "" {
		panic("unconfigured log_scrape.start_time")
	}
	parseTs, err = time.Parse("2006-01-02 15:04:05", startTm)
	if err != nil {
		panic(err)
	}
	localTs, localLoc := getTimestamp()
	if localTs.After(parseTs) {
		parseTs = localTs // having .pos.dat
		parseLoc = localLoc
	}

	conf := &scrapeConf{
		path:     path_,
		interval: itv,
	}

	err = lokiclient.BootStrap(func(s string, m map[string]string) (string, error) {
		unixNano_, errParse := strconv.ParseInt(m["unix_nano"], 10, 64)
		if errParse != nil {
			return "", errParse
		}
		return strconv.FormatInt(time.Unix(0, unixNano_).Add(-time.Duration(shift)*time.Hour).UnixNano(), 10), nil
	})
	if err != nil {
		panic(err)
	}

	sig := make(chan os.Signal, 1)
	done := make(chan bool)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		{
			fmt.Println("Graceful Shutdown...")
			log.Warn("Graceful Shutdown...")
			close(done)
		}
	}()

	conf.doneChan = done
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = Worker(conf)
		if err != nil {
			panic(err)
		}
	}()

	for {
		select {
		case <-done:
			wg.Wait()
			err = setTimestamp()
			if err != nil {
				log.Error(err)
				fmt.Println(err.Error())
			}
			lokiclient.GetClient().Shutdown()
			return
		}
	}
}
