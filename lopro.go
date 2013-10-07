package main

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

//TODO: or you can redefine LogRecord
type LogRecord interface{}

type Report interface {
	New() Report
	Merge(report Report)
	Clear()

	Name() string
	Add(rec LogRecord)
	Output(path string)
}

type ReportManager struct {
	reports    []Report
	references []*ReportManager
}

func NewReportManager() *ReportManager {
	return &ReportManager{make([]Report, 0, 1), make([]*ReportManager, 0, 1)}
}

func (rm *ReportManager) Clone() *ReportManager {
	nrm := &ReportManager{make([]Report, len(rm.reports), len(rm.reports)), nil}
	for i, r := range rm.reports {
		nrm.reports[i] = r.New()
	}
	rm.references = append(rm.references, nrm)
	return nrm
}

func (rm *ReportManager) Reduce() {
	for i, r := range rm.reports {
		for _, nrm := range rm.references {
			r.Merge(nrm.reports[i])
			nrm.reports[i].Clear()
		}
	}
}

func (rm *ReportManager) Output(dir string) {
	for _, r := range rm.reports {
		path := dir + "/result-" + r.Name() + ".txt"
		r.Output(path)
	}
}

func (rm *ReportManager) RegisterReport(rpt Report) { rm.reports = append(rm.reports, rpt) }

func (rm *ReportManager) ProcessRecord(rec LogRecord) {
	for _, report := range rm.reports {
		report.Add(rec)
	}
}

type WorkerStats struct {
	files, bytes, bytesCompressed, records int64
}

func (s *WorkerStats) Merge(ws *WorkerStats) {
	s.files += ws.files
	s.bytes += ws.bytes
	s.bytesCompressed += ws.bytesCompressed
	s.records += ws.records
}

func (s *WorkerStats) ToString() string {
	return fmt.Sprintf("files=%d, bytes=%d, bytesCompressed=%d, records=%d", s.files, s.bytes, s.bytesCompressed, s.records)
}

type Parser interface {
	Clone() Parser
	Reset(r io.Reader)
	NextRecord() (int, interface{}, error)
}

type Worker struct {
	tasks chan string
	exit  chan bool

	id        int
	stats     WorkerStats
	reportMgr *ReportManager
	parser    Parser
}

func NewWorker(tasks chan string, exit chan bool, id int, reportMgr *ReportManager, parser Parser) *Worker {
	return &Worker{tasks: tasks, exit: exit, id: id, reportMgr: reportMgr, parser: parser}
}

func (w *Worker) Run() {
	for {
		file := <-w.tasks
		if file == "" {
			w.exit <- true
			break
		}

		err := w.Process(file)
		if err != nil {
			log.Printf("failed to process %s: %v\n", file, err)
		}
	}
}

type DefaultReport struct {
	result map[string]int64
}

func (r *DefaultReport) Merge(nr *DefaultReport) {
	for k, v := range nr.result {
		r.result[k] += v
	}
}

func (r *DefaultReport) Clear() { r.result = make(map[string]int64) }
func (r *DefaultReport) Output(path string) {
	fp, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	defer fp.Close()

	for k, v := range r.result {
		fp.WriteString(fmt.Sprintf("%s,%d\n", k, v))
	}
}

func (w *Worker) Process(file string) error {
	log.Printf("[%d]processing %s...\n", w.id, file)

	fi, err := os.Stat(file)
	if err != nil {
		return err
	}

	fp, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fp.Close()

	var zfp io.Reader
	if strings.HasSuffix(file, ".gz") {
		gzfp, err := gzip.NewReader(fp)
		if err != nil {
			return err
		}
		zfp = gzfp
		defer gzfp.Close()
	} else if strings.HasSuffix(file, ".bz2") {
		zfp = bzip2.NewReader(fp)
	} else {
		zfp = fp
	}

	fin := bufio.NewReaderSize(zfp, 8*1024*1024)
	w.parser.Reset(fin)

	for {
		bytes, rec, err := w.parser.NextRecord()
		if err != nil {
			if err != io.EOF {
				log.Printf("failed to parse: file=%s, %v\n", file, err)
			} else {
				break
			}
		}

		w.reportMgr.ProcessRecord(rec)
		w.stats.bytes += int64(bytes)
		w.stats.records += 1
	}

	w.stats.bytesCompressed += fi.Size()
	w.stats.files += 1
	return nil
}

type CSVParser struct {
	comma  byte
	reader *csv.Reader
}

func NewCSVParser(comma byte) *CSVParser { return &CSVParser{comma: comma, reader: nil} }

func (lp *CSVParser) Reset(r io.Reader) {
	lp.reader = csv.NewReader(r)
	lp.reader.Comma = rune(lp.comma)
	lp.reader.TrimLeadingSpace = true
}

func (lp *CSVParser) Clone() Parser { return NewCSVParser(lp.comma) }
func (lp *CSVParser) NextRecord() (int, interface{}, error) {
	r, err := lp.reader.Read()
	return 0, r, err
}

type QuickReport struct {
	DefaultReport
	keys []int
}

func NewQuickReport(keys []int) *QuickReport {
	return &QuickReport{DefaultReport{make(map[string]int64)}, keys}
}

func (qr *QuickReport) New() Report      { return NewQuickReport(qr.keys) }
func (qr *QuickReport) Name() string     { return "quick" }
func (qr *QuickReport) Merge(rpt Report) { qr.DefaultReport.Merge(&rpt.(*QuickReport).DefaultReport) }

func (qr *QuickReport) Add(rec LogRecord) {
	r, ok := rec.([]string)
	if !ok {
		return
	}

	//TODO: implement report logic
	var key string
	for i, k := range qr.keys {
		if i > 0 {
			key += ","
		}
		if k >= len(r) {
			continue
		} else {
			key += r[k]
		}
	}

	qr.result[key] += 1
}

func main() {
	var in *string = flag.String("in", ".", "input directory")
	var out *string = flag.String("out", ".", "output directory")
	var nprocs *int = flag.Int("procs", 1, "number of processes")
	var comma *string = flag.String("comma", ",", "separator")
	var keys *string = flag.String("keys", "0", "keys")
	flag.Parse()

	fi, err := os.Stat(*in)
	if err != nil {
		return
	}

	files := make([]string, 0, 4096)
	if fi.IsDir() {
		fis, _ := ioutil.ReadDir(*in)
		for _, fi := range fis {
			if !fi.IsDir() {
				files = append(files, *in+"/"+fi.Name())
			}
		}
	} else {
		files = append(files, *in)
	}

	log.Printf("%d files to process\n", len(files))

	ks := make([]int, 0, 1)
	for _, s := range strings.Split(*keys, ",") {
		i, err := strconv.Atoi(s)
		if err != nil {
			continue
		}
		ks = append(ks, i)
	}
	if len(ks) == 0 {
		return
	}

	parser := NewCSVParser((*comma)[0])
	reportMgr := NewReportManager()
	//TODO: register reports
	reportMgr.RegisterReport(NewQuickReport(ks))

	nworkers := *nprocs
	runtime.GOMAXPROCS(nworkers)

	workers := make([]*Worker, nworkers)
	tasks := make(chan string, nworkers)
	exit := make(chan bool, nworkers)

	workers[0] = NewWorker(tasks, exit, 0, reportMgr, parser)
	for i := 1; i < nworkers; i++ {
		workers[i] = NewWorker(tasks, exit, i, reportMgr.Clone(), parser.Clone())
	}

	for _, w := range workers {
		go w.Run()
	}

	nfiles := len(files)
	for i, file := range files {
		log.Printf("%d/%d (%d%%): +%s\n", i, nfiles, int(i*100.0/nfiles), file)
		tasks <- file
	}

	// wait for all workers to exit
	for _, _ = range workers {
		tasks <- ""
		<-exit
	}

	master := workers[0]
	for _, w := range workers {
		log.Printf("Worker[%d]: %s\n", w.id, w.stats.ToString())
		if w == master {
			continue
		}
		master.stats.Merge(&w.stats)
	}

	reportMgr.Reduce()
	log.Printf("Total: %s\n", master.stats.ToString())

	reportMgr.Output(*out)
}
