golopro
=======

Tiny multithreaded log processing framework in golang

The default implementation essentially can do the work of <code>cut/awk | sort | uniq -c </code> for multiple CSV compatible (e.g., Apache access logs) files usingle golang's multithreading. 

## Usage

<pre><code>
jack@jack-VirtualBox:~/work/golopro$ ./lopro -help
Usage of ./lopro:
  -comma=",": separator
  -in=".": input directory
  -keys="0": keys, starts with 0
  -out=".": output directory
  -procs=1: number of processes
</code></pre>

### Hints
* Use <code>ln -s</code> to link the log files to the input directory
* Compressed the files to save disk I/O

## Customization
There are two interfaces to be implemented.

* the parser

<pre><code>
type Parser interface {
  Clone() Parser
  Reset(r io.Reader)
  NextRecord() (int, interface{}, error)
}
</code></pre>

Sample implementation of CSV parser: CSVParser

<pre><code>
...
func (lp *CSVParser) NextRecord() (length int, record interface{}, err error) {
  r, err := lp.reader.Read()
  return 0, r, err
}
...
</code></pre>

* and the report (counting logic)

<pre><code>
type Report interface {
  New() Report
  Merge(report Report)
  Clear()

  Name() string
  Add(rec LogRecord)
  Output(path string)
}
</code></pre>

A simple counting report: QuickReport
<pre><code>
func (qr *QuickReport) Add(rec LogRecord) {
  r, ok := rec.([]string)
  if !ok {
    return
  }

  var key string
  //TODO: construct the key
  ...
  
  qr.result[key] += 1
}
</code></pre>

* and probably some tweaks for the main() function

<pre><code>
  ...
  parser := NewCSVParser((*comma)[0])
  reportMgr := NewReportManager()
  reportMgr.RegisterReport(NewQuickReport(ks))
  ...
</code></pre>

