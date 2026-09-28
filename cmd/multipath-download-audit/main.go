// A separate offline executable; never imported by the multipath runtime.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/sagernet/sing-box/protocol/multipath/audit"
)

func main() {
	journal := flag.String("journal", "", "journalctl JSONL or text file")
	instance := flag.String("instance", "", "server inbound tag")
	manifest := flag.String("manifest", "", "collector manifest (verifies binary/config/epoch/cursors and journal hash)")
	epoch := flag.String("epoch", "", "explicit epoch if journal contains restarts")
	flag.Parse()
	if *journal == "" || *instance == "" {
		flag.Usage()
		os.Exit(2)
	}
	f, err := os.Open(*journal)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer f.Close()
	if *manifest != "" {
		m, e := os.Open(*manifest)
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(2)
		}
		capture, e := audit.VerifyDownloadCapture(f, m, *instance)
		m.Close()
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		if *epoch != "" && *epoch != capture.Epoch {
			fmt.Fprintln(os.Stderr, "manifest epoch differs from requested epoch")
			os.Exit(1)
		}
		*epoch = capture.Epoch
	}
	report, err := audit.AnalyzeServerDownload(f, *instance, *epoch)
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if writeErr := encoder.Encode(report); writeErr != nil {
		fmt.Fprintln(os.Stderr, writeErr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
