package main

import (
	"flag"
	"log"
	"strings"

	"github.com/fdabek/dumpy"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	root := flag.String("directory", "", "Directory to scan")
	bucket := flag.String("bucket", "", "Bucket for chunks")
	mode := flag.String("mode", "", "backup|restore")
	chown := flag.Bool("chown", true, "automatic chown")

	flag.Parse()

	dumpy.InitStorageClient()
	if *mode == "backup" {
		roots := strings.Split(*root, ",")
		dumpy.BackupFromRoots(*bucket, roots)
	} else if *mode == "interactive" {
		dumpy.InteractiveRestoreTerminal(*bucket, *chown)
	} else {
		log.Fatalf("Not supported\n")
	}
}
