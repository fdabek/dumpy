package main

import "fmt"
import "os"
import "log"
import "flag"
import "io"
import md5 "crypto/md5"
import hex "encoding/hex"
import "path"

func main() {
	filename := flag.String("directory", "", "Directory to scan")
	flag.Parse()

	f,err := os.Open(*filename)
	if err != nil {
		log.Fatalf("Unable to open %s", *filename);
	}

	for {
		files,err := f.Readdir(100)
		if err == io.EOF {
			log.Print("EOF on readdir")
			break;
		}
		if err != nil {
			log.Fatal("Readdir failed")
		}
		for _,stat := range files {
			log.Printf("Name: %s\nSize: %d\n", stat.Name(), stat.Size())
			path := path.Join(*filename, stat.Name())
			f,err := os.Open(path)
			if err != nil {
				log.Printf("Couldn't open %s. Skipping it.", path)
				continue
			}
			for {
				b := make([]byte, 1<<20)
				n,err := f.Read(b)
				if n == 0 {
					log.Printf("EOF on %s", f.Name())
					break
				} else if err != nil {
					log.Fatal("Non EOF error on ", f.Name())
				}
				csum := md5.Sum(b[:n])
				fmt.Printf("%s: %s", f.Name(), hex.EncodeToString(csum[:]))
			}
		}
	}
}
