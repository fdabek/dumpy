package main

import "fmt"
import "os"
import "log"
import "flag"
import "io"
import md5 "crypto/md5"
import hex "encoding/hex"
import "path"

func walkDirectory(root string) <-chan string {
	out := make(chan string)
	var queue []string
	queue = append(queue, root)

	go func() {
		for len(queue) > 0 {
			d := queue[0]
			queue = queue[:1]
		
			f,err := os.Open(d)
			if err != nil {
				log.Print("Unable to open: %s\n", d)
				continue
			}

			for {
				infos,err := f.Readdir(100)
				if err == io.EOF {
					log.Print("EOF on readdir")
					break;
				}
				if err != nil {
					log.Fatal("Readdir failed")
				}

				for _,stat := range infos {
					full_path := path.Join(d, stat.Name())
					if stat.IsDir() {
						queue = append(queue, full_path)
					} else {
						out <- full_path
					}
				}
			}
			
		}
		close(out)
	}()

	return out
}

type Chunk struct {
	filename string
	offset   int64
	md5sum   string
}

func hashFiles(files <-chan string) <-chan Chunk {
	out := make(chan Chunk)

	go func() { for path := range files {
		f,err := os.Open(path)
		if err != nil {
			log.Printf("Couldn't open %s. Skipping it.", path)
		}
		var i int64
		i = 0
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
			c := Chunk{filename: path, offset: i, md5sum: hex.EncodeToString(csum[:])}
			out <- c
			i += int64(n)
		}
	}
		close(out)
	}()
	return out
}


func main() {
	root := flag.String("directory", "", "Directory to scan")
	flag.Parse()

	files := walkDirectory(*root)
	for c := range hashFiles(files) {
		fmt.Printf("%s (%d): %s\n", c.filename, c.offset, c.md5sum)
	}
}
