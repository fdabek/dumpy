package main

import "fmt"
import "os"
import "log"
import "flag"
import md5 "crypto/md5"
import hex "encoding/hex"
import "path"

// SRSLY?
func min(x, y int64) int64 {
    if x < y {
        return x
    }
    return y
}

type Chunk struct {
	path     string
	info     os.FileInfo
	offset   int64
	md5sum   string
	data     []byte
}

func walkDirectory(root string) <-chan Chunk {
	out := make(chan Chunk)
	var queue []string
	queue = append(queue, root)

	go func() {
		for len(queue) > 0 {
			d := queue[0]
			queue = queue[1:]
		
			f,err := os.Open(d)
			if err != nil {
				log.Print("Unable to open: %s\n", d)
				continue
			}

			for {
				infos,err := f.Readdir(100)
				if len(infos) == 0 {
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
						var o int64
						o = 0
						for o < stat.Size() {
							size := min(1 << 20, stat.Size() - o)
							c := Chunk{path: full_path, info: stat, offset: o, md5sum: "empty", data: make([]byte, size)}
							out <- c
							o += (1 << 20)
						}
					}
				}
			}
			
		}
		close(out)
	}()

	return out
}

func hashFiles(chunks <-chan Chunk) <-chan Chunk {
	out := make(chan Chunk)

	go func() {
		for c := range chunks {
			f,err := os.Open(c.path)
			if err != nil {
				log.Printf("Couldn't open %s. Skipping it.", c.path)
				continue
			}
			n,err := f.ReadAt(c.data, c.offset)
			if err != nil {
				log.Fatal("Non EOF error on ", f.Name())
			} else if  n != len(c.data) {
				log.Fatal("Short read: %d v %d (on %s)\n", n, len(c.data), c.path)
			}
			csum := md5.Sum(c.data[:])
			c.md5sum = hex.EncodeToString(csum[:])
			out <- c
		}
		close(out)
	}()
	return out
}

func main() {
	root := flag.String("directory", "", "Directory to scan")
	flag.Parse()

	chunks := walkDirectory(*root)
	for c := range hashFiles(chunks) {
		fmt.Printf("%s (%d): %s\n", c.path, c.offset, c.md5sum)
	}
}
