package main

import "bufio"
import "fmt"
import md5 "crypto/md5"
import hex "encoding/hex"
import "encoding/json"
import "flag"
import "log"
import "os"
import "path"
import "sync"
import "time"

// SRSLY?
func min(x, y int64) int64 {
    if x < y {
        return x
    }
    return y
}

type Chunk struct {
	Path     string
	FileSize     int64
	FileModTime  time.Time
	FilePerm     os.FileMode
	Offset   int64
	Md5sum   string
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
							c := Chunk{Path: full_path, FileSize: stat.Size(), FileModTime: stat.ModTime(), FilePerm: stat.Mode(), Offset: o, Md5sum: "empty", data: make([]byte, size)}
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
			f,err := os.Open(c.Path)
			if err != nil {
				log.Printf("Couldn't open %s. Skipping it.", c.Path)
				continue
			}
			n,err := f.ReadAt(c.data, c.Offset)
			if err != nil {
				log.Fatal("Non EOF error on ", f.Name())
			} else if  n != len(c.data) {
				log.Fatal("Short read: %d v %d (on %s)\n", n, len(c.data), c.Path)
			}
			csum := md5.Sum(c.data[:])
			c.Md5sum = hex.EncodeToString(csum[:])
			out <- c
		}
		close(out)
	}()
	return out
}

func writeJSON(chunks <-chan Chunk, filename string) {
	f,err := os.Create(filename)
	if err != nil {
		log.Fatal("Failed to open ", filename);
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for c := range chunks {
		err := enc.Encode(c)
		if (err != nil) {
			log.Fatal("Failed to encode")
		}

	}
	w.Flush()
}

func uploadChunks(chunks <-chan Chunk, bucket string) {
	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			for c := range chunks {
				CreateChunk(bucket, c.Md5sum, c.data)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}


func filterChunks(chunks <-chan Chunk, existing map[string]bool) <-chan Chunk {
	out := make(chan Chunk)
	go func() {
		for c := range chunks {
			if (existing[c.Md5sum] == true) {
				fmt.Println("Skipping ", c.Md5sum, ". already uploaded") 
			} else {
				out <- c
			}
		}
		close(out)
	}()
	return out
}

func main() {
	root := flag.String("directory", "", "Directory to scan")
	bucket := flag.String("bucket", "", "Bucket for chunks")
	flag.Parse()

	initStorageClient()

	existing := make(map[string]bool)
	for s := range ListBucket(*bucket) {
		existing[s] = true  // really dumb set
		fmt.Println(s);
	}

	chunks := walkDirectory(*root)
	filtered := filterChunks(hashFiles(chunks), existing)
	uploadChunks(filtered, *bucket)
}
