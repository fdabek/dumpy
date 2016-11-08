package main

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
import storage	"cloud.google.com/go/storage"

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

func writeJSON(chunks <-chan Chunk, writer *storage.Writer) {
	enc := json.NewEncoder(writer)
	for c := range chunks {
		fmt.Println(c.Path)
		err := enc.Encode(c)
		if (err != nil) {
			log.Fatal("Failed to encode")
		}
		
	}
	writer.Close()
}

func uploadChunks(chunks <-chan Chunk, bucket string) <-chan Chunk{
	out := make(chan Chunk)
	go func() {
		var wg sync.WaitGroup
		wg.Add(50)
		for i := 0; i < 50; i++ {
			go func() {
				for c := range chunks {
					CreateChunk(bucket, c.Md5sum, c.data)
					c.data = nil
					out <- c
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(out)
	}()
	return out
}


func mergeTwo(a <-chan Chunk, b <-chan Chunk) <-chan Chunk {
	var wg sync.WaitGroup
	out := make(chan Chunk)
	
	output := func(c <-chan Chunk) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}

	wg.Add(2)
	go output(a)
	go output(b)
	
	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func filterChunks(chunks <-chan Chunk, existing map[string]bool) (<-chan Chunk, <-chan Chunk) {
	out_existing := make(chan Chunk)
	out_new := make(chan Chunk)
	go func() {
		for c := range chunks {
			if (existing[c.Md5sum] == true) {
				fmt.Println("Skipping ", c.Md5sum, ". already uploaded") 
				out_existing <- c
			} else {
				out_new <- c
			}
		}
		close(out_new)
		close(out_existing)
	}()
	return out_new, out_existing
}

func restore(bucket string, metadata string) {
	chunks := make(chan Chunk)
	var wg sync.WaitGroup

	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func() {
			for c := range chunks {
				// add in the data
				c.data = readObject(bucket, c.Md5sum)

				err := os.MkdirAll(path.Dir(c.Path), 0777)
				if err != nil {
					log.Fatal(err)
				}
				f, err := os.OpenFile(c.Path, os.O_RDWR | os.O_CREATE, c.FilePerm)
				if err != nil {
					log.Fatal("Error opening: ", err)
				}
				n, err := f.WriteAt(c.data, c.Offset)
				if err != nil || n != len(c.data) {
					log.Fatal("error writing to ", c.Path, " ", err)
				}
				f.Close()
			}
			wg.Done()
			
		}()
	}

	r := GetReader(bucket, metadata)
	dec := json.NewDecoder(r)
	for dec.More() {
		var c Chunk
		err := dec.Decode(&c)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(c.Path, " ")

		// make relative
		c.Path = c.Path[1:]
		chunks <- c
	}
	close(chunks)

	wg.Wait()
}

func main() {
	root := flag.String("directory", "", "Directory to scan")
	bucket := flag.String("bucket", "", "Bucket for chunks")
	mode := flag.String("mode", "", "backup|restore")
	metadata := flag.String("metadata", "", "metadta file name. Maybe the date?")
	flag.Parse()

	initStorageClient()

	if *mode == "backup" {
		existing := make(map[string]bool)
		for s := range ListBucket(*bucket) {
			existing[s] = true  // really dumb set
			fmt.Println(s);
		}

		chunks := walkDirectory(*root)  // get all chunks in source file system
		n, e := filterChunks(hashFiles(chunks), existing)  // hash them to find new and existing ones
		u := uploadChunks(n, *bucket) // upload the new ones, spit out chunks after uploaded
		j := mergeTwo(e, u)  // write everything to the JSON file (if a chunk gets here it's in GCS)
		w := GetWriter(*bucket, *metadata)
		writeJSON(j, w)
	} else if (*mode == "restore") {
		restore(*bucket, *metadata)
	} else {
		log.Fatal("Unkown mode: ", *mode)
	}
}
