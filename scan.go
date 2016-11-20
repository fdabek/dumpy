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
import terminal  "golang.org/x/crypto/ssh/terminal"
import "strings"
import "os/exec"
import "strconv"
import "bytes"

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

func Restore(bucket string, chunks []Chunk) {
	// TODO: verify all chunks are present
	for _,c := range chunks {
		// add in the data
		c.data = readObject(bucket, c.Md5sum)
		c.Path = c.Path[1:]
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
	}
}

func RestoreAll(bucket string, metadata string) {
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

		// make relative
		c.Path = c.Path[1:]
		chunks <- c
	}
	close(chunks)

	wg.Wait()
}

type FsState struct {
	pwd *FsEntry
	term *terminal.Terminal
}

type Command struct {
	name string
	min_args int
	max_args int
	usage string
	cmd func(pwd *FsState, args []string)
}

func VerifyCommand(cmd *Command, args []string) bool {
	num_args := len(args) - 1;  // -1 for command name in args[0]
	if num_args > cmd.max_args || num_args < cmd.min_args {
		return false
	}
	return true
}

func DiskUsage(dir string) int64 {
	path, err := exec.LookPath("du")
	if err != nil {
		log.Fatal("Install du")
	}
	cmd := exec.Command(path, "-sb", dir)
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	s := out.String()
	parts := strings.Split(s, "\t")
	ret,_ := strconv.ParseInt(parts[0], 10, 64)
	return ret
}

func ProgressBar(total_bytes int64, in <-chan Chunk) <-chan Chunk{
	out := make(chan Chunk)
	go func() {
		var done_bytes int64
		done_bytes = 0
		for chunk := range in {
			done_bytes += int64(len(chunk.data))
			chunk.data = nil
			fmt.Printf("\r Finished %d of %d", done_bytes, total_bytes)
		}
		close(out)
	}()
	return out
}

func main() {
	root := flag.String("directory", "", "Directory to scan")
	bucket := flag.String("bucket", "", "Bucket for chunks")
	mode := flag.String("mode", "", "backup|restore")

	flag.Parse()

	initStorageClient()

	if *mode == "backup" {
		bytes := DiskUsage(*root)
		existing := make(map[string]bool)
		for s := range ListBucket(*bucket) {
			existing[s] = true  // really dumb set
		}

		chunks := walkDirectory(*root)  // get all chunks in source file system
		n, e := filterChunks(hashFiles(chunks), existing)  // hash them to find new and existing ones
		u := uploadChunks(n, *bucket) // upload the new ones, spit out chunks after uploaded
		j := mergeTwo(e, u)  // write everything to the JSON file (if a chunk gets here it's in GCS)

		// generate a name for the backup: metadata/hostname/YY/MM/DD/HH/MM
		t := time.Now()
		host,_ := os.Hostname();
		prefix := t.Format("06/01/02/03/04")
		metadata_filename := "/metadata/" + host + "/" + prefix + "/backup.json"
		fmt.Println("Writing metadata to ", metadata_filename)
		w := GetWriter(*bucket, metadata_filename, "application/json")
		writeJSON(ProgressBar(bytes, j), w)
	} else if (*mode == "restore") {
//		restore(*bucket, *restore_json)
	} else if (*mode == "interactive") {
		// Set up the terminal
		if !terminal.IsTerminal(0) {
			log.Fatal("stdin not a terminal")
		}
		oldState, err := terminal.MakeRaw(0)
		if err != nil {
			log.Fatal(err)
		}
		defer terminal.Restore(0, oldState)
		n := terminal.NewTerminal(os.Stdin, ">")

		// Insert the metadata directories:
		root := MakeDirEntry("", nil)
		for s := range ListMetadata(*bucket) {
			d := InsertPath(strings.TrimSuffix(strings.TrimPrefix(s, "/metadata"), "backup.json"), root)
			d.lazy_file_maker = func() { InsertFromJSON(d, "dumpy", s) }
		}

		var fs_state *FsState
		fs_state = &FsState{root, n}

		// Set up commands:
		cmds := make(map[string]Command)
		cmds["ls"] = Command{"ls", 0, 0, "ls ; List current directory", func(state *FsState, args []string) {
			c := ListDir(state.pwd)
			for f := range c {
				n.Write([]byte(FormatFilename(f) + "\r\n"))
			}
		}}
		cmds["cd"] = Command{"cd", 1, 1, "cd dir ; Change directory", func(state *FsState, args []string) {
			new := ChangeDir(state.pwd, args[1])
			if new == nil {
				n.Write([]byte("Error changing to" + args[1] + "\r\n"))
			} else {
				state.pwd = new
			}
		}}
		cmds["restore"] = Command{"restore", 1, 1, "restore target ; Restore a file or directory", func(state *FsState, args []string) {
			f := GetFSEntry(state.pwd, args[1])
			if f == nil {
				state.term.Write([]byte("Failed to open " + args[1] + "\r\n"))
				return
			}
			if f.file {
				Restore(*bucket, f.chunks)
			} else {
				restore_func := func(f *FsEntry, path []*FsEntry) {
					if f.file {
						state.term.Write([]byte("Restoring: " + f.name + "..."))
						Restore(*bucket, f.chunks)
						state.term.Write([]byte("done.\r\n"));
					}
				}
				Walk(state.pwd, 1023, restore_func)
			}
		}}

		// Wait for commands:
		for {
			line,_ := n.ReadLine()
			if line == "exit" {
				break
			}
			parts := strings.Split(line, " ")

			cmd,ok := cmds[parts[0]]
			if !ok {
				fs_state.term.Write([]byte("Unknown command " + parts[0] + "\r\n"))
			} else {
				if VerifyCommand(&cmd, parts) {
					cmd.cmd(fs_state, parts)
				} else {
					fs_state.term.Write([]byte(cmd.usage + "\r\n"))
				}
			}
		}
	}
}
