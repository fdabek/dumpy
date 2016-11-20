package main

import "encoding/json"
import "log"
import "strings"

type FsEntry struct  {
	name string
	file bool
	lazy_file_maker func()
	chunks []Chunk
	children map[string]*FsEntry
}

func InsertFromJSON(root *FsEntry, bucket string, md string) {
	if (root.file) {
		log.Fatal("Can't insert under a file")
	}

	r := GetReader(bucket, md)
	dec := json.NewDecoder(r)
	for dec.More() {
		var c Chunk
		err := dec.Decode(&c)
		if err != nil {
			log.Fatal(err)
		}
		f := InsertPath(c.Path, root)
		f.chunks = append(f.chunks, c)
	}
}

func MakeDirEntry(name string, parent *FsEntry) *FsEntry {
	dir := new(FsEntry)
	dir.file = false
	dir.children = make(map[string]*FsEntry)
	dir.children[".."] = parent
	dir.children["."] = dir
	dir.name = name
	dir.lazy_file_maker = nil
	return dir
}

func MakeFileEntry(name string, parent *FsEntry) *FsEntry {
	file := new(FsEntry)
	file.file = true
	file.name = name
	file.lazy_file_maker = nil
	file.children = nil
	return file
} 

func MaybeInsertSubDir(root* FsEntry, dirname string) *FsEntry {
	dir,ok := root.children[dirname]
	if (!ok) {
		dir = MakeDirEntry(dirname, root)
		root.children[dirname] = dir
	}
	return dir
}

func InsertPath(path string, root *FsEntry) *FsEntry {
	if !strings.HasPrefix(path, "/") {
		log.Fatal("doesn't start with slash: ", path)
	}
	path = path[1:]
	parts := strings.SplitN(path, "/", 2)

	// special case for path ending with a directory
	if strings.Count(path, "/") == 1 && strings.HasSuffix(path, "/") {
		return MaybeInsertSubDir(root, strings.TrimSuffix(path, "/"))
	}

	if len(parts) > 1 {
		// first bit was a directory. Add it if necessary and recurse
		return InsertPath("/" + parts[1], MaybeInsertSubDir(root, parts[0]))
	} else {
		// file, just add it
		if root.children[parts[0]] == nil  {
			root.children[parts[0]] = MakeFileEntry(parts[0], root)
		}
		return root.children[parts[0]]
	}
}

func Walk(dir *FsEntry, depth int, callback func(*FsEntry, []*FsEntry)) {
	walkHelper(dir, depth, callback, []*FsEntry{})
}

func walkHelper(dir *FsEntry, depth int, callback func(*FsEntry, []*FsEntry), path []*FsEntry) {
	if dir.lazy_file_maker != nil {
		dir.lazy_file_maker()
		dir.lazy_file_maker = nil
	}

	q := make([]*FsEntry, 0)
	for name,entry := range dir.children {
		if (name == "." || name == "..") {
			continue
		}

		callback(entry, path)
		if (entry.file == false) {
			q = append(q, entry)
		}
	}

	if (depth == 1) {
		return
	}

	// recurse
	for _,entry := range q {
		walkHelper(entry, depth - 1, callback, append(path, entry))
	}
}

func ListDir(dir* FsEntry) <-chan *FsEntry {
	ret := make(chan *FsEntry)
	cb := func(dir* FsEntry, parents []*FsEntry) {
		ret <- dir
	}
	go func() {
		Walk(dir, 1, cb)
		close(ret)
	}()
	return ret
}

func ChangeDir(dir *FsEntry, arg string) *FsEntry {
	kid,ok := dir.children[arg]
	if !ok || kid.file == true {
		return nil
	}
	return kid
}

func GetFSEntry(dir *FsEntry, filename string) *FsEntry {
	f := dir.children[filename]
	return f
}

func FormatFilename(entry *FsEntry) string {
	if entry.file {
		return entry.name
	}
	return entry.name + "/"
}
