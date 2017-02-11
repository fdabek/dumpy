package main

import (
	"io/ioutil"
	"log"

	"sync"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage	"cloud.google.com/go/storage"
        "google.golang.org/api/option"
)

var (
	client *storage.Client
	ctx    context.Context
)

func initStorageClient() {
	jsonKey, err := ioutil.ReadFile("cred.json")
	if err != nil {
		log.Fatalf("Need file 'cred.json' with service account credentials.\n")
	}
	conf, err := google.JWTConfigFromJSON(
		jsonKey,
		storage.ScopeReadWrite,
	)
	if err != nil {
		log.Fatal(err)
	}
	ctx = context.Background()
	opt := option.WithTokenSource(conf.TokenSource(ctx))
	client, err = storage.NewClient(ctx, opt)
	if err != nil {
		log.Fatal(err)
	}
}

func CreateChunk(bucket string, path string, data []byte) {
	objHandle := client.Bucket(bucket).Object(path)
	_, err := objHandle.Attrs(ctx)
	if err == nil {
		return
	}
 	if err != storage.ErrObjectNotExist {
	   log.Fatal("Error getting attributes: ", err)
	}
	w := objHandle.NewWriter(ctx)
	w.ContentType = "application/octet-stream"

	_, err = w.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// TODO(fdabek): refactor above to use GetWriter
func GetWriter(bucket string, path string, content_type string) *storage.Writer {
	objHandle := client.Bucket(bucket).Object(path)
	_, err := objHandle.Attrs(ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		log.Fatal("Error opening ", path)
	}
	writer := objHandle.NewWriter(ctx)
	writer.ContentType = content_type
	return writer
}

func readObject(bucket string, path string) []byte {
	objHandle := client.Bucket(bucket).Object(path)
	r, err := objHandle.NewReader(ctx)
	if err != nil { log.Fatal(err) }
	data, err := ioutil.ReadAll(r)
	if err != nil { log.Fatal(err) }
	err = r.Close()
	if err != nil { log.Fatal(err) }
	return data
}

func GetReader(bucket string, path string) *storage.Reader {
	objHandle := client.Bucket(bucket).Object(path)
	r, err := objHandle.NewReader(ctx)
	if err != nil { log.Fatal(err) }
	return r
}

func deleteObject(bucket string, path string) error {
	objHandle := client.Bucket(bucket).Object(path)
	return objHandle.Delete(ctx)
}

func ListBucket(bucket string) <-chan string {
	out := make(chan string)
	prefixes := []string{"0", "2", "4", "6", "8", "a", "c", "e", "f", "g"}

	var wg sync.WaitGroup
	for index, p := range prefixes {
		p := p  // go is stupid
		index := index // really stupid
		if (p == "g") {
			break
		}

		wg.Add(1)
		go func() {
			q := new(storage.Query)
			q.Prefix = p
			objects := client.Bucket(bucket).Objects(ctx, q)
			for {
				attr, err := objects.Next()
				if err != nil {
					break
				}
				if ((string)(attr.Name[0]) >= prefixes[index + 1]) {
					break
				}
				out <- attr.Name
			}
			wg.Done()
		}()	
	}

	go func() {
		// wait on some blocking thing
		wg.Wait()
		close(out)
	}()

	return out
}

func ListMetadata(bucket string) <-chan string {
	out := make(chan string)

	go func() {
		q := new(storage.Query)
		q.Prefix = "/metadata"

		objects := client.Bucket(bucket).Objects(ctx, q)
		for {
			attr, err := objects.Next()
			if err != nil {
				break
			}
			out <- attr.Name
		}
		close(out)
	}()	
	return out
}
