package main

import (
	"fmt"
	"io/ioutil"
	"log"

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
		fmt.Printf("Object '%s' already exists\n", path)
		return
	}
	if err != storage.ErrObjectNotExist {
	   log.Fatal(err)
	}
	w := objHandle.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	// optional: set custom metadata
//	if w.Metadata == nil{
//		w.Metadata = make(map[string]string)
//	}
//	w.Metadata["Filename"] = filename

	_, err = w.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Wrote '%s' of size %d\n", path, len(data))
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

func deleteObject(bucket string, path string) error {
	objHandle := client.Bucket(bucket).Object(path)
	return objHandle.Delete(ctx)
}

func ListBucket(bucket string) <-chan string {
	out := make(chan string)

	go func() {
		objects := client.Bucket(bucket).Objects(ctx, nil)
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
