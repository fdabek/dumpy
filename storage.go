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

const (
	// Note: you must change it to the name of your bucket
	bucketName = "dumpy-test"

	samplePath = "/foo/bar.txt"
)

var (
	client *storage.Client
	ctx    context.Context

	sampleData []byte
)

func init() {
	d, err := ioutil.ReadFile("main.go")
	if err != nil {
	   log.Fatal(err)
}
	sampleData = d
}

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

func createObject(path string, data []byte) {
	objHandle := client.Bucket(bucketName).Object(path)
	_, err := objHandle.Attrs(ctx)
	if err == nil {
		fmt.Printf("Object '%s' already exists\n", path)
		return
	}
	if err != storage.ErrObjectNotExist {
	   log.Fatal(err)
	}
	w := objHandle.NewWriter(ctx)
	w.ContentType = "text/plain"
	// optional: set custom metadata
	if w.Metadata == nil {
		w.Metadata = make(map[string]string)
	}
//	sha1Hex := util.Sha1HexOfBytes(data)
//	w.Metadata["SHA1"] = sha1Hex
	_, err = w.Write(data)
	if err != nil {
	log.Fatal(err)
}
	err = w.Close()
	if err != nil {
	log.Fatal(err)
}
	fmt.Printf("Wrote '%s' of size %d", path, len(data))
}

func readObject(path string) []byte {
	objHandle := client.Bucket(bucketName).Object(path)
	r, err := objHandle.NewReader(ctx)
	if err != nil { log.Fatal(err) }
	data, err := ioutil.ReadAll(r)
	if err != nil { log.Fatal(err) }
	err = r.Close()
	if err != nil { log.Fatal(err) }
	attrs, err := objHandle.Attrs(ctx)
	if err != nil { log.Fatal(err) }
	sha1Hex := attrs.Metadata["SHA1"]
	fmt.Printf("Read '%s' of size %d, sha1: %s\n", path, len(data), sha1Hex)
	return data
}

func deleteObject(path string) {
	objHandle := client.Bucket(bucketName).Object(path)
	err := objHandle.Delete(ctx)
	if err != nil {
	log.Fatal(err)
}
	fmt.Printf("Deleted '%s'\n", path)
}

func listObjects() {
	objects := client.Bucket(bucketName).Objects(ctx, nil)
	for {
		attr, err := objects.Next()
		if err != nil {
		 break
		 }

            fmt.Println(attr.Name)
	}
}

func main() {
	initStorageClient()
	createObject(samplePath, sampleData)
	listObjects()
	readObject(samplePath)
	deleteObject(samplePath)
}
