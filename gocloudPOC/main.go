package main

import (
	"context"
	"io/ioutil"
	"log"

	"gocloud.dev/blob"

	// Import the blob packages we want to be able to open.
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

func main() {
	// Define our input.
	// if len(os.Args) != 3 {
	// 	log.Fatal("usage: upload BUCKET_URL FILE")
	// }
	// bucketURL := os.Args[1]
	// file := os.Args[2]

	// bucketURL := "DefaultEndpointsProtocol=https;AccountName=tommypoc;AccountKey=0e3S5vbs0Rf0ma8U3+gSJ2qylrz2aZQLb/I9GbnayQDKU737uwsEZuqTupuUcNVNPZ6ToMoP0eJBRcnJRwfMKA==;EndpointSuffix=core.windows.net"
	bucketURL := "azblob://tommyblob"
	file := "/Users/toforl1/Downloads/localToAzureBlob-poc/tommy.txt"

	ctx := context.Background()
	// Open a connection to the bucket.
	b, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		log.Fatalf("Failed to setup bucket: %s", err)
	}
	defer b.Close()

	// Prepare the file for upload.
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}

	w, err := b.NewWriter(ctx, file, nil)
	if err != nil {
		log.Fatalf("Failed to obtain writer: %s", err)
	}
	_, err = w.Write(data)
	if err != nil {
		log.Fatalf("Failed to write to bucket: %s", err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to close: %s", err)
	}
}
