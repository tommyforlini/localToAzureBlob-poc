package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Azure Storage Quickstart Sample - Demonstrate how to upload, list, download, and delete blobs.
//
// Documentation References:
// - What is a Storage Account - https://docs.microsoft.com/azure/storage/common/storage-create-storage-account
// - Blob Service Concepts - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-Concepts
// - Blob Service Go SDK API - https://godoc.org/github.com/Azure/azure-storage-blob-go
// - Blob Service REST API - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-REST-API
// - Scalability and performance targets - https://docs.microsoft.com/azure/storage/common/storage-scalability-targets
// - Azure Storage Performance and Scalability checklist https://docs.microsoft.com/azure/storage/common/storage-performance-checklist
// - Storage Emulator - https://docs.microsoft.com/azure/storage/common/storage-use-emulator

func randomString() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return strconv.Itoa(r.Int())
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

var (
	root  string
	files []string
	err   error
)

var wg = &sync.WaitGroup{}

func main() {
	fmt.Printf("Azure Blob storage quick start sample\n")

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 12 * time.Hour,
		},
	})

	// Create a random string for the quick start container
	containerName := fmt.Sprintf("quickstart-%s", randomString())

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Create the container
	fmt.Printf("Creating a container named %s\n", containerName)
	ctx := context.Background() // This example uses a never-expiring context
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	handleErrors(err)

	// Create a file to test the upload and download.
	// fmt.Printf("Creating a dummy file to test the upload and download\n")
	// data := []byte("hello world this is a blob\n")
	// fileName := randomString()
	// err = ioutil.WriteFile(fileName, data, 0700)
	// handleErrors(err)

	root := "/Users/toforl1/VirtualBox VMs"
	// root := "/Users/toforl1/dev/react-go-spa/ui"
	files, err = FilePathWalkDir(root)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			defer done(time.Now(), f)

			// fmt.Println(f)

			// Here's how to upload a blob.
			blobURL := containerURL.NewBlockBlobURL(f)
			file, err := os.Open(f)
			handleErrors(err)

			fmt.Printf("Uploading the file with blob name: %s\n", f)
			_, err = azblob.UploadFileToBlockBlob(context.TODO(), file, blobURL, azblob.UploadToBlockBlobOptions{
				BlockSize:/*4*/ 100 * 1024 * 1024,
				Parallelism:/*16*/ /*50*/ 100})
			file.Close()
			handleErrors(err)

		}(f)

	}
	wg.Wait()

	//
	//
	//
	//
	//
	//
	//
	// List the container that we have created above
	fmt.Println("Listing the blobs in the container:")
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
		handleErrors(err)

		// 	// ListBlobs returns the start of the next segment; you MUST use this to get
		// 	// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// 	// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fmt.Print("	Blob name: " + blobInfo.Name + "\n")
		}
	}

	fmt.Printf("Press enter key to  exit the application.\n")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Printf("Good bye!\n")

	//
	//
	//
	//
	//
	//
	//
	//
	// Here's how to download the blob
	// downloadResponse, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)

	// NOTE: automatically retries are performed if the connection fails
	// bodyStream := downloadResponse.Body(azblob.RetryReaderOptions{MaxRetryRequests: 20})

	// read the body into a buffer
	// downloadedData := bytes.Buffer{}
	// _, err = downloadedData.ReadFrom(bodyStream)
	// handleErrors(err)

	// The downloaded blob data is in downloadData's buffer. :Let's print it
	// fmt.Printf("Downloaded the blob: " + downloadedData.String())

	// Cleaning up the quick start by deleting the container and the file created locally
	// fmt.Printf("Press enter key to delete the sample files, example container, and exit the application.\n")
	// bufio.NewReader(os.Stdin).ReadBytes('\n')
	// fmt.Printf("Cleaning up.\n")
	// containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	// file.Close()
	// os.Remove(fileName)
}

func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func done(start time.Time, file string) {
	elapsed := time.Since(start)
	nr.RecordCustomMetric(code, 1)
	fmt.Printf("%s took %s and completed at %v\n", file, elapsed, time.Now().Format("2006-01-02 15:04:05"))
}
