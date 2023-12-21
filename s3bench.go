/*

*TODO:
0.  add logic to check if objectSize < MPU size (fallback to regular puts.)
1.  parallelize deletes by putting into multiple channels.
1.  Make a 'runtime' test: where it will re-run over and over for the specified runtime
2.  have a 'simplified' output mode to dump only the relevant bits: so when running on many hosts it can be easier to aggregate.
4.  keep track of rtt for each part (to generate latency distribution per part)
5.  for really large objects (non-multipart), figure out the right way to make sure data generation doesn't take too long.
5.  figure out how to deal with (gracefully) 500 errors (eg: exponential backoff/retry)



*/
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	mrand "math/rand"
	"net/http"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	version      = "Version 1.0"
	opRead       = "Read"
	opWrite      = "Write"
	opRangedRead = "Ranged-Read"
	minChunkSize = 20 * 1024 * 1024
	commitSize = 1000
)

var bufferBytes []byte

func main() {

	// really need to split this thing up into more functions one day.

	csvOutput := flag.Bool("csvOutput", false, "enable output to CSV file")
	optypes := []string{"read", "write", "both", "ranges"}
	operationListString := strings.Join(optypes[:], ", ")
	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	region := flag.String("region", "vast-west", "AWS region to use, eg: us-west-1|us-east-1, etc")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	operations := flag.String("operations", "write", "ops:"+operationListString)
	multipart := flag.Bool("multipart", false, "perform multipart uploads")
	partSize := flag.Int64("partSize", 500*1024*1024, "size for MPU parts")
	multiUploaders := flag.Int("multiUploaders", 5, "number of MPU uploaders per client..this is multiplicative with numClients..")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", getHostname()+"_loadgen_test/", "prefix of the object name that will be used")
	objectSize := flag.Int64("objectSize", 80*1024*1024, "size of individual requests in bytes (must be smaller than main memory)")
	rangeSize := flag.Int64("rangeSize", 1024, "when specifying -operations ranges , this is the range size.")
	numRequests := flag.Int64("numRequests", 10*1024, "when specifying -operations ranges , this is the total number of requests to issue.")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	batchSize := flag.Int("batchSize", 1000, "per-prefix batchsize")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	skipCleanup := flag.Bool("skipCleanup", false, "skip deleting objects created by this tool at the end of the run")
	skipBucketCreate := flag.Bool("skipBucketCreate", false, "skip pre-creation of bucket. bucket MUST exist for this to work.")
	verbose := flag.Bool("verbose", false, "print verbose per thread status")
	disableMD5 := flag.Bool("disableMD5", true, "for v4 auth: disable source md5sum calcs (faster)")
	cpuprofile := flag.Bool("cpuprofile", false, "profile this mofo")
	moreRando := flag.Bool("moreRando", false, "moreRando")
	chunkSize := flag.Int64("chunkSize", 31*1024, "When using -moreRando : chunk size to defeat dedup.")

	flag.Parse()

	//for ranged reads...we can have more clients than files.
	if strings.Contains(*operations, "ranges") {
		fmt.Printf("Doing ranged reads with %d threads vs %d objects\n", *numClients, *numSamples)

	} else {
		if *numClients > *numSamples || *numSamples < 1 {

			fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", *numClients, *numSamples)
			os.Exit(1)
		}
	}

	if *endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}
	//make sure that the right sizes are used

	if *multipart {

		if *partSize < 5242880 {
			fmt.Printf("multipart size must be at least 5242880 bytes, you specificied %v", *partSize)
			flag.PrintDefaults()
			os.Exit(1)

		}
		numParts := *objectSize / *partSize
		if numParts > 10000 {
			fmt.Printf("Using an -objectSize of %v and a -partSize of %v results in too many parts ( %v ), make one bigger/smaller...", *objectSize, *partSize, numParts)
			flag.PrintDefaults()
			os.Exit(1)

		}

	}

	var opTypeExists = false
	for op := range optypes {
		if optypes[op] == *operations {
			opTypeExists = true
		}
	}

	if !opTypeExists {
		fmt.Printf("operation type must be one of: %s \n", operationListString)
		os.Exit(1)
	}

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       *numSamples,
		rangeSize:        *rangeSize,
		numRequests:      *numRequests,
		batchSize:        *batchSize,
		numClients:       uint(*numClients),
		objectSize:       *objectSize,
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
		accessKey:        *accessKey,
		accessSecret:     *accessSecret,
		region:           *region,
		operations:       *operations,
		multipart:        *multipart,
		partSize:         *partSize,
		multiUploaders:   *multiUploaders,
		disableMD5:       *disableMD5,
		moreRando:        *moreRando,
		chunkSize:        *chunkSize,

		verbose: *verbose,
	}
	fmt.Println(version)
	fmt.Println(params)
	fmt.Println()

	if *cpuprofile {
		f, err := os.Create("cpuprofile.file")
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	mrand.Seed(time.Now().UnixNano())
	//make a session for makebucket and cleanup purposes.

	svc := makeS3session(params, params.endpoints[0])

	if !*skipBucketCreate {
		log.Println("creating bucket if required")

		makeBucket(bucketName, svc)
	} else {
		log.Println("skipping bucket creation..it better exist!")
	}

	/*
		function flow:
		1.  startClients -> loops through the -numClients and generates s3 sessions, then launches a go routine (startclient)
		1.5 : those go routines 'sit at the ready'
		2.  startclient : these are the 'at the ready' clients, ready to do work
		3.  params.Run : this launches a goroutine to run params.submitLoad
		4. params.submitload : calls params.makeKeyList to make a list of keys
		4.5 for each item in the keylist, it adds a request to the params.requests channel
		5.  Now we are back in 'startclient', where the work gets done.
		5.5 it loops through items in the params.request channel, and performs the actual work
		5.75 : results are put into the params.responses channel
		6.  The params.Run function collects the results, and returns them to the original (main) function.


	*/
	StartClients(params)
	/*

		1.  allow choice of write, read (maybe later list, etc)
		2.  if write only, just skip doing reads
		3.  if both, then just do writes and then reads
		4.  if reads: then we have to first check if there is existing data in the bucket/prefix we can use.

	*/

	if strings.Contains(params.operations, "write") {
		if *multipart {
			//need to make a minimum amount of random data, or the slicer won't be very effective.
			if *partSize < minChunkSize/2 {
				chunkSize := minChunkSize
				bufferBytes = makeData(int64(chunkSize))
			} else {
				chunkSize := *partSize * 2
				bufferBytes = makeData(chunkSize)
			}

		} else { //not multipart
			if *objectSize < minChunkSize/2 {
				chunkSize := minChunkSize
				bufferBytes = makeData(int64(chunkSize))
			} else {
				chunkSize := *objectSize * 2
				bufferBytes = makeData(chunkSize)
			}

		}

		fmt.Printf("Running %s test...\n", opWrite)
		writeResult := params.Run(opWrite)
		fmt.Println(params)
		fmt.Println(writeResult)
		fmt.Println()

	} else if strings.Contains(params.operations, "read") {

		fmt.Printf("Running %s test...\n", opRead)
		readResult := params.Run(opRead)
		fmt.Println(params)
		fmt.Println(readResult)

		fmt.Println()

	} else if strings.Contains(params.operations, "ranges") {

		fmt.Printf("Running %s test...\n", opRangedRead)
		readResult := params.Run(opRangedRead)
		fmt.Println(params)
		fmt.Println(readResult)

		fmt.Println()

	} else if strings.Contains(params.operations, "both") {
		if *multipart {
			chunkSize := *partSize * 2
			bufferBytes = makeData(chunkSize)
		} else {
			chunkSize := *objectSize * 2
			bufferBytes = makeData(chunkSize)
		}
		fmt.Printf("Running %s test...\n", opWrite)
		writeResult := params.Run(opWrite)

		fmt.Printf("Running %s test...\n", opRead)
		readResult := params.Run(opRead)
		fmt.Println(writeResult)
		fmt.Println()
		fmt.Println(readResult)
		fmt.Println()

	}

	if *csvOutput {
		err := outputToCSV([]Result{writeResult, readResult}, "output.csv")
		if err != nil {
			fmt.Printf("Error writing to CSV: %s\n", err)
			os.Exit(1)
		}
		fmt.Println("Results written to output.csv")
	}

	if !*skipCleanup {
		params.cleanup(svc)

	}

}

//first, make a func which will create the session setup.

func makeS3session(params Params, endpoint string) *s3.S3 {
	var httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	cfg := &aws.Config{
		Credentials:                   credentials.NewStaticCredentials(params.accessKey, params.accessSecret, ""),
		Region:                        aws.String(params.region),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(params.disableMD5),
		Endpoint:                      aws.String(endpoint),
		HTTPClient:                    httpClient,
	}
	sess := session.New(cfg)

	s3client := s3.New(sess)
	return s3client

}

func getHostname() string {
	hostName, err := os.Hostname()
	if err != nil {
		log.Fatal("couldn't get hostname from machine. you must manaully specify -objectNamePrefix instead: %v ", err)
		os.Exit(1)
	}
	return hostName
}

// next, a function to create a bucket

func makeBucket(bucketName *string, svc *s3.S3) {
	cparams := &s3.CreateBucketInput{
		Bucket: bucketName, // required
	}
	//	if _, derr := svc.CreateBucket(cparams); derr != nil && !isBucketAlreadyOwnedByYouErr(derr) {

	if _, derr := svc.CreateBucket(cparams); derr != nil && !bucketExists(derr) {

		log.Fatal(derr)
	}

}

//for the cleanup phase, delete the bucket

func delBucket(bucketName *string, svc *s3.S3) {
	cparams := &s3.DeleteBucketInput{
		Bucket: bucketName, // required
	}
	if _, derr := svc.DeleteBucket(cparams); derr != nil {
		log.Fatal(derr)
	}
	fmt.Printf("deleted bucket %v\n", *bucketName)

}

// function to generate data

func makeData(dataSize int64) []byte {

	/*
		This just makes XX bytes of random data.  Its kind of slow, so we just run this at the beginning of the job and re-use this buffer over and over.  The side effect is that this dedupe's a lot.
		Look at the 'betterSlicer' function to see how we deal with this.
	*/

	fmt.Printf("Generating %v bytes in-memory sample data... ", dataSize)
	timeGenData := time.Now()

	bufferBytes = make([]byte, dataSize, dataSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()
	return bufferBytes

}

func betterSlicer(chunkSize int64) []byte {

	/*basically, just return a slice of the bufferbytes slice from a random offset. The idea is this:
	* the bufferBytes slice is twice the size of the chunk being requested
	* if we choose a random starting offset within bufferBytes for each chunk, we effecitvely 'randomize' the chunk, since it will
	shift the bytes.  In theory, since the bufferBytes source data is 100% random, we should be OK, although a similarity based compression
	algorithm might defeat it (have to test)

	I had tried to come up with something 'smarter', whereby I set a chunksize of 16KB, and used random offsets for each 16KB chunk to assemble an object, however that
	seemed to increase runtime, as there is a small cost for each random access into the slice.  Its negligible for a small number of iterations, but it adds up.
	Since it appears that the code below does not defeat VAST similarity, maybe there is some tradeoff to make here.  perhaps adjust the chunk size to 1M.  However: how
	to do that without copying slices?  Is there a way to:
	1.  create a function that will return a single slice
	2.  that function would calculate XX rnadom 'starting points' within the bufferBytes slice
	3.  iterate through each starting point to yield a different start/end
	4.  do all this without copying any bytes.  Note that this would probably help eliminate dedup completely as well.

	Some drawbacks:
	* large objects (eg: 10G) which are not multipart mean that we have to generate a large buffer (eg: 20G). Perhaps that's OK.
	* Note: this does NOT defeat VAST similarity compression.  It only defeats regular compression & dedup.



	*/

	randStop := int64(len(bufferBytes)) - chunkSize

	randStart := mrand.Int63n(randStop - 1)
	randEnd := randStart + chunkSize

	chunky := bufferBytes[randStart:randEnd]

	return chunky

}

func sliceBuilder(dataSize int64, moreRando bool, chunkSize int64) []byte {

	/*



		try to defeat dedup:
		1.  use a 31k block size.
		2.  randomize each segment via betterslicer
		3.  build a slice per object, use append (slow..)

		seems like it does defeat dedup, but the cost is high (cuts write perf in half or more)
		also, it doesn't defeat sim , if the bs=31k. It does mostly defeat sim if you bring the block size down to 1k (but then you get some dedup since it divides into 32k evenly..perhaps 3k is beter..)

		turns out the assignment might be faster than append, : https://stackoverflow.com/questions/38654729/golang-slice-append-vs-assign-performance


	*/
	//make a slice
	// for objects smaller than the 31k chunksize, need to shrink the chunksize to  match.
	if chunkSize > dataSize {
		chunkSize = dataSize
	}

	byteStorage := make([]byte, chunkSize, dataSize)
	if moreRando {

		//set initial cursor to zero?
		byteStorage = byteStorage[:0]

		var curr, partLength int64
		var remaining = dataSize
		partNumber := 1
		for curr = 0; remaining != 0; curr += partLength {
			if remaining < chunkSize { //for the last chunk, which may be smaller.
				partLength = remaining
				//byteStorage[curr] = betterSlicer(partLength)
				byteStorage = append(byteStorage, betterSlicer(partLength)...)
			} else { // normal path
				partLength = chunkSize
				byteStorage = append(byteStorage, betterSlicer(partLength)...) // this appears to be slow, cuts b/w in half.

			}
			remaining -= partLength
			partNumber++
		}

	} else {
		byteStorage = betterSlicer(dataSize)
	}

	return byteStorage

}

func (params *Params) cleanup(svc *s3.S3) {

	/*
		this is single threaded, running against only a single endpoint (the same one which is used for makebucket).  It may be useful to
		parallelize this, but for now..its relatively quick.

	*/
	fmt.Println()
	fmt.Printf("Cleaning up %d objects...\n", params.numSamples)
	delStartTime := time.Now()

	numSuccessfullyDeleted := 0
	bigList := params.makeKeyList()

	keyList := make([]*s3.ObjectIdentifier, 0, params.batchSize)
	for i, key := range bigList {

		bar := s3.ObjectIdentifier{
			Key: key,
		}

		keyList = append(keyList, &bar)
		if len(keyList) == params.batchSize || i == params.numSamples-1 {
			if params.verbose {
				fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
			}
			delparams := &s3.DeleteObjectsInput{
				Bucket: aws.String(params.bucketName),
				Delete: &s3.Delete{
					Objects: keyList}}
			_, err := svc.DeleteObjects(delparams)
			if err == nil {
				numSuccessfullyDeleted += len(keyList)
				if params.verbose {

					fmt.Printf("Succeeded\n")
				}
			} else {
				fmt.Printf("Failed (%v)\n", err)
			}
			//set cursor to 0 so we can move to the next batch.
			keyList = keyList[:0]

		}
		i++
	}
	fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, params.numSamples, time.Since(delStartTime))

	// delete the bucket

	delBucket(&params.bucketName, svc)

}

func bucketExists(err error) bool {

	// there might be two different responses, depending on implementation.

	if aErr, ok := err.(awserr.Error); ok {
		switch aErr.Code() {
		case s3.ErrCodeBucketAlreadyOwnedByYou:
			return aErr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou
		case s3.ErrCodeBucketAlreadyExists:
			return aErr.Code() == s3.ErrCodeBucketAlreadyExists
		}
	}
	return false
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()
	//need to make a clause for ranged reads.
	var reqCount int
	if strings.Contains(params.operations, "ranges") {
		reqCount = int(params.numRequests)
	} else {
		reqCount = params.numSamples
	}

	// Start submitting load requests
	go params.submitLoad(op)

	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, reqCount), operation: op}

	for i := 0; i < reqCount; i++ {
		resp := <-params.responses
		errorString := ""
		if resp.err != nil {
			result.numErrors++
			fmt.Printf("error: %s", resp.err)
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.numOps++
			//TODO need to adjust this for ranged-reads.
			if strings.Contains(params.operations, "ranges") {
				result.bytesTransmitted = result.bytesTransmitted + params.rangeSize
			} else {
				result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			}
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
		}
		if params.verbose {
			fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
				op, resp.duration.Seconds(), i+1, params.numSamples,
				(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
				errorString)
		}
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	return result
}

func (params *Params) makeKeyList() []*string {
	/*
		goal here is to divide up all the requests so that there are no more than $batchSize per prefix-$num/ .
		EG: if the prefix is 'andy' , then we want to make $(numsamples /  ) sub-prefixes , so that the result is:
		bucket/andy/1/
		bucket/andy/2/
		etc
		That way we can fan out the objects more.
	*/
	keyList := make([]*string, 0, params.batchSize)
	bigList := make([]*string, 0, params.numSamples)
	numDirs := 0
	for i := 0; i < params.numSamples; i++ {
		bar := string(i)
		keyList = append(keyList, &bar)
		if len(keyList) == params.batchSize || i == params.numSamples-1 {

			for j := 0; j < len(keyList); j++ {
				pref := params.objectNamePrefix + "/" + strconv.Itoa(numDirs) + "/"
				key := aws.String(fmt.Sprintf("%s%d", pref, j))
				bigList = append(bigList, key)

			}

			//increment the dirname
			numDirs++
			//set cursor to 0 so we can move to the next batch.
			keyList = keyList[:0]

		}
	}

	return bigList
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {

	/*if we want to do some kind of bucket distribution, this is one place to do it. Another place
		might be in the makeKeyList function.
	Maybe do like this?

		1. count the buckets, and divide up the total number of keys to put (last bucket will get less)
		2. iterate through the keys, similarly to the makeKeyList function, assining each key to a bucket
		3.  submit

	*/
	bucket := aws.String(params.bucketName)
	bigList := params.makeKeyList()

	// this is so we randomize the list iteration.
	randSeed := mrand.NewSource(time.Now().UnixNano())
	r := mrand.New(randSeed)

	//  this is where we create all the requests, one per key.
	// now actually submit the load. each iteration chooses a random key from the list.
	// inefficient, but for ranged reads we need a whole separate block for now.

	if op != opRangedRead {
		for _, f := range r.Perm(len(bigList)) {

			if op == opWrite {

				if params.multipart {
					// once per object, there will be a separate setup for individual parts.
					params.requests <- &s3.CreateMultipartUploadInput{
						Bucket: bucket,
						Key:    bigList[f],
					}
				} else {
					//not multipart

					params.requests <- &s3.PutObjectInput{
						Bucket: bucket,
						Key:    bigList[f],
						Body:   bytes.NewReader(sliceBuilder(params.objectSize, params.moreRando, params.chunkSize)),
						//Body:   bytes.NewReader(betterSlicer(params.objectSize)),
					}
				}
			} else if op == opRead {

				params.requests <- &s3.GetObjectInput{
					Bucket: bucket,
					Key:    bigList[f],
				}
			} else {
				panic("Developer error")
			}
		}
	} else { // ranged read block
		/*for the 'ranged-read' operation type, we need to do
		something different.
		1.  decide on a runtime (or number of requests. perhaps numrequests is easier)
		2.  for each request, randomize:
			* a key from the bigList
			* the offset/range
		*/

		for i := 0; i < int(params.numRequests); i++ {
			//randomize the key.
			randoKey := bigList[mrand.Intn(len(bigList))]

			//make a random offset.
			stopMax := int64(params.objectSize) - params.rangeSize //this is the HIGHEST value to choose for randstop
			randStart := mrand.Int63n(stopMax - 1)
			randStop := (randStart - 1) + params.rangeSize
			rangeString := "bytes=" + strconv.FormatInt(randStart, 10) + "-" + strconv.FormatInt(randStop, 10)

			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    randoKey,
				Range:  aws.String(rangeString),
			}

		}

	}

}

func StartClients(params Params) {

	for i := 0; i < int(params.numClients); i++ {
		svc := makeS3session(params, params.endpoints[i%len(params.endpoints)])

		go params.startClient(svc)
		time.Sleep(1 * time.Millisecond)
	}

}

// Run an individual load request
func (params *Params) startClient(svc *s3.S3) {

	for request := range params.requests {
		putStartTime := time.Now()
		var err error
		numBytes := params.objectSize

		switch r := request.(type) {
		case *s3.PutObjectInput:

			req, _ := svc.PutObjectRequest(r)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()

		case *s3.GetObjectInput:
			if params.multipart {
				// https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_download_object.go
				downloader := s3manager.NewDownloaderWithClient(svc, func(u *s3manager.Downloader) {
					u.PartSize = params.partSize
					u.Concurrency = params.multiUploaders
				})
				numBytes = 0
				file, openErr := os.OpenFile("/dev/null", os.O_WRONLY, 644) //perhaps there's a better way, but this seems to work.
				if openErr != nil {
					panic("woops")
				}

				numBytes, err := downloader.Download(file, r)
				if err != nil {
					fmt.Printf("error: %v", err)
				}

				if numBytes != params.objectSize {
					err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
				}
			} else { //not multipart.
				req, resp := svc.GetObjectRequest(r)
				err = req.Send()
				numBytes = 0
				if err == nil {
					numBytes, err = io.Copy(ioutil.Discard, resp.Body)
				} else {
					fmt.Printf("there was a read error: %v , %v", resp, err)
				}
				if strings.Contains(params.operations, "ranges") {
					//if doing ranged reads, use an alternate calc.
					if numBytes != params.rangeSize {
						err = fmt.Errorf("expected range length %d, actual %d", params.rangeSize, numBytes)
					}

				} else {
					if numBytes != params.objectSize {
						err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
					}
				}
			}

		case *s3.CreateMultipartUploadInput:
			//first, make a MPU, and get the ID.

			mpu, _ := svc.CreateMultipartUpload(r)

			//we need a place to store info about completed parts
			ph := new(partholder)

			//this time , we need to randomize the endpoint list, in case the number of multi-uploaders is smaller than the full list.
			randEndpoints := make([]string, len(params.endpoints))
			perm := mrand.Perm(len(params.endpoints))
			for i, v := range perm {
				randEndpoints[v] = params.endpoints[i]
			}
			//make a channel just for this one object. all part-uploads for this object will flow through it.
			ch := make(chan Req)
			//also make a waitgroup
			wg := new(sync.WaitGroup)
			//and ..make a mutex/locker, so that we can lock before we update the parts list.
			mlock := new(sync.Mutex)
			//we need some more clients, specifically, one per '-multiuploader'
			for i := 0; i < int(params.multiUploaders); i++ {
				svc := makeS3session(*params, randEndpoints[i%len(randEndpoints)])

				//increment wg counter
				wg.Add(1)
				// now, pass along the params, svc, mpu id, and completedParts list, and the wg/channel/and sync so that the sub-client has it.
				go startMultipartUploader(*params, svc, mpu, ph, wg, ch, mlock)

			}

			//now, need to iterate through the partslist, and shove into the channel.
			var curr, partLength int64
			var remaining = params.objectSize
			partNumber := 1
			for curr = 0; remaining != 0; curr += partLength {
				if remaining < params.partSize { //this is for sending the 'last part' , which will be smaller.
					//this can probably be simplified, but for now leaving this way to ensure that we don't make extra copies of the slice.
					partLength = remaining
					//byteSlice := make([]byte, partLength) //new slice which is the size of the last part
					//copy(byteSlice, bufferBytes)          //copy bytes from our 'normal' slice into this one. it might be better to
					// use the betterSlicer for this now (faster?) but its only one part per object, and
					// its the smaller one..so whatever.

					//actually send the request to the channel.
					ch <- &s3.UploadPartInput{
						Body: bytes.NewReader(sliceBuilder(partLength, params.moreRando, params.chunkSize)),
						//Body:          bytes.NewReader(betterSlicer(partLength)),
						Bucket:        mpu.Bucket,
						Key:           mpu.Key,
						PartNumber:    aws.Int64(int64(partNumber)),
						UploadId:      mpu.UploadId,
						ContentLength: aws.Int64(partLength),
					}
				} else { //this is the 'normal' case, where we use the user defined partSize
					partLength = params.partSize

					//actually send the request to the channel.
					ch <- &s3.UploadPartInput{
						Body: bytes.NewReader(sliceBuilder(params.partSize, params.moreRando, params.chunkSize)),
						//Body:          bytes.NewReader(betterSlicer(params.partSize)),
						Bucket:        mpu.Bucket,
						Key:           mpu.Key,
						PartNumber:    aws.Int64(int64(partNumber)),
						UploadId:      mpu.UploadId,
						ContentLength: aws.Int64(partLength),
					}
				}

				remaining -= partLength
				partNumber++
			}
			close(ch) //close the channel

			wg.Wait() //now we wait..

			//sort the parts list once we have all of them. s3 cares that the list is ordered.
			sort.Sort(partSorter(ph.parts))

			//now, assuming all went well, complete the MPU
			cparams := &s3.CompleteMultipartUploadInput{
				Bucket:          mpu.Bucket,
				Key:             mpu.Key,
				UploadId:        mpu.UploadId,
				MultipartUpload: &s3.CompletedMultipartUpload{Parts: ph.parts},
			}

			_, err = svc.CompleteMultipartUpload(cparams)
			if err != nil {
				fmt.Printf("errror with mpu complete: %v", err)

			}

		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes}
	}
}

func startMultipartUploader(params Params, svc *s3.S3, mpu *s3.CreateMultipartUploadOutput, ph *partholder, wg *sync.WaitGroup, ch chan Req, mlock *sync.Mutex) {
	defer wg.Done()
	for request := range ch {
		switch r := request.(type) {
		case *s3.UploadPartInput: //in the future, maybe we can also have a case for ranged-read (eg; the GET equivalent)
			req, uoutput := svc.UploadPartRequest(r)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD") //this makes a HUGE difference in CPU.
			err := req.Send()

			if err != nil {
				fmt.Printf("errror with mpu: %v", err)
			}
			part := &s3.CompletedPart{} // setup a place this part info , then populate with needed data.
			part.SetPartNumber(*r.PartNumber)
			part.SetETag(*uoutput.ETag)

			mlock.Lock() // before we append this part info into the list, lock.

			ph.parts = append(ph.parts, part)

			mlock.Unlock()

		}

	}

}

// this is to make sure we have a way to properly sort the parts list.
type partSorter []*s3.CompletedPart

func (a partSorter) Len() int           { return len(a) }
func (a partSorter) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a partSorter) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

type partholder struct {
	parts []*s3.CompletedPart
}

// Specifies the parameters for a given test
type Params struct {
	operation  string
	operations string
	requests   chan Req

	responses        chan Resp
	numSamples       int
	batchSize        int
	numClients       uint
	objectSize       int64
	rangeSize        int64
	numRequests      int64
	partSize         int64
	multiUploaders   int
	multipart        bool
	objectNamePrefix string
	bucketName       string
	accessKey        string
	accessSecret     string
	region           string
	endpoints        []string
	verbose          bool
	disableMD5       bool
	moreRando        bool
	chunkSize        int64
}

func (params Params) String() string {

	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      %s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           %s\n", params.bucketName)
	output += fmt.Sprintf("objectNamePrefix: %s\n", params.objectNamePrefix)
	// there's probably a better way to scale this, but for now just do something easy
	if float64(params.objectSize)/(1024*1024) > 1024 {
		if float64(params.objectSize)/(1024*1024*1024) > 1024 {
			output += fmt.Sprintf("objectSize:       %0.4f TB\n", float64(params.objectSize)/(1024*1024*1024*1024))

		} else { //means its GB
			output += fmt.Sprintf("objectSize:       %0.4f GB\n", float64(params.objectSize)/(1024*1024*1024))

		}
	} else { //means its MB
		output += fmt.Sprintf("objectSize:       %0.4f MB\n", float64(params.objectSize)/(1024*1024))

	}

	output += fmt.Sprintf("numClients:       %d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       %d\n", params.numSamples)
	if strings.Contains(params.operations, "ranges") {
		output += fmt.Sprintf("totalRequests:       %d\n", params.numRequests)
	}

	output += fmt.Sprintf("batchSize:       %d\n", params.batchSize)
	if params.multipart {
		output += fmt.Sprintf("multipart enabled:       %v\n", params.multipart)
		output += fmt.Sprintf("partSize:       %v MB\n", float64(params.partSize)/(1024*1024))
	}
	//calculate the total size of the test
	totalSize := int64(params.numSamples) * params.objectSize
	output += fmt.Sprintf("Total size of data set : %0.4f GB\n", float64(totalSize)/(1024*1024*1024))

	output += fmt.Sprintf("verbose:       %v\n", params.verbose)
	return output
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	numOps           int64
	opDurations      []float64
	totalDuration    time.Duration
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
	report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	report += fmt.Sprintf("Ops/sec:  %0.2f ops/s\n", float64(r.numOps)/r.totalDuration.Seconds())
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)
	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Max:       %0.4f s\n", r.operation, r.percentile(100))
		report += fmt.Sprintf("%s times 99th %%ile: %0.4f s\n", r.operation, r.percentile(99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.4f s\n", r.operation, r.percentile(90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.4f s\n", r.operation, r.percentile(75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.4f s\n", r.operation, r.percentile(50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.4f s\n", r.operation, r.percentile(25))
		report += fmt.Sprintf("%s times Min:       %0.4f s\n", r.operation, r.percentile(0))
	}
	return report
}

func (r Result) percentile(i int) float64 {
	if i >= 100 {
		i = len(r.opDurations) - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(len(r.opDurations)))
	}
	return r.opDurations[i]
}

func outputToCSV(results []Result, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	err = writer.Write([]string{
		"Operation",
		"Bytes Transmitted (MB)",
		"Total Duration (s)",
		"Throughput (MB/s)",
		"Number of Errors",
		"Max Time (s)",
		"99th Percentile Time (s)",
		"90th Percentile Time (s)",
		"75th Percentile Time (s)",
		"Median Time (s)",
		"25th Percentile Time (s)",
		"Min Time (s)",
	})
	if err != nil {
		return err
	}

	// Write data
	for _, result := range results {
		record := []string{
			result.operation,
			fmt.Sprintf("%0.3f", float64(result.bytesTransmitted)/(1024*1024)),
			fmt.Sprintf("%0.3f", result.totalDuration.Seconds()),
			fmt.Sprintf("%0.2f", (float64(result.bytesTransmitted)/(1024*1024))/result.totalDuration.Seconds()),
			strconv.Itoa(result.numErrors),
			fmt.Sprintf("%0.3f", result.percentile(100)),
			fmt.Sprintf("%0.3f", result.percentile(99)),
			fmt.Sprintf("%0.3f", result.percentile(90)),
			fmt.Sprintf("%0.3f", result.percentile(75)),
			fmt.Sprintf("%0.3f", result.percentile(50)),
			fmt.Sprintf("%0.3f", result.percentile(25)),
			fmt.Sprintf("%0.3f", result.percentile(0)),
		}
		err = writer.Write(record)
		if err != nil {
			return err
		}
	}

	return nil
}

type Req interface{}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
}