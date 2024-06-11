package main

import (
	"bytes"
	"crypto/tls"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	opRead               = "Read"
	opWrite              = "Write"
	opPutObjectRetention = "PutObjectRetention"
	opHead               = "Head"
	//max that can be deleted at a time via DeleteObjects()
	commitSize = 1000
)

var bufferBytes []byte

func main() {
	csvOutput := flag.Bool("csvOutput", false, "enable output to CSV file")
	csvOutputFile := flag.String("csvOutputFile", "output.csv", "csv output file name")
	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	region := flag.String("region", "igneous-test", "AWS region to use, eg: us-west-1|us-east-1, etc")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", "loadgen_test_", "prefix of the object name that will be used")
	objectSize := flag.Int64("objectSize", 80*1024*1024, "size of individual requests in bytes (must be smaller than main memory)")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	skipCleanup := flag.Bool("skipCleanup", false, "skip deleting objects created by this tool at the end of the run")
	skipWrite := flag.Bool("skipWrite", false, "skip write operation benchmarks")
	skipRead := flag.Bool("skipRead", false, "skip read operation benchmarks")
	skipPutObjectRetention := flag.Bool("skipPutObjectRetention", true, "skip PutObjectRetention operation benchmarks (default: true)")
	skipHead := flag.Bool("skipHead", true, "skip skipHead operation benchmarks (default: true)")
	//metaOpMultiplier := flag.Int("metaOpMultiplier", 1, "multiplier of metadata operations: e.g. if set to 10, this metadata operations will be exectued 10 times on the same object")
	verbose := flag.Bool("verbose", false, "print verbose per thread status")
	tlsVerifyDisable := flag.Bool("tlsVerifyDisable", false, "disable TLS verify")

	flag.Parse()

	if *numClients > *numSamples || *numSamples < 1 {
		fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", *numClients, *numSamples)
		os.Exit(1)
	}

	if *endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       *numSamples,
		numClients:       uint(*numClients),
		objectSize:       *objectSize,
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
		verbose:          *verbose,
		tlsVerifyDisable: *tlsVerifyDisable,
		skipWrite:        *skipWrite,
		//metaOpMultiplier: *metaOpMultiplier,
	}
	fmt.Println(params)
	fmt.Println()

	// Generate the data from which we will do the writting
	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()
	bufferBytes = make([]byte, *objectSize, *objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()

	if *tlsVerifyDisable {
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Start the load clients and run a write test followed by a read test
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String(*region),
		S3ForcePathStyle: aws.Bool(true),
	}
	params.StartClients(cfg)

	// write operation
	if *skipWrite {
		fmt.Printf("Running %s test skipped...\n", opWrite)
	} else {
		fmt.Printf("Running %s test...\n", opWrite)
	}
	numSamplesWrite := params.numSamples
	if params.skipWrite {
		numSamplesWrite -= params.numSamples
	}
	writeResult := params.Run(opWrite, numSamplesWrite)
	fmt.Println()

	// read operation
	var readResult Result
	if *skipRead {
		fmt.Printf("Running %s test skipped...\n", opRead)
	} else {
		fmt.Printf("Running %s test...\n", opRead)
		readResult = params.Run(opRead, params.numSamples)
	}
	fmt.Println()

	// put-object-retention operation
	var putObjectRetentionResult Result
	if *skipPutObjectRetention {
		fmt.Printf("Running %s test skipped...\n", opPutObjectRetention)
	} else {
		fmt.Printf("Running %s test...\n", opPutObjectRetention)
		putObjectRetentionResult = params.Run(opPutObjectRetention, params.numSamples)
	}
	fmt.Println()

	// head operation
	var headResult Result
	if *skipHead {
		fmt.Printf("Running %s test skipped...\n", opHead)
	} else {
		fmt.Printf("Running %s test...\n", opHead)
		headResult = params.Run(opHead, params.numSamples)
	}
	fmt.Println()

	// Repeating the parameters of the test followed by the results
	fmt.Println(params)
	fmt.Println()

	var results []Result
	if *skipWrite {
		fmt.Println("Write test skipped...")
	} else {
		fmt.Println(writeResult)
		results = append(results, writeResult)
	}
	fmt.Println()
	if *skipRead {
		fmt.Println("Read test skipped...")
	} else {
		fmt.Println(readResult)
		results = append(results, readResult)
	}
	fmt.Println()
	if *skipPutObjectRetention {
		fmt.Println("Put-object-retention test skipped...")
	} else {
		fmt.Println(putObjectRetentionResult)
		results = append(results, putObjectRetentionResult)
	}
	fmt.Println()
	if *skipHead {
		fmt.Println("Head test skipped...")
	} else {
		fmt.Println(headResult)
		results = append(results, headResult)
	}

	if *csvOutput && len(results) > 0 {
		err := outputToCSV(results, *csvOutputFile)
		if err != nil {
			fmt.Printf("Error writing to CSV: %s\n", err)
			os.Exit(1)
		}
		fmt.Println("Results written to file")
	}

	// Do cleanup if required
	if !*skipCleanup {
		fmt.Println()
		fmt.Printf("Cleaning up %d objects...\n", *numSamples)
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, commitSize)
		for i := 0; i < *numSamples; i++ {
			bar := s3.ObjectIdentifier{
				Key: aws.String(fmt.Sprintf("%s%d", *objectNamePrefix, i)),
			}
			keyList = append(keyList, &bar)
			if len(keyList) == commitSize || i == *numSamples-1 {
				fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				params := &s3.DeleteObjectsInput{
					Bucket: aws.String(*bucketName),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(params)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					fmt.Printf("Succeeded\n")
				} else {
					fmt.Printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, *numSamples, time.Since(delStartTime))
	}
}

func (params *Params) Run(op string, numSamples int) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	excludeTime := time.Duration(0)

	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, numSamples), operation: op}
	for i := 0; i < numSamples; i++ {
		resp := <-params.responses

		if resp.excludeStatistic {
			excludeTime += resp.duration
			continue
		}

		errorString := ""
		if resp.err != nil {
			result.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
		}
		if params.verbose {
			if result.operation != opPutObjectRetention && result.operation != opHead {
				fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
					op, resp.duration.Seconds(), i+1, params.numSamples,
					(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
					errorString)
			} else {
				fmt.Printf("%v operation completed in %0.2fs (%d/%d)%s\n",
					op, resp.duration.Seconds(), i+1, params.numSamples,
					errorString)
			}

		}
	}

	result.totalDuration = time.Since(startTime)
	// exclude retention time, it affects on throughput
	result.totalDuration = result.totalDuration - excludeTime
	sort.Float64s(result.opDurations)
	return result
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)
	for i := 0; i < params.numSamples; i++ {
		key := aws.String(fmt.Sprintf("%s%d", params.objectNamePrefix, i))
		if op == opWrite {
			if !params.skipWrite {
				params.requests <- &s3.PutObjectInput{
					Bucket: bucket,
					Key:    key,
					Body:   bytes.NewReader(bufferBytes),
				}
			}

			// put object retention
			// -> Iteratre over all existing samples and put a 1sec retention on it
		} else if op == opPutObjectRetention {
			// create retention object with 1 sec retention
			retention := &s3.ObjectLockRetention{
				Mode: aws.String("GOVERNANCE"),
				RetainUntilDate: aws.Time(
					time.Now().Add(time.Second * 2),
				),
			}
			// create putobjectretention request with above config and send to client queue
			params.requests <- &s3.PutObjectRetentionInput{
				Bucket:                    bucket,
				Key:                       key,
				Retention:                 retention,
				BypassGovernanceRetention: aws.Bool(true),
			}

		} else if op == opRead {
			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
			}
		} else if op == opHead {
			params.requests <- &s3.HeadObjectInput{
				Bucket: bucket,
				Key:    key,
			}
		} else {
			panic("Developer error")
		}
	}
}

func (params *Params) StartClients(cfg *aws.Config) {
	for i := 0; i < int(params.numClients); i++ {
		cfg.Endpoint = aws.String(params.endpoints[i%len(params.endpoints)])
		go params.startClient(cfg)
		time.Sleep(1 * time.Millisecond)
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {
	svc := s3.New(session.New(), cfg)
	for request := range params.requests {
		putStartTime := time.Now()
		var err error
		numBytes := params.objectSize

		var excludeStatistic bool
		switch r := request.(type) {
		case *s3.PutObjectInput:
			req, _ := svc.PutObjectRequest(r)
			// Disable payload checksum calculation (very expensive)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
		case *s3.PutObjectRetentionInput:
			req, _ := svc.PutObjectRetentionRequest(r)
			err = req.Send()
			excludeStatistic = false
		case *s3.HeadObjectInput:
			req, _ := svc.HeadObjectRequest(r)
			err = req.Send()
			excludeStatistic = false
		case *s3.GetObjectInput:
			req, resp := svc.GetObjectRequest(r)
			err = req.Send()
			numBytes = 0
			if err == nil {
				numBytes, err = io.Copy(ioutil.Discard, resp.Body)
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes, excludeStatistic}
	}
}

// Specifies the parameters for a given test
type Params struct {
	requests               chan Req
	responses              chan Resp
	numSamples             int
	numClients             uint
	objectSize             int64
	objectNamePrefix       string
	bucketName             string
	endpoints              []string
	verbose                bool
	tlsVerifyDisable       bool
	skipWrite              bool
	skipRead               bool
	skipPutObjectRetention bool
	skipHead               bool
	//metaOpMultiplier       int
}

func (params Params) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      		%s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           		%s\n", params.bucketName)
	output += fmt.Sprintf("objectNamePrefix: 		%s\n", params.objectNamePrefix)
	output += fmt.Sprintf("objectSize:       		%0.4f MB\n", float64(params.objectSize)/(1024*1024))
	output += fmt.Sprintf("numClients:       		%d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       		%d\n", params.numSamples)
	output += fmt.Sprintf("skipWrite:       	 	%t\n", params.skipWrite)
	output += fmt.Sprintf("skipRead:       	 	%t\n", params.skipRead)
	output += fmt.Sprintf("skipPutObjectRetention:       	 %t\n", params.skipPutObjectRetention)
	output += fmt.Sprintf("skipHead:       	 	%t\n", params.skipHead)
	//output += fmt.Sprintf("metaOpMultiplier:       	%t\n", params.metaOpMultiplier)
	return output
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	// don't print throughput metrics for metadata operations
	if r.operation != opPutObjectRetention && r.operation != opHead {
		report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
		report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	}
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)

	// slog.Info("Operation durations", "opDurations", r.opDurations)
	// slog.Info("Length of operation durations", "len(opDurations)", len(r.opDurations))

	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Max:       %0.3f s\n", r.operation, r.percentile(100))
		report += fmt.Sprintf("%s times 99th %%ile: %0.3f s\n", r.operation, r.percentile(99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.3f s\n", r.operation, r.percentile(90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.3f s\n", r.operation, r.percentile(75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.3f s\n", r.operation, r.percentile(50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.3f s\n", r.operation, r.percentile(25))
		report += fmt.Sprintf("%s times Min:       %0.3f s\n", r.operation, r.percentile(0))
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
		// slog.Info("Result", "result.operation", result.operation)
		// slog.Info("Result", "result.opDurations", result.opDurations)
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
	err              error
	duration         time.Duration
	numBytes         int64
	excludeStatistic bool
}
