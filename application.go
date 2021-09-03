package application

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/minio/minio-go"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// AppContext describes the environment of the application including
// permanent connections and defaults
type AppContext struct {
	options        *optionFile
	S3Client       *minio.Client
	DBClient       *mongo.Client
	DBContext      context.Context
	DBCancel       context.CancelFunc
	logBuffer      *bytes.Buffer
	logTopic       string
	MaxResults     int64
	CountriesURL   string
	RegionsURL     string
	AirportsURL    string
	RunwaysURL     string
	FrequenciesURL string
}

// Optionfile descibes the content of the options file
type sourceOptions struct {
	CountriesURL   string `json:"countries-url"`
	RegionsURL     string `json:"regions-url"`
	AirportsURL    string `json:"airports-url"`
	RunwaysURL     string `json:"runways-url"`
	FrequenciesURL string `json:"frequencies-url"`
}

type storageOptions struct {
	Server string `json:"server"`
	Key    string `json:"key"`
	Secret string `json:"secret"`
}

type optionFile struct {
	Source     sourceOptions  `json:"source"`
	Storage    storageOptions `json:"storage"`
	Database   string         `json:"database"`
	MaxResults int64          `json:"max-results"`
}

func readOptions() (*optionFile, error) {
	var options optionFile

	optionFile, err := os.Open("options.json")
	if err != nil {
		return nil, err
	}

	defer optionFile.Close()
	decoder := json.NewDecoder(optionFile)
	err = decoder.Decode(&options)
	if err != nil {
		return nil, err
	}

	return &options, nil
}

func (appContext *AppContext) connectMinio(applicationOptions *optionFile) error {
	// Connect to S3
	minioClient, err := minio.New(
		applicationOptions.Storage.Server,
		applicationOptions.Storage.Key,
		applicationOptions.Storage.Secret,
		false)
	if err != nil {
		return err
	}

	// Check the csv bucket
	bucketFound, err := minioClient.BucketExists("csv")
	if err != nil {
		return err
	}
	if !bucketFound {
		err = minioClient.MakeBucket("csv", "us-east-1")
		if err != nil {
			return err
		}
	}

	// Check the log bucket
	bucketFound, err = minioClient.BucketExists("log")
	if err != nil {
		return err
	}
	if !bucketFound {
		err = minioClient.MakeBucket("log", "us-east-1")
		if err != nil {
			return err
		}
	}

	// Register result
	appContext.S3Client = minioClient

	return nil
}

// CreateAppContext reads the application options and initializes permanent connections and defaults
func CreateAppContext() (*AppContext, error) {

	applicationOptions, err := readOptions()
	if err != nil {
		return nil, err
	}

	// Set up appContext
	appContext := AppContext{
		options:        applicationOptions,
		MaxResults:     applicationOptions.MaxResults,
		CountriesURL:   applicationOptions.Source.CountriesURL,
		RegionsURL:     applicationOptions.Source.RegionsURL,
		AirportsURL:    applicationOptions.Source.AirportsURL,
		RunwaysURL:     applicationOptions.Source.RunwaysURL,
		FrequenciesURL: applicationOptions.Source.FrequenciesURL}

	// Connect to Minio
	err = appContext.connectMinio(applicationOptions)
	if err != nil {
		return nil, err
	}

	return &appContext, nil
}

// DBOpen connects to the MongoDB, which we cannot keep open for too long
func (appContext *AppContext) DBOpen() error {
	// Connect to MongoDB
	dbContext, dbCancel := context.WithTimeout(context.Background(), time.Second*10)
	dbOptions := options.Client().ApplyURI(appContext.options.Database).SetDirect(true)
	dbClient, err := mongo.Connect(dbContext, dbOptions)
	if err != nil {
		dbCancel()
		return err
	}

	// Check the connection
	err = dbClient.Ping(dbContext, nil)
	if err != nil {
		dbCancel()
		return err
	}

	// Register it
	appContext.DBClient = dbClient
	appContext.DBContext = dbContext
	appContext.DBCancel = dbCancel

	return nil
}

// DBClose disconnects from the MongoDB
func (appContext *AppContext) DBClose() error {
	// Already closed
	if appContext.DBClient == nil {
		return nil
	}

	// And not dropped
	if appContext.DBContext.Err() == nil {
		appContext.DBClient.Disconnect(appContext.DBContext)
	}

	// Register it
	appContext.DBClient = nil
	appContext.DBContext = nil
	appContext.DBCancel = nil

	return nil
}

// LogFile creates a new logfile for the given topic in the logfolder
func (appContext *AppContext) LogFile(topic string) (io.Writer, error) {

	appContext.logBuffer = new(bytes.Buffer)
	appContext.logTopic = topic
	log.SetOutput(appContext.logBuffer)

	return appContext.logBuffer, nil
}

// LogPrintln inserts a message in the logfile
func (appContext *AppContext) LogPrintln(s string) {
	if len(s) != 0 {
		log.Println(s)
	}
}

// LogError inserts an error in the logfile if there is one
func (appContext *AppContext) LogError(err error) {
	if err != nil {
		log.Println(err)
	}
}

// LogClose moves the buffer to S3 in one go
func (appContext *AppContext) LogClose() {

	log.SetOutput(os.Stderr)

	logDate := time.Now().Format("20060102-150405")
	logName := fmt.Sprintf("%s-%s.txt", appContext.logTopic, logDate)

	s3Client := appContext.S3Client
	_, err := s3Client.PutObject("log", logName, appContext.logBuffer, -1,
		minio.PutObjectOptions{ContentType: "text/plain"})
	appContext.logBuffer = nil

	if err != nil {
		log.Panicf("Could not write logfile\n")
	}

}

func (appContext *AppContext) Destroy() {
	if appContext.DBCancel != nil {
		appContext.DBCancel()
	}
}
