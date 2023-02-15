package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd/util"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	datastoreEngineFlag = "datastore-engine"
	datastoreURIFlag    = "datastore-uri"
	versionFlag         = "version"
	awsRegionFlag		= "aws-region"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate the database schema needed for OpenFGA.`,
		RunE:  runMigration,
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}

	viper.SetEnvPrefix("OPENFGA")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	bindMigrateFlags(cmd)

	return cmd
}

func runMigration(_ *cobra.Command, _ []string) error {
	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)
	version := viper.GetUint(versionFlag)
	awsRegion := viper.GetString(awsRegionFlag)

	goose.SetLogger(goose.NopLogger())

	switch engine {
	case "memory":
		return nil
	case "mysql":
		db, err := sql.Open("mysql", uri)
		if err != nil {
			log.Fatal("failed to parse the config from the connection uri", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				log.Fatal("failed to close the db", err)
			}
		}()

		if err := goose.SetDialect("mysql"); err != nil {
			log.Fatal("failed to initialize the migrate command", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if version > 0 {
			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				log.Fatal(err)
			}

			int64Version := int64(version)
			if int64Version < currentVersion {
				if err := goose.DownTo(db, assets.MySQLMigrationDir, int64Version); err != nil {
					log.Fatal(err)
				}
			}

			if err := goose.UpTo(db, assets.MySQLMigrationDir, int64Version); err != nil {
				log.Fatal(err)
			}
		}

		if err := goose.Up(db, assets.MySQLMigrationDir); err != nil {
			log.Fatal(err)
		}

		return nil
	case "postgres":
		db, err := sql.Open("pgx", uri)
		if err != nil {
			log.Fatal("failed to parse the config from the connection uri", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				log.Fatal("failed to close the db", err)
			}
		}()

		if err := goose.SetDialect("postgres"); err != nil {
			log.Fatal("failed to initialize the migrate command", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if version > 0 {
			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				log.Fatal(err)
			}

			int64Version := int64(version)
			if int64Version < currentVersion {
				if err := goose.DownTo(db, assets.PostgresMigrationDir, int64Version); err != nil {
					log.Fatal(err)
				}
			}

			if err := goose.UpTo(db, assets.PostgresMigrationDir, int64Version); err != nil {
				log.Fatal(err)
			}

			return nil
		}

		if err := goose.Up(db, assets.PostgresMigrationDir); err != nil {
			log.Fatal(err)
		}

		return nil
	case "dynamo":
		
    	// sess, err := session.NewSession(&aws.Config{
        // 	Region: aws.String("eu-west-2"),
		// 	Endpoint: aws.String(uri),
		// 	Credentials: credentials.NewStaticCredentials("foo", "bar", ""),
		// })

		fmt.Println("region %s", awsRegion)

		sess, err := session.NewSession(&aws.Config{     
    		Endpoint: aws.String("http://localhost:8000")}, 
		)

		if err != nil {
        	log.Fatalf("Got error creating session: %s", err)
    	}
		
		svc := dynamodb.New(sess)

    // 
		// db := dynamodb.New(sess)
		tableName := "tuple"

		input := &dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("primary_key"),
					AttributeType: aws.String("S"),
			}},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("primary_key"),
					KeyType:       aws.String("HASH"),
			}},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			TableName: aws.String(tableName),
		}

		result, err := svc.CreateTable(input)
		
		if err != nil {
			log.Fatalf("Got error calling CreateTable: %s", err)
		}

		fmt.Println("Created table: %s", tableName)
		fmt.Println("result: ", result)
		// tuple, authorization_model, store, assertion, changelog

		return nil


	default:
		return fmt.Errorf("unknown datastore engine type: %s", engine)
	}
}

func bindMigrateFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String(datastoreEngineFlag, "", "(required) the datastore engine that will be used for persistence")
	util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))

	flags.String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))

	flags.Uint(versionFlag, 0, "`the version to migrate to. If omitted, the latest version of the schema will be used`")
	util.MustBindPFlag("version", flags.Lookup(versionFlag))

	flags.String(awsRegionFlag, "", "the aws region to connect to for dynamo")
	util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
}
