package assets

import "embed"

const (
	MySQLMigrationDir    = "migrations/mysql"
	PostgresMigrationDir = "migrations/postgres"
	DynamoMigrationDir = "migrations/dynamo"
)

//go:embed migrations/*
var EmbedMigrations embed.FS

//go:embed playground/*
var EmbedPlayground embed.FS
