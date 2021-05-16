package mongist

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var client *mongo.Client

func TestMain(m *testing.M) {
	uri := os.Getenv("MONGIST_URI")

	newClient, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = newClient.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer newClient.Disconnect(ctx)

	client = newClient
	m.Run()
}

func TestPing(t *testing.T) {
	err := client.Ping(aCtx(), readpref.Primary())
	if err != nil {
		t.Fatal(err)
	}
}

func TestDatabases(t *testing.T) {
	databases, err := client.ListDatabaseNames(aCtx(), bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(databases)
}

func TestQuery(t *testing.T) {
	db := client.Database(os.Getenv("MONGIST_DB"))
	coleection := db.Collection(os.Getenv("MONGIST_COLLECTION"))
	result := coleection.FindOne(aCtx(), bson.M{})

	var doc bson.M
	err := result.Decode(&doc)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(doc)
}

func aCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return ctx
}
