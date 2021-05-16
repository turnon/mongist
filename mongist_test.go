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
	result := collection().FindOne(aCtx(), bson.M{})

	var doc bson.M
	err := result.Decode(&doc)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(doc)
}

func TestAgg(t *testing.T) {
	matchStage := bson.D{{"$match", bson.D{}}}
	groupStage := bson.D{{"$group", bson.D{{"_id", "$director"}, {"total", bson.D{{"$sum", 1}}}}}}
	sortStage := bson.D{{"$sort", bson.M{"total": -1}}}

	agg, err := collection().Aggregate(aCtx(), mongo.Pipeline{matchStage, groupStage, sortStage})
	if err != nil {
		t.Fatal(err)
	}

	var result []bson.M
	if err = agg.All(aCtx(), &result); err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}

func TestAggUnwind(t *testing.T) {
	matchStage := bson.D{{"$match", bson.D{}}}
	unwindStage := bson.D{{"$unwind", "$stars"}}
	groupStage := bson.D{{"$group", bson.D{{"_id", "$stars"}, {"total", bson.D{{"$sum", 1}}}}}}
	sortStage := bson.D{{"$sort", bson.M{"total": -1}}}

	agg, err := collection().Aggregate(aCtx(), mongo.Pipeline{matchStage, unwindStage, groupStage, sortStage})
	if err != nil {
		t.Fatal(err)
	}

	var result []bson.M
	if err = agg.All(aCtx(), &result); err != nil {
		t.Fatal(err)
	}
	t.Log(result)
}

func aCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	return ctx
}

func collection() *mongo.Collection {
	db := client.Database(os.Getenv("MONGIST_DB"))
	return db.Collection(os.Getenv("MONGIST_COLLECTION"))
}
