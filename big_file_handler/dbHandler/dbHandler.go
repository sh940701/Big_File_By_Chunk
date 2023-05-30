package dbHandler

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
)

// ConnectMongoDB mongoDB 연결작업
func ConnectMongoDB(host, db, model string) error {
	var err error
	client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(host))
	if err != nil {
		return err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	fmt.Println("Connect")

	database = client.Database(db)
	collection = database.Collection(model)

	return nil
}

func InsertDocuments(documents []interface{}) error {
	_, err := collection.InsertMany(context.TODO(), documents)
	return err
}

func DisConnectMongoDB() error {
	err := client.Disconnect(context.Background())
	if err != nil {
		return err
	}

	fmt.Println("MongoDB 연결 종료")

	return nil
}
