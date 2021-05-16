package mongist

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Mongist struct {
	Collection *mongo.Collection
	Ctx        context.Context
	Match      bson.D
	Unwinds    []Unwind
	Group      bson.D
	Sort       bson.D
}

type Unwind struct {
	Path                       string
	PreserveNullAndEmptyArrays bool
}

func (m *Mongist) Grouping() ([]bson.M, error) {
	if m.Collection == nil {
		return nil, errors.New("No collection given !")
	}

	ctx := m.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	pipeline := m.getPipeline()

	agg, err := m.Collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}

	var result []bson.M
	if err = agg.All(ctx, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (m *Mongist) getPipeline() mongo.Pipeline {
	pipeline := make(mongo.Pipeline, 0, 0)

	if m.Match != nil {
		matchStage := bson.D{{"$match", m.Match}}
		pipeline = append(pipeline, matchStage)
	}

	if m.Unwinds != nil {
		for _, unwind := range m.Unwinds {
			pipeline = append(pipeline, bson.D{{"$unwind", bson.M{"path": unwind.Path, "preserveNullAndEmptyArrays": unwind.PreserveNullAndEmptyArrays}}})
		}
	}

	pipeline = append(pipeline, bson.D{{"$group", m.Group}})

	if m.Match != nil {
		matchStage := bson.D{{"$match", m.Match}}
		pipeline = append(pipeline, matchStage)
	}

	if m.Sort != nil {
		pipeline = append(pipeline, bson.D{{"$sort", m.Sort}})
	}

	return pipeline
}
