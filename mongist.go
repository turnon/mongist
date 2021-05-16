package mongist

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const Count = "count"

type Stat struct {
	Collection *mongo.Collection
	Ctx        context.Context
	Match      bson.D
	Unwinds    []Unwind
	Group
	Sort
}

type Unwind struct {
	Path                       string
	PreserveNullAndEmptyArrays bool
}

type Group struct {
	Path  string
	Count bool
}

type Sort bson.D

func (g *Group) generate() bson.D {
	group := bson.M{"_id": g.Path}
	if g.Count {
		group[Count] = bson.D{{"$sum", 1}}
	}
	return bson.D{{"$group", group}}
}

func (m *Stat) Grouping() ([]bson.M, error) {
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

func (m *Stat) getPipeline() mongo.Pipeline {
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

	pipeline = append(pipeline, m.Group.generate())

	if m.Match != nil {
		matchStage := bson.D{{"$match", m.Match}}
		pipeline = append(pipeline, matchStage)
	}

	if m.Sort != nil {
		pipeline = append(pipeline, bson.D{{"$sort", m.Sort}})
	}

	return pipeline
}
