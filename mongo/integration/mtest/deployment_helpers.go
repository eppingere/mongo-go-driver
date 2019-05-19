package mtest

import "go.mongodb.org/mongo-driver/bson"

// BatchIdentifier specifies the keyword to identify the batch in a cursor response.
type BatchIdentifier string

const (
	FirstBatch BatchIdentifier = "firstBatch"
	NextBatch BatchIdentifier = "nextBatch"
)

// CreateCursorResponse creates a response for a cursor command.
func CreateCursorResponse(cursorID int64, ns string, identifier BatchIdentifier, batch ...bson.D) bson.D {
	batchArr := bson.A{}
	for _, doc := range batch {
		batchArr = append(batchArr, doc)
	}

	return bson.D{
		{"ok", 1},
		{"cursor", bson.D{
			{"id", cursorID},
			{"ns", ns},
			{string(identifier), batchArr},
		}},
	}
}

// CreateCommandErrorResponse creates a response with a command error.
func CreateCommandErrorResponse(code int32, msg string, name string, labels ...string) bson.D {
	res := bson.D{
		{"ok", 0},
		{"code", code},
		{"errmsg", msg},
		{"codeName", name},
	}
	if len(labels) > 0 {
		var labelsArr bson.A
		for _, label := range labels {
			labelsArr = append(labelsArr, label)
		}
		res = append(res, bson.E{Key: "labels", Value: labelsArr})
	}
	return res
}

// CreateSuccessResponse creates a response for a successful operation with the given elements.
func CreateSuccessResponse(elems ...bson.E) bson.D {
	res := bson.D{
		{"ok", 1},
	}
	return append(res, elems...)
}
