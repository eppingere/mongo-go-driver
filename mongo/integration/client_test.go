package integration

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/internal/testutil/assert"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const certificatesDir = "../../../data/certificates"

type negateCodec struct {
	ID int64 `bson:"_id"`
}

func (e *negateCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	return vw.WriteInt64(val.Int())
}

// DecodeValue negates the value of ID when reading
func (e *negateCodec) DecodeValue(ectx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	i, err := vr.ReadInt64()
	if err != nil {
		return err
	}

	val.SetInt(i * -1)
	return nil
}

func TestClient(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	mt.Run("database", func(mt *mtest.T) {
		db := mt.Client.Database("foo")
		assert.Equal(mt, "foo", db.Name(), "db name mismatch; expected 'foo', got '%s'", db.Name())
	})

	registryOpts := options.Client().
		SetRegistry(bson.NewRegistryBuilder().RegisterCodec(reflect.TypeOf(int64(0)), &negateCodec{}).Build())
	mt.RunOpts("registry passed to cursors", mtest.NewOptions().ClientOptions(registryOpts), func(mt *mtest.T) {

		_, err := mt.Coll.InsertOne(mtest.Background, negateCodec{ID: 10})
		assert.Nil(mt, err, "error inserting document: %v", err)
		var got negateCodec
		err = mt.Coll.FindOne(mtest.Background, bson.D{}).Decode(&got)
		assert.Nil(mt, err, "Find error: %v", err)

		assert.Equal(mt, int64(-10), got.ID, "ID mismatch; expected -10, got %d", got.ID)
	})

	securityOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
		MinServerVersion: "3.0",
	})
	mt.RunOpts("tls connection", securityOpts, func(mt *mtest.T) {
		if !mt.AuthEnabled() {
			mt.Skip("skipping because auth is not enabled")
		}

		var result bson.Raw
		err := mt.Coll.Database().RunCommand(mtest.Background, bson.D{
			{"serverStatus", 1},
		}).Decode(&result)
		assert.Nil(mt, err, "serverStatus error: %v", err)

		security := result.Lookup("security")
		assert.Equal(mt, bson.TypeEmbeddedDocument, security.Type,
			"security field type mismatch; expected %v, got %v", bson.TypeEmbeddedDocument, security.Type)
		_, found := security.Document().LookupErr("SSLServerSubjectName")
		assert.Nil(mt, found, "no SSLServerSubjectName found in document")
		_, found = security.Document().LookupErr("SSLServerHasCertificateAuthority")
		assert.Nil(mt, found, "no SSLServerHasCertificateAuthority found in document")
	})

	mt.Run("x509", func(mt *mtest.T) {
		if !mt.AuthEnabled() {
			mt.Skip("skipping because auth is not enabled")
		}

		const user = "C=US,ST=New York,L=New York City,O=MongoDB,OU=other,CN=external"
		db := mt.Client.Database("$external")

		// We don't care if the user doesn't already exist.
		_ = db.RunCommand(
			mtest.Background,
			bson.D{{"dropUser", user}},
		)
		err := db.RunCommand(
			mtest.Background,
			bson.D{
				{"createUser", user},
				{"roles", bson.A{
					bson.D{{"role", "readWrite"}, {"db", "test"}},
				}},
			},
		).Err()
		assert.Nil(mt, err, "error creating user: %v", err)

		baseConnString := mt.ConnString()
		cs := fmt.Sprintf(
			"%s&sslClientCertificateKeyFile=%s&authMechanism=MONGODB-X509",
			baseConnString,
			path.Join(certificatesDir, "client.pem"),
		)
		authClient, err := mongo.Connect(mtest.Background, options.Client().ApplyURI(cs))
		assert.Nil(mt, err, "error creating auth client: %v", err)
		defer func() { _ = authClient.Disconnect(mtest.Background) }()

		rdr, err := authClient.Database("test").RunCommand(mtest.Background, bson.D{
			{"connectionStatus", 1},
		}).DecodeBytes()
		assert.Nil(mt, err, "connectionStatus error: %v", err)
		users, err := rdr.LookupErr("authInfo", "authenticatedUsers")
		assert.Nil(mt, err, "authenticatedUsers not found in response")
		elems, err := users.Array().Elements()
		assert.Nil(mt, err, "error getting users elements: %v", err)

		for _, userElem := range elems {
			rdr := userElem.Value().Document()
			var u struct {
				User string
				DB   string
			}

			if err := bson.Unmarshal(rdr, &u); err != nil {
				continue
			}
			if u.User == user && u.DB == "$external" {
				return
			}
		}
		mt.Fatal("unable to find authenticated user")
	})

	mt.Run("replace topology error", func(mt *mtest.T) {
		c, err := mongo.NewClient(options.Client().ApplyURI(mt.ConnString()))
		assert.Nil(mt, err, "error creating client: %v", err)

		_, err = c.StartSession()
		assert.Equal(mt, err, mongo.ErrClientDisconnected,
			"error mismatch; expected %v, got %v", mongo.ErrClientDisconnected, err)
		_, err = c.ListDatabases(mtest.Background, bson.D{})
		assert.Equal(mt, err, mongo.ErrClientDisconnected,
			"error mismatch; expected %v, got %v", mongo.ErrClientDisconnected, err)
		err = c.Ping(mtest.Background, nil)
		assert.Equal(mt, err, mongo.ErrClientDisconnected,
			"error mismatch; expected %v, got %v", mongo.ErrClientDisconnected, err)
		err = c.Disconnect(mtest.Background)
		assert.Equal(mt, err, mongo.ErrClientDisconnected,
			"error mismatch; expected %v, got %v", mongo.ErrClientDisconnected, err)
	})

	mt.RunOpts("list databases", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		testCases := []struct{
			name string
			filter bson.D
			hasTestDb bool
		}{
			{"no filter", bson.D{}, true},
			{"filter", bson.D{{"name", "foobar"}}, false},
		}

		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				res, err := mt.Client.ListDatabases(mtest.Background, tc.filter)
				assert.Nil(mt, err, "error running ListDatabases: %v", err)

				var found bool
				for _, db := range res.Databases {
					if db.Name == mtest.TestDb {
						found = true
						break
					}
				}
				assert.Equal(mt, tc.hasTestDb, found,
					"found mismatch; expected to find test db: %v, found: %v", tc.hasTestDb, found)
			})
		}
	})

	mt.RunOpts("list database names", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		testCases := []struct{
			name string
			filter bson.D
			hasTestDb bool
		}{
			{"no filter", bson.D{}, true},
			{"filter", bson.D{{"name", "foobar"}}, false},
		}

		for _, tc := range testCases {
			mt.Run(tc.name, func(mt *mtest.T) {
				dbs, err := mt.Client.ListDatabaseNames(mtest.Background, tc.filter)
				assert.Nil(mt, err, "error running ListDatabaseNames: %v", err)

				var found bool
				for _, db := range dbs {
					if db == mtest.TestDb {
						found = true
						break
					}
				}
				assert.Equal(mt, tc.hasTestDb, found,
					"found mismatch; expected to find test db: %v, found: %v", tc.hasTestDb, found)
			})
		}
	})

	mt.Run("nil document error", func(mt *mtest.T) {
		_, err := mt.Client.Watch(mtest.Background, nil)
		watchErr := errors.New("can only transform slices and arrays into aggregation pipelines, but got invalid")
		assert.Equal(mt, watchErr, err, "error mismatch; expected %v, got %v", watchErr, err)

		_, err = mt.Client.ListDatabases(mtest.Background, nil)
		assert.Equal(mt, mongo.ErrNilDocument, err, "error mismatch; expected %v, got %v", mongo.ErrNilDocument, err)

		_, err = mt.Client.ListDatabaseNames(mtest.Background, nil)
		assert.Equal(mt, mongo.ErrNilDocument, err, "error mismatch; expected %v, got %v", mongo.ErrNilDocument, err)
	})

	mt.RunOpts("ping", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		mt.Run("default read preference", func(mt *mtest.T) {
			err := mt.Client.Ping(mtest.Background, nil)
			assert.Nil(mt, err, "Ping error: %v", err)
		})
	})

	mt.RunOpts("disconnect", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		mt.Run("nil context", func(mt *mtest.T) {
			err := mt.Client.Disconnect(nil)
			assert.Nil(mt, err, "Disconnect error: %v", err)
		})
	})

	mt.RunOpts("watch", mtest.NewOptions().CreateClient(false), func(mt *mtest.T) {
		mt.Run("disconnected", func(mt *mtest.T) {
			c, err := mongo.NewClient(options.Client().ApplyURI(mt.ConnString()))
			assert.Nil(mt, err, "NewClient error: %v", err)
			_, err = c.Watch(mtest.Background, mongo.Pipeline{})
			assert.Equal(mt, mongo.ErrClientDisconnected, err,
				"error mismatch; expected %v, got %v", mongo.ErrClientDisconnected, err)
		})
	})

	endSessionsOpts := mtest.NewOptions().Constraints(mtest.RunOnBlock{
		MinServerVersion: "3.6",
	})
	mt.RunOpts("end sessions", endSessionsOpts, func(mt *mtest.T) {
		_, err := mt.Client.ListDatabases(mtest.Background, bson.D{})
		assert.Nil(mt, err, "ListDatabases error: %v", err)

		mt.ClearEvents()
		err = mt.Client.Disconnect(mtest.Background)
		assert.Nil(mt, err, "Disconnect error: %v", err)

		started := mt.GetStartedEvent()
		assert.Equal(mt, "endSessions", started.CommandName,
			"cmd name mismatch; expected 'endSessions', got '%s'", started.CommandName)
	})
}
