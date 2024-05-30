package firestarter

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientDocGet(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	docSnap, err := client.Doc("collection-1/document-1-1").Get(ctx)
	assert.Nil(t, err)

	docData := docSnap.Data()
	assert.Equal(t, "value-1-1-1", docData["field1"])
	assert.Equal(t, "value-1-1-2", docData["field2"])
	assert.Equal(t, []interface{}{1.0, 2.0, 3.0}, docData["field6"]) // test pb.ArrayValue
	assert.Equal(t, map[string]interface{}{
		"subfield1": "subvalue-1-1-1-1",
		"subfield2": "subvalue-1-1-1-2",
	}, docData["field7"]) // test pb.MapValue
	assert.Equal(t, []byte("1234567890"), docData["field9"]) // test pb.BytesValue

	// test subcollection/subdocument
	docSnap, err = client.Doc("collection-2/document-2-4/subcollection-2-4/subdocument-2-4-2").Get(ctx)
	assert.Nil(t, err)

	docData = docSnap.Data()
	assert.Equal(t, "value-2-4-2-1", docData["field1"])
	assert.Equal(t, "value-2-4-2-2", docData["field2"])
}

func TestClientDocGet_InvalidPath(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	docSnap, err := client.Doc("invalid-path").Get(ctx)
	assert.NotNil(t, err)
	assert.Nil(t, docSnap)
}

func TestClientDocGetAll(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	docSnaps, err := client.Collection("collection-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)
}

func TestClientOrderBy(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test descending order
	docSnaps, err := client.Collection("collection-1").OrderBy("field1", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test ascending order
	docSnaps, err = client.Collection("collection-1").OrderBy("field2", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// test number field desc
	docSnaps, err = client.Collection("collection-1").OrderBy("field3", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test number field asc
	docSnaps, err = client.Collection("collection-1").OrderBy("field3", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// test timestamp field desc
	docSnaps, err = client.Collection("collection-1").OrderBy("field8", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test timestamp field asc
	docSnaps, err = client.Collection("collection-1").OrderBy("field8", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// test bytes field desc
	docSnaps, err = client.Collection("collection-1").OrderBy("field9", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test bytes field asc
	docSnaps, err = client.Collection("collection-1").OrderBy("field9", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// test map field desc
	docSnaps, err = client.Collection("collection-1").OrderBy("field7", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test map field asc
	docSnaps, err = client.Collection("collection-1").OrderBy("field7", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// test array field desc
	docSnaps, err = client.Collection("collection-1").OrderBy("field6", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// test array field asc
	docSnaps, err = client.Collection("collection-1").OrderBy("field6", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// multiple order by, desc
	docSnaps, err = client.Collection("collection-1").
		OrderBy("field4", firestore.Desc).
		OrderBy("field1", firestore.Desc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-1", docSnaps[1].Ref.ID)

	// multiple order by, desc
	docSnaps, err = client.Collection("collection-1").
		OrderBy("field4", firestore.Desc).
		OrderBy("field1", firestore.Asc).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)
}

func TestClientLimit(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	docSnaps, err := client.Collection("collection-1").Limit(1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
}

func TestClientWhere_string(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field1", "==", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field4", "==", "equal").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field4", "==", "non-existent").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field1", "<", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field1", "<=", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field1", ">", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field1", ">=", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field1", "!=", "value-1-1-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field1", "in", []string{"value-1-1-1", "value-1-2-1"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field1", "in", []string{"value-1-1-1", "xxxx"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field1", "in", []string{"xxxx", "yyyy"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field1", "not-in", []string{"value-1-1-1", "value-1-2-1"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field1", "not-in", []string{"value-1-1-1", "xxxx"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field1", "not-in", []string{"xxxx", "yyyy"}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_int64(t *testing.T) {
	t.Skip("JSON can't store int64, so this test is not possible without a different loader")

	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field3", "==", 123).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field3", "<", 123).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field3", "<=", 123).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field3", ">", 123).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field3", ">=", 123).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field3", "!=", 113).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []int64{113, 123}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []int64{113, 0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []int64{0, 1}).Documents(ctx).GetAll()
	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []int64{113, 123}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []int64{113, 0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []int64{0, 1}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_float64(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field3", "==", 123.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field3", "<", 123.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field3", "<=", 123.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field3", ">", 123.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field3", ">=", 123.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field3", "!=", 113.0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []float64{113.0, 123.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []float64{113.0, 0.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field3", "in", []float64{0.0, 1.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []float64{113.0, 123.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []float64{113.0, 0.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field3", "not-in", []float64{0.0, 1.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_bool(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field5", "==", true).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field5", "!=", true).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field5", "in", []bool{true, false}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field5", "in", []bool{}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field5", "not-in", []bool{true, false}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field5", "not-in", []bool{}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_timestamp(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	jan1, _ := time.Parse(time.RFC3339, "2001-01-01T00:00:00Z")
	feb1, _ := time.Parse(time.RFC3339, "2001-02-01T00:00:00Z")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field8", "==", jan1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field8", "<", feb1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field8", "<=", feb1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field8", ">", jan1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field8", ">=", jan1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field8", "!=", jan1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field8", "in", []time.Time{jan1, feb1}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field8", "in", []time.Time{jan1, time.Now()}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field8", "in", []time.Time{}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field8", "not-in", []time.Time{jan1, feb1}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field8", "not-in", []time.Time{jan1, time.Now()}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field8", "not-in", []time.Time{}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_bytes(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field9", "==", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field9", "<", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field9", "<=", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field9", ">", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field9", ">=", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field9", "!=", []byte("1234567890")).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field9", "in", [][]byte{[]byte("1234567890"), []byte("ABCDEFG")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field9", "in", [][]byte{[]byte("1234567890"), []byte("xxxx")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field9", "in", [][]byte{[]byte("xxxx"), []byte("yyyy")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field9", "not-in", [][]byte{[]byte("1234567890"), []byte("ABCDEFG")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field9", "not-in", [][]byte{[]byte("1234567890"), []byte("xxxx")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field9", "not-in", [][]byte{[]byte("xxxx"), []byte("yyyy")}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestMainWhere_map(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test ==
	docSnaps, err := client.Collection("collection-1").Where("field7", "==", map[string]interface{}{
		"subfield1": "subvalue-1-1-1-1",
		"subfield2": "subvalue-1-1-1-2",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <
	docSnaps, err = client.Collection("collection-1").Where("field7", "<", map[string]interface{}{
		"subfield1": "subvalue-1-2-1-1",
		"subfield2": "subvalue-1-2-1-2",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// test <=
	docSnaps, err = client.Collection("collection-1").Where("field7", "<=", map[string]interface{}{
		"subfield1": "subvalue-1-2-1-1",
		"subfield2": "subvalue-1-2-2-2",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test >
	docSnaps, err = client.Collection("collection-1").Where("field7", ">", map[string]interface{}{
		"subfield1": "subvalue-1-1-1-1",
		"subfield2": "subvalue-1-1-2-1",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test >=
	docSnaps, err = client.Collection("collection-1").Where("field7", ">=", map[string]interface{}{
		"subfield1": "subvalue-1-1-1-1",
		"subfield2": "subvalue-1-1-1-2",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	// test !=
	docSnaps, err = client.Collection("collection-1").Where("field7", "!=", map[string]interface{}{
		"subfield1": "subvalue-1-1-1-1",
		"subfield2": "subvalue-1-1-1-2",
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	// test in
	docSnaps, err = client.Collection("collection-1").Where("field7", "in", []map[string]interface{}{
		{
			"subfield1": "subvalue-1-1-1-1",
			"subfield2": "subvalue-1-1-1-2",
		},
		{
			"subfield1": "subvalue-1-2-1-1",
			"subfield2": "subvalue-1-2-1-2",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field7", "in", []map[string]interface{}{
		{
			"subfield1": "subvalue-1-1-1-1",
			"subfield2": "subvalue-1-1-1-2",
		},
		{
			"subfield1": "xxxx",
			"subfield2": "yyyy",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field7", "in", []map[string]interface{}{
		{
			"subfield1": "xxxx",
			"subfield2": "yyyy",
		},
		{
			"subfield1": "zzzz",
			"subfield2": "aaaa",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test not-in
	docSnaps, err = client.Collection("collection-1").Where("field7", "not-in", []map[string]interface{}{
		{
			"subfield1": "subvalue-1-1-1-1",
			"subfield2": "subvalue-1-1-1-2",
		},
		{
			"subfield1": "subvalue-1-2-1-1",
			"subfield2": "subvalue-1-2-1-2",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-1").Where("field7", "not-in", []map[string]interface{}{
		{
			"subfield1": "subvalue-1-1-1-1",
			"subfield2": "subvalue-1-1-1-2",
		},
		{
			"subfield1": "xxxx",
			"subfield2": "yyyy",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field7", "not-in", []map[string]interface{}{
		{
			"subfield1": "xxxx",
			"subfield2": "yyyy",
		},
		{
			"subfield1": "zzzz",
			"subfield2": "aaaa",
		},
	}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
}

func TestClientWhere_array(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// test array-contains
	docSnaps, err := client.Collection("collection-1").Where("field6", "array-contains", 1).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field6", "array-contains", 3).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field6", "array-contains", 0).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

	// test array-contains-any
	docSnaps, err = client.Collection("collection-1").Where("field6", "array-contains-any", []float64{0.0, 5.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field6", "array-contains-any", []float64{1.0, 5.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)

	docSnaps, err = client.Collection("collection-1").Where("field6", "array-contains-any", []float64{0.0, 10.0}).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)

}

func TestClientWhereOr(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// both branches should match
	q1 := firestore.PropertyFilter{
		Path:     "field1",
		Operator: "==",
		Value:    "value-1-1-1",
	}

	q2 := firestore.PropertyFilter{
		Path:     "field2",
		Operator: "==",
		Value:    "value-1-2-2",
	}

	orFilter := firestore.OrFilter{
		Filters: []firestore.EntityFilter{q1, q2},
	}

	docSnaps, err := client.Collection("collection-1").WhereEntity(orFilter).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 2)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)
	assert.Equal(t, "document-1-2", docSnaps[1].Ref.ID)

	// only one branch should match
	q1 = firestore.PropertyFilter{
		Path:     "field1",
		Operator: "==",
		Value:    "value-1-1-1",
	}

	q2 = firestore.PropertyFilter{
		Path:     "field2",
		Operator: "==",
		Value:    "xxxxx",
	}

	orFilter = firestore.OrFilter{
		Filters: []firestore.EntityFilter{q1, q2},
	}

	docSnaps, err = client.Collection("collection-1").WhereEntity(orFilter).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-1", docSnaps[0].Ref.ID)

	// no branches should match
	q1 = firestore.PropertyFilter{
		Path:     "field1",
		Operator: "==",
		Value:    "xxxxx",
	}

	q2 = firestore.PropertyFilter{
		Path:     "field2",
		Operator: "==",
		Value:    "xxxxx",
	}

	orFilter = firestore.OrFilter{
		Filters: []firestore.EntityFilter{q1, q2},
	}

	docSnaps, err = client.Collection("collection-1").WhereEntity(orFilter).Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)
}

func TestClientWhere_dottedfield(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	docSnaps, err := client.Collection("collection-1").Where("field7.subfield2", "==", "subvalue-1-2-1-2").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)

	docSnaps, err = client.Collection("collection-1").Where("field7.subfield2", "==", "xxxx").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 0)
}

func TestClientWhere_nocollection(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	docSnaps, err := client.Collection("collection-nonexistent").Documents(ctx).GetAll()
	assert.Nil(t, err)
	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-nonexistent").Where("field1", "==", "value-1-1-1").Documents(ctx).GetAll()
	assert.Nil(t, err)
	assert.Len(t, docSnaps, 0)

	docSnaps, err = client.Collection("collection-nonexistent/non-existent-doc/sub-collection").Documents(ctx).GetAll()
	assert.Nil(t, err)
	assert.Len(t, docSnaps, 0)
}

func TestClientSet(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	jan1, _ := time.Parse(time.RFC3339, "2001-01-01T00:00:00Z")

	// create a new doc
	docRef := client.Doc("collection-1/document-1-1")
	_, err = docRef.Set(ctx, map[string]interface{}{
		"field1": "new-value-1-1-1",
		"field2": "new-value-1-1-2",
		"field6": []float64{10.0, 11.0, 12.0},
		"field7": map[string]interface{}{
			"subfield1": "new-subvalue-1-1-1-1",
			"subfield2": "new-subvalue-1-1-1-2",
			"subfield3": []float64{1.0, 2.0, 3.0},
		},
		"field8": jan1,
		"field9": []byte("1234567890abc"),
	})
	assert.Nil(t, err)

	docSnap, err := docRef.Get(ctx)
	assert.Nil(t, err)

	docData := docSnap.Data()
	assert.Equal(t, "new-value-1-1-1", docData["field1"])
	assert.Equal(t, "new-value-1-1-2", docData["field2"])
	assert.Equal(t, []interface{}{10.0, 11.0, 12.0}, docData["field6"])
	assert.Equal(t, map[string]interface{}{
		"subfield1": "new-subvalue-1-1-1-1",
		"subfield2": "new-subvalue-1-1-1-2",
		"subfield3": []interface{}{1.0, 2.0, 3.0},
	}, docData["field7"])
	assert.Equal(t, jan1, docData["field8"])
	assert.Equal(t, []byte("1234567890abc"), docData["field9"])

	// overwrite an existing doc
	docRef = client.Doc("collection-1/document-1-1")
	_, err = docRef.Set(ctx, map[string]interface{}{
		"field1": "new-value-1-1-1-1",
		"field2": "new-value-1-1-2-1",
		"field6": []float64{13.0, 14.0, 15.0},
		"field7": map[string]interface{}{},
	})
	assert.Nil(t, err)

	docSnap, err = docRef.Get(ctx)
	assert.Nil(t, err)

	docData = docSnap.Data()
	assert.Equal(t, "new-value-1-1-1-1", docData["field1"])
	assert.Equal(t, "new-value-1-1-2-1", docData["field2"])
	assert.Equal(t, []interface{}{13.0, 14.0, 15.0}, docData["field6"])
	assert.Equal(t, map[string]interface{}{}, docData["field7"])
	assert.Nil(t, docData["field8"])
}

func TestClientUpdate(t *testing.T) {
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	// precondition failure
	docRef := client.Doc("collection-1/document-xxxxx")
	_, err = docRef.Update(ctx, []firestore.Update{
		{
			Path:  "field2",
			Value: "new-value-1-1-2",
		},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	// successful update
	docRef = client.Doc("collection-1/document-1-1")
	_, err = docRef.Update(ctx, []firestore.Update{
		{
			Path:  "field2",
			Value: "new-value-1-1-2",
		},
	})
	assert.Nil(t, err)

	docSnap, err := docRef.Get(ctx)
	assert.Nil(t, err)

	docData := docSnap.Data()
	assert.Equal(t, "value-1-1-1", docData["field1"])
	assert.Equal(t, "new-value-1-1-2", docData["field2"])
}
