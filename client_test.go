package mockfs

import (
	"context"
	"testing"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/assert"
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

func TestClientSet(t *testing.T) {
	// TODO test overwriting an existing document?
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	// create a new doc
	docRef := client.Doc("collection-1/document-1-1")
	_, err = docRef.Set(ctx, map[string]interface{}{
		"field1": "new-value-1-1-1",
		"field2": "new-value-1-1-2",
		"field7": map[string]interface{}{
			"subfield1": "new-subvalue-1-1-1-1",
			"subfield2": "new-subvalue-1-1-1-2",
		},
	})
	assert.Nil(t, err)

	docSnap, err := docRef.Get(ctx)
	assert.Nil(t, err)

	docData := docSnap.Data()
	assert.Equal(t, "new-value-1-1-1", docData["field1"])
	assert.Equal(t, "new-value-1-1-2", docData["field2"])
	assert.Equal(t, map[string]interface{}{
		"subfield1": "new-subvalue-1-1-1-1",
		"subfield2": "new-subvalue-1-1-1-2",
	}, docData["field7"])
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
	assert.NotNil(t, err) // TODO: send proper PreconditionFailure error?

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
