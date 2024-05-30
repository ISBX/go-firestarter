# go-firestarter

`go-firestarter` an emulator for Google Firestore. It stores documents and collections in memory and performs queries against that in-memory store.

`go-firestarter` was forked from `go-mockfs` (https://github.com/weathersource/go-mockfs). `go-mockfs` is a low level mock for Google Firestore matching the request's protobuf message and returning a response protofbuf message. `go-firestarter` differs by implementing the logic for creating/updating documents and querying.

## Missing Functionality
* NULL/NaN value handling
* Order By on field with different types between documents
  * https://firebase.google.com/docs/firestore/manage-data/data-types#value_type_ordering
* Order By existence
  * https://firebase.google.com/docs/firestore/query-data/order-limit-data#limitations
* Various limitations/edge-cases
  * https://firebase.google.com/docs/firestore/query-data/queries#query_limitations
* Transactions
* Update off of deprecated import
* Aggregation queries
  * https://firebase.google.com/docs/firestore/query-data/aggregation-queries
* Vector types

## How To Use?
`client_test.go` (https://github.com/ISBX/go-firestarter/blob/master/client_test.go) is a good reference.

An example:
```
	ctx := context.Background()
	client, srv, err := New()
	assert.Nil(t, err)
	defer srv.Close()

	srv.LoadFromJSONFile("test.json")

	docSnaps, err := client.Collection("collection-1").Where("field1", "==", "value-1-2-1").Documents(ctx).GetAll()
	assert.Nil(t, err)

	assert.Len(t, docSnaps, 1)
	assert.Equal(t, "document-1-2", docSnaps[0].Ref.ID)
```

### func (s *MockServer) LoadFromJSONFile(filePath string)
Since JSON types only cover a subset of Firestore types, `LoadFromJSONFile` will parse strings for Timestamps and Bytes.
* If the string is a RFC3339 (https://pkg.go.dev/time#pkg-constants), the value will be stored as a `time.Time` internally and returned as a `pb.Value_TimestampValue`.
* If the string is a data URL, the value will be stored as a `[]byte` and returned as a `pb.Value_BytesValue`.

`test.json` (https://github.com/ISBX/go-firestarter/blob/master/test.json) has a few examples.