// This file has been modified for the original found at
// https://github.com/GoogleCloudPlatform/google-cloud-go/blob/master/firestore/mock_test.go
//
// Copyright 2017 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockfs

// A simple mock server.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "google.golang.org/genproto/googleapis/firestore/v1"
	empty "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrDocumentNotFound = fmt.Errorf("document not found")
var ErrCollectionNotFound = fmt.Errorf("collection not found")

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getDocumentPath(fullPath string) string {
	// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
	parts := strings.Split(fullPath, "/")
	if len(parts) < 6 {
		return ""
	}
	path := strings.Join(parts[5:], "/")

	return path
}

func (s *MockServer) getDocumentByPath(path string) (*Document, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid document path: %s", path)
	}
	if len(parts)%2 != 0 {
		// should be collectionId/documentId/collectionId/documentId/... and ending in a documentId
		return nil, fmt.Errorf("invalid document path: %s", path)
	}

	// pointer to current document, start at root
	document := Document{
		subcollections: s.data,
	}
	for i := 0; i < len(parts); i += 2 {
		var ok bool
		collectionId := parts[i]
		documentId := parts[i+1]
		collection, ok := document.subcollections[collectionId]
		if !ok {
			return nil, ErrCollectionNotFound
		}
		document, ok = collection.documents[documentId]
		if !ok {
			return nil, ErrDocumentNotFound
		}
	}

	return &document, nil
}

func (s *MockServer) getCollectionByPath(path string) (*Collection, error) {
	parts := strings.Split(path, "/")
	if len(parts) > 1 && len(parts)%2 == 0 {
		// should be collectionId/documentId/collectionId/documentId/... and ending in a collectionId
		return nil, fmt.Errorf("invalid collection path: %s", path)
	}

	collectionId := parts[len(parts)-1]
	parts = parts[:len(parts)-1]

	document := &Document{
		subcollections: s.data,
	}

	if len(parts) > 0 {
		var err error
		document, err = s.getDocumentByPath(strings.Join(parts, "/"))
		if err != nil {
			return nil, err
		}
	}

	collection, ok := document.subcollections[collectionId]
	if !ok {
		return nil, ErrCollectionNotFound
	}

	return &collection, nil
}

func (s *MockServer) newDocumentWithPath(path string) (*Document, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid document path: %s", path)
	}
	if len(parts)%2 != 0 {
		// should be collectionId/documentId/collectionId/documentId/... and ending in a documentId
		return nil, fmt.Errorf("invalid document path: %s", path)
	}

	// pointer to current document, start at root
	d := Document{
		subcollections: s.data,
	}
	for i := 0; i < len(parts); i += 2 {
		var ok bool
		collectionId := parts[i]
		documentId := parts[i+1]
		c, ok := d.subcollections[collectionId]
		if !ok {
			c = Collection{
				documents: map[string]Document{},
			}
			d.subcollections[collectionId] = c
		}
		d, ok = c.documents[documentId]
		if !ok {
			d = Document{
				name:           documentId,
				subcollections: map[string]Collection{},
				fields:         map[string]interface{}{},
			}
			c.documents[documentId] = d
		}
	}

	return &d, nil
}

// GetDocument overrides the FirestoreServer GetDocument method
func (s *MockServer) GetDocument(ctx context.Context, req *pb.GetDocumentRequest) (*pb.Document, error) {
	// not sure when/if this is actually called?
	// client.Doc("collection-1/document-1-1").Get(ctx) seems to use BatchGetDocuments

	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	// `projects/{project_id}/databases/{database_id}/documents/{document_path}`.
	path := getDocumentPath(req.GetName())
	document, err := s.getDocumentByPath(path)
	if err != nil {
		return nil, err
	}

	return document.ToProto(req.GetName()), nil
}

// Commit overrides the FirestoreServer Commit method
func (s *MockServer) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	fmt.Println("Commit") // used for Set?
	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	writes := req.GetWrites()

	responses := []*pb.WriteResult{}

	for _, write := range writes {
		path := getDocumentPath(write.GetUpdate().Name)

		fmt.Println("write", write)

		doc, err := s.getDocumentByPath(path)
		if err != nil {
			fmt.Println("err", err)
			if errors.Is(err, ErrDocumentNotFound) || errors.Is(err, ErrCollectionNotFound) {
				// Collections are created on the fly so can be missing
				// if updating a document, then return error if document doesn't exist
				if write.GetCurrentDocument().GetExists() {
					return nil, err
				}
				doc = &Document{
					name:           path,
					subcollections: map[string]Collection{},
					fields:         map[string]interface{}{},
				}

				doc, err = s.newDocumentWithPath(path)
				if err != nil {
					return nil, err
				}
			} else {
				// TODO precondition failed error
				return &pb.CommitResponse{}, err
			}
		}
		fmt.Println(doc)

		updateMask := write.GetUpdateMask().GetFieldPaths()
		fmt.Println("updateMask", updateMask)
		updateFields := write.GetUpdate().GetFields()
		if len(updateMask) == 0 {
			for field, value := range updateFields {
				doc.SetWithValue(field, value)
			}
		} else {
			for _, field := range updateMask {
				fmt.Println("updating", field, updateFields[field])
				doc.SetWithValue(field, updateFields[field])
			}
		}
		responses = append(responses, &pb.WriteResult{
			UpdateTime: timestamppb.Now(),
		})
	}

	return &pb.CommitResponse{
		WriteResults: responses,
	}, nil
}

// BatchGetDocuments overrides the FirestoreServer BatchGetDocuments method
func (s *MockServer) BatchGetDocuments(req *pb.BatchGetDocumentsRequest, bs pb.Firestore_BatchGetDocumentsServer) error {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()
	for _, docId := range req.Documents {
		path := getDocumentPath(docId)
		document, err := s.getDocumentByPath(path)
		if err != nil {
			readTime := timestamppb.Now()
			response := &pb.BatchGetDocumentsResponse{
				Result:   &pb.BatchGetDocumentsResponse_Missing{Missing: docId},
				ReadTime: readTime,
			}
			err = bs.Send(response)
			if err != nil {
				return err
			}
			return nil
		}
		readTime := timestamppb.Now()
		response := &pb.BatchGetDocumentsResponse{
			Result: &pb.BatchGetDocumentsResponse_Found{
				Found: document.ToProto(docId),
			},
			ReadTime: readTime,
		}
		err = bs.Send(response)
		if err != nil {
			return err
		}
	}
	return nil
}

func lessThanVal(aval, bval interface{}) bool {
	// TODO support Map (any other existing types?)
	// https://firebase.google.com/docs/firestore/manage-data/data-types#data_types
	switch aval.(type) {
	case string:
		return aval.(string) < bval.(string)
	case int:
		return aval.(int) < bval.(int)
	case float64:
		return aval.(float64) < bval.(float64)
	case bool:
		// false < true
		return !aval.(bool) && bval.(bool)
	case time.Time:
		return aval.(time.Time).Before(bval.(time.Time))
	case []byte:
		return string(aval.([]byte)) < string(bval.([]byte))
	case []interface{}:
		aArr := aval.([]interface{})
		bArr := bval.([]interface{})
		alen := len(aArr)
		blen := len(bArr)
		len := min(alen, blen)
		for i := 0; i < len; i++ {
			if lessThanVal(aArr[0], bArr[0]) {
				return true
			} else if lessThanVal(bArr[0], aArr[0]) {
				return false
			}
		}
	}
	return false
}

func lessThan(a Document, b Document, field string, direction pb.StructuredQuery_Direction) bool {
	aval := a.Get(field)
	bval := b.Get(field)
	if direction == pb.StructuredQuery_ASCENDING {
		return lessThanVal(aval, bval)
	} else if direction == pb.StructuredQuery_DESCENDING {
		return lessThanVal(bval, aval)
	}
	// shouldn't get here?
	return false
}

// RunQuery overrides the FirestoreServer RunQuery method
func (s *MockServer) RunQuery(req *pb.RunQueryRequest, qs pb.Firestore_RunQueryServer) error {
	s.dataLock.RLock()
	defer s.dataLock.RUnlock()

	squery := req.GetStructuredQuery()

	// get collection
	collection, err := s.getCollectionByPath(squery.GetFrom()[0].GetCollectionId())
	if err != nil {
		if errors.Is(err, ErrCollectionNotFound) || errors.Is(err, ErrDocumentNotFound) {
			collection = &Collection{
				documents: map[string]Document{},
			}
		} else {
			return err
		}
	}

	// filter documents in collection
	filteredDocs := []Document{}

	where := squery.GetWhere()
	for _, doc := range collection.documents {
		if matchFilter(doc, where) {
			filteredDocs = append(filteredDocs, doc)
		}
	}

	// sort documents - if unspecified, sort by name
	orderBys := squery.GetOrderBy()
	sort.Slice(filteredDocs, func(i, j int) bool {
		for _, orderBy := range orderBys {
			field := orderBy.GetField().GetFieldPath()
			if field == "__name__" || field == "DocumentID" {
				if orderBy.GetDirection() == pb.StructuredQuery_ASCENDING {
					if filteredDocs[i].name < filteredDocs[j].name {
						return true
					} else if filteredDocs[i].name > filteredDocs[j].name {
						return false
					}
				} else {
					if filteredDocs[i].name > filteredDocs[j].name {
						return true
					} else if filteredDocs[i].name < filteredDocs[j].name {
						return false
					}
				}
			} else {
				if lessThan(filteredDocs[i], filteredDocs[j], field, orderBy.GetDirection()) {
					return true
				} else if lessThan(filteredDocs[j], filteredDocs[i], field, orderBy.GetDirection()) {
					return false
				}
			}
		}
		return filteredDocs[i].name < filteredDocs[j].name
	})

	// limit and offset
	limit := int(squery.GetLimit().GetValue())
	offset := int(squery.GetOffset())

	if limit == 0 {
		limit = len(filteredDocs)
	}

	if offset+limit > len(filteredDocs) {
		filteredDocs = filteredDocs[offset:]
	} else {
		filteredDocs = filteredDocs[offset : offset+limit]
	}

	if len(filteredDocs) == 0 {
		response := &pb.RunQueryResponse{
			ReadTime: timestamppb.Now(),
		}
		err = qs.Send(response)
		if err != nil {
			return err
		}
		return nil
	}
	for _, doc := range filteredDocs {
		response := &pb.RunQueryResponse{
			// get the fullPath of the document
			// does `projectID` really matter?
			Document: doc.ToProto("projects/projectID/databases/(default)/documents/" + doc.name),
			ReadTime: timestamppb.Now(),
		}
		err = qs.Send(response)
		if err != nil {
			return err
		}
	}
	return nil
}

// BeginTransaction overrides the FirestoreServer BeginTransaction method
func (s *MockServer) BeginTransaction(ctx context.Context, req *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	// TODO
	fmt.Println("BeginTransaction")
	return nil, nil
}

// Rollback overrides the FirestoreServer Rollback method
func (s *MockServer) Rollback(ctx context.Context, req *pb.RollbackRequest) (*empty.Empty, error) {
	// TODO
	fmt.Println("Rollback")
	return nil, nil
}

// Listen overrides the FirestoreServer Listen method
func (s *MockServer) Listen(stream pb.Firestore_ListenServer) error {
	// TODO
	fmt.Print("Listen")
	return nil
}
