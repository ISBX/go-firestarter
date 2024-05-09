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
	"encoding/json"
	"fmt"
	"os"
	"sync"

	gsrv "github.com/weathersource/go-gsrv"
	pb "google.golang.org/genproto/googleapis/firestore/v1"
)

// MockServer mocks the pb.FirestoreServer interface
// (https://godoc.org/google.golang.org/genproto/googleapis/firestore/v1beta1#FirestoreServer)
type MockServer struct {
	pb.FirestoreServer
	Addr string

	srv      *gsrv.Server
	data     map[string]collection
	dataLock sync.RWMutex
}

func newServer() (*MockServer, error) {
	srv, err := gsrv.NewServer()
	if err != nil {
		return nil, err
	}
	mock := &MockServer{
		Addr: srv.Addr,

		srv:  srv,
		data: map[string]collection{},
	}
	pb.RegisterFirestoreServer(srv.Gsrv, mock)
	srv.Start()
	return mock, nil
}

// Reset returns the MockServer to an empty state.
func (s *MockServer) Reset() {
	s.dataLock.Lock()
	s.data = map[string]collection{}
	s.dataLock.Unlock()
}

func (s *MockServer) Close() {
	s.srv.Close()
}

// LoadFromFile loads a JSON file into the MockServer.
func (s *MockServer) LoadFromJSONFile(filePath string) error {
	jsonBytes, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	jsonMap := make(map[string]interface{})
	err = json.Unmarshal(jsonBytes, &jsonMap)
	if err != nil {
		return err
	}

	s.dataLock.Lock()
	defer s.dataLock.Unlock()

	for collectionName, collectionData := range jsonMap {
		data, ok := collectionData.(map[string]interface{})
		if !ok {
			return fmt.Errorf("collection %v data is not a map: %v", collectionName, collectionData)
		}
		collection, err := parseCollection(collectionName, data)
		if err != nil {
			return err
		}

		s.data[collectionName] = *collection
	}

	return nil
}
