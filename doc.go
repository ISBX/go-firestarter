// Package mockfs mocks Google Firestore for Golang testing. This code has been
// extracted from the unit tests of the official Go Firestore package
// (cloud.google.com/go/firestore) and edited to make it sutible for publication
// as a stand-alone package.
package mockfs

import (
	"fmt"
	"strings"

	pb "google.golang.org/genproto/googleapis/firestore/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type collection struct {
	documents map[string]document
}

type document struct {
	name           string
	subcollections map[string]collection
	fields         map[string]interface{}
}

func mapToFields(mapvals map[string]interface{}) map[string]*pb.Value {
	// TODO timestamp?
	fields := map[string]*pb.Value{}
	for key, value := range mapvals {
		switch v := value.(type) {
		case string:
			fields[key] = &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v}}
		case int:
			fields[key] = &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(v)}}
		case float64:
			fields[key] = &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: v}}
		case bool:
			fields[key] = &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: v}}
		case map[string]interface{}:
			fields[key] = &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: mapToFields(v)}}}
		}
	}
	return fields
}

func (d *document) ToProto(fullPath string) *pb.Document {
	aTimestamp := timestamppb.Now()
	doc := &pb.Document{
		Name:       fullPath,
		CreateTime: aTimestamp,
		UpdateTime: aTimestamp,
		Fields:     mapToFields(d.fields),
	}

	return doc
}

func (d *document) Get(name string) interface{} {
	parts := strings.Split(name, ".")

	current := d.fields
	for _, part := range parts {
		if current[part] == nil {
			return nil
		}
		value := current[part]
		switch v := value.(type) {
		case map[string]interface{}:
			current = v
		default:
			return value
		}
	}
	return nil
}

func (d *document) SetWithValue(name string, value *pb.Value) {
	switch value := value.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		d.fields[name] = value.IntegerValue
	case *pb.Value_DoubleValue:
		d.fields[name] = value.DoubleValue
	case *pb.Value_TimestampValue:
		// TODO
		d.fields[name] = value.TimestampValue
	case *pb.Value_StringValue:
		d.fields[name] = value.StringValue
	case *pb.Value_BooleanValue:
		d.fields[name] = value.BooleanValue
	case *pb.Value_MapValue:
		// TODO
		d.fields[name] = value.MapValue.Fields
	case *pb.Value_ArrayValue:
		// TODO
		d.fields[name] = value.ArrayValue.Values
	}
}

func parseCollection(path string, collectionData map[string]interface{}) (*collection, error) {
	collection := collection{
		documents: map[string]document{},
	}

	for documentName, documentData := range collectionData {
		ddata, ok := documentData.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("document %v data is not a map: %v", documentName, documentData)
		}
		newDoc, err := parseDocument(path+"/"+documentName, ddata)
		if err != nil {
			return nil, err
		}
		collection.documents[documentName] = *newDoc
		if !ok {
			return nil, fmt.Errorf("document %v data is not a map: %v", documentName, documentData)
		}
	}

	return &collection, nil
}

func parseDocument(path string, documentData map[string]interface{}) (*document, error) {
	newDoc := document{
		name:           path,
		subcollections: map[string]collection{},
		fields:         map[string]interface{}{},
	}

	for key, value := range documentData {
		if key == "__collections__" {
			collections, ok := value.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("subcollections %v data is not a map: %v", key, value)
			}
			for collectionName, collectionData := range collections {
				cdata, ok := collectionData.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("collection %v data is not a map: %v", collectionName, collectionData)
				}
				newCollection, err := parseCollection(path+"/"+collectionName, cdata)
				if err != nil {
					return nil, err
				}
				newDoc.subcollections[collectionName] = *newCollection
			}
		} else {
			newDoc.fields[key] = value
		}
	}

	return &newDoc, nil
}
