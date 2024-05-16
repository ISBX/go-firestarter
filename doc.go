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

type Collection struct {
	documents map[string]Document
}

type Document struct {
	name           string
	subcollections map[string]Collection
	fields         map[string]interface{}
}

func valueToProtoValue(value interface{}) *pb.Value {
	switch v := value.(type) {
	case string:
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v}}
	case int:
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(v)}}
	case float64:
		return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: v}}
	case bool:
		return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: v}}
	case map[string]interface{}:
		return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: mapToFields(v)}}}
	case []interface{}:
		arrValues := []*pb.Value{}
		for _, val := range v {
			pbv := valueToProtoValue(val)
			arrValues = append(arrValues, pbv)
		}
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: arrValues}}}
	}
	return nil
}

func mapToFields(mapvals map[string]interface{}) map[string]*pb.Value {
	fields := map[string]*pb.Value{}
	for key, value := range mapvals {
		fields[key] = valueToProtoValue(value)
	}
	return fields
}

func pbMapToMap(mapvals map[string]*pb.Value) map[string]interface{} {
	// TODO timestamp?
	fields := map[string]interface{}{}
	for key, value := range mapvals {
		switch v := value.GetValueType().(type) {
		case *pb.Value_StringValue:
			fields[key] = v.StringValue
		case *pb.Value_IntegerValue:
			fields[key] = v.IntegerValue
		case *pb.Value_DoubleValue:
			fields[key] = v.DoubleValue
		case *pb.Value_BooleanValue:
			fields[key] = v.BooleanValue
		case *pb.Value_MapValue:
			fields[key] = pbMapToMap(v.MapValue.Fields)
		}
	}
	return fields
}

func (d *Document) ToProto(fullPath string) *pb.Document {
	aTimestamp := timestamppb.Now()
	doc := &pb.Document{
		Name:       fullPath,
		CreateTime: aTimestamp,
		UpdateTime: aTimestamp,
		Fields:     mapToFields(d.fields),
	}

	return doc
}

func (d *Document) Get(name string) interface{} {
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

func (d *Document) SetWithValue(name string, value *pb.Value) {
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
		d.fields[name] = pbMapToMap(value.MapValue.Fields)
	case *pb.Value_ArrayValue:
		// TODO
		d.fields[name] = value.ArrayValue.Values
	}
}

func parseCollection(path string, collectionData map[string]interface{}) (*Collection, error) {
	collection := Collection{
		documents: map[string]Document{},
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

func parseDocument(path string, documentData map[string]interface{}) (*Document, error) {
	newDoc := Document{
		name:           path,
		subcollections: map[string]Collection{},
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
