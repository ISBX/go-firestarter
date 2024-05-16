package mockfs

import (
	"time"

	pb "google.golang.org/genproto/googleapis/firestore/v1"
)

func matchFilter(doc Document, where *pb.StructuredQuery_Filter) bool {
	if where == nil {
		return true
	}

	if where.GetCompositeFilter() != nil {
		return matchCompositeFilter(doc, where.GetCompositeFilter())
	}

	if where.GetFieldFilter() != nil {
		return matchFieldFilter(doc, where.GetFieldFilter())
	}

	return false
}

func matchCompositeFilter(doc Document, filter *pb.StructuredQuery_CompositeFilter) bool {
	if filter.GetOp() == pb.StructuredQuery_CompositeFilter_AND {
		for _, filter := range filter.GetFilters() {
			if filter.GetCompositeFilter() != nil {
				if !matchCompositeFilter(doc, filter.GetCompositeFilter()) {
					return false
				}
			}

			if filter.GetFieldFilter() != nil {
				if !matchFieldFilter(doc, filter.GetFieldFilter()) {
					return false
				}
			}
		}
		return true
	} else if filter.GetOp() == 2 { // OR but not defined in this version of our import
		for _, filter := range filter.GetFilters() {
			if filter.GetCompositeFilter() != nil {
				if matchCompositeFilter(doc, filter.GetCompositeFilter()) {
					return true
				}
			}

			if filter.GetFieldFilter() != nil {
				if matchFieldFilter(doc, filter.GetFieldFilter()) {
					return true
				}
			}
		}
		return false
	}
	return false
}

func matchFieldFilter(doc Document, filter *pb.StructuredQuery_FieldFilter) bool {
	field := filter.GetField().GetFieldPath()
	value := filter.GetValue()
	op := filter.GetOp()

	v := doc.Get(field)
	if v == nil {
		return false
	}

	return matchValue(v, op, value)
}

func matchValue(value interface{}, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	// TODO maps
	switch v := value.(type) {
	case string:
		return matchStringValue(v, op, filterValue)
	case int:
		return matchNumberValue(v, op, filterValue)
	case float64:
		return matchNumberValue(v, op, filterValue)
	case bool:
		return matchBoolValue(v, op, filterValue)
	case time.Time:
		return matchTimeValue(v, op, filterValue)
	case []interface{}:
		return matchArrayValue(v, op, filterValue)
	}
	return false
}

func matchStringValue(value string, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return value == filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_LESS_THAN:
		return value < filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return value <= filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_GREATER_THAN:
		return value > filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return value >= filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return value != filterValue.GetStringValue()
	case pb.StructuredQuery_FieldFilter_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == ref.GetStringValue() {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == ref.GetStringValue() {
				return false
			}
		}
		return true
	}
	return false
}

func filterValueAsInt64(value *pb.Value) int64 {
	switch value := value.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		return value.IntegerValue
	case *pb.Value_DoubleValue:
		return int64(value.DoubleValue)
	}
	return 0
}

func filterValueAsFloat64(value *pb.Value) float64 {
	switch value := value.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		return float64(value.IntegerValue)
	case *pb.Value_DoubleValue:
		return value.DoubleValue
	}
	return 0
}

func matchNumberValue(value interface{}, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	// if value is int64, convert filterValue to int64
	// if value is float64, convert filterValue to float64

	switch v := value.(type) {
	case int64:
		return matchIntValue(v, op, filterValue)
	case float64:
		return matchDoubleValue(v, op, filterValue)
	}
	return false
}

func matchIntValue(value int64, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return value == filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_LESS_THAN:
		return value < filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return value <= filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_GREATER_THAN:
		return value > filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return value >= filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return value != filterValueAsInt64(filterValue)
	case pb.StructuredQuery_FieldFilter_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == filterValueAsInt64(ref) {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == filterValueAsInt64(ref) {
				return false
			}
		}
		return true
	}
	return false
}

func matchDoubleValue(value float64, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return value == filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_LESS_THAN:
		return value < filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return value <= filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_GREATER_THAN:
		return value > filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return value >= filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return value != filterValueAsFloat64(filterValue)
	case pb.StructuredQuery_FieldFilter_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == filterValueAsFloat64(ref) {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == filterValueAsFloat64(ref) {
				return false
			}
		}
		return true
	}
	return false
}

func matchBoolValue(value bool, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return value == filterValue.GetBooleanValue()
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return value != filterValue.GetBooleanValue()
	case pb.StructuredQuery_FieldFilter_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == ref.GetBooleanValue() {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value == ref.GetBooleanValue() {
				return false
			}
		}
		return true
	}
	return false
}

func matchTimeValue(value time.Time, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		return value.Equal(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_LESS_THAN:
		return value.Before(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return value.Before(filterValue.GetTimestampValue().AsTime()) || value.Equal(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_GREATER_THAN:
		return value.After(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return value.After(filterValue.GetTimestampValue().AsTime()) || value.Equal(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return !value.Equal(filterValue.GetTimestampValue().AsTime())
	case pb.StructuredQuery_FieldFilter_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value.Equal(ref.GetTimestampValue().AsTime()) {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_NOT_IN:
		for _, ref := range filterValue.GetArrayValue().Values {
			if value.Equal(ref.GetTimestampValue().AsTime()) {
				return false
			}
		}
		return true
	}
	return false
}

func matchArrayValue(value []interface{}, op pb.StructuredQuery_FieldFilter_Operator, filterValue *pb.Value) bool {
	switch op {
	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		for _, value := range value {
			if matchValue(value, pb.StructuredQuery_FieldFilter_EQUAL, filterValue) {
				return true
			}
		}
	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		for _, value := range value {
			if matchValue(value, pb.StructuredQuery_FieldFilter_IN, filterValue) {
				return true
			}
		}
	}
	return false
}
