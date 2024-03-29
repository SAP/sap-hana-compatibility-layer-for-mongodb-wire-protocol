// SPDX-FileCopyrightText: 2021 FerretDB Inc.
//
// SPDX-License-Identifier: Apache-2.0

// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"golang.org/x/exp/constraints"
)

// compareResult represents the result of a comparison.
type CompareResult int

const (
	equal CompareResult = iota
	less
	greater
	notEqual // but not less or greater; for example, two NaNs
)

// compareScalars compares two scalar values.
func CompareScalars(a, b any) CompareResult {
	if a == nil {
		panic("a is nil")
	}
	if b == nil {
		panic("b is nil")
	}

	switch a := a.(type) {
	case float64:
		switch b := b.(type) {
		case float64:
			if math.IsNaN(a) && math.IsNaN(b) {
				return equal
			}
			return compareOrdered(a, b)
		case int32:
			return compareNumbers(a, int64(b))
		case int64:
			return compareNumbers(a, b)
		default:
			return notEqual
		}

	case string:
		b, ok := b.(string)
		if ok {
			return compareOrdered(a, b)
		}
		return notEqual

	// case Binary:
	//	b, ok := b.(types.Binary)
	//	if !ok {
	//		return notEqual
	//	}
	//	al, bl := len(a.B), len(b.B)
	//	if al != bl {
	//		return compareOrdered(al, bl)
	//	}
	//	if a.Subtype != b.Subtype {
	//		return compareOrdered(a.Subtype, b.Subtype)
	//	}
	//	switch bytes.Compare(a.B, b.B) {
	//	case 0:
	//		return equal
	//	case -1:
	//		return less
	//	case 1:
	//		return greater
	//	default:
	//		panic("unreachable")
	//	}

	case ObjectID:
		b, ok := b.(ObjectID)
		if !ok {
			return notEqual
		}
		switch bytes.Compare(a[:], b[:]) {
		case 0:
			return equal
		case -1:
			return less
		case 1:
			return greater
		default:
			panic("unreachable")
		}

	case bool:
		b, ok := b.(bool)
		if !ok {
			return notEqual
		}
		if a == b {
			return equal
		}
		if b {
			return less
		}
		return greater

	case time.Time:
		b, ok := b.(time.Time)
		if ok {
			return compareOrdered(a.UnixNano(), b.UnixNano())
		}
		return notEqual

	// case NullType:
	//	_, ok := b.(types.NullType)
	//	if ok {
	//		return equal
	//	}
	//	return notEqual

	case Regex:
		return notEqual // ???

	case int32:
		switch b := b.(type) {
		case float64:
			return filterCompareInvert(compareNumbers(b, int64(a)))
		case int32:
			return compareOrdered(a, b)
		case int64:
			return compareOrdered(int64(a), b)
		default:
			return notEqual
		}

	case Timestamp:
		b, ok := b.(Timestamp)
		if ok {
			return compareOrdered(a, b)
		}
		return notEqual

	case int64:
		switch b := b.(type) {
		case float64:
			return filterCompareInvert(compareNumbers(b, a))
		case int32:
			return compareOrdered(a, int64(b))
		case int64:
			return compareOrdered(a, b)
		default:
			return notEqual
		}

	default:
		panic(fmt.Sprintf("unhandled type %T", a))
	}
}

//// compare compares the filter to the value of the document, whether it is a composite type or a scalar type.
//func compare(docValue, filter any) compareResult {
//	if docValue == nil {
//		panic("docValue is nil")
//	}
//	if filter == nil {
//		panic("filter is nil")
//	}
//
//	switch docValue := docValue.(type) {
//	case *Document:
//		return notEqual
//
//	case *types.Array:
//		for i := 0; i < docValue.Len(); i++ {
//			arrValue := must.NotFail(docValue.Get(i)).(any)
//
//			_, isValueArr := arrValue.(*types.Array)
//			_, isValueDoc := arrValue.(*types.Document)
//			if isValueArr || isValueDoc {
//				return notEqual
//			}
//
//			switch compareScalars(arrValue, filter) {
//			case equal:
//				return equal
//			case greater:
//				return greater
//			case less:
//				return less
//			case notEqual:
//				continue
//			}
//		}
//		return notEqual
//
//	default:
//		return compareScalars(docValue, filter)
//	}
//}

// filterCompareInvert swaps less and greater, keeping equal and notEqual.
func filterCompareInvert(res CompareResult) CompareResult {
	switch res {
	case equal:
		return equal
	case less:
		return greater
	case greater:
		return less
	case notEqual:
		return notEqual
	default:
		panic("unreachable")
	}
}

// compareOrdered compares two values of the same type using ==, <, > operators.
func compareOrdered[T constraints.Ordered](a, b T) CompareResult {
	if a == b {
		return equal
	}
	if a < b {
		return less
	}
	if a > b {
		return greater
	}
	return notEqual
}

// compareNumbers compares two numbers.
func compareNumbers(a float64, b int64) CompareResult {
	return compareOrdered(a, float64(b))
}
