package workeragent

import (
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (
	jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

func strictDecodeJSON(content []byte, destination any) error {
	if destination == nil {
		return errors.New("JSON destination is required")
	}
	if err := rejectDuplicateJSONKeys(content); err != nil {
		return err
	}
	if err := rejectNonCanonicalJSONKeys(content, reflect.TypeOf(destination)); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON document contains trailing data")
		}
		return fmt.Errorf("decode trailing JSON data: %w", err)
	}
	return nil
}

func rejectNonCanonicalJSONKeys(content []byte, destinationType reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return validateCanonicalJSONKeys(value, destinationType)
}

func validateCanonicalJSONKeys(value any, destinationType reflect.Type) error {
	if destinationType == nil {
		return nil
	}
	for destinationType.Kind() == reflect.Pointer {
		if usesCustomJSONUnmarshaler(destinationType) {
			return nil
		}
		destinationType = destinationType.Elem()
	}
	if usesCustomJSONUnmarshaler(destinationType) {
		return nil
	}
	switch destinationType.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		fields := canonicalJSONFields(destinationType)
		for key, child := range object {
			fieldType, exact := fields[key]
			if !exact {
				for canonical := range fields {
					if strings.EqualFold(key, canonical) {
						return fmt.Errorf("non-canonical JSON key %q; want %q", key, canonical)
					}
				}
				continue
			}
			if err := validateCanonicalJSONKeys(child, fieldType); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		array, ok := value.([]any)
		if !ok {
			return nil
		}
		for _, child := range array {
			if err := validateCanonicalJSONKeys(child, destinationType.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok || destinationType.Key().Kind() != reflect.String {
			return nil
		}
		for _, child := range object {
			if err := validateCanonicalJSONKeys(child, destinationType.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func usesCustomJSONUnmarshaler(valueType reflect.Type) bool {
	if valueType == nil {
		return false
	}
	if valueType.Implements(jsonUnmarshalerType) || valueType.Implements(textUnmarshalerType) {
		return true
	}
	return valueType.Kind() != reflect.Pointer &&
		(reflect.PointerTo(valueType).Implements(jsonUnmarshalerType) ||
			reflect.PointerTo(valueType).Implements(textUnmarshalerType))
}

func canonicalJSONFields(structType reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if !field.IsExported() && !field.Anonymous {
			continue
		}
		tag := field.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		fieldType := field.Type
		if field.Anonymous && name == "" {
			for fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() == reflect.Struct && !usesCustomJSONUnmarshaler(fieldType) {
				for promotedName, promotedType := range canonicalJSONFields(fieldType) {
					fields[promotedName] = promotedType
				}
				continue
			}
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
	return fields
}

func rejectDuplicateJSONKeys(content []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if err := walkUniqueJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON document contains trailing data")
		}
		return fmt.Errorf("decode trailing JSON data: %w", err)
	}
	return nil
}

func walkUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, duplicate := keys[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			keys[key] = struct{}{}
			if err := walkUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return errors.New("JSON object is not terminated")
		}
	case '[':
		for decoder.More() {
			if err := walkUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("JSON array is not terminated")
		}
	default:
		return errors.New("JSON document contains an unexpected delimiter")
	}
	return nil
}
