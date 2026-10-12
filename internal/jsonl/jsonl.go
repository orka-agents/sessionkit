// Package jsonl reads bounded JSONL and rejects ambiguous JSON encodings.
package jsonl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/orka-agents/sessionkit/internal/budget"
	"github.com/orka-agents/sessionkit/internal/model"
)

// Error never includes JSON keys, strings, or record bodies.
type Error struct{ Code, Message string }

func (e *Error) Error() string { return e.Message }

type Reader struct {
	input   *bufio.Reader
	tracker *budget.Tracker
}

func New(input io.Reader, tracker *budget.Tracker) *Reader {
	size := min(int64(32<<10), max(int64(1), tracker.Limits().MaxLineBytes))
	return &Reader{input: bufio.NewReaderSize(&chargedReader{input, tracker}, int(size)), tracker: tracker}
}

type chargedReader struct {
	input   io.Reader
	tracker *budget.Tracker
}

func (r *chargedReader) Read(p []byte) (int, error) {
	if err := r.tracker.Check(); err != nil {
		return 0, err
	}
	remaining := r.tracker.RemainingBytes()
	// One probe byte distinguishes an exact-size file from an oversized file.
	if remaining < int64(len(p)) {
		p = p[:max(int64(0), remaining)+1]
	}
	n, err := r.input.Read(p)
	if e := r.tracker.Bytes(int64(n)); e != nil {
		return 0, e
	}
	return n, err
}

// Next returns a raw line without its LF. Every record, including the last,
// must end in LF. A line's limit excludes the terminating LF.
func (r *Reader) Next() ([]byte, error) {
	if err := r.tracker.Check(); err != nil {
		return nil, err
	}
	if _, err := r.input.Peek(1); err != nil {
		return nil, err
	}
	if err := r.tracker.Record(); err != nil {
		return nil, err
	}
	var line []byte
	limit := r.tracker.Limits().MaxLineBytes
	for {
		part, err := r.input.ReadSlice('\n')
		complete := len(part) > 0 && part[len(part)-1] == '\n'
		if complete {
			part = part[:len(part)-1]
		}
		if int64(len(part)) > limit-int64(len(line)) {
			return nil, &model.BudgetError{Limit: "line_bytes"}
		}
		if needed := len(line) + len(part); needed > cap(line) {
			capacity := min(limit, max(int64(needed), 2*int64(cap(line))))
			next := make([]byte, len(line), int(capacity))
			copy(next, line)
			line = next
		}
		line = append(line, part...)
		if complete {
			return line, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(line) == 0 {
				return nil, io.EOF
			}
			return nil, &Error{"incomplete_tail", "final JSONL record is missing its terminating newline"}
		}
		if err != nil {
			return nil, err
		}
	}
}

// Decode reads one JSON object. Its caller charges input bytes; Decode charges
// nodes and enforces line size, depth, UTF-8, duplicate keys, and trailing data.
// Numbers retain their original spelling as json.Number.
func Decode(data []byte, tracker *budget.Tracker) (map[string]any, error) {
	if err := tracker.Check(); err != nil {
		return nil, err
	}
	if int64(len(data)) > tracker.Limits().MaxLineBytes {
		return nil, &model.BudgetError{Limit: "line_bytes"}
	}
	if !utf8.Valid(data) {
		return nil, &Error{"invalid_utf8", "JSON contains invalid UTF-8"}
	}
	if err := unicodeEscapes(data, tracker); err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	w := walker{decoder: d, tracker: tracker}
	value, err := w.value(0)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		return nil, invalidJSON()
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, &Error{"invalid_record", "JSON record must be an object"}
	}
	return object, nil
}

// CheckFields enforces exact struct field names after bounded Decode. Map keys
// retain their own case-sensitive meaning, including free-form parameter maps.
func CheckFields(value, target any, component string, tracker *budget.Tracker) error {
	return checkFields(value, reflect.TypeOf(target), component, tracker)
}

func checkFields(value any, schema reflect.Type, component string, tracker *budget.Tracker) error {
	if err := tracker.Check(); err != nil {
		return err
	}
	for schema.Kind() == reflect.Pointer {
		schema = schema.Elem()
	}
	switch schema.Kind() {
	case reflect.Struct:
		object, _ := value.(map[string]any)
		for name, child := range object {
			field, found := fieldType(schema, name)
			if !found {
				return &model.IntegrityError{Component: component, Reason: "invalid schema"}
			}
			if err := checkFields(child, field, component, tracker); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		array, _ := value.([]any)
		for _, child := range array {
			if err := checkFields(child, schema.Elem(), component, tracker); err != nil {
				return err
			}
		}
	case reflect.Map:
		object, _ := value.(map[string]any)
		for _, child := range object {
			if err := checkFields(child, schema.Elem(), component, tracker); err != nil {
				return err
			}
		}
	}
	return nil
}

func fieldType(schema reflect.Type, name string) (reflect.Type, bool) {
	for index := 0; index < schema.NumField(); index++ {
		field := schema.Field(index)
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if tag == "-" {
			continue
		}
		if field.Anonymous && tag == "" {
			embedded := field.Type
			if embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				if found, ok := fieldType(embedded, name); ok {
					return found, true
				}
				continue
			}
		}
		if field.PkgPath != "" {
			continue
		}
		if tag == "" {
			tag = field.Name
		}
		if tag == name {
			return field.Type, true
		}
	}
	return nil, false
}

type walker struct {
	decoder *json.Decoder
	tracker *budget.Tracker
}

// encoding/json replaces unpaired UTF-16 surrogates with U+FFFD. Reject them
// before decoding so inspection does not silently reinterpret a JSON string.
func unicodeEscapes(data []byte, tracker *budget.Tracker) error {
	inString := false
	for i := 0; i < len(data); i++ {
		if i%(16<<10) == 0 {
			if err := tracker.Check(); err != nil {
				return err
			}
		}
		if data[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			break
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return invalidJSON()
		}
		value, ok := hex4(data[i+1 : i+5])
		if !ok {
			return invalidJSON()
		}
		i += 4
		if value >= 0xd800 && value <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return &Error{"invalid_unicode", "JSON contains an unpaired Unicode surrogate"}
			}
			low, ok := hex4(data[i+3 : i+7])
			if !ok || low < 0xdc00 || low > 0xdfff {
				return &Error{"invalid_unicode", "JSON contains an unpaired Unicode surrogate"}
			}
			i += 6
		} else if value >= 0xdc00 && value <= 0xdfff {
			return &Error{"invalid_unicode", "JSON contains an unpaired Unicode surrogate"}
		}
	}
	return nil
}

func hex4(data []byte) (uint16, bool) {
	var value uint16
	for _, c := range data {
		value <<= 4
		switch {
		case c >= '0' && c <= '9':
			value += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			value += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			value += uint16(c - 'A' + 10)
		default:
			return 0, false
		}
	}
	return value, true
}

func invalidJSON() error { return &Error{"invalid_json", "JSON record is malformed"} }

func (w *walker) token() (json.Token, error) {
	if err := w.tracker.Node(); err != nil {
		return nil, err
	}
	token, err := w.decoder.Token()
	if err != nil {
		return nil, invalidJSON()
	}
	return token, nil
}

func (w *walker) value(depth int) (any, error) {
	token, err := w.token()
	if err != nil {
		return nil, err
	}
	delim, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	if depth >= w.tracker.Limits().MaxDepth {
		return nil, &model.BudgetError{Limit: "depth"}
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for w.decoder.More() {
			keyToken, err := w.token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, invalidJSON()
			}
			if _, exists := object[key]; exists {
				return nil, &Error{"duplicate_key", "JSON object contains a duplicate key"}
			}
			value, err := w.value(depth + 1)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		end, err := w.token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim('}') {
			return nil, invalidJSON()
		}
		return object, nil
	case '[':
		var array []any
		for w.decoder.More() {
			value, err := w.value(depth + 1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		end, err := w.token()
		if err != nil {
			return nil, err
		}
		if end != json.Delim(']') {
			return nil, invalidJSON()
		}
		return array, nil
	default:
		return nil, invalidJSON()
	}
}
