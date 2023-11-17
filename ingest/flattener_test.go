package ingest_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"scratchdb/ingest"
)

func TestFlattenJSON(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		input      string
		useIndices bool

		expected  []string
		expectErr error
		errMsg    string
	}{
		{
			name:      "Valid JSON",
			input:     `{"a": 1, "b": 2}`,
			expected:  []string{`{"a":1,"b":2}`},
			expectErr: nil,
		},
		{
			name:      "Nested JSON",
			input:     `{"a": {"b": 1}}`,
			expected:  []string{`{"a_b":1}`},
			expectErr: nil,
		},
		{
			name:      "Array JSON",
			input:     `{"a": [1,2,3]}`,
			expected:  []string{`{"a":1}`, `{"a":2}`, `{"a":3}`},
			expectErr: nil,
		},
		{
			name:       "Deep Nested JSON",
			input:      `{"a": [1,2,3], "b": {"c": {"d": 1, "e": [{"f": {"g": 1}}]}}}`,
			useIndices: true,
			expected: []string{
				`{"a_0":1,"b_c_d":1,"b_c_e_0_f_g":1}`,
				`{"a_1":2,"b_c_d":1,"b_c_e_0_f_g":1}`,
				`{"a_2":3,"b_c_d":1,"b_c_e_0_f_g":1}`,
			},
			expectErr: nil,
		},
		{
			name:      "Empty Array JSON",
			input:     `{"a": []}`,
			expected:  []string{`{"a":null}`},
			expectErr: nil,
		},
		{
			name:      "Unsupported JSON",
			input:     `"unsupported json"`,
			expected:  nil,
			expectErr: &json.UnmarshalTypeError{},
			errMsg:    "json: cannot unmarshal string into Go value of type map[string]interface {}",
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {
			flattened, err := ingest.FlattenJSON(tc.input, nil, tc.useIndices)
			if tc.expectErr != nil {
				assert.Error(t, err)
				assert.ErrorAs(t, err, &tc.expectErr)
				assert.Equal(t, tc.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, flattened)
			}
		})
	}
}
