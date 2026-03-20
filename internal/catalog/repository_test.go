package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatPrefixQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "string vacío",
			query:    "",
			expected: "",
		},
		{
			name:     "una palabra",
			query:    "aceite",
			expected: "aceite:*",
		},
		{
			name:     "múltiples palabras con espacios extra",
			query:    " lubrix   chocolate ",
			expected: "lubrix:* & chocolate:*",
		},
		{
			name:     "caracteres especiales",
			query:    `lubrix's "chocolate"`,
			expected: "lubrixs:* & chocolate:*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPrefixQuery(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}
