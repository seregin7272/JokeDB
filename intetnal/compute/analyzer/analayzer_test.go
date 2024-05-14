package analyzer_test

import (
	"jokedb/intetnal/compute/analyzer"
	"jokedb/intetnal/storage/engine"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Analyze(t *testing.T) {
	a := analyzer.New()

	t.Run("ok", func(t *testing.T) {
		cases := map[string]struct {
			want   analyzer.Action
			tokens []string
		}{
			"set": {
				want: analyzer.Action{
					Type: engine.SET,
					KV: engine.KV{
						Key:   "key",
						Value: "value",
					},
				},
				tokens: []string{"SET", "key", "value"}},
			"get": {
				want: analyzer.Action{
					Type: engine.GET,
					KV: engine.KV{
						Key: "key",
					},
				},
				tokens: []string{"GET", "key"}},
			"del": {
				want: analyzer.Action{
					Type: engine.DEL,
					KV: engine.KV{
						Key: "key",
					},
				},
				tokens: []string{"DEL", "key"}},
		}

		for name, tt := range cases {
			t.Run(name, func(t *testing.T) {
				got, err := a.Analyze(tt.tokens)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("err", func(t *testing.T) {
		cases := map[string]struct {
			tokens []string
		}{
			"empty": {
				tokens: []string{}},
			"not_found_type": {
				tokens: []string{"UPD", "key"}},
			"set": {
				tokens: []string{"SET", "key"}},
			"get": {
				tokens: []string{"GET"}},
			"del": {
				tokens: []string{"DEL"}},
		}

		for name, tt := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := a.Analyze(tt.tokens)
				require.Error(t, err)
			})
		}
	})
}
