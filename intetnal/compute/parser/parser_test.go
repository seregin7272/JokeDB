package parser_test

import (
	"jokedb/intetnal/compute/parser"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenization(t *testing.T) {
	p := parser.New()
	t.Run("ok", func(t *testing.T) {
		in := "token1 token2 token3"
		want := []string{"token1", "token2", "token3"}
		got, err := p.Tokenization(in)
		require.NoError(t, err)
		require.Equal(t, want, got)
	})

	t.Run("not_valid", func(t *testing.T) {
		in := "token1 to+ken2 token3"
		_, err := p.Tokenization(in)
		require.ErrorIs(t, err, parser.ErrNotValidSymbol)
	})
}
