package cassandra

import (
	"bytes"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

func Test_formatQuery(t *testing.T) {
	session := gocql.Session{}
	longBlob := bytes.Repeat([]byte("This is a very long string!"), 100)
	query := session.Query("INSERT INTO some_table (id, first_name, long_blob) VALUES (?, ?, ?)", 1234, "Jon Snow", longBlob)
	expectedOutput := `[query ANY "INSERT INTO some_table (id, first_name, long_blob) VALUES (?, ?, ?)" ` +
		`[1234 Jon Snow [84 104 105 115 32 105 115 32 97 32 118 101 114 121 32 108 111 110 103 32 115 116 114 105 110 103 33...]]`
	require.Equal(t, expectedOutput, formatQuery(query))
}
