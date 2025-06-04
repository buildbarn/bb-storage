package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func formatQuery(query *gocql.Query) string {
	valuesBuilder := strings.Builder{}
	for i, v := range query.Values() {
		if i != 0 {
			valuesBuilder.WriteString(" ")
		}
		s := fmt.Sprintf("%v", v)
		maxFieldStrlen := 100
		valuesBuilder.WriteString(s[:min(len(s), maxFieldStrlen)])
		if len(s) >= maxFieldStrlen {
			valuesBuilder.WriteString("...")
		}
	}
	return fmt.Sprintf("[query %s \"%s\" [%s]]", query.GetConsistency(), query.Statement(), valuesBuilder.String())
}
