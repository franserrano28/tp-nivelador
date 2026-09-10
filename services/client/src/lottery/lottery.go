package lottery

import (
	"fmt"
	"strings"
)

type Bet struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    string
}

func ParseToBet(line string) (Bet, error) {
	fields := strings.Split(line, ",")

	if len(fields) != 5 {
		return Bet{}, fmt.Errorf("parse-bet: invalid line format, expected 5 fields, got %d", len(fields))
	}

	return Bet{
		FirstName: fields[0],
		LastName:  fields[1],
		Document:  fields[2],
		Birthdate: fields[3],
		Number:    fields[4],
	}, nil
}