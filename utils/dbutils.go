// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	DeadlockDBErrorMessage = "Deadlock found when trying to get lock; try restarting transaction"
	TimeoutDBErrorMessage  = "Lock wait timeout exceeded; try restarting transaction"
)

func ErrIsDuplicateEntryError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry")
}

func ErrIsLockError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), DeadlockDBErrorMessage) ||
		strings.Contains(err.Error(), TimeoutDBErrorMessage))
}

func ForceParseTimeParam(dsn string) (string, error) {
	// Parse dsn into a url
	u, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}

	if u.Params == nil {
		u.Params = make(map[string]string)
	}
	u.Params["parseTime"] = "true"

	// Re-encode as a string
	return u.FormatDSN(), nil
}

func DateFormat(startTime time.Time, endTime time.Time, columnName string) string {
	monthsBetween := int(endTime.Month() - startTime.Month())
	yearsBetween := endTime.Year() - startTime.Year()
	var dateFormat string
	switch {
	// if the date range is greater than or equal to one month the values are averaged per month
	case (monthsBetween >= 1 || monthsBetween < 0 || startTime.Year() == 1) && yearsBetween == 0:
		dateFormat = "DATE_FORMAT(" + columnName + ",'%Y-%m-01')"
	// if the date range is greater than or equal to one year the values are averaged per year
	case yearsBetween > 0:
		dateFormat = "DATE_FORMAT(" + columnName + ",'%Y-01-01')"
	default:
		dateFormat = "DATE_FORMAT(" + columnName + ",'%Y-%m-%d')"
	}
	return dateFormat
}
