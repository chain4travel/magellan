// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
//
// This file contains helper functions

package cfg

import (
	"fmt"
	"time"
)

func GetDatepartBasedOnDateParams(pStartTime time.Time, pEndTime time.Time) string {
	differenceInDays := int64(pEndTime.Sub(pStartTime).Hours() / 24)
	fmt.Print("diff in days:")
	fmt.Println(differenceInDays)

	if differenceInDays <= 1 {
		return "day"
	} else if differenceInDays > 1 && differenceInDays <= 7 {
		return "week"
	} else if differenceInDays > 7 && differenceInDays <= 31 {
		return "month"
	}

	return ""
}