package utils

import (
	"strconv"
	"strings"
)

func GetSizeFromStringtoFloat(size string) float64 {
	sizeArray := strings.Split(size, " ")
	switch {
	case sizeArray[1] == "TiB":
		finalSize, _ := strconv.ParseFloat(sizeArray[0], 64)
		return finalSize * 1099511627776
	case sizeArray[1] == "GiB":
		finalSize, _ := strconv.ParseFloat(sizeArray[0], 64)
		return finalSize * 1073741824
	case sizeArray[1] == "MiB":
		finalSize, _ := strconv.ParseFloat(sizeArray[0], 64)
		return finalSize * 1048576
	case sizeArray[1] == "KiB":
		finalSize, _ := strconv.ParseFloat(sizeArray[0], 64)
		return finalSize * 1024
	default:
		finalSize, _ := strconv.ParseFloat(sizeArray[0], 64)
		return finalSize
	}
}
