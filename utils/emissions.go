package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
)

// TODO: change the variable to constant
var chainNames = [19]string{"algorand", "avalanche", "bitcoin", "caminocolumbus", "cardano", "cosmos", "kava", "polkadot", "solana", "tezos", "tron"}

func GetDailyEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService) models.Emissions {
	var dailyEmissions []models.EmissionsResult
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	for _, chain := range chainNames {
		intensityFactor := CarbonIntensityFactor(chain, startDatef, endDatef, config)
		if len(intensityFactor) > 0 {
			dailyEmissions = append(dailyEmissions, intensityFactor[0])
		}
	}
	if dailyEmissions == nil {
		return models.Emissions{Name: "Daily Emissions", Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: "Daily Emissions", Value: dailyEmissions}
}

func GetNetworkEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService) models.Emissions {
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	emissionsResult := network(chainNames[3], startDatef, endDatef, config)
	if emissionsResult == nil {
		return models.Emissions{Name: "Network Emissions", Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: "Network Emissions", Value: emissionsResult}
}
func GetNetworkEmissionsPerTransaction(startDate time.Time, endDate time.Time, config cfg.EndpointService) models.Emissions {
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	emissionsResult := transaction(chainNames[3], startDatef, endDatef, config)
	if emissionsResult == nil {
		return models.Emissions{Name: "Network Emissions per Transaction", Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: "Network Emissions per Transaction", Value: emissionsResult}
}

func CarbonIntensityFactor(chain string, startDate string, endDate string, config cfg.EndpointService) []models.EmissionsResult {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/carbon-intensity-factor?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			return response
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return response
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			return response
		}

	}
	return response
}

func network(chain string, startDate string, endDate string, config cfg.EndpointService) []models.EmissionsResult {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
			return response
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			fmt.Println(err)
			return response
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			fmt.Println(err)
			return response
		}
		for i, Value := range response {
			formatValue := strconv.FormatFloat(Value.Value, 'f', 2, 64)
			parsedValue, err := strconv.ParseFloat(formatValue, 64)

			if err != nil {
				fmt.Println(err)
			}

			response[i].Value = parsedValue
		}
	}

	return response
}

func transaction(chain string, startDate string, endDate string, config cfg.EndpointService) []models.EmissionsResult {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/transaction?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
			return response
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			fmt.Println(err)
			return response
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			fmt.Println(err)
			return response
		}
		for i, Value := range response {
			formatValue := strconv.FormatFloat(Value.Value, 'f', 2, 64)
			parsedValue, err := strconv.ParseFloat(formatValue, 64)

			if err != nil {
				fmt.Println(err)
			}

			response[i].Value = parsedValue
		}
	}

	return response
}
