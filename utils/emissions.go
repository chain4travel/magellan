package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/chain4travel/magellan/cfg"
)

// TODO: change the variable to constant
var chainNames = [19]string{"algorand", "avalanche", "bitcoin", "caminocolumbus", "cardano", "cosmos", "kava", "polkadot", "solana", "tezos", "tron"}

func GetDailyEmissios(startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	var dailyEmissions []cfg.Emissions
	for _, chain := range chainNames {
		intensityFactor := CarbonIntensityFactor(chain, startDate, endDate, config)
		if len(intensityFactor) > 0 {
			dailyEmissions = append(dailyEmissions, intensityFactor[0])
		}
	}
	return dailyEmissions
}

func GetNetworkEmissions(startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	return network(chainNames[3], startDate, endDate, config)
}
func GetNetworkEmissionsPerTransaction(startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	return transaction(chainNames[3], startDate, endDate, config)
}

func CarbonIntensityFactor(chain string, startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	var response []cfg.Emissions
	url := fmt.Sprintf("%s/carbon-intensity-factor?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	req.Header.Add("Authorization", config.AutorizationToken)

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
	return response
}

func network(chain string, startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	var response []cfg.Emissions
	url := fmt.Sprintf("%s/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	req.Header.Add("Authorization", config.AutorizationToken)

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
	return response
}

func transaction(chain string, startDate string, endDate string, config cfg.EndpointService) []cfg.Emissions {
	var response []cfg.Emissions
	url := fmt.Sprintf("%s/transaction?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	req.Header.Add("Authorization", config.AutorizationToken)

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
	return response
}
