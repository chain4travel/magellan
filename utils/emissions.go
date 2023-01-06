package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
)

// TODO: change the variable to constant
var chainNames = [19]string{"algorand", "avalanche", "bitcoin", "caminocolumbus", "cardano", "cosmos", "ethereum", "kava", "polkadot", "solana", "tezos", "tron"}

func GetDailyEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	var dailyEmissions []models.EmissionsResult
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	networkName := GetNetworkName(rpc)
	for _, chain := range chainNames {
		intensityFactor := CarbonIntensityFactor(chain, startDatef, endDatef, config)
		if len(intensityFactor) > 0 {
			dailyEmissions = append(dailyEmissions, intensityFactor[0])
		}
	}
	if dailyEmissions == nil {
		return models.Emissions{Name: fmt.Sprintf("Daily emissions %s", networkName), Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: fmt.Sprintf("Daily emissions %s", networkName), Value: dailyEmissions}
}

func GetNetworkEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	networkName := GetNetworkName(rpc)
	emissionsResult := network(chainNames[3], startDatef, endDatef, config)
	if emissionsResult == nil {
		return models.Emissions{Name: fmt.Sprintf("Network Emissions %s", networkName), Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions %s", networkName), Value: emissionsResult}
}

func GetNetworkEmissionsPerTransaction(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	startDatef := startDate.Format("2006-01-02")
	endDatef := endDate.Format("2006-01-02")
	networkName := GetNetworkName(rpc)
	emissionsResult := transaction(chainNames[3], startDatef, endDatef, config)
	if emissionsResult == nil {
		return models.Emissions{Name: fmt.Sprintf("Network Emissions per Transaction %s", networkName), Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions per Transaction %s", networkName), Value: emissionsResult}
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
func GetNetworkName(rpc string) string {
	var response models.NetworkNameResponse
	url := fmt.Sprintf("%s/ext/info", rpc)
	payload := strings.NewReader(`{
    "jsonrpc":"2.0",
    "id"     :1,
    "method" :"info.getNetworkName",
    "params" :{
    }
	}`)

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		fmt.Println(err)
		return ""
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return response.Result.NetworkName
}
