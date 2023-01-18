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

// constant array with the chain ids in used in co2-api-inmutableinsights - if any chain id is missing just add them to the array
var chainNames = []string{"algorand", "avalanche", "bitcoin", "caminocolumbus", "cardano", "cosmos", "ethereum", "kava", "polkadot", "solana", "tezos", "tron"}
var columbusChainID string = chainNames[3]

func GetDailyEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	var dailyEmissions []models.EmissionsResult
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	networkName, _ := GetNetworkName(rpc)
	for _, chain := range chainNames {
		intensityFactor, err := CarbonIntensityFactor(chain, startDatef, endDatef, config)
		if len(intensityFactor) > 0 && err == nil {
			dailyEmissions = append(dailyEmissions, intensityFactor...)
		}
	}
	if dailyEmissions == nil {
		return models.Emissions{Name: fmt.Sprintf("Daily emissions %s", networkName), Value: []models.EmissionsResult{}}
	}
	return models.Emissions{Name: fmt.Sprintf("Daily emissions %s", networkName), Value: dailyEmissions}
}

func GetNetworkEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	networkName, _ := GetNetworkName(rpc)
	emissionsResult, errEmissions := network(columbusChainID, startDatef, endDatef, config)
	if errEmissions != nil {
		return models.Emissions{Name: fmt.Sprintf("Network Emissions %s", networkName), Value: []models.EmissionsResult{}}, errEmissions
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions %s", networkName), Value: emissionsResult}, nil
}

func GetNetworkEmissionsPerTransaction(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	networkName, _ := GetNetworkName(rpc)
	emissionsResult, errEmissions := transaction(columbusChainID, startDatef, endDatef, config)
	if errEmissions != nil {
		emissions := models.Emissions{Name: fmt.Sprintf("Network Emissions per Transaction %s", networkName), Value: []models.EmissionsResult{}}
		return emissions, errEmissions
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions per Transaction %s", networkName), Value: emissionsResult}, nil
}

func CarbonIntensityFactor(chain string, startDate string, endDate string, config cfg.EndpointService) ([]models.EmissionsResult, error) {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/carbon-intensity-factor?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			return response, err
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return response, err
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			return response, err
		}
	}
	return response, nil
}

func network(chain string, startDate string, endDate string, config cfg.EndpointService) ([]models.EmissionsResult, error) {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			return response, err
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return response, err
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			return response, err
		}

		for i, Value := range response {
			formatValue := strconv.FormatFloat(Value.Value, 'f', 2, 64)
			parsedValue, err := strconv.ParseFloat(formatValue, 64)

			if err != nil {
				continue
			}

			response[i].Value = parsedValue
		}
	}

	return response, nil
}

func transaction(chain string, startDate string, endDate string, config cfg.EndpointService) ([]models.EmissionsResult, error) {
	var response []models.EmissionsResult
	url := fmt.Sprintf("%s/transaction?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
	if config.AuthorizationToken != "" {
		req.Header.Add("Authorization", config.AuthorizationToken)

		res, err := client.Do(req)
		if err != nil {
			return response, err
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return response, err
		}
		err = json.Unmarshal(body, &response)
		if err != nil {
			return response, err
		}
		for i, Value := range response {
			formatValue := strconv.FormatFloat(Value.Value, 'f', 2, 64)
			parsedValue, err := strconv.ParseFloat(formatValue, 64)

			if err != nil {
				continue
			}

			response[i].Value = parsedValue
		}
	}

	return response, nil
}
func GetNetworkName(rpc string) (string, error) {
	var response models.NetworkNameResponse
	url := fmt.Sprintf("%s/ext/info", rpc)
	payloadStruct := &models.BodyRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "info.getNetworkName",
		Params:  struct{}{},
	}
	payloadJSON, err := json.Marshal(payloadStruct)
	if err != nil {
		return "", err
	}
	payload := strings.NewReader(string(payloadJSON))

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return "", err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", err
	}
	return response.Result.NetworkName, nil
}
