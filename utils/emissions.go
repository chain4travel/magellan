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

const (
	// camino chain id used in co2-api-inmutableinsights
	columbusChainID string = "caminocolumbus"
	perYear         string = "Per Year"
	perMonth        string = "Per Month"
	perDay          string = "Per Day"
)

var countryIDs = []string{"TAIWAN", "UNITED_STATES", "GERMANY", "NETHERLANDS", "BELGIUM", "FINLAND"}

func GetDailyEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	var dailyEmissions []models.EmissionsResult
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	networkName, _ := GetNetworkName(rpc)
	chainIDs, _ := AccesibleChains(config)
	for _, chain := range chainIDs {
		intensityFactor, err := CarbonIntensityFactor(chain, startDatef, endDatef, config)
		if len(intensityFactor) > 0 && err == nil {
			dailyEmissions = append(dailyEmissions, models.EmissionsResult{
				Chain: chain,
				Value: GetAvgEmissionsValue(intensityFactor),
			})
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
	monthsBetween := int(endDate.Month() - startDate.Month())
	yearsBetween := endDate.Year() - startDate.Year()
	networkName, _ := GetNetworkName(rpc)

	emissions := models.Emissions{
		Name: fmt.Sprintf("Network Emissions %s", networkName),
	}

	emissionsResult, errEmissions := network(columbusChainID, startDatef, endDatef, config)
	if errEmissions != nil || emissionsResult == nil {
		emissions.Value = []models.EmissionsResult{}
		return emissions, errEmissions
	}

	switch {
	// if the date range is greater than or equal to one month the values are averaged per month
	case (monthsBetween >= 1 || monthsBetween < 0 || startDate.Year() == 1) && yearsBetween == 0:
		emissionsPermonth, err := GetAvgEmissionsFilter(emissionsResult, false)
		if err != nil {
			emissions.Value = []models.EmissionsResult{}
			return emissions, errEmissions
		}
		emissions.Filter = perMonth
		emissions.Value = emissionsPermonth
	// if the date range is greater than or equal to one year the values are averaged per year
	case yearsBetween > 0:
		emissionsPerYear, err := GetAvgEmissionsFilter(emissionsResult, true)
		if err != nil {
			emissions.Value = []models.EmissionsResult{}
			return emissions, errEmissions
		}
		emissions.Filter = perYear
		emissions.Value = emissionsPerYear
	default:
		emissions.Filter = perDay
		emissions.Value = emissionsResult
	}
	return emissions, nil
}

func GetCountryEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	networkName, _ := GetNetworkName(rpc)
	var (
		emissionsInfo []*models.CountryEmissionsResult
		err           error
	)
	for _, country := range countryIDs {
		countryInfo, errCountry := Country(country, startDatef, endDatef, config)
		if errCountry == nil {
			emissionsInfo = append(emissionsInfo, &models.CountryEmissionsResult{
				Country: country,
				Value:   GetAvgEmissionsValue(countryInfo),
			})
		} else {
			err = errCountry
		}
	}
	if emissionsInfo == nil {
		return models.Emissions{Name: fmt.Sprintf("Network Emissions Per Country %s", networkName), Value: []models.CountryEmissionsResult{}}, err
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions Per Country %s", networkName), Value: emissionsInfo}, nil
}
func GetNetworkEmissionsPerTransaction(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	monthsBetween := int(endDate.Month() - startDate.Month())
	yearsBetween := endDate.Year() - startDate.Year()
	networkName, _ := GetNetworkName(rpc)
	emissions := models.Emissions{
		Name: fmt.Sprintf("Network Emissions per Transaction %s", networkName),
	}
	emissionsResult, errEmissions := transaction(columbusChainID, startDatef, endDatef, config)
	if errEmissions != nil || emissionsResult == nil {
		emissions.Value = []models.EmissionsResult{}
		return emissions, errEmissions
	}
	switch {
	// if the date range is greater than or equal to one month the values are averaged per month
	case (monthsBetween >= 1 || monthsBetween < 0 || startDate.Year() == 1) && yearsBetween == 0:
		emissionsPermonth, err := GetAvgEmissionsFilter(emissionsResult, false)
		if err != nil {
			emissions.Value = []models.EmissionsResult{}
			return emissions, errEmissions
		}
		emissions.Filter = perMonth
		emissions.Value = emissionsPermonth
	// if the date range is greater than or equal to one year the values are averaged per year
	case yearsBetween > 0:
		emissionsPerYear, err := GetAvgEmissionsFilter(emissionsResult, true)
		if err != nil {
			emissions.Value = []models.EmissionsResult{}
			return emissions, errEmissions
		}
		emissions.Filter = perYear
		emissions.Value = emissionsPerYear
	default:
		emissions.Filter = perDay
		emissions.Value = emissionsResult
	}
	return emissions, nil
}

func CarbonIntensityFactor(chain string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	var response []*models.EmissionsResult
	url := fmt.Sprintf("%s/co2/carbon-intensity-factor/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
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

func network(chain string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	var response []*models.EmissionsResult
	url := fmt.Sprintf("%s/co2/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
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

func transaction(chain string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	var response []*models.EmissionsResult
	url := fmt.Sprintf("%s/co2/transaction?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
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

func Country(country string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	var response []*models.EmissionsResult
	url := fmt.Sprintf("%s/co2/carbon-intensity-factor/country?from=%s&to=%s&country=%s", config.URLEndpoint, startDate, endDate, country)
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
func AccesibleChains(config cfg.EndpointService) ([]string, error) {
	var response []string
	url := fmt.Sprintf("%s/misc/accessible_chains", config.URLEndpoint)
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
func GetAvgEmissionsValue(e []*models.EmissionsResult) float64 {
	var Total float64
	for _, value := range e {
		Total += value.Value
	}
	Avg := Total / float64(len(e))
	formatValue := strconv.FormatFloat(Avg, 'f', 2, 64)
	parsedValue, err := strconv.ParseFloat(formatValue, 64)
	if err != nil {
		return 0.0
	}
	return parsedValue
}
func GetAvgEmissionsFilter(e []*models.EmissionsResult, perYear bool) ([]*models.EmissionsResult, error) {
	var (
		Total           float64
		resultsPerMonth []*models.EmissionsResult
		count           int
		avg             float64
		temporalDate    time.Time
	)
	date, err := stringToDate(e[0].Time)
	if perYear {
		temporalDate = time.Date(date.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		temporalDate = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC)
	}

	for id, emission := range e {
		date, err = stringToDate(emission.Time)
		switch {
		case date.Month() == temporalDate.Month() && date.Year() == temporalDate.Year() && id != (len(e)-1) && !perYear:
			Total += emission.Value
			count++
		case date.Year() == temporalDate.Year() && id != (len(e)-1) && perYear:
			Total += emission.Value
			count++
		default:
			avg = Total / float64(count)
			formatValue := strconv.FormatFloat(avg, 'f', 2, 64)
			parsedValue, _ := strconv.ParseFloat(formatValue, 64)
			resultsPerMonth = append(resultsPerMonth, &models.EmissionsResult{
				Time:  strings.Split(temporalDate.String(), " ")[0],
				Chain: emission.Chain,
				Value: parsedValue,
			})
			Total = emission.Value
			count = 1
			if perYear {
				temporalDate = time.Date(date.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
			} else {
				temporalDate = time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, time.UTC)
			}
		}
	}

	return resultsPerMonth, err
}
func stringToDate(date string) (time.Time, error) {
	// parse a string "YYYY-MM-DD" e.g. "2022-12-25" to time.Time using timeLayout "2006-01-02" predefined in
	// go Time package
	t, err := time.Parse("2006-01-02", date)
	return t, err
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
