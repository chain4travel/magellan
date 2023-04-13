package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ava-labs/avalanchego/api/info"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
)

const (
	perYear         string = "Per Year"
	perMonth        string = "Per Month"
	perDay          string = "Per Day"
	createReqErrMsg string = "Fail to create the request"
	sendReqErrMsg   string = "Fail to send the request"
	reqBodyErrMsg   string = "Fail to read request body"
	jsonErrMsg      string = "Fail to json unmarshal in struct"
)

var countryIDs = []string{"UNITED_STATES", "GERMANY", "UNITED_KINGDOM", "AUSTRALIA", "SINGAPORE", "JAPAN", "ICELAND", "NORWAY", "CHINA", "SWEDEN"}

func GetDailyEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) models.Emissions {
	dailyEmissions := []models.EmissionsResult{}
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	chainIDs, _ := accessibleChains(config)
	for _, chain := range chainIDs {
		networkEmissions, err := network(chain, startDatef, endDatef, config)
		if len(networkEmissions) > 0 && err == nil {
			dailyEmissions = append(dailyEmissions, models.EmissionsResult{
				Chain: chain,
				Value: getAvgEmissionsValue(networkEmissions),
			})
		}
	}
	return models.Emissions{Name: "Daily emissions", Value: dailyEmissions}
}

func GetNetworkEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (*models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	infoClient := info.NewClient(rpc)
	networkName, _ := infoClient.GetNetworkName(context.Background())
	emissionsResult := []*models.EmissionsResult{}
	emissions := &models.Emissions{
		Name:  "Network Emissions",
		Value: emissionsResult,
	}
	NetworkInmutable, err := getNetworkNameInmutable(networkName, config)
	if NetworkInmutable != "" {
		emissionsResult, err = network(NetworkInmutable, startDatef, endDatef, config)
		if err != nil || emissionsResult == nil {
			return emissions, err
		}
		err = getEmissionsResults(emissions, emissionsResult, endDate, startDate)
	}
	return emissions, err
}

func GetCountryEmissions(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string, logger logging.Logger) (models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	infoClient := info.NewClient(rpc)
	networkName, _ := infoClient.GetNetworkName(context.Background())
	networkInmutable, _ := getNetworkNameInmutable(networkName, config)
	emissionsInfo := []*models.CountryEmissionsResult{}
	networkInfo, err := carbonIntensityFactor(networkInmutable, startDatef, endDatef, config, logger)
	if err == nil {
		emissionsInfo = append(emissionsInfo, &models.CountryEmissionsResult{
			Country: networkName,
			Value:   getAvgEmissionsValue(networkInfo),
		})
	}
	for _, countryID := range countryIDs {
		countryInfo, err := country(countryID, startDatef, endDatef, config, logger)
		if err == nil {
			emissionsInfo = append(emissionsInfo, &models.CountryEmissionsResult{
				Country: countryID,
				Value:   getAvgEmissionsValue(countryInfo),
			})
		}
	}
	return models.Emissions{Name: fmt.Sprintf("Network Emissions Per Country %s", networkName), Value: emissionsInfo}, nil
}
func GetNetworkEmissionsPerTransaction(startDate time.Time, endDate time.Time, config cfg.EndpointService, rpc string) (*models.Emissions, error) {
	startDatef := strings.Split(startDate.String(), " ")[0]
	endDatef := strings.Split(endDate.String(), " ")[0]
	infoClient := info.NewClient(rpc)
	networkName, _ := infoClient.GetNetworkName(context.Background())
	emissionsResult := []*models.EmissionsResult{}
	emissions := &models.Emissions{
		Name:  "Network Emissions Per Transaction",
		Value: emissionsResult,
	}
	NetworkInmutable, err := getNetworkNameInmutable(networkName, config)
	if NetworkInmutable != "" {
		emissionsResult, err = transaction(NetworkInmutable, startDatef, endDatef, config)
		if err != nil || emissionsResult == nil {
			return emissions, err
		}
		err = getEmissionsResults(emissions, emissionsResult, endDate, startDate)
	}
	return emissions, err
}

func network(chain string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	response := []*models.EmissionsResult{}
	url := fmt.Sprintf("%s/co2/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
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
	return response, nil
}

func transaction(chain string, startDate string, endDate string, config cfg.EndpointService) ([]*models.EmissionsResult, error) {
	response := []*models.EmissionsResult{}
	url := fmt.Sprintf("%s/co2/transaction?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
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
	return response, nil
}

func country(country string, startDate string, endDate string, config cfg.EndpointService, logger logging.Logger) ([]*models.EmissionsResult, error) {
	response := []*models.EmissionsResult{}
	url := fmt.Sprintf("%s/co2/carbon-intensity-factor/country?from=%s&to=%s&country=%s", config.URLEndpoint, startDate, endDate, country)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		logger.Warn(createReqErrMsg + " in country intensity factor: " + err.Error())
		return response, err
	}

	req.Header.Add("Authorization", config.AuthorizationToken)

	res, err := client.Do(req)
	if err != nil {
		logger.Warn(sendReqErrMsg + " in country intensity factor: " + err.Error())
		return response, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		logger.Warn(reqBodyErrMsg + " in country intensity factor: " + err.Error())
		return response, err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		logger.Warn(jsonErrMsg + " in country intensity factor: " + err.Error())
		return response, err
	}

	return response, nil
}
func accessibleChains(config cfg.EndpointService) ([]string, error) {
	response := []string{}
	url := fmt.Sprintf("%s/misc/accessible_chains", config.URLEndpoint)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return response, err
	}
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
	return response, nil
}
func getAvgEmissionsValue(e []*models.EmissionsResult) float64 {
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
func getAvgEmissionsFilter(e []*models.EmissionsResult, perYear bool) ([]*models.EmissionsResult, error) {
	var (
		Total        float64
		count        int
		avg          float64
		temporalDate time.Time
	)
	resultsPerMonth := []*models.EmissionsResult{}
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
	t, err := time.Parse(strings.Split(time.RFC3339, "T")[0], date)
	return t, err
}

func getNetworkNameInmutable(networkName string, config cfg.EndpointService) (string, error) {
	chainIDs, err := accessibleChains(config)
	for _, chain := range chainIDs {
		if strings.Contains(strings.ToLower(chain), strings.ToLower(networkName)) {
			return chain, err
		}
	}
	return "", err
}

func getEmissionsResults(emissions *models.Emissions, emissionsResult []*models.EmissionsResult, endDate time.Time, startDate time.Time) error {
	monthsBetween := int(endDate.Month() - startDate.Month())
	yearsBetween := endDate.Year() - startDate.Year()
	switch {
	// if the date range is greater than or equal to one month the values are averaged per month
	case (monthsBetween >= 1 || monthsBetween < 0 || startDate.Year() == 1) && yearsBetween == 0:
		emissionsPermonth, err := getAvgEmissionsFilter(emissionsResult, false)
		if err != nil {
			return err
		}
		emissions.Filter = perMonth
		emissions.Value = emissionsPermonth
	// if the date range is greater than or equal to one year the values are averaged per year
	case yearsBetween > 0:
		emissionsPerYear, err := getAvgEmissionsFilter(emissionsResult, true)
		if err != nil {
			return err
		}
		emissions.Filter = perYear
		emissions.Value = emissionsPerYear
	default:
		emissions.Filter = perDay
		emissions.Value = emissionsResult
	}
	return nil
}

func carbonIntensityFactor(chain string, startDate string, endDate string, config cfg.EndpointService, logger logging.Logger) ([]*models.EmissionsResult, error) {
	response := []*models.EmissionsResult{}
	url := fmt.Sprintf("%s/co2/carbon-intensity-factor/network?chain=%s&from=%s&to=%s", config.URLEndpoint, chain, startDate, endDate)
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		logger.Warn(createReqErrMsg + " in carbon intensity factor: " + err.Error())
		return response, err
	}
	req.Header.Add("Authorization", config.AuthorizationToken)

	res, err := client.Do(req)
	if err != nil {
		logger.Warn(sendReqErrMsg + " in carbon intensity factor: " + err.Error())
		return response, err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		logger.Warn(reqBodyErrMsg + " in carbon intensity factor: " + err.Error())
		return response, err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		logger.Warn(jsonErrMsg + " in carbon intensity factor: " + err.Error())
		return response, err
	}

	return response, nil
}
