package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
)

func PeerIndex(peers *models.PeersResponse, nodeID string) int {
	for idx, peer := range peers.Result.Peers {
		if peer.NodeID == nodeID {
			return idx
		}
	}
	return -1
}
func GetDate(unixTime string) (string, error) {
	// Date in Unix Format
	unixDateInt, err := strconv.ParseInt(unixTime, 10, 64)
	if err != nil {
		return "", err
	}
	// UnixDate in Time format struct
	dateFTime := time.Unix(unixDateInt, 0)
	return strings.Split(dateFTime.String(), " -")[0], nil
}

func getDuration(startTime string, endTime string) (string, error) {
	const d = " Days"
	unixDateInt, err := strconv.ParseInt(startTime, 10, 64)
	if err != nil {
		return "", err
	}
	start := time.Unix(unixDateInt, 0)
	unixDateInt, err = strconv.ParseInt(endTime, 10, 64)
	if err != nil {
		return "- " + d, err
	}
	end := time.Unix(unixDateInt, 0)
	if err != nil {
		return "- " + d, err
	}
	difference := end.Sub(start)
	duration := int(difference.Hours() / 24)
	return strconv.Itoa(duration) + d, nil
}

func GetValidatorsGeoIPInfo(rpc string, geoIPConfig *cfg.EndpointService) (models.GeoIPValidators, error) {
	var validatorList []*models.Validator
	geoValidatorsInfo := &models.GeoIPValidators{
		Name: "GeoIPInfo",
	}
	var errGeoIP error
	validators, err := GetCurrentValidators(rpc)
	if err != nil {
		geoValidatorsInfo.Value = []*models.Validator{}
		return *geoValidatorsInfo, err
	}
	peers, _ := GetPeers(rpc)
	if !reflect.DeepEqual(validators, models.ValidatorsResponse{}) && !reflect.DeepEqual(peers, models.PeersResponse{}) &&
		len(validators.Result.Validators) > 0 {
		for i := 0; i < len(validators.Result.Validators); i++ {
			validatorGeoIPInfo := setValidatorInfo(&validators.Result.Validators[i])
			indexPeerWithSameID := PeerIndex(&peers, validators.Result.Validators[i].NodeID)
			if indexPeerWithSameID >= 0 {
				errGeoIP = setGeoIPInfo(validatorGeoIPInfo, peers.Result.Peers[indexPeerWithSameID].IP, geoIPConfig)
			}
			validatorList = append(validatorList, validatorGeoIPInfo)
		}
		geoValidatorsInfo.Value = validatorList
	} else {
		geoValidatorsInfo.Value = []*models.Validator{}
	}

	if errGeoIP != nil {
		return *geoValidatorsInfo, errGeoIP
	}
	return *geoValidatorsInfo, nil
}

func setValidatorInfo(validator *models.ValidatorInfo) *models.Validator {
	startTime, _ := GetDate(validator.StartTime)
	endTime, _ := GetDate(validator.EndTime)
	duration, _ := getDuration(validator.StartTime, validator.EndTime)
	return &models.Validator{
		NodeID:    validator.NodeID,
		TxID:      validator.TxID,
		Connected: validator.Connected,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  duration,
		Uptime:    validator.Uptime,
	}
}

func setGeoIPInfo(validatorInfo *models.Validator, peerIP string, config *cfg.EndpointService) error {
	geoIPInfo, err := GetLocationByIP(peerIP, config)
	validatorInfo.IP = strings.Split(peerIP, ":")[0]
	validatorInfo.Country = geoIPInfo.Country
	validatorInfo.Lng = geoIPInfo.Lon
	validatorInfo.Lat = geoIPInfo.Lat
	validatorInfo.CountryISO = geoIPInfo.CountryCode
	validatorInfo.City = geoIPInfo.City
	return err
}

func GetCurrentValidators(rpc string) (models.ValidatorsResponse, error) {
	var response models.ValidatorsResponse
	url := fmt.Sprintf("%s/ext/bc/P", rpc)
	payloadStruct := &models.BodyRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "platform.getCurrentValidators",
		Params: models.CurrentValidatorsParams{
			//SubnetID: nil,
			NodeIDs: []string{},
		},
	}
	payloadJSON, err := json.Marshal(payloadStruct)
	if err != nil {
		return response, err
	}

	payload := strings.NewReader(string(payloadJSON))

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return response, err
	}

	req.Header.Add("Content-Type", "application/json")

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

func GetPeers(rpc string) (models.PeersResponse, error) {
	var response models.PeersResponse
	url := fmt.Sprintf("%s/ext/info", rpc)
	payloadStruct := &models.BodyRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "info.peers",
		Params: models.PeersParams{
			NodeIDs: []string{},
		},
	}
	payloadJSON, err := json.Marshal(payloadStruct)
	if err != nil {
		return response, err
	}

	payload := strings.NewReader(string(payloadJSON))
	client := &http.Client{}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		return response, err
	}
	req.Header.Add("Content-Type", "application/json")

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

func GetLocationByIP(ip string, config *cfg.EndpointService) (models.IPAPIResponse, error) {
	var response models.IPAPIResponse
	ip = strings.Split(ip, ":")[0]
	url := fmt.Sprintf("%s%s", config.URLEndpoint, ip)
	// Perform the HTTP GET request
	client := &http.Client{}

	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return response, err
	}
	defer res.Body.Close()

	// Read the response body
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
