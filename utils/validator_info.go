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
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/models"
)

func PeerIndex(peers []info.Peer, nodeID ids.NodeID) int {
	for idx, peer := range peers {
		if peer.ID == nodeID {
			return idx
		}
	}
	return -1
}
func GetDate(unixTime uint64) (string, error) {
	// Date in Unix Format
	timestamp := int64(unixTime)
	// UnixDate in Time format struct
	dateFTime := time.Unix(timestamp, 0)
	return strings.Split(dateFTime.String(), " -")[0], nil
}

func getDuration(startTime uint64, endTime uint64) string {
	const d = " Days"
	timestamp := int64(startTime)
	start := time.Unix(timestamp, 0)
	timestamp = int64(endTime)
	end := time.Unix(timestamp, 0)
	difference := end.Sub(start)
	duration := int(difference.Hours() / 24)
	return strconv.Itoa(duration) + d
}

func GetValidatorsGeoIPInfo(rpc string, geoIPConfig *cfg.EndpointService, log logging.Logger) (models.GeoIPValidators, error) {
	validatorList := []*models.Validator{}
	geoValidatorsInfo := &models.GeoIPValidators{
		Name: "GeoIPInfo",
	}
	pvmClient := platformvm.NewClient(rpc)
	infoClient := info.NewClient(rpc)
	validators, err := pvmClient.GetCurrentValidators(context.Background(), ids.ID{}, []ids.NodeID{})
	if err != nil {
		geoValidatorsInfo.Value = validatorList
		return *geoValidatorsInfo, err
	}
	peers, _ := infoClient.Peers(context.Background())
	for _, validator := range validators {
		validatorGeoIPInfo := setValidatorInfo(validator)
		indexPeerWithSameID := PeerIndex(peers, validator.NodeID)
		if indexPeerWithSameID >= 0 {
			err = setGeoIPInfo(validatorGeoIPInfo, peers[indexPeerWithSameID].IP, geoIPConfig)
			if err != nil {
				log.Error(err.Error())
			}
		}
		validatorList = append(validatorList, validatorGeoIPInfo)
	}
	geoValidatorsInfo.Value = validatorList

	return *geoValidatorsInfo, nil
}

func setValidatorInfo(validator platformvm.ClientPermissionlessValidator) *models.Validator {
	startTime, _ := GetDate(validator.StartTime)
	endTime, _ := GetDate(validator.EndTime)
	duration := getDuration(validator.StartTime, validator.EndTime)
	return &models.Validator{
		NodeID:    validator.NodeID,
		TxID:      validator.TxID,
		Connected: *validator.Connected,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  duration,
		Uptime:    *validator.Uptime,
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

func GetLocationByIP(ip string, config *cfg.EndpointService) (models.IPAPIResponse, error) {
	var response models.IPAPIResponse
	ip = strings.Split(ip, ":")[0]
	url := fmt.Sprintf("%s%s?key=%s", config.URLEndpoint, ip, config.AuthorizationToken)
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
