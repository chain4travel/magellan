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
)

func PeerIndex(peers *cfg.PeersResponse, nodeID string) int {
	for idx, peer := range peers.Result.Peers {
		if peer.NodeID == nodeID {
			return idx
		}
	}
	return -1
}
func GetDate(unixTime string) string {
	// Date in Unix Format
	unixDateInt, _ := strconv.ParseInt(unixTime, 10, 64)
	// UnixDate in Time format struct
	dateFTime := time.Unix(unixDateInt, 0)
	return strings.Split(dateFTime.String(), " -")[0]
}
func getDuration(startTime string, endTime string) string {
	start, _ := time.Parse("2006-01-02 15:04:05", startTime)
	end, _ := time.Parse("2006-01-02 15:04:05", endTime)
	difference := end.Sub(start)
	duration := int(difference.Hours() / 24)
	return strconv.Itoa(duration) + " Days"
}
func GetValidatorsGeoIPInfo(rpc string, geoIPConfig cfg.EndpointService) cfg.GeoIPValidators {
	var validatorList []cfg.Validator
	validators := GetCurrentValidators(rpc)
	peers := GetPeers(rpc)
	for i := 0; i < len(validators.Result.Validators); i++ {
		validator := validators.Result.Validators[i]
		indexPeerWithSameID := PeerIndex(&peers, validator.NodeID)
		if indexPeerWithSameID >= 0 {
			validatorList = append(validatorList, SetValidatorInfo(&validators.Result.Validators[i], &peers.Result.Peers[indexPeerWithSameID], true, geoIPConfig))
		} else {
			validatorList = append(validatorList, SetValidatorInfo(&validators.Result.Validators[i], nil, false, geoIPConfig))
		}
	}
	geoValidatorsInfo := cfg.GeoIPValidators{
		Name:  "GeoIPInfo",
		Value: validatorList,
	}
	return geoValidatorsInfo
}

func SetValidatorInfo(validator *cfg.ValidatorInfo, peer *cfg.PeerInfo, peerFlag bool, config cfg.EndpointService) cfg.Validator {
	startTime := GetDate(validator.StartTime)
	endTime := GetDate(validator.EndTime)
	var info cfg.Validator
	if peerFlag {
		geoIPInfo := GetLocationByIP(peer.IP, config)
		info = cfg.Validator{
			NodeID:     validator.NodeID,
			IP:         peer.IP,
			TxID:       validator.TxID,
			Connected:  validator.Connected,
			StartTime:  startTime,
			EndTime:    endTime,
			Duration:   getDuration(startTime, endTime),
			Uptime:     validator.Uptime,
			Country:    geoIPInfo.Country,
			Lng:        geoIPInfo.Lon,
			Lat:        geoIPInfo.Lat,
			CountryISO: geoIPInfo.CountryCode,
			City:       geoIPInfo.City,
		}
	} else {
		info = cfg.Validator{
			NodeID:     validator.NodeID,
			IP:         "",
			TxID:       validator.TxID,
			Connected:  validator.Connected,
			StartTime:  startTime,
			EndTime:    endTime,
			Duration:   getDuration(startTime, endTime),
			Uptime:     validator.Uptime,
			Country:    "",
			Lng:        0.0,
			Lat:        0.0,
			CountryISO: "",
			City:       "",
		}
	}
	return info
}
func GetCurrentValidators(rpc string) cfg.ValidatorsResponse {
	var response cfg.ValidatorsResponse
	url := fmt.Sprintf("%s/ext/bc/P", rpc)

	payload := strings.NewReader(`{
		"jsonrpc": "2.0",
		"method": "platform.getCurrentValidators",
		"params": {
			"subnetID":null,
			"nodeIDs":[]
		},
		"id": 1
	}`)

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		fmt.Println(err)
		return response
	}

	req.Header.Add("Content-Type", "application/json")

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

func GetPeers(rpc string) cfg.PeersResponse {
	var response cfg.PeersResponse
	url := fmt.Sprintf("%s/ext/info", rpc)

	payload := strings.NewReader(`{
		"jsonrpc":"2.0",
		"id"     :1,
		"method" :"info.peers",
		"params" :{
			"nodeIDs": []
		}
	}`)

	client := &http.Client{}

	req, err := http.NewRequest("POST", url, payload)

	if err != nil {
		fmt.Println(err)
		return response
	}
	req.Header.Add("Content-Type", "application/json")

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
func GetLocationByIP(ip string, config cfg.EndpointService) cfg.IPAPIResponse {
	var response cfg.IPAPIResponse
	ip = strings.Split(ip, ":")[0]
	url := fmt.Sprintf("%s%s", config.URLEndpoint, ip)
	// Perform the HTTP GET request
	client := &http.Client{}

	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		fmt.Println(err)
		return response
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return response
	}
	defer res.Body.Close()

	// Read the response body
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
