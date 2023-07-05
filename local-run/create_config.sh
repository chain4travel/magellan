#!/bin/bash
############################################################
# Usage                                                    #
############################################################
USAGE()
{
   # Display Help
   echo "Creates configuration file to be used to run magellan"
   echo
   echo "Usage create_config.sh -d <domain> -p <port> -t <protocol>"
   echo "options:"
   echo "-d <domain>    domain to get rpc calls from eg: columbus.camino.network or it can be IP address"
   echo "-p <port>      port for RPC calls"
   echo "-t <protocol>      http or https for api node url"
   echo "-h             Print Usage."
   echo " defaults: create_config.sh -d 127.0.0.1 -p 9650 -t http"
   echo
}

############################################################
# Default Variables                                                #
############################################################
DOMAIN="127.0.0.1"
PORT="9650"
PROTOCOL="http"


############################################################
# main                                                     #
############################################################

while getopts ":hn:d:t:p:" option; do
   case $option in
      h) # display Usage
         USAGE
         exit;;
      d) # Enter a domain
         DOMAIN=$OPTARG;;
      p) # Enter a port
         PORT=$OPTARG;;
      t) # Enter a protocol
         PROTOCOL=$OPTARG;;
     \?) # Invalid option
         echo "Error: Invalid option"
         USAGE
         exit;;
   esac
done

NETWORK_ID=$(curl -X POST --data '{
    "jsonrpc":"2.0",
    "id"     :1,
    "method" :"info.getNetworkID"
}' -H 'content-type:application/json;' $PROTOCOL://$DOMAIN:$PORT/ext/info | jq -r '.result.networkID')

CCHIAN_ID=$(curl -X POST --data '{
    "jsonrpc":"2.0",
    "id"     :1,
    "method" :"info.getBlockchainID",
    "params": {
        "alias":"C"
    }
}' -H 'content-type:application/json;' $PROTOCOL://$DOMAIN:$PORT/ext/info | jq -r '.result.blockchainID')

PCHIAN_ID=$(curl -X POST --data '{
    "jsonrpc":"2.0",
    "id"     :1,
    "method" :"info.getBlockchainID",
    "params": {
        "alias":"P"
    }
}' -H 'content-type:application/json;' $PROTOCOL://$DOMAIN:$PORT/ext/info | jq -r '.result.blockchainID')

XCHIAN_ID=$(curl -X POST --data '{
    "jsonrpc":"2.0",
    "id"     :1,
    "method" :"info.getBlockchainID",
    "params": {
        "alias":"X"
    }
}' -H 'content-type:application/json;' $PROTOCOL://$DOMAIN:$PORT/ext/info | jq -r '.result.blockchainID')


rm -rf ./config.json
sed -e 's/DOMAIN/'"$DOMAIN"'/' -e 's/PROTOCOL/'"$PROTOCOL"'/' -e 's/PORT/'"$PORT"'/' -e 's/NETWORK_ID/'"$NETWORK_ID"'/' -e 's/CCHIAN_ID/'"$CCHIAN_ID"'/' -e 's/PCHIAN_ID/'"$PCHIAN_ID"'/' -e 's/XCHIAN_ID/'"$XCHIAN_ID"'/' ./config_template.json > config.json

