package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type InfluxJsonLnsDown struct {
	Name      string        `json:"name"`
	Fields    LnsDownFields `json:"fields"`
	Tags      LnsDownTags   `json:"tags"`
	Timestamp uint64        `json:"timestamp"`
}
type LnsDownFields struct {
	Data      string `json:"data"`
	Confirmed bool   `json:"confirmed"`
	FPort     uint64 `json:"fPort"`
}
type LnsDownTags struct {
	DeviceId   string `json:"deviceId"`
	DeviceType string `json:"deviceType"`
	Direction  string `json:"direction"`
	Host       string `json:"host"`
	Origin     string `json:"origin"`
	Reference  string `json:"reference"`
}

// type LnsDown struct {
// 	Measurement string `json:"measurement"`
// 	Reference   string `json:"reference"`
// 	DeviceId    string `json:"deviceId"`
// 	Confirmed   bool   `json:"confirmed"`
// 	FPort       uint64 `json:"fPort"`
// 	Data        string `json:"data"`
// 	Timestamp   uint64 `json:"timestamp"`
// 	// Object any
// }

type LnsChirpstackV4Down struct {
	DeviceId  string
	Confirmed bool
	FPort     uint64
	Data      string
	// Object any
}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {
	id := uuid.New().String()

	var influxJsonLnsDown InfluxJsonLnsDown
	var lnsChirpstackV4Down LnsChirpstackV4Down

	var sbMqttSubClientId strings.Builder
	var sbMqttPubClientId strings.Builder
	var sbPubTopic strings.Builder
	sbMqttSubClientId.WriteString("mqtt-rewrite-to-lns-chirpstackv4-")
	sbMqttSubClientId.WriteString(id)
	sbMqttPubClientId.WriteString("mqtt-rewrite-to-lns-chirpstackv4-")
	sbMqttPubClientId.WriteString(id)

	mqttSubBroker := "mqtt://mqtt.maua.br:1883"
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := ""
	mqttSubPassword := ""
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	mqttSubTopics := map[string]byte{
		"IMT/LNS/+/+/down/chirpstackv4": byte(mqttSubQos), // DET
		// cf909d7a-a970-4473-9ef3-2c0618e1eb63 // Emma
		// 33d3fb39-c249-4f8d-b105-2706af00bf5c // EnergyMeter
		// 3522bea0-3ecc-43dd-a676-824d2efc9a5a // GaugePressure
		// c31059f6-2a9a-49c5-80e5-c912829f433a // Hydrometer
		// 815dcf59-4f43-45c7-9b0a-e264db48232d // MauaSat
		// 4ae0c733-e9b5-482f-8542-3d08f8e6d077 // SmartLight
		// 25e85005-adc9-48d6-89e1-b4f677cf18ef // WaterTankLevel
		// a7d603f2-3de4-4516-82f5-3323a3a80467 // WeatherStation
	}

	mqttPubBroker := "mqtt://networkserver2.maua.br:1883"
	mqttPubClientId := sbMqttPubClientId.String()
	mqttPubUser := ""
	mqttPubPassword := ""
	mqttPubQos := 0

	mqttPubOpts := MQTT.NewClientOptions()
	mqttPubOpts.AddBroker(mqttPubBroker)
	mqttPubOpts.SetClientID(mqttPubClientId)
	mqttPubOpts.SetUsername(mqttPubUser)
	mqttPubOpts.SetPassword(mqttPubPassword)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttSubClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	pClient := MQTT.NewClient(mqttPubOpts)
	if token := pClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttPubBroker)
	}

	if token := mqttSubClient.SubscribeMultiple(mqttSubTopics, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for {
		incoming := <-c
		t := strings.Split(incoming[0], "/")

		var chirpstackV4ApplicationId string
		switch t[2] {
		case "DET":
			chirpstackV4ApplicationId = "deb35cab-8a9a-42a9-b19e-0cd2ac859cc8"

		case "Emma":
			chirpstackV4ApplicationId = "cf909d7a-a970-4473-9ef3-2c0618e1eb63"

		case "EnergyMeter":
			chirpstackV4ApplicationId = "33d3fb39-c249-4f8d-b105-2706af00bf5c"

		case "GaugePressure":
			chirpstackV4ApplicationId = "3522bea0-3ecc-43dd-a676-824d2efc9a5a"

		case "Hydrometer":
			chirpstackV4ApplicationId = "c31059f6-2a9a-49c5-80e5-c912829f433a"

		case "MauaSat":
			chirpstackV4ApplicationId = "815dcf59-4f43-45c7-9b0a-e264db48232d"

		case "SmartLight":
			chirpstackV4ApplicationId = "4ae0c733-e9b5-482f-8542-3d08f8e6d077"

		case "WaterTankLevel":
			chirpstackV4ApplicationId = "25e85005-adc9-48d6-89e1-b4f677cf18ef"

		case "WeatherStation":
			chirpstackV4ApplicationId = "a7d603f2-3de4-4516-82f5-3323a3a80467"
		}

		json.Unmarshal([]byte(incoming[1]), &influxJsonLnsDown)
		lnsChirpstackV4Down.DeviceId = influxJsonLnsDown.Tags.DeviceId
		lnsChirpstackV4Down.Confirmed = influxJsonLnsDown.Fields.Confirmed
		// a := influxJsonLnsDown.Fields
		lnsChirpstackV4Down.FPort = influxJsonLnsDown.Fields.FPort
		lnsChirpstackV4Down.Data = influxJsonLnsDown.Fields.Data
		// fmt.Printf("RECEIVED MESSAGE DeviceId: %s\n", lnsChirpstackV4Down.DeviceId)
		// fmt.Printf("RECEIVED MESSAGE Confirmed: %v\n", influxJsonLnsDown.Fields.Confirmed)
		// fmt.Printf("RECEIVED MESSAGE FPort: %s\n", lnsChirpstackV4Down.FPort)
		// fmt.Printf("RECEIVED MESSAGE Data: %s\n", lnsChirpstackV4Down.Data)
		fmt.Printf("RECEIVED MESSAGE RAW: %s\n", incoming[1])
		// fmt.Printf("a: %s\n", a)

		var sbPubMessage strings.Builder
		sbPubMessage.WriteString(`{`)
		sbPubMessage.WriteString(`devEui:`)
		sbPubMessage.WriteString(lnsChirpstackV4Down.DeviceId)
		sbPubMessage.WriteString(`,confirmed:`)
		sbPubMessage.WriteString(strconv.FormatBool(lnsChirpstackV4Down.Confirmed))
		sbPubMessage.WriteString(`,fPort:`)
		sbPubMessage.WriteString(strconv.FormatUint(uint64(lnsChirpstackV4Down.FPort), 10))
		sbPubMessage.WriteString(`,data:"`)
		sbPubMessage.WriteString(lnsChirpstackV4Down.Data)
		sbPubMessage.WriteString(`"}`)

		deviceId := t[3]
		sbPubTopic.Reset()
		sbPubTopic.WriteString("application/")
		sbPubTopic.WriteString(chirpstackV4ApplicationId)
		sbPubTopic.WriteString("/device/")
		sbPubTopic.WriteString(deviceId)
		sbPubTopic.WriteString("/command/down")
		// fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		fmt.Printf("Pub NS2: %s\n", sbPubMessage.String())

		token := pClient.Publish(sbPubTopic.String(), byte(mqttPubQos), false, sbPubMessage.String())
		token.Wait()
	}
}
