#!/bin/sh
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic results
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic standardWeather
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic weatherAlerts
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic DBinfo
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic readingsPerStation
docker exec broker2 kafka-topics --bootstrap-server broker2:29093 --replication-factor 2  --create --topic readingsPerLocation

curl -X POST localhost:8083/connectors -H "Content-Type:application/json" -H "Accept:application/json" -d @config/source.json
curl -X POST localhost:8083/connectors -H "Content-Type:application/json" -H "Accept:application/json" -d @config/readings_per_location_sink.json
curl -X POST localhost:8083/connectors -H "Content-Type:application/json" -H "Accept:application/json" -d @config/readings_per_station_sink.json