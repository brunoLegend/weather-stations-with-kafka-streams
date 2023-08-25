package pt.uc.is;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import pt.uc.is.dto.*;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsApp {
    static String standardTopic = "standardWeather";
    static String alertTopic = "weatherAlerts";
    static String resultsTopic = "results";
    static String readingsPerStationTopic = "readingsPerStation";
    static String readingsPerLocationTopic = "readingsPerLocation";

    static KStream<String, String> standardKStream;
    static KStream<String, String> alertKStream;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        standardKStream = builder.stream(standardTopic);
        alertKStream = builder.stream(alertTopic);

        // ex 1 -- Count temperature readings of standard weather events per weather station.
        countReadingsPerStation();

        // ex 2 -- Count temperature readings of standard weather events per location.
        countReadingsPerLocation();

        // ex 3 -- Get minimum and maximum temperature per weather station.
        minMaxTempPerStation();

        // ex4 -- Get minimum and maximum temperature per location (Students should compute these values in Fahrenheit).
        minMaxTempPerLocation();

        // ex5 -- Count the total number of alerts per weather station.
        totalAlertsPerStation();

        // ex6 -- Count the total alerts per type.
        totalAlertsPerType();

        // ex7 -- Get minimum temperature of weather stations with red alert events.
        minTempStationsWithRedEvents();

        // ex8 -- Get maximum temperature of each location of alert events for the last hour
        // (students are allowed to define a different value for the time window).
        maxTempLocationWithAlertEventsLastHour();

        // ex9 -- Get minimum temperature per weather station in red alert zones.
        minTempStationsInRedAlerts();

        // ex10 -- Get the average temperature per weather station.
        avgTempPerStation();

        // ex11 -- Get the average temperature of weather stations with red alert events for the last hour
        // (students are allowed to define a different value for the time window).
        avgTempStationsWithRedAlertsLastHour();


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }


    // ===================================== ex 1 ====================================== //
    public static void countReadingsPerStation() {
        KTable<String, Long> countEvents = standardKStream
                .peek(((key, value) -> System.out.println("Ex1: " + "Recebido key:" + key + " value:" + value)))
                .groupByKey()
                .count();

        countEvents
                .toStream()
                .peek((stationId, count) -> System.out.println("Ex1: " + count + " temperature readings at station " + stationId))
                .mapValues((k, v) -> readingPerStationSchema(k, String.valueOf(v)))
                .to(readingsPerStationTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static String readingPerStationSchema(String key, String value) {
        return
                "{\"schema\":{\"type\":\"struct\",\"fields\":"+
                        "["+
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},"+
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"count\"" + "}" +
                        "],"+
                        "\"optional\":false},"+
                        "\"payload\":{\"id\":\"" + key + "\",\"count\":\"" + value + "\"}}";
    }


    // ===================================== ex 2 ====================================== //
    public static void countReadingsPerLocation() {
        KTable<String, Long>  countPerLocation = standardKStream
                .peek(((key, value) -> System.out.println("Ex2: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value)-> KeyValue.pair(parseWeatherStandard(value).getLocation(), key))
                .groupByKey()
                .count();

        countPerLocation
                .toStream()
                .peek((location, count) -> System.out.println("Ex2: " + count + " temperature readings at location " + location))
                .mapValues((k, v) -> readingPerLocationSchema(k, String.valueOf(v)))
                .to(readingsPerLocationTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static String readingPerLocationSchema(String key, String value) {
        return
                "{\"schema\":{\"type\":\"struct\",\"fields\":"+
                        "["+
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},"+
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"count\"" + "}" +
                        "],"+
                        "\"optional\":false},"+
                        "\"payload\":{\"id\":\"" + key + "\",\"count\":\"" + value + "\"}}";
    }


    // ===================================== ex 3 ====================================== //
    public static void minMaxTempPerStation() {
        getMinTempStations()
                .toStream()
                .peek((station, temp) -> System.out.println("Ex3: " + "Station Id: " + station + " Minimum Temperature: " + temp))
                .mapValues(((key, value) -> "Ex3: " + "Station Id: " + key + " Minimum Temperature: " + value))
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));

        getMaxTempStations()
                .toStream()
                .peek((station, temp) -> System.out.println("Ex3: " + "Station Id: " + station + " Maximum Temperature: " + temp))
                .mapValues(((key, value) -> "Ex3: " + "Station Id: " + key + " Maximum Temperature: " + value))
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static KTable<String, Long> getMinTempStations() {
        return standardKStream
                .peek(((key, value) -> System.out.println("minTempStations: " + "Recebido key:" + key + " value:" + value)))
                .mapValues(value -> parseWeatherStandard(value).getTemperature())
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(((value1, value2) -> {
                    if (value1 < value2) {
                        return value1;
                    } else {
                        return value2;
                    }
                }));
    }

    public static KTable<String, Long> getMaxTempStations() {
        return standardKStream
                .peek(((key, value) -> System.out.println("maxTempStations: " + "Recebido key:" + key + " value:" + value)))
                .mapValues(value -> parseWeatherStandard(value).getTemperature())
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(((value1, value2) -> {
                    if (value1 > value2) {
                        return value1;
                    } else {
                        return value2;
                    }
                }));
    }


    // ===================================== ex 4 ====================================== //
    public static void minMaxTempPerLocation() {
        getMinTempLocation().toStream()
                .peek((key, value) -> System.out.println("Ex4: " + "Location: " + key + " Minimum Temperature: " + value))
                .mapValues((location, temp) -> "Ex4: " + "Location: " + location + " Minimum Temperature: " + temp)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));

        getMaxTempLocation().toStream()
                .peek((key, value) -> System.out.println("Ex4: " + "Location: " + key + " Maximum Temperature: " + value))
                .mapValues((location, temp) -> "Ex4: " + "Location: " + location + " Maximum Temperature: " + temp)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static KTable<String, Long> getMinTempLocation() {
        return standardKStream
                .peek(((key, value) -> System.out.println("minTempLocations: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(parseWeatherStandard(value).getLocation(), parseWeatherStandard(value).getTemperature()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(((value1, value2) -> {
                    if (value1 < value2) {
                        return value1;
                    } else {
                        return value2;
                    }
                }));
    }

    public static KTable<String, Long> getMaxTempLocation() {
        return standardKStream
                .peek(((key, value) -> System.out.println("maxTempLocations: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(parseWeatherStandard(value).getLocation(), parseWeatherStandard(value).getTemperature()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(((value1, value2) -> {
                    if (value1 > value2) {
                        return value1;
                    } else {
                        return value2;
                    }
                }));
    }


    // ===================================== ex 5 ====================================== //
    public static void totalAlertsPerStation() {
        KTable<String, Long> countAlerts = alertKStream
                .peek(((key, value) -> System.out.println("Ex5: " + "Recebido key:" + key + " value:" + value)))
                .groupByKey()
                .count();

        countAlerts.toStream()
                .peek((word, count) -> System.out.println("Station alert: " + word + " -> " + count))
                .mapValues((stationId, count) -> "Ex5: " + count + " alerts at station " + stationId)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    // ===================================== ex 6 ====================================== //
    public static void totalAlertsPerType() {
        KTable<String, Long> countAlertTypes = alertKStream
                .peek(((key, value) -> System.out.println("Ex6: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(parseWeatherAlert(value).getEventColor(), key))
                .groupByKey()
                .count();

        countAlertTypes.toStream()
                .peek((key, value) -> System.out.println("Ex6: " + "Alert type:" + key + " Count:" + value))
                .mapValues((alert, count) -> "Ex6: " + "Alert type:" + alert + " Count:" + count)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    // ===================================== ex 7 ====================================== //
    public static void minTempStationsWithRedEvents() {
        KTable<String, String> minRedAlert = alertKStream
                .peek(((key, value) -> System.out.println("Ex7: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(key, parseWeatherAlert(value).getEventColor()))
                .groupByKey()
                .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> {
                    if (newValue.equals("red")) {
                        aggValue += 1;  // ja teve um red event
                    }
                    return aggValue;
                }, Materialized.with(Serdes.String(), Serdes.Long()))
                .filter(((key, value) -> value >= 1))
                .join(getMinTempStations(), (leftVal, rightVal) -> "" + rightVal);

        minRedAlert.toStream()
                .groupByKey()
                .reduce((k, v) -> v)
                .toStream()
                .peek((key, value) -> System.out.println("Ex7: " + "Red alert station id:" + key + " Min temp:" + value))
                .mapValues((k, v) -> "Ex7: " + "Red alert station id:" + k + " Min temp:" + v)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    // ===================================== ex 8 ====================================== //
    public static void maxTempLocationWithAlertEventsLastHour() {
        Duration windowSize = Duration.ofSeconds(60);
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(windowSize, windowSize);

        KTable<String, String> maxTempByLocation = standardKStream
                .peek(((key, value) -> System.out.println("Ex8 maxTemp: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(parseWeatherStandard(value).getLocation(), parseWeatherStandard(value).getTemperature()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(((value1, value2) -> {
                    if (value1 > value2) {
                        return value1;
                    } else {
                        return value2;
                    }
                }))
                .mapValues(v -> "" + v);

        KTable<String, String> alertsLastHour = alertKStream
                .peek(((key, value) -> System.out.println("Ex8 alerts: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(parseWeatherAlert(value).getLocation(), parseWeatherAlert(value).getEventColor()))
                .groupByKey()
                .windowedBy(tumblingWindow)
                .count()
                .toStream((wk, v) -> wk.key())
                .groupByKey()
                .count()
                .mapValues((k, v) -> "" + v);

        maxTempByLocation
                .join(alertsLastHour, (leftV, rightV) -> "" + leftV)
                .toStream()
                .peek((k, v) -> System.out.println("Ex8: " + "alert location: " + k + " Max temp: " + v))
                .mapValues((location, temp) -> "Ex8: " + "alert location: " + location + " Max temp: " + temp)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    // ===================================== ex 9 ====================================== //
    public static void minTempStationsInRedAlerts() {
        // Zonas que estão agora a vermelho
        KStream<String, String> redZones = alertKStream
                .peek(((key, value) -> System.out.println("Ex9: " + "Recebido key:" + key + " value:" + value)))
                .mapValues(value -> parseWeatherAlert(value).getEventColor())
                .groupByKey()
                .reduce((old, newV) -> newV)
                .toStream()
                .filter(((key, value) -> value.equals("red")));

        // join com ktable de temperaturas minimas
        redZones.join(getMinTempStations(), (leftValue, rightValue) -> "" + rightValue)
                .peek((stationId, count) -> System.out.println("Ex9: " + "Red alert station id:" + stationId + " MinTemp:" + count))
                .mapValues((stationId, count) -> "Ex9: " + "Red alert station id:" + stationId + " MinTemp:" + count)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }


    // ===================================== ex 10 ====================================== //
    public static void avgTempPerStation() {
        KTable<String, String> averageTempStation = getAverageTempStations();

        averageTempStation.toStream()
                .peek((stationId, average) -> System.out.println("Ex10: " + "Station " + stationId + " Average temp: " + average))
                .mapValues((stationId, average) -> "Ex10: " + "Station " + stationId + " Average temp: " + average)
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static KTable<String, String> getAverageTempStations() {
        return standardKStream
                .peek(((key, value) -> System.out.println("avgTemps: " + "Recebido key:" + key + " value:" + value)))
                .map((key, value) -> KeyValue.pair(key, parseWeatherStandard(value).getTemperature()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .aggregate(() -> new int[]{0, 0}, (aggKey, newValue, aggValue) -> {
                    aggValue[0] += 1;
                    aggValue[1] += newValue;

                    return aggValue;
                }, Materialized.with(Serdes.String(), new IntArraySerde()))
                .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0");
    }


    // ===================================== ex 11 ====================================== //
    public static void avgTempStationsWithRedAlertsLastHour() {
        Duration windowSize = Duration.ofSeconds(20);
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(windowSize, windowSize);

        KTable<String, String> averageTemps = getAverageTempStations();

        KTable<String, String> redStationsInLastHour = alertKStream
                .peek(((key, value) -> System.out.println("Ex11 redStation: " + "Recebido key:" + key + " value:" + value)))
                .mapValues(value -> parseWeatherAlert(value).getEventColor())
                .filter(((key, value) -> value.equals("red")))
                .groupByKey()
                .windowedBy(tumblingWindow)
                .reduce((oldV, newV) -> newV)
                .toStream((wk, v) -> wk.key())
                .groupByKey()
                .reduce((oldV, newV) -> newV)
                .mapValues(v -> "" + v);

        averageTemps.join(redStationsInLastHour, (leftV, rightV) -> "" + leftV)
                .toStream()
                .peek((stationId, count) -> System.out.println("Ex11: " + "Red alert station id:" + stationId + " AvgTemp:" + count))
                .map((stationId, temp) -> KeyValue.pair(stationId, "Ex11: " + "Red alert station id: " + stationId + " avg temp: " + temp))
                .to(resultsTopic, Produced.with(Serdes.String(), Serdes.String()));
    }



    // ================================= Parse JSON Objects ============================= //
    public static WeatherStandard parseWeatherStandard(String json) {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.fromJson(json, WeatherStandard.class);
    }

    public static WeatherAlert parseWeatherAlert(String json) {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.fromJson(json, WeatherAlert.class);
    }
}
