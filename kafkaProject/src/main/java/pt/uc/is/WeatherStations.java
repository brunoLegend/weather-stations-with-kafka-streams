package pt.uc.is;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import pt.uc.is.dto.*;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class WeatherStations {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "live-test");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put("bootstrap.servers", "localhost:29092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // intervalo entre criação de eventos
        int time = 30000;

        // tópicos
        String infoTopic = "DBinfo";
        String standardTopic = "standardWeather";
        String alertTopic = "weatherAlerts";

        // producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // parsing
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();

        // Alert types
        String[] color = {"green", "red"};
        String[] eventType = {"rain", "blizzard", "storm", "hurricane"};

/*
        // PARA TESTES ---------------------------
        WeatherStandard weatherStandard1 = new WeatherStandard(1L, "Lisboa", 40L);
        WeatherStandard weatherStandard2 = new WeatherStandard(2L, "Coimbra", 40L);
        WeatherStandard weatherStandard3 = new WeatherStandard(3L, "Porto", 40L);

        producer.send(new ProducerRecord<>(standardTopic, String.valueOf(weatherStandard1.getId()), gson.toJson(weatherStandard1)));
        producer.send(new ProducerRecord<>(standardTopic, String.valueOf(weatherStandard2.getId()), gson.toJson(weatherStandard2)));
        producer.send(new ProducerRecord<>(standardTopic, String.valueOf(weatherStandard3.getId()), gson.toJson(weatherStandard3)));

        System.out.println("Sending standard weather");
        System.out.println(gson.toJson(weatherStandard1));
        System.out.println("Sending standard weather");
        System.out.println(gson.toJson(weatherStandard2));
        System.out.println("Sending standard weather");
        System.out.println(gson.toJson(weatherStandard3));

        WeatherAlert weatherAlert1 = new WeatherAlert("rain", "Lisboa", "green");
        WeatherAlert weatherAlert2 = new WeatherAlert("rain", "Porto", "green");
        WeatherAlert weatherAlert3 = new WeatherAlert("rain", "Coimbra", "green");

        producer.send(new ProducerRecord<>(alertTopic, String.valueOf(1), gson.toJson(weatherAlert1)));
        producer.send(new ProducerRecord<>(alertTopic, String.valueOf(3), gson.toJson(weatherAlert2)));
        producer.send(new ProducerRecord<>(alertTopic, String.valueOf(2), gson.toJson(weatherAlert3)));

        System.out.println("Sendind alert");
        System.out.println(gson.toJson(weatherAlert1));
        System.out.println("Sendind alert");
        System.out.println(gson.toJson(weatherAlert2));
        System.out.println("Sendind alert");
        System.out.println(gson.toJson(weatherAlert3));



        weatherStandard1 = new WeatherStandard(1L, "Lisboa", 20L);
        producer.send(new ProducerRecord<>(standardTopic, String.valueOf(weatherStandard1.getId()), gson.toJson(weatherStandard1)));

        weatherAlert1 = new WeatherAlert("rain", "Lisboa", "red");
        producer.send(new ProducerRecord<>(alertTopic, String.valueOf(1), gson.toJson(weatherAlert1)));

        Thread.sleep(3000);
*/
        // ---------------------------------------
        while (true) {
            consumer.subscribe(Collections.singletonList(infoTopic));
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

            // percorrer lista de localizacoes
            for (ConsumerRecord<String, String> record : records) {
                JSONObject payload = getPayload(record.value());


                // Gera Standard Weather Event e envia
                Long randomTemp = (long) (Math.random() * (30 - -5) + -5);
                WeatherStandard weatherStandard = new WeatherStandard((Long) payload.get("station_id"), (String) payload.get("location"), randomTemp);
                System.out.println("Sending standard weather");
                System.out.println(gson.toJson(weatherStandard));

                producer.send(new ProducerRecord<>(standardTopic, String.valueOf(weatherStandard.getId()), gson.toJson(weatherStandard)));

                // Probabilidade de gerar eventos de alerta
                int randomProb = (int) (Math.random() * 100);
                int prob = 30; // gera 30% das vezes
                if (randomProb <= prob) {
                    // Gera Weather Alert Event e envia
                    Random randomColor = new Random();
                    Random randomEvent = new Random();
                    WeatherAlert weatherAlert = new WeatherAlert(eventType[randomEvent.nextInt(eventType.length)],
                            (String) payload.get("location"),
                            color[randomColor.nextInt(color.length)]);
                    System.out.println("Sendind alert");
                    System.out.println(gson.toJson(weatherAlert));

                    producer.send(new ProducerRecord<>(alertTopic, String.valueOf(payload.get("station_id")), gson.toJson(weatherAlert)));
                }
            }
            Thread.sleep(time);
        }
    }

    public static JSONObject getPayload(String element) {
        JSONObject json = null;
        JSONParser parser = new JSONParser();

        try {
            json = (JSONObject) parser.parse(element);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Obter dados do cliente
        JSONObject payload = (JSONObject) json.get("payload");

        return payload;
    }
}
