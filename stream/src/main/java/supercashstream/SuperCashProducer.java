package supercashstream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class SuperCashProducer {

   private static final String TOPIC = "cashflow";
   private static final String BOOTSTRAP_SERVERS = "broker:9092";
   private static final ObjectMapper mapper = new ObjectMapper();

   public static void main(String[] args) throws Exception {

      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

         List<String> shopIds = Arrays.asList("S001", "S002", "S003");
         List<String> productIds = Arrays.asList("P100", "P200", "P300", "P400");
         Random random = new Random();

         System.out.println("🚀 Starting producer... Press Ctrl+C to stop.");

         while (true) {

            String shopId = shopIds.get(random.nextInt(shopIds.size()));
            String productId = productIds.get(random.nextInt(productIds.size()));
            int quantity = ThreadLocalRandom.current().nextInt(1, 10);
            double price = ThreadLocalRandom.current().nextDouble(1.0, 50.0);
            String orderId = UUID.randomUUID().toString();

            ObjectNode json = mapper.createObjectNode();
            json.put("shop_id", shopId);
            json.put("order_id", orderId);
            json.put("product_id", productId);
            json.put("quantity", quantity);
            json.put("price", price);

            String jsonString = mapper.writeValueAsString(json);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, shopId, jsonString);
            producer.send(record);

            System.out.printf("➡️ Sent: key=%s | value=%s%n", shopId, jsonString);

            Thread.sleep(1000);
         }
      }
   }
}