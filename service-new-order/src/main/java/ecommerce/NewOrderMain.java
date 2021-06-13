package ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            try(var emailDispatcher = new KafkaDispatcher<String>()) {
                for (var i = 0; i < 10; i++) {

                    var orderId = UUID.randomUUID().toString();
                    var userId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var order = new Order(userId, orderId, amount);

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
