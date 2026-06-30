package com.zubayer.bookingworkerservice.messaging;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Local Imports
import com.zubayer.bookingworkerservice.model.Order;
import com.zubayer.bookingworkerservice.dto.BookingConfirmedEvent;
import com.zubayer.bookingworkerservice.repository.OrderRepository;

import java.util.function.Consumer;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BookingConsumerConfig {

    private final OrderRepository orderRepository;

    @Bean
    public Consumer<BookingConfirmedEvent> processBookingOrder() {

        return (event) -> {

            log.info("Received booking confirmation for Item: {}, User: {}", event.itemId(), event.userId());

            if (orderRepository.existsByTransactionId(event.transactionId())) {
                log.warn("Duplicate message detected for transaction id: {}. Skipping record persistence.", event.transactionId());
                return;
            }

            Order order = Order.builder()
                    .transactionId(event.transactionId())
                    .userId(event.userId())
                    .itemId(event.itemId())
                    .status("CONFIRMED")
                    .createdAt(event.timestamp())
                    .build();

            orderRepository.save(order);

            log.info("Successfully persisted Order ID: {} for Transaction: {}", order.getId(), event.transactionId());

        };

    }

}
