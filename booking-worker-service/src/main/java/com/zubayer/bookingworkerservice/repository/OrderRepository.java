package com.zubayer.bookingworkerservice.repository;

import com.zubayer.bookingworkerservice.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
    boolean existsByTransactionId(String transactionId);
}
