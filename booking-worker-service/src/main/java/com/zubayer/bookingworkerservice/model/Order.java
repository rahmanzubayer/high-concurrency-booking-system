package com.zubayer.bookingworkerservice.model;

import lombok.*;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "orders", uniqueConstraints = @UniqueConstraint(columnNames = "transaction_id"))
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "transaction_id", nullable = false, unique = true, length = 64)
    private String transactionId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "item_id", nullable = false, length = 64)
    private String itemId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /**
     * Equality on the business key, never the generated id.
     * <p>
     * Lombok's @Data derives equals/hashCode from every field, including @Id. That id is
     * null before persist and assigned by the database afterward — so an entity's hashCode
     * CHANGES mid-transaction. Put it in a HashSet before saving, and you can no longer find
     * it in that set after saving. transaction_id is set before persist and never changes.
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof Order other)) return false;

        return transactionId != null && transactionId.equals(other.getTransactionId());
    }

    @Override
    public int hashCode() {
        return Order.class.hashCode();
    }

}
