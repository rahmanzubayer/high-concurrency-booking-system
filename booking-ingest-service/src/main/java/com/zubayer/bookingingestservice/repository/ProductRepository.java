package com.zubayer.bookingingestservice.repository;

import com.zubayer.bookingingestservice.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, String> {}
