package group.ventis.dto;


import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
public class Transaction {

    private String transactionId;
    private String productId;
    private String productName;
    private String productCategory;
    private double productPrice;
    private String productBrand;
    private int productQuantity;
    private double totalAmount;
    private String currency;
    private String customerId;
    private Timestamp transactionDate;
    private String paymentMethod;

}