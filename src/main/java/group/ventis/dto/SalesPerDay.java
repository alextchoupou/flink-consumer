package group.ventis.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;

@AllArgsConstructor @Data
public class SalesPerDay {
    private Date transactionDate;
    private Double totalAmount;
}
