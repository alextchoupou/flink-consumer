package group.ventis.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Date;
@AllArgsConstructor
@Data
public class SalesPerCategory {
    private Date transactionDate;
    private String category;
    private Double totalAmount;

}
