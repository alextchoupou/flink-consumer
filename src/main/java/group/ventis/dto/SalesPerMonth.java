package group.ventis.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor @Data
public class SalesPerMonth {
    private int month;
    private  int year;
    private Double totalAmount;
}
