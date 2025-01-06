package group.ventis.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

@Data
@NoArgsConstructor
public class Operation {
    @JsonProperty("id")
    private String id;
    @JsonProperty("montant")
    private double montant;
    @JsonProperty("sign")
    private String sign;
    @JsonProperty("accountNumber")
    private String accountNumber;
    @JsonProperty("libelle")
    private String libelle;
    @JsonProperty("date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Date date;
    private long operationsCountLast1Min;
    private long operationsCountLast5Min;
    private long operationsCountLast10Min;
    private long debitCountLast1Min;
    private long debitCountLast5Min;
    private long debitCountLast10Min;
    private long creditCountLast1Min;
    private long creditCountLast5Min;
    private long creditCountLast10Min;

    private double debitSumLast1Min;
    private double debitSumLast5Min;
    private double debitSumLast10Min;


    private long operationCountLast1h;
    private long operationCountLast24h;
    private long debitCountLast1h;
    private long debitCountLast24h;
    private long creditCountLast1h;
    private long creditCountLast24h;

    private double debitSumLast1h;
    private double debitSumLast24h;

    private long operationCountLast1Month;
    private long operationCountLast2Month;
    private long operationCountLast3Month;
    private long operationCountLast6Month;
    private long debitCountLast1Month;
    private long debitCountLast2Month;
    private long debitCountLast3Month;
    private long debitCountLast6Month;
    private long creditCountLast1Month;
    private long creditCountLast2Month;
    private long creditCountLast3Month;
    private long creditCountLast6Month;

    private double debitSumLast1Month;
    private double debitSumLast2Month;
    private double debitSumLast3Month;
    private double debitSumLast6Month;

    private boolean fraude;

}