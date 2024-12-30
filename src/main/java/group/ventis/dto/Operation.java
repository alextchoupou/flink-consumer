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
    private long operationCountLast24h;
}