package group.ventis.dto;


import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;
import java.time.LocalDateTime;


@Data
@NoArgsConstructor
public class Transaction {

//    private String transactionId;
//    private String productId;
//    private String productName;
//    private String productCategory;
//    private double productPrice;
//    private String productBrand;
//    private int productQuantity;
//    private double totalAmount;
//    private String currency;
//    private String customerId;
//    private Timestamp transactionDate;
//    private String paymentMethod;

    @JsonProperty("pk")
    private long pk;
    @JsonProperty("cdate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Timestamp cdate;
    @JsonProperty("accountnumber")
    private String accountnumber;
    @JsonProperty("accountcode")
    private String accountcode;
    @JsonProperty("accountpk")
    private String accountpk;
    @JsonProperty("closed")
    private boolean closed;
    @JsonProperty("code")
    private String code;
    @JsonProperty("creditamount")
    private double creditamount;
    @JsonProperty("currencycode")
    private String currencycode;
    @JsonProperty("currencypk")
    private int currencypk;
    @JsonProperty("debitamount")
    private double debitamount;
    @JsonProperty("filtrereport")
    private boolean filtrereport;
    @JsonProperty("fxrate")
    private byte fxrate;
    @JsonProperty("grossamount")
    private double grossamount;
    @JsonProperty("grossamountacccur")
    private double grossamountacccur;
    @JsonProperty("lockoperationreference")
    private String lockoperationreference;
    @JsonProperty("netamount")
    private double netamount;
    @JsonProperty("netamountacccur")
    private double netamountacccur;
    @JsonProperty("opeinfo")
    private String opeinfo;
    @JsonProperty("operationtypecode")
    private String  operationtypecode;
    @JsonProperty("operationtypepk")
    private String operationtypepk;
    @JsonProperty("originopecode")
    private String originopecode;
    @JsonProperty("originopepk")
    private String originopepk;
    @JsonProperty("originopetype")
    private String originopetype;
    @JsonProperty("processed")
    private boolean processed;
    @JsonProperty("reference")
    private String reference;
    @JsonProperty("relatedoperation")
    private String relatedoperation;
    @JsonProperty("reservationreference")
    private String reservationreference;
    @JsonProperty("reversed")
    private boolean reversed;
    @JsonProperty("reversedfinancialopecode")
    private String reversedfinancialopecode;
    @JsonProperty("reversedfinancialopepk")
    private String reversedfinancialopepk;
    @JsonProperty("sign")
    private String sign;
    @JsonProperty("totalcomission")
    private long totalcomission;
    @JsonProperty("totalcommissionacccur")
    private long totalcommissionacccur;
    @JsonProperty("totalfracccur")
    private long totalfracccur;
    @JsonProperty("totalfiscalretention")
    private long totalfiscalretention;
    @JsonProperty("totalstamp")
    private long totalstamp;
    @JsonProperty("totaltaf")
    private long totaltaf;
    @JsonProperty("totalvat")
    private long totalvat;
    @JsonProperty("totalvatacccur")
    private long totalvatacccur;
    @JsonProperty("tradedate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Timestamp tradedate;
    @JsonProperty("unitcode")
    private String unitcode;
    @JsonProperty("unitpk")
    private String unitpk;
    @JsonProperty("udate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Timestamp udate;
    @JsonProperty("valuedate")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Timestamp valuedate;
    @JsonProperty("versionnum")
    private int versionnum;
    @JsonProperty("totalcss")
    private long totalcss;
    @JsonProperty("totalcssacccur")
    private long totalcssacccur;
    @JsonProperty("mailsent")
    private boolean mailsent;
    @JsonProperty("smssent")
    private boolean smssent;
    @JsonProperty("whatsappsent")
    private boolean whatsappsent;
    @JsonProperty("force")
    private boolean force;
    /*@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private Timestamp transactionDate;*/
}