package group.ventis.dto;


import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Date;

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

    private String pk;
    private Date cdate;
    private String accountcode;
    private String	accountpk;
    private boolean closed;
    private String code;
    private double creditamount;
    private String currencycode;
    private byte currencypk;
    private double debitamount;
    private boolean differed;
    private String  differedcondition;
    private String extreference;
    private byte filtrereport;
    private byte fxrate;
    private byte  globalreference;
    private double grossamount;
    private double grossamountacccur;
    private String lockoperationreference;
    private double netamount;
    private double netamountacccur;
    private String opeinfo;
    private String  operationtypecode;
    private int operationtypepk;
    private String originopecode;
    private String originopepk;
    private String originopetype;
    private String paymentmeanref;
    private boolean processed;
    private String reference;
    private String relatedoperation;
    private byte reservationreference;
    private boolean reversed;
    private String reversedfinancialopecode;
    private int reversedfinancialopepk;
    private String sign;
    private long totalcomission;
    private long totalcommissionacccur;
    private byte totalfracccur;
    private byte totalfiscalretention;
    private byte totalstamp;
    private byte totaltaf;
    private int totalvat;
    private long totalvatacccur;
    private Date tradedate;
    private String unitcode;
    private byte unitpk;
    private Timestamp udate;
    private Date valuedate;
    private byte versionnum;
    private int totalcss;
    private int totalcssacccur;
    private boolean mailsent;
    private boolean smssent;
    private boolean whatsappsent;
    private boolean forc;
}