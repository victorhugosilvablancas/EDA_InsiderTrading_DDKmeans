package uaq.dcc.datasets;

import java.sql.Timestamp;

/**
 *
 * File records definition.
 */
public class FileRecordsdata {
    public String URL;
    public String accessionNumber;
    public Timestamp filingDate; //yyyymmdd
    public Integer filerCik;
    public String transactionType;
    public Integer tableRow;
    public String securityTitle;
    public String securityTitleFn;
    public Double conversionOrExercisePrice;
    public String conversionOrExercisePriceFn;
    public Timestamp transactionDate; //yyyy-mm-dd
    public String transactionDateFn;
    public Timestamp deemedExecutionDate; //yyyy-mm-dd
    public String deemedExecutionDateFn;
    public Integer transactionFormType;
    public String transactionCode;
    public Integer equitySwapInvolved;
    public String transactionCodeFn ;
    public String transactionTimeliness ;
    public String transactionTimelinessFn ;
    public Integer transactionShares;
    public String transactionSharesFn ;
    public Double transactionTotalValue;
    public String transactionTotalValueFn ;
    public Double transactionPricePerShare;
    public String transactionPricePerShareFn ;
    public String transactionAcquiredDisposedCode;
    public String transactionAcquiredDisposedCdFn ;
    public Timestamp exerciseDate; //yyyy-mm-dd
    public String exerciseDateFn;
    public Timestamp expirationDate; //yyyy-mm-dd
    public String expirationDateFn;
    public String underlyingSecurityTitle;
    public String underlyingSecurityTitleFn;
    public Integer underlyingSecurityShares;
    public String underlyingSecuritySharesFn;
    public Double underlyingSecurityValue;
    public String underlyingSecurityValueFn;
    public String sharesOwnedFollowingTransaction;
    public String sharesOwnedFolwngTransactionFn; //"F2,F4"
    public Double valueOwnedFollowingTransaction;
    public String valueOwnedFolwngTransactionFn;
    public String directOrIndirectOwnership;
    public String directOrIndirectOwnershipFn;
    public String natureOfOwnership;
    public String natureOfOwnershipFn;
    
    public static final String TABLE="HARVARD_INSIDERTRADING";
    
    public FileRecordsdata() {
        URL="";
        accessionNumber="";
        filingDate=new Timestamp(System.currentTimeMillis()); //yyyymmdd
        filerCik=0;
        transactionType="";
        tableRow=0;
        securityTitle="";
        securityTitleFn="";
        conversionOrExercisePrice=0.0;
        conversionOrExercisePriceFn="";
        transactionDate=new Timestamp(System.currentTimeMillis()); //yyyy-mm-dd
        transactionDateFn="";
        deemedExecutionDate=new Timestamp(System.currentTimeMillis()); //yyyy-mm-dd
        deemedExecutionDateFn="";
        transactionFormType=0;
        transactionCode="";
        equitySwapInvolved=0;
        transactionCodeFn ="";
        transactionTimeliness ="";
        transactionTimelinessFn ="";
        transactionShares=0;
        transactionSharesFn ="";
        transactionTotalValue=0.0;
        transactionTotalValueFn ="";
        transactionPricePerShare=0.0;
        transactionPricePerShareFn ="";
        transactionAcquiredDisposedCode="";
        transactionAcquiredDisposedCdFn ="";
        exerciseDate=new Timestamp(System.currentTimeMillis()); //yyyy-mm-dd
        exerciseDateFn="";
        expirationDate=new Timestamp(System.currentTimeMillis()); //yyyy-mm-dd
        expirationDateFn="";
        underlyingSecurityTitle="";
        underlyingSecurityTitleFn="";
        underlyingSecurityShares=0;
        underlyingSecuritySharesFn="";
        underlyingSecurityValue=0.0;
        underlyingSecurityValueFn="";
        sharesOwnedFollowingTransaction="";
        sharesOwnedFolwngTransactionFn="";
        valueOwnedFollowingTransaction=0.0;
        valueOwnedFolwngTransactionFn="";
        directOrIndirectOwnership="";
        directOrIndirectOwnershipFn="";
        natureOfOwnership="";
        natureOfOwnershipFn="";
    }
    private static final String QUOTATIONS="\"";
    private static final String COMMA=",";
    public FileRecordsdata(String line) {
        Timestamp f=new Timestamp(System.currentTimeMillis());
        URL="";
        accessionNumber="";
        filingDate=f;
        filerCik=0;
        transactionType="";
        tableRow=0;
        securityTitle="";
        securityTitleFn="";
        conversionOrExercisePrice=0.0;
        conversionOrExercisePriceFn="";
        transactionDate=f;
        transactionDateFn="";
        deemedExecutionDate=f;
        deemedExecutionDateFn="";
        transactionFormType=0;
        transactionCode="";
        equitySwapInvolved=0;
        transactionCodeFn ="";
        transactionTimeliness ="";
        transactionTimelinessFn ="";
        transactionShares=0;
        transactionSharesFn ="";
        transactionTotalValue=0.0;
        transactionTotalValueFn ="";
        transactionPricePerShare=0.0;
        transactionPricePerShareFn ="";
        transactionAcquiredDisposedCode="";
        transactionAcquiredDisposedCdFn ="";
        exerciseDate=f;
        exerciseDateFn="";
        expirationDate=f;
        expirationDateFn="";
        underlyingSecurityTitle="";
        underlyingSecurityTitleFn="";
        underlyingSecurityShares=0;
        underlyingSecuritySharesFn="";
        underlyingSecurityValue=0.0;
        underlyingSecurityValueFn="";
        sharesOwnedFollowingTransaction="";
        sharesOwnedFolwngTransactionFn="";
        valueOwnedFollowingTransaction=0.0;
        valueOwnedFolwngTransactionFn="";
        directOrIndirectOwnership="";
        directOrIndirectOwnershipFn="";
        natureOfOwnership="";
        natureOfOwnershipFn="";
        
        //quitando la coma extra
        int idx=0;
        int fdx=0;
        
        //System.out.println(line);
        line=EraseQuotations(line);
        
        int max=46;
        idx=0;
        String cade="";
        for (int i=0;i<max;i++) {
            fdx=line.indexOf(COMMA,idx);
            if (fdx>=0) {
                //System.out.println(i+":"+line.substring(idx, fdx));
                cade=line.substring(idx, fdx);
            } else {
                //System.out.println(i+":"+line.substring(idx));
                cade=line.substring(idx);
            }
            switch (i) {
                case 0: URL=cade; 
                break;
                case 1: accessionNumber=cade; 
                break;
                case 2: if (cade.length()>0) filingDate=getDate(cade); break;
                case 3: filerCik=onlynumbers(cade).intValue(); break;
                case 4: transactionType=cade; break;
                case 5: tableRow=onlynumbers(cade).intValue(); break;
                case 6: securityTitle=cade; break;
                case 7: securityTitleFn=cade; break;
                case 8: conversionOrExercisePrice=onlynumbers(cade); break;
                case 9: conversionOrExercisePriceFn=cade; break;
                case 10: if (cade.length()>0) transactionDate=getDate(cade); break;
                case 11: transactionDateFn=cade; break;
                case 12: if (cade.length()>0) deemedExecutionDate=getDate(cade); break;
                case 13: deemedExecutionDateFn=cade; break;
                case 14: transactionFormType=onlynumbers(cade).intValue(); break;
                case 15: transactionCode=cade; break;
                case 16: equitySwapInvolved=onlynumbers(cade).intValue(); break;
                case 17: transactionCodeFn =cade; break;
                case 18: transactionTimeliness =cade; break;
                case 19: transactionTimelinessFn =cade; break;
                case 20: transactionShares=onlynumbers(cade).intValue(); break;
                case 21: transactionSharesFn =cade; break;
                case 22: transactionTotalValue=onlynumbers(cade); break;
                case 23: transactionTotalValueFn =cade; break;
                case 24: transactionPricePerShare=onlynumbers(cade); break;
                case 25: transactionPricePerShareFn =cade; break;
                case 26: transactionAcquiredDisposedCode=cade; break;
                case 27: transactionAcquiredDisposedCdFn =cade; break;
                case 28: if (cade.length()>0) exerciseDate=getDate(cade); break;
                case 29: exerciseDateFn=cade; break;
                case 30: if (cade.length()>0) expirationDate=getDate(cade); break;
                case 31: expirationDateFn=cade; break;
                case 32: underlyingSecurityTitle=cade; break;
                case 33: underlyingSecurityTitleFn=cade; break;
                case 34: underlyingSecurityShares=onlynumbers(cade).intValue(); break;
                case 35: underlyingSecuritySharesFn=cade; break;
                case 36: underlyingSecurityValue=onlynumbers(cade); break;
                case 37: underlyingSecurityValueFn=cade; break;
                case 38: sharesOwnedFollowingTransaction=cade; break;
                case 39: sharesOwnedFolwngTransactionFn=cade; break; //"F2,F4"
                case 40: valueOwnedFollowingTransaction=onlynumbers(cade); break;
                case 41: valueOwnedFolwngTransactionFn=cade; break;
                case 42: directOrIndirectOwnership=cade; break;
                case 43: directOrIndirectOwnershipFn=cade; break;
                case 44: natureOfOwnership=cade; break;
                case 45: natureOfOwnershipFn=cade; break;
            }
            idx=fdx+1;
        }
    }
    public Timestamp getDate(String afecha) {
        Timestamp f=new Timestamp(System.currentTimeMillis());
        if (afecha.contains(":")) {
            int idx=afecha.indexOf(":")-3;
            afecha=afecha.substring(0, idx);
        }
        afecha=afecha.replace(" ", "");
        afecha=afecha.replace("--", "-");
        //System.out.println(afecha);
        if (afecha.length()==8) f=Timestamp.valueOf(afecha.substring(0, 4)+"-"+afecha.substring(4, 6)+"-"+afecha.substring(4, 6)+" 15:01:1.1");
        else f=Timestamp.valueOf(afecha.substring(0, 4)+"-"+afecha.substring(5, 7)+"-"+afecha.substring(8)+" 15:01:1.1");
        return f;
    }
    public Double onlynumbers(String anumero) {
        Double rnumero=0.0;
        if (anumero!=null) {
            if (anumero.length()>0) {
                boolean esnegativo=anumero.contains("-");
                if (!esnegativo) esnegativo=anumero.contains("(");
                if (esnegativo) {
                    anumero=anumero.replace("-","");
                    anumero=anumero.replace("(","");
                    anumero=anumero.replace(")","");
                }
                String letra="0";
                for (int i=0;i<anumero.length();i++) {
                    switch (anumero.substring(i,i+1)) {
                        case "0": letra+=anumero.substring(i,i+1); break;
                        case "1": letra+=anumero.substring(i,i+1); break;
                        case "2": letra+=anumero.substring(i,i+1); break;
                        case "3": letra+=anumero.substring(i,i+1); break;
                        case "4": letra+=anumero.substring(i,i+1); break;
                        case "5": letra+=anumero.substring(i,i+1); break;
                        case "6": letra+=anumero.substring(i,i+1); break;
                        case "7": letra+=anumero.substring(i,i+1); break;
                        case "8": letra+=anumero.substring(i,i+1); break;
                        case "9": letra+=anumero.substring(i,i+1); break;
                        case ".": letra+=anumero.substring(i,i+1); break;
                    }
                }
                rnumero=Double.valueOf(letra);
                if (esnegativo) rnumero=rnumero*-1;
            }
        }
        return rnumero;
    }
    
    public String string() {
        return URL+"\t"
                + accessionNumber+"\t"
                + filingDate+"\t" //yyyymmdd
                + filerCik+"\t"
                + transactionType+"\t"
                + tableRow+"\t"
                + securityTitle+"\t"
                + securityTitleFn+"\t"
                + conversionOrExercisePrice+"\t"
                + conversionOrExercisePriceFn+"\t"
                + transactionDate+"\t" //yyyy-mm-dd
                + transactionDateFn+"\t"
                + deemedExecutionDate+"\t" //yyyy-mm-dd
                + deemedExecutionDateFn+"\t"
                + transactionFormType+"\t"
                + transactionCode+"\t"
                + equitySwapInvolved+"\t"
                + transactionCodeFn +"\t"
                + transactionTimeliness +"\t"
                + transactionTimelinessFn +"\t"
                + transactionShares+"\t"
                + transactionSharesFn +"\t"
                + transactionTotalValue+"\t"
                + transactionTotalValueFn +"\t"
                + transactionPricePerShare+"\t"
                + transactionPricePerShareFn +"\t"
                + transactionAcquiredDisposedCode+"\t"
                + transactionAcquiredDisposedCdFn +"\t"
                + exerciseDate+"\t" //yyyy-mm-dd
                + exerciseDateFn+"\t"
                + expirationDate+"\t" //yyyy-mm-dd
                + expirationDateFn+"\t"
                + underlyingSecurityTitle+"\t"
                + underlyingSecurityTitleFn+"\t"
                + underlyingSecurityShares+"\t"
                + underlyingSecuritySharesFn+"\t"
                + underlyingSecurityValue+"\t"
                + underlyingSecurityValueFn+"\t"
                + sharesOwnedFollowingTransaction+"\t"
                + sharesOwnedFolwngTransactionFn+"\t" //"F2,F4"
                + valueOwnedFollowingTransaction+"\t"
                + valueOwnedFolwngTransactionFn+"\t"
                + directOrIndirectOwnership+"\t"
                + directOrIndirectOwnershipFn+"\t"
                + natureOfOwnership+"\t"
                + natureOfOwnershipFn
                ;
    }
    
    public static final String CREATE_TABLE="create table "+TABLE+" ("
            + "URL varchar2(128),"
            + "accessionNumber varchar2(20),"
            + "filingDate timestamp(6)," //yyyymmdd
            + "filerCik number(12),"
            + "transactionType varchar2(30),"
            + "tableRow number(6),"
            + "securityTitle varchar2(50),"
            + "securityTitleFn varchar2(20),"
            + "conversionOrExercisePrice number(9,2),"
            + "conversionOrExercisePriceFn varchar2(20),"
            + "transactionDate timestamp(6)," //yyyy-mm-dd
            + "transactionDateFn varchar2(20),"
            + "deemedExecutionDate timestamp(6)," //yyyy-mm-dd
            + "deemedExecutionDateFn varchar2(20),"
            + "transactionFormType number(6),"
            + "transactionCode varchar2(10),"
            + "equitySwapInvolved number(6),"
            + "transactionCodeFn varchar2(20),"
            + "transactionTimeliness varchar2(20),"
            + "transactionTimelinessFn varchar2(20),"
            + "transactionShares number(6),"
            + "transactionSharesFn varchar2(20),"
            + "transactionTotalValue number(9,2),"
            + "transactionTotalValueFn varchar2(20),"
            + "transactionPricePerShare number(9,2),"
            + "transactionPricePerShareFn varchar2(20),"
            + "transactionAcquiredDisposedCode varchar2(10),"
            + "transactionAcquiredDisposedCdFn varchar2(20),"
            + "exerciseDate timestamp(6)," //yyyy-mm-dd
            + "exerciseDateFn varchar2(20),"
            + "expirationDate timestamp(6)," //yyyy-mm-dd
            + "expirationDateFn varchar2(20),"
            + "underlyingSecurityTitle varchar2(30),"
            + "underlyingSecurityTitleFn varchar2(20),"
            + "underlyingSecurityShares number(6),"
            + "underlyingSecuritySharesFn varchar2(20),"
            + "underlyingSecurityValue number(9,2),"
            + "underlyingSecurityValueFn varchar2(20),"
            + "sharesOwnedFollowingTransaction number(6),"
            + "sharesOwnedFolwngTransactionFn varchar2(20)," //"F2,F4"
            + "valueOwnedFollowingTransaction number(9,2),"
            + "valueOwnedFolwngTransactionFn varchar2(20),"
            + "directOrIndirectOwnership varchar2(10),"
            + "directOrIndirectOwnershipFn varchar2(20),"
            + "natureOfOwnership varchar2(30),"
            + "natureOfOwnershipFn varchar2(20) "
            + ")";

    public static String EraseQuotations(String linea) {
        String cade=linea;
        if (cade.contains(QUOTATIONS)) {
            int ini=0;
            int idx=0;
            int fdx=0;
            String corchete="";
            while (fdx<cade.length()) {
                idx=cade.indexOf(QUOTATIONS,ini);
                if (idx>=0) {
                    fdx=cade.indexOf(QUOTATIONS,idx+1);

                    if (fdx>idx) corchete=cade.substring(idx, fdx);
                    corchete=corchete.replace(",","-");
                    cade=cade.substring(0, idx)+corchete+cade.substring(fdx);

                    //System.out.println(idx+":"+fdx+"<"+cade.length());
                    //System.out.println(cade);
                    ini=fdx+1;
                } else break;
            }
        }
        return cade;
    }

    public String stringUseful() {
        return URL+"\t"
                + accessionNumber+"\t"
                + filingDate+"\t" //yyyymmdd
                + filerCik+"\t"
                +"U\t"+ transactionType+"\t"
                + tableRow+"\t"
                +"U\t"+ securityTitle+"\t"
                + securityTitleFn+"\t"
                + conversionOrExercisePrice+"\t"
                + conversionOrExercisePriceFn+"\t"
                +"U\t"+ transactionDate+"\t" //yyyy-mm-dd
                + transactionDateFn+"\t"
                + deemedExecutionDate+"\t" //yyyy-mm-dd
                + deemedExecutionDateFn+"\t"
                + transactionFormType+"\t"
                +"U\t"+ transactionCode+"\t"
                +"U\t"+ equitySwapInvolved+"\t"
                + transactionCodeFn +"\t"
                + transactionTimeliness +"\t"
                + transactionTimelinessFn +"\t"
                +"U\t"+ transactionShares+"\t"//3
                + transactionSharesFn +"\t"
                + transactionTotalValue+"\t"
                + transactionTotalValueFn +"\t"
                +"U\t"+ transactionPricePerShare+"\t"
                + transactionPricePerShareFn +"\t"
                +"U\t"+ transactionAcquiredDisposedCode+"\t"
                + transactionAcquiredDisposedCdFn +"\t"
                +"U\t"+ exerciseDate+"\t" //yyyy-mm-dd
                +"U\t"+ exerciseDateFn+"\t"
                + expirationDate+"\t" //yyyy-mm-dd
                + expirationDateFn+"\t"
                + underlyingSecurityTitle+"\t"
                + underlyingSecurityTitleFn+"\t"
                +"U\t"+ underlyingSecurityShares+"\t"
                + underlyingSecuritySharesFn+"\t"
                + underlyingSecurityValue+"\t"
                + underlyingSecurityValueFn+"\t"
                + sharesOwnedFollowingTransaction+"\t"
                + sharesOwnedFolwngTransactionFn+"\t" //"F2,F4"
                + valueOwnedFollowingTransaction+"\t"
                + valueOwnedFolwngTransactionFn+"\t"
                +"U\t"+ directOrIndirectOwnership+"\t"
                + directOrIndirectOwnershipFn+"\t"
                + natureOfOwnership+"\t"
                + natureOfOwnershipFn
                ;
    }
    public static final String[][] VARIABLELIST=new String[][] {
        {"String","URL"},
        {"String","accessionNumber"},
        {"Timestamp","filingDate"}, //yyyymmdd
        {"Integer","filerCik"},
        {"String","transactionType"},
        {"Integer","tableRow"},
        {"String","securityTitle"},
        {"String","securityTitleFn"},
        {"Double","conversionOrExercisePrice"},
        {"String","conversionOrExercisePriceFn"},
        {"Timestamp","transactionDate"}, //yyyy-mm-dd
        {"String","transactionDateFn"},
        {"Timestamp","deemedExecutionDate"}, //yyyy-mm-dd
        {"String","deemedExecutionDateFn"},
        {"Integer","transactionFormType"},
        {"String","transactionCode"},
        {"Integer","equitySwapInvolved"},
        {"String","transactionCodeFn "},
        {"String","transactionTimeliness "},
        {"String","transactionTimelinessFn "},
        {"Integer","transactionShares"},
        {"String","transactionSharesFn "},
        {"Double","transactionTotalValue"},
        {"String","transactionTotalValueFn "},
        {"Double","transactionPricePerShare"},
        {"String","transactionPricePerShareFn "},
        {"String","transactionAcquiredDisposedCode"},
        {"String","transactionAcquiredDisposedCdFn "},
        {"Timestamp","exerciseDate"}, //yyyy-mm-dd
        {"String","exerciseDateFn"},
        {"Timestamp","expirationDate"}, //yyyy-mm-dd
        {"String","expirationDateFn"},
        {"String","underlyingSecurityTitle"},
        {"String","underlyingSecurityTitleFn"},
        {"Integer","underlyingSecurityShares"},
        {"String","underlyingSecuritySharesFn"},
        {"Double","underlyingSecurityValue"},
        {"String","underlyingSecurityValueFn"},
        {"String","sharesOwnedFollowingTransaction"},
        {"String","sharesOwnedFolwngTransactionFn"}, //"F2,F4"
        {"Double","valueOwnedFollowingTransaction"},
        {"String","valueOwnedFolwngTransactionFn"},
        {"String","directOrIndirectOwnership"},
        {"String","directOrIndirectOwnershipFn"},
        {"String","natureOfOwnership"},
        {"String","natureOfOwnershipFn"},
        {"10,643,865","Total records"},
    };
    public static final String[] HEAD=new String[] {
        "Name",
        "Type",
    };
    public static String[][] Table() {
        String[][] atable=new String[VARIABLELIST.length][HEAD.length];
        for (int i=0;i<VARIABLELIST.length;i++) {
            atable[i][0]=VARIABLELIST[i][1];
            atable[i][1]=VARIABLELIST[i][0];
        }
        return atable;
    }
    
}
