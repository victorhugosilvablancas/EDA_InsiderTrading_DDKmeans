package uaq.dcc.edakmeansanalysis;

import uaq.dcc.datasets.Dataversos;
import uaq.dcc.datasets.EquitySwaps;
import uaq.dcc.datasets.Exercises;
import uaq.dcc.datasets.Ownership;
import uaq.dcc.datasets.PricePerShares;
import uaq.dcc.datasets.Shares;
import uaq.dcc.datasets.Titles;
import uaq.dcc.datasets.Transactiondata;
import uaq.dcc.datasets.Transactions;
/**
 *
 * @author Victor Hugo Silva Blancas
 * @institution Universidad Autónoma de Querétaro, México
 * @school Facultad de Informática
 * @course Doctorado en Ciencias de la Computación
 * @year 2024
 */
public class EdaKmeansAnalysis {

    public static void main(String[] args) {
        Librerias l=new Librerias();
        
        
        
        Dataversos datos=new Dataversos(uaq.dcc.datasets.Dataversos.HARVARD_INSIDERTRADING);
        //datos.getCampos();
        //datos.PonCampos();
        //datos.getDatos(100);
        //datos.PonDatos();
        //Dataversos.LaTeX=false;
        //datos.Select(new int[] {8,10,20,24,34,38});
        /*
        String linea="\"Contingent Warrant, when issued\",Common Stock,,30827,,\"Hola,hoy\",,30827,\"F2,F4\"";
        System.out.println(linea);
        System.out.println(Registrosdata.QuitarComillas(linea));
        */
        /*
        //l.ComandoSQL("drop table "+Transactiondata.TABLA);
        Transactions tran=new Transactions();
        //tran.getDatos();
        tran.getLista();
        tran.getValorTotal();
        //tran.PonTablaLaTex();
        tran.PonTabla();
        
        
        Shares share=new Shares();
        //share.getDatos();
        share.getLista();
        //share.PonTablaLaTeX();
        share.PonTabla();
        
        
        //l.ComandoSQL("drop table "+PricePerSharesdata.TABLA);
        PricePerShares pps=new PricePerShares();
        //pps.getDatos();
        pps.getLista();
        pps.PonTablaLaTeX();
        
        
        //l.ComandoSQL("drop table "+Exercisesdata.TABLA+"");
        Exercises exer=new Exercises();
        //exer.getDatos();
        exer.getLista();
        //exer.PonTablaLaTeX();
        exer.PonTabla();
        
        */
        //l.ComandoSQL("drop table "+Titlesdata.TABLA+"");
        Titles title=new Titles();
        //title.getDatos();
        title.getLista();
        title.PonTabla(true);
        
        /*
        //l.ComandoSQL("drop table "+Ownershipdata.TABLA+"");
        Ownership own=new Ownership();
        //own.getDatos();
        own.PonTabla(false);
        
        //l.ComandoSQL("drop table "+EquitySwapsdata.TABLA+"");
        EquitySwaps swaps=new EquitySwaps();
        //swaps.getDatos();
        swaps.PonTabla(true);
*/
    }
}
