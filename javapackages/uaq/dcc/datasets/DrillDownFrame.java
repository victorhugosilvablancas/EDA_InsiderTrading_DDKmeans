package uaq.dcc.datasets;

import javax.swing.table.DefaultTableModel;
import java.awt.datatransfer.StringSelection;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.event.KeyEvent;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JTable;
import uaq.dcc.edakmeansanalysis.MessageFrame;
import uaq.dcc.edakmeansanalysis.Tools;

/**
 *
 * Frame for user interaction
 */
public class DrillDownFrame extends javax.swing.JFrame {
    private Transactionstype type;
    private Shares share;
    private PricePerShares pps;
    private Exercise exer;
    private Titles title;
    private Ownership own;
    private EquitySwaps swaps;
    
    private Tools l=new Tools();

    /**
     * Class that defines user interaction frame
     */
    public DrillDownFrame() {
        initComponents();
        setLocation(200,120);
        
        type=new Transactionstype();
        share=new Shares();
        pps=new PricePerShares();
        exer=new Exercise();
        title=new Titles();
        own=new Ownership();
        swaps=new EquitySwaps();
        
        //l.SQLcommand(Exercisesdata.DROP_TABLE);
        //l.SQLcommand(Exercisesdata.CREATE_TABLE);
            
        GetLists();
        ShowTablesDD();
    }
    
    private void ShowTablesDD() {
        String[] head=Transactionstypedata.Head().split(Tools.TAB);
        
        tablatransactions.setModel(new DefaultTableModel(
                type.DrawTable(),
                head));
        
        head=Sharesdata.Head().split(Tools.TAB);
        tablashares.setModel(new DefaultTableModel(
                share.DrawTable(),
                head
                ));
        
        head=PricePerSharesdata.Head().split(Tools.TAB);
        tablaprice.setModel(new DefaultTableModel(
                pps.DrawTable(),
                head
                ));
        
        head=Exercisesdata.Head().split(Tools.TAB);
        tablaexercises.setModel(new DefaultTableModel(
                exer.DrawTable(),
                head
                ));
        
        head=Titlesdata.Head().split(Tools.TAB);
        tablatitles.setModel(new DefaultTableModel(
                title.DrawTable(),
                head
                ));
        
        head=Ownershipdata.Head().split(Tools.TAB);
        tablaownership.setModel(new DefaultTableModel(
                own.DrawTable(),
                head
                ));
        
        head=EquitySwapsdata.Head().split(Tools.TAB);
        tablaequity.setModel(new DefaultTableModel(
                swaps.DrawTable(),
                head
                ));
    }
    private void GetLists() {
        type.getList();
        share.getList();
        pps.getList();
        exer.getList();
        title.getList();
        own.getList();
        swaps.getList();
        
        String[] alista=new String[8];
        for (int i=0;i<8;i++)
            alista[i]=tabulador.getTitleAt(i);
        
        exporttolatex.setModel(new DefaultComboBoxModel(alista));
        exporttolatex.setSelectedIndex(-1);
        
        tablavariables.setModel(new DefaultTableModel(
                FileRecordsdata.Table(),
                FileRecordsdata.HEAD
                ));
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        pTodo = new javax.swing.JPanel();
        pcabeza = new javax.swing.JPanel();
        exporttolatexLb = new javax.swing.JLabel();
        exporttolatex = new javax.swing.JComboBox<>();
        tabulador = new javax.swing.JTabbedPane();
        jScrollPane8 = new javax.swing.JScrollPane();
        tablavariables = new javax.swing.JTable();
        jScrollPane1 = new javax.swing.JScrollPane();
        tablatransactions = new javax.swing.JTable();
        jScrollPane2 = new javax.swing.JScrollPane();
        tablashares = new javax.swing.JTable();
        jScrollPane3 = new javax.swing.JScrollPane();
        tablaprice = new javax.swing.JTable();
        jScrollPane4 = new javax.swing.JScrollPane();
        tablaexercises = new javax.swing.JTable();
        jScrollPane5 = new javax.swing.JScrollPane();
        tablatitles = new javax.swing.JTable();
        jScrollPane6 = new javax.swing.JScrollPane();
        tablaownership = new javax.swing.JTable();
        jScrollPane7 = new javax.swing.JScrollPane();
        tablaequity = new javax.swing.JTable();
        pTools = new javax.swing.JPanel();
        RestoreAllLists = new javax.swing.JButton();
        maxrecordsLb = new javax.swing.JLabel();
        maxrecords = new javax.swing.JTextField();
        jScrollPane9 = new javax.swing.JScrollPane();
        report = new javax.swing.JTextArea();

        setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        setTitle("Drill-Down Analysis");

        pcabeza.setBorder(javax.swing.BorderFactory.createEtchedBorder());

        exporttolatexLb.setText("Export to LaTeX:");

        exporttolatex.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Item 1", "Item 2", "Item 3", "Item 4" }));
        exporttolatex.addPopupMenuListener(new javax.swing.event.PopupMenuListener() {
            public void popupMenuCanceled(javax.swing.event.PopupMenuEvent evt) {
            }
            public void popupMenuWillBecomeInvisible(javax.swing.event.PopupMenuEvent evt) {
                exporttolatexPopupMenuWillBecomeInvisible(evt);
            }
            public void popupMenuWillBecomeVisible(javax.swing.event.PopupMenuEvent evt) {
            }
        });

        javax.swing.GroupLayout pcabezaLayout = new javax.swing.GroupLayout(pcabeza);
        pcabeza.setLayout(pcabezaLayout);
        pcabezaLayout.setHorizontalGroup(
            pcabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pcabezaLayout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(exporttolatexLb)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(exporttolatex, javax.swing.GroupLayout.PREFERRED_SIZE, 125, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );
        pcabezaLayout.setVerticalGroup(
            pcabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pcabezaLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pcabezaLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(exporttolatexLb)
                    .addComponent(exporttolatex, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(7, Short.MAX_VALUE))
        );

        tablavariables.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane8.setViewportView(tablavariables);

        tabulador.addTab("lit_deriv fields", jScrollPane8);

        tablatransactions.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane1.setViewportView(tablatransactions);

        tabulador.addTab("Transactions", jScrollPane1);

        tablashares.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane2.setViewportView(tablashares);

        tabulador.addTab("Shares", jScrollPane2);

        tablaprice.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane3.setViewportView(tablaprice);

        tabulador.addTab("Price per Shares", jScrollPane3);

        tablaexercises.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane4.setViewportView(tablaexercises);

        tabulador.addTab("Exercices", jScrollPane4);

        tablatitles.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane5.setViewportView(tablatitles);

        tabulador.addTab("Titles", jScrollPane5);

        tablaownership.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane6.setViewportView(tablaownership);

        tabulador.addTab("Ownership", jScrollPane6);

        tablaequity.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null},
                {null, null, null, null}
            },
            new String [] {
                "Title 1", "Title 2", "Title 3", "Title 4"
            }
        ));
        jScrollPane7.setViewportView(tablaequity);

        tabulador.addTab("Equity Swaps", jScrollPane7);

        RestoreAllLists.setText("Restore all Lists");
        RestoreAllLists.setToolTipText("");
        RestoreAllLists.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                RestoreAllListsActionPerformed(evt);
            }
        });

        maxrecordsLb.setText("Max Records");

        maxrecords.setHorizontalAlignment(javax.swing.JTextField.RIGHT);
        maxrecords.setText("1000000");
        maxrecords.setToolTipText("Max 9999999");
        maxrecords.addFocusListener(new java.awt.event.FocusAdapter() {
            public void focusGained(java.awt.event.FocusEvent evt) {
                maxrecordsFocusGained(evt);
            }
        });
        maxrecords.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyPressed(java.awt.event.KeyEvent evt) {
                maxrecordsKeyPressed(evt);
            }
        });

        report.setColumns(20);
        report.setRows(5);
        jScrollPane9.setViewportView(report);

        javax.swing.GroupLayout pToolsLayout = new javax.swing.GroupLayout(pTools);
        pTools.setLayout(pToolsLayout);
        pToolsLayout.setHorizontalGroup(
            pToolsLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pToolsLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pToolsLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                    .addComponent(jScrollPane9)
                    .addGroup(pToolsLayout.createSequentialGroup()
                        .addComponent(maxrecordsLb)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(maxrecords, javax.swing.GroupLayout.PREFERRED_SIZE, 83, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(RestoreAllLists)))
                .addContainerGap(563, Short.MAX_VALUE))
        );
        pToolsLayout.setVerticalGroup(
            pToolsLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pToolsLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pToolsLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(RestoreAllLists)
                    .addComponent(maxrecordsLb)
                    .addComponent(maxrecords, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jScrollPane9, javax.swing.GroupLayout.DEFAULT_SIZE, 448, Short.MAX_VALUE)
                .addContainerGap())
        );

        tabulador.addTab("Tools", pTools);

        javax.swing.GroupLayout pTodoLayout = new javax.swing.GroupLayout(pTodo);
        pTodo.setLayout(pTodoLayout);
        pTodoLayout.setHorizontalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(tabulador)
                    .addComponent(pcabeza, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap())
        );
        pTodoLayout.setVerticalGroup(
            pTodoLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(pTodoLayout.createSequentialGroup()
                .addContainerGap()
                .addComponent(pcabeza, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(tabulador)
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pTodo, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(pTodo, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void RestoreAllListsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_RestoreAllListsActionPerformed
        if (maxrecords.getText().length()>7) 
            maxrecords.setText(maxrecords.getText().substring(0, 7));
        try {
            int imax=Integer.parseInt(maxrecords.getText());
            
            String areport="";
            type.getDataset();
            areport+=Transactionstypedata.TABLE+" complete\n";
            share.getDataset();
            areport+=Sharesdata.TABLE+" complete\n";
            pps.getDataset(imax);
            areport+=PricePerSharesdata.TABLE+" complete\n";
            exer.getDataset(imax);
            areport+=Exercisesdata.TABLE+" complete\n";
            title.getDataset(imax);
            areport+=Titlesdata.TABLE+" complete\n";
            own.getDataset(imax);
            areport+=Ownershipdata.TABLE+" complete\n";
            swaps.getDataset(imax);
            areport+=EquitySwapsdata.TABLE+" complete\n";
            GetLists();
            ShowTablesDD();
            report.setText(areport);
            new MessageFrame("Data Restored").setVisible(true);
        } catch (Exception e) {
            new MessageFrame("Invalid Number").setVisible(true);
        }
    }//GEN-LAST:event_RestoreAllListsActionPerformed

    private void exporttolatexPopupMenuWillBecomeInvisible(javax.swing.event.PopupMenuEvent evt) {//GEN-FIRST:event_exporttolatexPopupMenuWillBecomeInvisible
        ExportToLaTeX(exporttolatex.getSelectedIndex());
    }//GEN-LAST:event_exporttolatexPopupMenuWillBecomeInvisible

    private void maxrecordsFocusGained(java.awt.event.FocusEvent evt) {//GEN-FIRST:event_maxrecordsFocusGained
        maxrecords.selectAll();
    }//GEN-LAST:event_maxrecordsFocusGained

    private void maxrecordsKeyPressed(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_maxrecordsKeyPressed
        if (evt.getKeyCode()==KeyEvent.VK_ENTER) RestoreAllLists.requestFocus();
        if (evt.getKeyCode()==KeyEvent.VK_LEFT) RestoreAllLists.requestFocus();
    }//GEN-LAST:event_maxrecordsKeyPressed


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton RestoreAllLists;
    private javax.swing.JComboBox<String> exporttolatex;
    private javax.swing.JLabel exporttolatexLb;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JScrollPane jScrollPane3;
    private javax.swing.JScrollPane jScrollPane4;
    private javax.swing.JScrollPane jScrollPane5;
    private javax.swing.JScrollPane jScrollPane6;
    private javax.swing.JScrollPane jScrollPane7;
    private javax.swing.JScrollPane jScrollPane8;
    private javax.swing.JScrollPane jScrollPane9;
    private javax.swing.JTextField maxrecords;
    private javax.swing.JLabel maxrecordsLb;
    private javax.swing.JPanel pTodo;
    private javax.swing.JPanel pTools;
    private javax.swing.JPanel pcabeza;
    private javax.swing.JTextArea report;
    private javax.swing.JTable tablaequity;
    private javax.swing.JTable tablaexercises;
    private javax.swing.JTable tablaownership;
    private javax.swing.JTable tablaprice;
    private javax.swing.JTable tablashares;
    private javax.swing.JTable tablatitles;
    private javax.swing.JTable tablatransactions;
    private javax.swing.JTable tablavariables;
    private javax.swing.JTabbedPane tabulador;
    // End of variables declaration//GEN-END:variables

    /**
     * Exports to clipboard the selected table to LaTeX format.
     * @param ipage tab number
     */
    private void ExportToLaTeX(int ipage) {
        if (ipage>=0) {
            JTable mytable=new JTable();
            switch (ipage) {
                case 0: mytable=tablavariables; break;
                case 1: mytable=tablatransactions; break;
                case 2: mytable=tablashares; break;
                case 3: mytable=tablaprice; break;
                case 4: mytable=tablaexercises; break;
                case 5: mytable=tablatitles; break;
                case 6: mytable=tablaownership; break;
                case 7: mytable=tablaequity; break;
            }
            if (mytable.getRowCount()>0) {
                String atitle=tabulador.getTitleAt(ipage);

                int c=mytable.getColumnCount();
                int r=mytable.getRowCount();
                String columnsalign="";
                String columns="";
                for (int j=0;j<c;j++) {
                    columnsalign+="l ";
                    columns+="\\textbf{"+mytable.getColumnName(j)+"} ";
                    if (j==(c-1)) columns+="\\\\\n";
                    else columns+="& ";
                }
                String cade="\\begin{table}[H]\n"
                        + "\\caption{"+atitle+".\\label{tabla:"+atitle.replace(" ","")+"}}\n"
                        + "\\centering\n"
                        + "\\begin{tabular}{"+columnsalign+"}\n"
                        + "\\hline\n"
                        + columns
                        + "\\hline\n"
                        ;
                String acell="";
                for (int i=0;i<r;i++) {
                    for (int j=0;j<c;j++) {
                        acell=(String)mytable.getValueAt(i, j);
                        acell=acell.replace("%", "\\%");
                        cade+=acell;
                        if (j==(c-1)) cade+="\\\\\n";
                        else cade+="& ";
                    }
                }
                cade+="""
                      \\hline
                      \\end{tabular}
                      \\noindent{\\footnotesize{Source: Proposed work.}}
                      \\end{table}""";
                StringSelection stringSelection = new StringSelection(cade);
                Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
                clipboard.setContents(stringSelection, null);
                new MessageFrame("LaTeX formatted table is already in Clipboard.").setVisible(true);
            } else new MessageFrame("No data found.").setVisible(true);
        }
    }

}
