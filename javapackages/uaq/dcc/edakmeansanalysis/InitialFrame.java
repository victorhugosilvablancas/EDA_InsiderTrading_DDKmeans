package uaq.dcc.edakmeansanalysis;

import uaq.dcc.datasets.DrillDownFrame;
import uaq.dcc.ploteo.PloteoFrame;
import uaq.dcc.oracle.OracleInstanceFrame;

/**
 *
 * Initialization frame for execution.
 */
public class InitialFrame extends javax.swing.JFrame {
    private Tools l=new Tools();

    /**
     * Creates new form InitialFrame
     */
    public InitialFrame() {
        initComponents();
        setLocation(0,0);
        ShowMenus();
    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jMenuBar1 = new javax.swing.JMenuBar();
        pFuncionalities = new javax.swing.JMenu();
        iOracle = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JPopupMenu.Separator();
        iDrillDown = new javax.swing.JMenuItem();
        jSeparator2 = new javax.swing.JPopupMenu.Separator();
        jMenu1 = new javax.swing.JMenu();
        iTransactionType = new javax.swing.JMenuItem();
        iTransactionShares = new javax.swing.JMenuItem();
        iPricePerShare = new javax.swing.JMenuItem();
        iExerciseDateFunction = new javax.swing.JMenuItem();
        iSecurityTitle = new javax.swing.JMenuItem();
        iOwnership = new javax.swing.JMenuItem();
        iEquitySwaps = new javax.swing.JMenuItem();
        pTools = new javax.swing.JMenu();
        iAbout = new javax.swing.JMenuItem();
        jSeparator5 = new javax.swing.JPopupMenu.Separator();
        iQuit = new javax.swing.JMenuItem();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("EdaKmeansFrame");

        pFuncionalities.setText("Functionalities");
        pFuncionalities.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N

        iOracle.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iOracle.setText("Oracle Instance");
        iOracle.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iOracleActionPerformed(evt);
            }
        });
        pFuncionalities.add(iOracle);
        pFuncionalities.add(jSeparator1);

        iDrillDown.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iDrillDown.setText("Drill-Down Analysis");
        iDrillDown.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iDrillDownActionPerformed(evt);
            }
        });
        pFuncionalities.add(iDrillDown);
        pFuncionalities.add(jSeparator2);

        jMenu1.setText("K-means Classification");
        jMenu1.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N

        iTransactionType.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iTransactionType.setText("Transaction Type");
        iTransactionType.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iTransactionTypeActionPerformed(evt);
            }
        });
        jMenu1.add(iTransactionType);

        iTransactionShares.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iTransactionShares.setText("Transaction Shares");
        iTransactionShares.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iTransactionSharesActionPerformed(evt);
            }
        });
        jMenu1.add(iTransactionShares);

        iPricePerShare.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iPricePerShare.setText("Price per Share");
        iPricePerShare.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iPricePerShareActionPerformed(evt);
            }
        });
        jMenu1.add(iPricePerShare);

        iExerciseDateFunction.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iExerciseDateFunction.setText("Exercise Date Function");
        iExerciseDateFunction.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iExerciseDateFunctionActionPerformed(evt);
            }
        });
        jMenu1.add(iExerciseDateFunction);

        iSecurityTitle.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iSecurityTitle.setText("Security Title");
        iSecurityTitle.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iSecurityTitleActionPerformed(evt);
            }
        });
        jMenu1.add(iSecurityTitle);

        iOwnership.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iOwnership.setText("Direct or Indirect Ownership");
        iOwnership.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iOwnershipActionPerformed(evt);
            }
        });
        jMenu1.add(iOwnership);

        iEquitySwaps.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iEquitySwaps.setText("Equity Swaps");
        iEquitySwaps.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iEquitySwapsActionPerformed(evt);
            }
        });
        jMenu1.add(iEquitySwaps);

        pFuncionalities.add(jMenu1);

        jMenuBar1.add(pFuncionalities);

        pTools.setText("Tools");
        pTools.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N

        iAbout.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iAbout.setText("About...");
        iAbout.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iAboutActionPerformed(evt);
            }
        });
        pTools.add(iAbout);
        pTools.add(jSeparator5);

        iQuit.setFont(new java.awt.Font("Segoe UI", 0, 18)); // NOI18N
        iQuit.setText("Quit");
        iQuit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                iQuitActionPerformed(evt);
            }
        });
        pTools.add(iQuit);

        jMenuBar1.add(pTools);

        setJMenuBar(jMenuBar1);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 9, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void iOracleActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iOracleActionPerformed
        new OracleInstanceFrame().setVisible(true);
    }//GEN-LAST:event_iOracleActionPerformed

    private void iDrillDownActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iDrillDownActionPerformed
        new DrillDownFrame().setVisible(true);
    }//GEN-LAST:event_iDrillDownActionPerformed

    private void iTransactionTypeActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iTransactionTypeActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_TRANSACTIONS).setVisible(true);
    }//GEN-LAST:event_iTransactionTypeActionPerformed

    private void iAboutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iAboutActionPerformed
        new MessageFrame(Tools.ABOUT_TEXT).setVisible(true);
    }//GEN-LAST:event_iAboutActionPerformed

    private void iQuitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iQuitActionPerformed
        System.exit(0);
    }//GEN-LAST:event_iQuitActionPerformed

    private void iTransactionSharesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iTransactionSharesActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_SHARES).setVisible(true);
    }//GEN-LAST:event_iTransactionSharesActionPerformed

    private void iPricePerShareActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iPricePerShareActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_PRICE).setVisible(true);
    }//GEN-LAST:event_iPricePerShareActionPerformed

    private void iExerciseDateFunctionActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iExerciseDateFunctionActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_EXERCISE).setVisible(true);
    }//GEN-LAST:event_iExerciseDateFunctionActionPerformed

    private void iSecurityTitleActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iSecurityTitleActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_TITLES).setVisible(true);
    }//GEN-LAST:event_iSecurityTitleActionPerformed

    private void iOwnershipActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iOwnershipActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_OWNERSHIP).setVisible(true);
    }//GEN-LAST:event_iOwnershipActionPerformed

    private void iEquitySwapsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_iEquitySwapsActionPerformed
        new PloteoFrame(PloteoFrame.HYPO_SWAPS).setVisible(true);
    }//GEN-LAST:event_iEquitySwapsActionPerformed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JMenuItem iAbout;
    private javax.swing.JMenuItem iDrillDown;
    private javax.swing.JMenuItem iEquitySwaps;
    private javax.swing.JMenuItem iExerciseDateFunction;
    private javax.swing.JMenuItem iOracle;
    private javax.swing.JMenuItem iOwnership;
    private javax.swing.JMenuItem iPricePerShare;
    private javax.swing.JMenuItem iQuit;
    private javax.swing.JMenuItem iSecurityTitle;
    private javax.swing.JMenuItem iTransactionShares;
    private javax.swing.JMenuItem iTransactionType;
    private javax.swing.JMenu jMenu1;
    private javax.swing.JMenuBar jMenuBar1;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JPopupMenu.Separator jSeparator5;
    private javax.swing.JMenu pFuncionalities;
    private javax.swing.JMenu pTools;
    // End of variables declaration//GEN-END:variables

    private void ShowMenus() {
        
    }
    
}
