package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.ProteinDAO;
import edu.mcw.rgd.dao.impl.SequenceDAO;
import edu.mcw.rgd.dao.impl.XdbIdDAO;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.Protein;
import edu.mcw.rgd.datamodel.Sequence;
import edu.mcw.rgd.datamodel.XdbId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;

public class ProteinStructures {

    static void t() throws Exception {

        ProteinDAO pdao = new ProteinDAO();
        SequenceDAO sdao = new SequenceDAO();
        XdbIdDAO xdao = new XdbIdDAO();
        Logger log = LogManager.getLogger("status");

        int proteinsSkipped = 0;
        int linesF1 = 0;
        int linesGood = 0;

        String dir = "/tmp/ws/human";
        int psKey = 70000;

        File fdir = new File(dir);
        File[] files = fdir.listFiles();
        for (File f : files) {
            if (f.isFile() && f.getName().endsWith(".pdb.gz")) {
                // protein acc is between dashes
                String fname = f.getName();
                String pureName = fname.substring(0, fname.length() - 7);
                int dashPos1 = fname.indexOf('-');
                int dashPos2 = fname.indexOf('-', dashPos1 + 1);
                int dashPos3 = fname.indexOf('-', dashPos2 + 1);
                int fragmentNr = Integer.parseInt(fname.substring(dashPos2 + 2, dashPos3));

                if (dashPos1 > 0 && dashPos2 > dashPos1) {
                    String proteinAcc = fname.substring(dashPos1 + 1, dashPos2);
                    List<Gene> genes = xdao.getActiveGenesByXdbId(XdbId.XDB_KEY_UNIPROT, proteinAcc);

                    String proteinAaRange;
                    Protein p = pdao.getProteinByUniProtId(proteinAcc);
                    if (p == null) {
                        proteinsSkipped++;
                        continue;
                    }
                    List<Sequence> seqs = sdao.getObjectSequences(p.getRgdId(), "uniprot_seq");
                    if (seqs.isEmpty()) {
                        proteinsSkipped++;
                        continue;
                    }
                    Sequence seq = seqs.get(0);
                    String seqData = seq.getSeqData();
                    int proteinLen = seqData.length();
                    if (fragmentNr == 1) {
                        if (proteinLen > 2700) {
                            proteinAaRange = "1-1400";
                        } else {
                            proteinAaRange = "1-" + proteinLen;
                        }
                        linesF1++;
                    } else {
                        // extract fragment nr
                        int start = 200 * (fragmentNr - 1) + 1;
                        int end = start + 1399;
                        if (end > proteinLen) {
                            end = proteinLen;
                        }
                        proteinAaRange = start + "-" + end;
                    }
                    linesGood++;

                    for (Gene g : genes) {
                        log.debug(psKey + " " + g.getSymbol() + " RGD:" + g.getRgdId() + " " + proteinAcc);
                        String sql = "INSERT INTO protein_structures (ps_key,name,modeller) VALUES(?,?,'AlphaFold')";
                        xdao.update(sql, psKey, pureName);
                        String sql2 = "INSERT INTO protein_structure_genes (rgd_id,ps_key,protein_acc_id,protein_aa_range) VALUES(?,?,?,?)";
                        xdao.update(sql2, g.getRgdId(), psKey, proteinAcc, proteinAaRange);
                        psKey++;
                    }

                }
            }
        }

        System.out.println("proteins skipped " + proteinsSkipped);
        System.out.println("proteins good    " + linesGood);
        System.out.println("proteins F1      " + linesF1);

        System.exit(0);
    }
}
