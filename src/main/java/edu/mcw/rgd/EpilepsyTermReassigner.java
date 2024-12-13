package edu.mcw.rgd;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;

public class EpilepsyTermReassigner {

    public static void main(String[] args) throws Exception {

        AbstractDAO dao = new AbstractDAO();

        //Gene symbol	Gene RGDID	old term	old term ID	new term	new term ID	reference RGDID
        //Gria2	61862	visual epilepsy	DOID:11832 	epilepsy	DOID:1826	737715

        String fname = "/tmp/d/epilepsy.txt";
        BufferedReader in = Utils.openReader(fname);
        String header = in.readLine();
        String line;

        while( (line=in.readLine())!=null ) {

            String[] fields = line.split("[\\t]", -1);
            int geneRgdId = Integer.parseInt(fields[1]);
            String oldTermAcc = fields[3].trim();
            String newTermAcc = fields[5].trim();
            int refRgdId = Integer.parseInt(fields[6].trim());

            String sql = "UPDATE full_annot SET term_acc=?,last_modified_date=SYSDATE,last_modified_by=26 "+
                    "WHERE annotated_object_rgd_id=? AND term_acc=? AND ref_rgd_id=?";
            int r = dao.update(sql, newTermAcc, geneRgdId, oldTermAcc, refRgdId);
            System.out.println("updated "+r+": newTermAcc="+newTermAcc+", geneRgdId="+geneRgdId+", oldTermAcc="+oldTermAcc+", refRgdId="+refRgdId);
        }

        in.close();

        System.out.println("done");
    }
}
