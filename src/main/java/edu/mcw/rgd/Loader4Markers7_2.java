package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.dao.impl.SSLPDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.SSLP;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/** load positions for markers on 7.2 assembly
 *  Apr 2021
 *
 *  file format: (sample few lines)
 *       psLayout
 *
 *      ---------------------------------------------------------------------------------------------------------------------------------------------------------------
 * D7Rat177 CM026980.1 - 74647624 74647814 190
 * D16Rat27 CM026989.1 - 57693912 57694346 430
 * D2Rat380 CM026977.1 - 151945555 151945587 32
 * D2Rat380 CM026989.1 + 7125655 7125707 48
 */
public class Loader4Markers7_2 {

    public static void main(String[] args) throws Exception {

        generateFastaForMarkers();

        // map genebank chr id to chr symbol, f.e. 'CM026984.1' -> '11'
        Map<String,String> chrMap = new HashMap<>();

        MapDAO mapDAO = new MapDAO();
        List<Chromosome> chromosomes = mapDAO.getChromosomes(372);
        for( Chromosome c: chromosomes ) {
            chrMap.put(c.getGenbankId(), c.getChromosome());
        }

        SSLPDAO sslpDao = new SSLPDAO();

        int positionsProcessed = 0;
        int positionsInserted = 0;
        int unmappedChromosomes = 0;

        String fname = "/tmp/markers_mRatBN7.2.csv";
        BufferedReader in = Utils.openReader(fname);
        String line;
        while( (line=in.readLine())!=null ) {
            String[] cols = line.split("[ ]+");
            // it must be exactly 6 columns
            if( cols.length!=6 ) {
                continue;
            }
            positionsProcessed++;

            String markerSymbol = cols[0];
            String genebankChr = cols[1];
            String strand = cols[2];
            int startPos = Integer.parseInt(cols[3]);
            int stopPos = Integer.parseInt(cols[4]);
            int len = Integer.parseInt(cols[5]);

            String chr = chrMap.get(genebankChr);
            if( chr==null ) {
                System.out.println("cannot map chromosome");
                continue;
            }
            List<SSLP> sslps = sslpDao.getActiveSSLPsByName(markerSymbol, 3);
            if( sslps.size()!=1 ) {
                System.out.println("cannot map sslp name");
                continue;
            }

            positionsInserted++;
        }
        in.close();

        System.out.println("positions processed: "+positionsProcessed);
        System.out.println("positions inserted: "+positionsInserted);
    }

    static void generateFastaForMarkers() throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter("rn6_markers.fa"));

        SSLPDAO sslpdao = new SSLPDAO();
        MapDAO mapDAO = new MapDAO();

        int speciesTypeKey = SpeciesType.RAT;
        int mapKey = 360;

        List<SSLP> sslps = sslpdao.getActiveSSLPs(speciesTypeKey);
        Collections.sort(sslps, new Comparator<SSLP>() {
            @Override
            public int compare(SSLP s1, SSLP s2) {
                return s1.getRgdId() - s2.getRgdId();
            }
        });

        for( SSLP sslp: sslps ) {

            List<MapData> mds = mapDAO.getMapData(sslp.getRgdId(), mapKey);
            int i = 1;
            for( MapData md: mds ) {
                String seq = getFastaSeq(mapKey, md.getChromosome(), md.getStartPos(), md.getStopPos());

                // write fasta
                out.write(">"+md.getRgdId()+"_"+i+"\n");
                while( seq.length()>64 ) {
                    out.write(seq.substring(0, 64)+"\n");
                    seq = seq.substring(64);
                }
                if( seq.length()>0 ) {
                    out.write(seq+"\n");
                }

                i++;
            }
        }

        out.close();
    }

    static String getFastaSeq(int mapKey, String chr, int startPos, int stopPos) throws Exception {
        String url = "https://pipelines.rgd.mcw.edu/rgdweb/seqretrieve/retrieve.html?mapKey="+mapKey+"&chr="+chr+"&startPos="+startPos+"&stopPos="+stopPos+"&format=text";
        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(url);
        String s = fd.download();
        return s;
    }
}
