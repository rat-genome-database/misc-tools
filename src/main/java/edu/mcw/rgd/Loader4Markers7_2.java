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

        int version = 2;
        generateFastaForMarkers(version);

        //loadPositions(args);
    }

    /** load positions for markers on 7.2 assembly
     *  Apr 2021
     *
     *  file format: (sample few lines)
     *       psLayout
     *
     *      ---------------------------------------------------------------------------------------------------------------------------------------------------------------
     * 10048_1 chr4 + 154902669 154902892 223
     * 10048_2 chr4 + 154902669 154902892 223
     *
     * marker_rgd_id followed by locus nr, chr, strand, start_pos, stop_pos, length
     */
    public static void loadPositions(String[] args) throws Exception {

        final int mapKey = 372; // rat 7.2 assembly
        final String srcPipeline = "MAPPER";

        MapDAO mapDAO = new MapDAO();

        int positionsProcessed = 0;
        int positionsInserted = 0;
        int positionsDuplicated = 0;
        int unmappedChromosomes = 0;

        String fname = "rn7_markers.csv";
        BufferedReader in = Utils.openReader(fname);
        String line;
        while( (line=in.readLine())!=null ) {
            String[] cols = line.split("[ ]+");
            // it must be exactly 6 columns
            if( cols.length!=6 ) {
                continue;
            }
            positionsProcessed++;

            String markerRgdId = cols[0];
            String chrStr = cols[1];
            String strand = cols[2];
            int startPos = Integer.parseInt(cols[3]);
            int stopPos = Integer.parseInt(cols[4]);
            int len = Integer.parseInt(cols[5]);

            // strip 'chr' prefix
            String chr = chrStr.substring(3);
            if( chr.length()>2 ) {
                unmappedChromosomes++;
                continue;
            }

            // strip locus from marker rgd id
            int underscorePos = markerRgdId.indexOf('_');
            int rgdId = Integer.parseInt(markerRgdId.substring(0, underscorePos));

            MapData md = new MapData();
            md.setRgdId(rgdId);
            md.setMapKey(mapKey);
            md.setChromosome(chr);
            md.setStartPos(startPos);
            md.setStopPos(stopPos);
            md.setStrand(strand);
            md.setSrcPipeline(srcPipeline);

            if( insertIfNew(md, mapDAO, srcPipeline) ) {
                positionsInserted++;
            } else {
                positionsDuplicated++;
            }
        }
        in.close();

        System.out.println("positions processed: "+positionsProcessed);
        System.out.println("positions inserted: "+positionsInserted);
        System.out.println("positions duplicated: "+positionsDuplicated);
        System.out.println("scaffold positions skipped from  loading: "+unmappedChromosomes);
    }

    /// return true if inserted; false otherwise
    static boolean insertIfNew(MapData mdForInsert, MapDAO mapDao, String srcPipeline) throws Exception {

        List<MapData> mdsInRgd = mapDao.getMapData(mdForInsert.getRgdId(), mdForInsert.getMapKey(), srcPipeline);
        for( MapData md: mdsInRgd ) {
            if( md.equalsByGenomicCoords(mdForInsert) ) {
                return false;
            }
        }

        mapDao.insertMapData(mdForInsert);
        return true;
    }

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
    public static void testPositions(String[] args) throws Exception {

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

    static void generateFastaForMarkers(int version) throws Exception {

        SSLPDAO sslpdao = new SSLPDAO();

        int speciesTypeKey = SpeciesType.RAT;

        List<SSLP> sslps = sslpdao.getActiveSSLPs(speciesTypeKey);
        Collections.sort(sslps, new Comparator<SSLP>() {
            @Override
            public int compare(SSLP s1, SSLP s2) {
                return s1.getRgdId() - s2.getRgdId();
            }
        });

        if( version==2 ) {
            generateFastaForMarkers2(sslps);
        } else {
            generateFastaForMarkers(360, "rn6", sslps);
            generateFastaForMarkers(60, "rn3_4", sslps);
        }

        System.out.println("DONE!");
        System.exit(0);
    }

    static void generateFastaForMarkers(int mapKey, String assembly, List<SSLP> markers) throws Exception {

        BufferedWriter out1 = new BufferedWriter(new FileWriter(assembly+"_markers.fa"));
        BufferedWriter out2 = new BufferedWriter(new FileWriter(assembly+"_markers_short.fa"));

        MapDAO mapDAO = new MapDAO();

        int s = 0;
        for( SSLP sslp: markers ) {
            s++;
            System.out.println(s+"/"+markers.size()+". "+sslp.getName());

            List<MapData> mds = mapDAO.getMapData(sslp.getRgdId(), mapKey);
            int i = 1;
            for( MapData md: mds ) {

                // skip loci longer than 10,000 bases
                int seqLen = Math.abs(md.getStopPos() - md.getStartPos()) + 1;
                if( seqLen >= 10000 ) {
                    continue;
                }
                String seq = getFastaSeq(mapKey, md.getChromosome(), md.getStartPos(), md.getStopPos());

                BufferedWriter out = seq.length() < 20 ? out2 : out1;

                // write fasta
                String header = ">"+md.getRgdId()+"_"+i+" chr"+md.getChromosome()+":"+md.getStartPos()+".."+md.getStopPos();

                writeFasta( header, seq, out );

                i++;
            }
        }

        out1.close();
        out2.close();
    }

    static void generateFastaForMarkers2(List<SSLP> markers) throws Exception {

        String fname1 = "rat_sequenced_markers.fa";
        String fname2 = "rat_invalid_markers.fa"; // sequenced markers where template sequence is not valid fasta characters
        String fname3 = "rat_markers_primers.fa";
        BufferedWriter out1 = Utils.openWriter(fname1);
        BufferedWriter out2 = Utils.openWriter(fname2);
        BufferedWriter out3 = Utils.openWriter(fname3);

        int cnt1 = 0;
        int cnt2 = 0;
        int cnt3 = 0;

        for( SSLP sslp: markers ) {

            // export sequence_template if available
            if( sslp.getTemplateSeq()!=null ) {

                String header = ">"+sslp.getRgdId()+"_"+sslp.getName()+"_"+sslp.getTemplateSeq().length();

                boolean isTemplateSeqValidFasta = true;
                for( int i=0; i<sslp.getTemplateSeq().length(); i++ ) {

                    char c = sslp.getTemplateSeq().charAt(i);
                    boolean isValidFasta = c=='A' || c=='C' || c=='G' || c=='T' || c=='N'
                                        || c=='a' || c=='c' || c=='g' || c=='t' || c=='n';
                    if( !isValidFasta ) {
                        isTemplateSeqValidFasta = false;
                        break;
                    }
                }

                if( isTemplateSeqValidFasta ) {

                    writeFasta( header, sslp.getTemplateSeq(), out1 );
                    cnt1 ++;

                } else {

                    writeFasta( header, sslp.getTemplateSeq(), out2 );
                    cnt2 ++;
                }
            }

            // export primers
            if( Utils.defaultString(sslp.getForwardSeq()).length() > 0
             && Utils.defaultString(sslp.getReverseSeq()).length() > 0 ) {

                int expectedSize = sslp.getExpectedSize() == null ? 0 : sslp.getExpectedSize();
                String header1 = ">" + sslp.getRgdId() + "_" + sslp.getName() + "_" + expectedSize + "_1";
                String header2 = ">" + sslp.getRgdId() + "_" + sslp.getName() + "_" + expectedSize + "_2";
                writeFasta(header1, sslp.getForwardSeq(), out3);
                writeFasta(header2, sslp.getReverseSeq(), out3);

                cnt3 ++;
            }
        }

        out1.close();
        out2.close();
        out3.close();

        System.out.println("active rat markers processed: "+markers.size());
        System.out.println("written "+cnt1+" valid markers to file "+fname1);
        System.out.println("written "+cnt2+" invalid markers to file "+fname2);
        System.out.println("written "+cnt3+" primer pairs to file "+fname3);
    }

    static void writeFasta( String header, String seq, BufferedWriter out ) throws IOException {

        // write fasta
        out.write(header+"\n");

        while( seq.length()>80 ) {
            out.write(seq.substring(0, 80)+"\n");
            seq = seq.substring(80);
        }
        if( seq.length()>0 ) {
            out.write(seq+"\n");
        }
    }

    static String getFastaSeq(int mapKey, String chr, int startPos, int stopPos) throws Exception {
        String url = "https://pipelines.rgd.mcw.edu/rgdweb/seqretrieve/retrieve.html?mapKey="+mapKey+"&chr="+chr+"&startPos="+startPos+"&stopPos="+stopPos+"&format=text";
        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(url);
        String s = fd.download();
        return s;
    }
}
