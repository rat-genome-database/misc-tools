package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.datamodel.MappedGene;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

public class IntergenicGeneRegions {

    public static void main( String[] args ) throws Exception {

        int mapKey = 38;
        String species = "H";
        String outFilename = "hg38_intergenic_regions.txt";

        mapKey = 372;
        species = "R";
        outFilename = "rn7_intergenic_regions.txt";

        BufferedWriter out = new BufferedWriter(new FileWriter(outFilename));
        out.write("#chr\tstart\tend\tgene1\tgene2\tregion_length_bp\tregion_name\n");

        GeneDAO geneDao = new GeneDAO();
        MapDAO mapDAO = new MapDAO();
        List<Chromosome> chromosomes = mapDAO.getChromosomes(mapKey);
        for( Chromosome c: chromosomes ) {

            List<MappedGene> genes = geneDao.getActiveMappedGenes( c.getChromosome(), 1, Integer.MAX_VALUE, mapKey );
            System.out.println("mapped genes for chr "+c.getChromosome()+"    = "+genes.size());

            genes.removeIf( g -> !g.getGene().getType().equals("protein-coding") );
            System.out.println("mapped protein coding genes for chr "+c.getChromosome()+"    = "+genes.size());

            int regionStart = 0;
            String prevGroupSymbols = "0";

            List<MappedGene> overlappingGenes;
            while( (overlappingGenes = getOverlappingGenes(genes)) != null ) {

                int groupStart = 0;
                int groupEnd = 0;
                String groupSymbols = null;
                for( MappedGene g: overlappingGenes ) {
                    if( groupStart == 0 ) {
                        groupStart = (int)g.getStart();
                    } else if( g.getStart() < groupStart ) {
                        groupStart = (int)g.getStart();
                    }

                    if( groupEnd == 0 ) {
                        groupEnd = (int)g.getStop();
                    } else if( g.getStop() > groupEnd ) {
                        groupEnd = (int)g.getStop();
                    }

                    if( groupSymbols==null ) {
                        groupSymbols = g.getGene().getSymbol();
                    } else {
                        groupSymbols += "-"+g.getGene().getSymbol();
                    }
                }

                int regionEnd = groupStart-1;
                int regionSize = regionEnd-regionStart+1;

                if( regionStart>0 )
                out.write(
                    "chr"+c.getChromosome()+"\t"+
                    regionStart+"\t"+
                    regionEnd+"\t"+
                    prevGroupSymbols+"\t"+
                    groupSymbols+"\t"+
                    regionSize+"\t"+
                    prevGroupSymbols+"_"+groupSymbols+"_"+species+toNumInUnits(regionSize)+"\n"
                );

                // move to next intergenic region
                regionStart = groupEnd+1;
                prevGroupSymbols = groupSymbols;
            }

            out.write("===\n");
        }

        out.close();
        System.out.println("OK");
    }

    public static String toNumInUnits(long bytes) {
        if (bytes <= 1024)
            return String.format("%dB", bytes);
        int u = 0;
        for ( ; bytes > 1024*1024; bytes >>= 10) {
            u++;
        }
        return String.format("%.1f%cB", bytes/1024f, "kMGTPE".charAt(u));
    }

    static List<MappedGene> getOverlappingGenes( List<MappedGene> genes ) {

        if( genes.isEmpty() ) {
            return null;
        }

        List<MappedGene> result = new ArrayList<>();
        MappedGene g1 = genes.remove(0);
        result.add(g1);

        while( !genes.isEmpty() ) {
            MappedGene g2 = genes.get(0);
            if( g1.getStop() > g2.getStart()  &&  g2.getStop() > g1.getStart() ) {
                // overlapping gene
                genes.remove(0);
                result.add(g2);
            } else {
                break;
            }
        }

        return result;
    }
}
