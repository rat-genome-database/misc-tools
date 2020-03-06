package edu.mcw.rgd;

import edu.mcw.rgd.dao.impl.GeneDAO;
import edu.mcw.rgd.dao.impl.OrthologDAO;
import edu.mcw.rgd.dao.impl.TranscriptDAO;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.mapping.MapManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class SynthenyFiles {

    GeneDAO geneDAO = new GeneDAO();
    OrthologDAO odao = new OrthologDAO();
    TranscriptDAO tdao = new TranscriptDAO();

    public static void main(String[] args) throws Exception {

        new SynthenyFiles().run();
    }

    void run() throws Exception {
        generateGff3ForRat();
        generateRatHumanOrthologs();
        generateRatMouseOrthologs();
        // TODO: Rat GO (OBO)
        // TODO: Rat GO terms to Rat genes (GAF2)
    }

    void generateGff3ForRat() throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter("data/Rat_GenomeFeature_for_syntheny.gff3"));
        out.write("##gff-version 3\n");

        List<MappedGene> genes = geneDAO.getActiveMappedGenes(360);
        System.out.println("generating gff3 file for syntheny for "+genes.size()+" genes");

        int i=0;
        for( MappedGene mg: genes ) {
            Gene g = mg.getGene();
            System.out.println(++i+"."+g.getSymbol()+" "+g.getType());

            Transcript t = getTranscript(g.getRgdId());

            if( t==null ) {
                // output mRNA line: f.e.
                // chr16	MGI	mRNA	31296192	31314808	.	-	.	ID=88056;Name=MGI_88056_Apod;Alias=Apod;Alias=MGI_88056;Dbxref=88056;Status=mRNA;Gene_Type=protein coding gene
                out.write("chr" + mg.getChromosome());
                out.write("\tRGD\tmRNA");
                out.write("\t" + mg.getStart() + "\t" + mg.getStop() + "\t.\t" + mg.getStrand() + "\t.");
                out.write("\tID=" + g.getRgdId());
                out.write(";Name=RGD_" + g.getRgdId() + "_" + g.getSymbol());
                out.write(";Alias=" + g.getSymbol());
                out.write(";Alias=RGD_" + g.getRgdId());
                out.write(";Dbxref=" + g.getRgdId());
                out.write(";Gene_Type=" + g.getType());
                out.write("\n");

                // transcript region -- export as a single exon
                out.write("chr" + mg.getChromosome());
                out.write("\tRGD\texon");
                out.write("\t" + mg.getStart() + "\t" + mg.getStop() + "\t.\t" + mg.getStrand() + "\t.");
                out.write("\tParent="+g.getRgdId());
                out.write("\n");
            } else {

                MapData pos = t.getGenomicPositions().get(0);

                // output mRNA line: f.e.
                //chr7	MGI	mRNA	28741934	28753879	.	+	.	ID=1919234;Name=MGI_1919234_Sars2;Alias=Sars2;Alias=MGI_1919234;Dbxref=1919234;Status=mRNA;Gene_Type=protein coding gene
                //chr7	MGI	exon	28741934	28742282	.	+	.	Parent=1919234;Status=mRNA
                //chr7	MGI	exon	28744242	28745756	.	+	.	Parent=1919234;Status=mRNA
                out.write("chr" + pos.getChromosome());
                out.write("\tRGD\tmRNA");
                out.write("\t" + pos.getStartPos() + "\t" + pos.getStopPos() + "\t.\t" + pos.getStrand() + "\t.");
                out.write("\tID=" + g.getRgdId());
                out.write(";Name=RGD_" + g.getRgdId() + "_" + g.getSymbol());
                out.write(";Alias=" + g.getSymbol());
                out.write(";Alias=RGD_" + g.getRgdId());
                out.write(";Dbxref=" + g.getRgdId());
                out.write(";Gene_Type=" + g.getType());
                out.write("\n");

                // exons
                List<TranscriptFeature> exons = tdao.getFeatures(t.getRgdId(), TranscriptFeature.FeatureType.EXON);
                for( TranscriptFeature exon: exons ) {
                    out.write("chr" + mg.getChromosome());
                    out.write("\tRGD\texon");
                    out.write("\t" + exon.getStartPos() + "\t" + exon.getStopPos() + "\t.\t" + exon.getStrand() + "\t.");
                    out.write("\tParent=" + g.getRgdId());
                    out.write("\n");
                }
            }
        }

        out.close();
    }

    Transcript getTranscript(int geneRgdId) throws Exception {
        List<Transcript> tlist = tdao.getTranscriptsForGene(geneRgdId, 360);
        if( tlist.isEmpty() ) {
            return null;
        }
        if( tlist.size()==1 ) {
            return tlist.get(0);
        }
        // look for NM_ NR_ transcripts
        for( Transcript t: tlist ) {
            if( t.getAccId().startsWith("N") ) {
                return t;
            }
        }
        // return any transcript
        return tlist.get(0);
    }

    void generateRatHumanOrthologs() throws Exception {
        generateOrthologs(SpeciesType.RAT, SpeciesType.HUMAN);

        System.out.println("rat-human-orthologs generated");
    }

    void generateRatMouseOrthologs() throws Exception {
        generateOrthologs(SpeciesType.RAT, SpeciesType.MOUSE);

        System.out.println("rat-mouse-orthologs generated");
    }

    void generateOrthologs(int speciesTypeKey1, int speciesTypeKey2) throws Exception {

        int sp1 = speciesTypeKey1;
        int sp2 = speciesTypeKey2;

        String spName1 = SpeciesType.getCommonName(sp1);
        String spName2 = SpeciesType.getCommonName(sp2);
        String fname = "data/"+spName1+spName2+"Homologs.tsv";
        BufferedWriter out = new BufferedWriter(new FileWriter(fname));

        out.write("##Type\tTaxonID1\tRgdID1\tSymbol1\tSeqID1\tStart1\tEnd1\tStrand1\tTaxonID2\tRgdID2\tSymbol2\tSeqID2\tStart2\tEnd2\tStrand2\n");

        int mapKey1 = MapManager.getInstance().getReferenceAssembly(sp1).getKey();
        int mapKey2 = MapManager.getInstance().getReferenceAssembly(sp2).getKey();
        int taxonId1 = SpeciesType.getTaxonomicId(sp1);
        int taxonId2 = SpeciesType.getTaxonomicId(sp2);

        List<MappedOrtholog> orthos = odao.getAllMappedOrthologs(sp1, sp2, mapKey1, mapKey2);
        for( MappedOrtholog o: orthos ) {
            out.write("orthologue");
            out.write("\t"+taxonId1);
            out.write("\t"+o.getSrcRgdId());
            out.write("\t"+o.getSrcGeneSymbol());
            out.write("\t"+o.getSrcChromosome());
            out.write("\t"+o.getSrcStartPos());
            out.write("\t"+o.getSrcStopPos());
            out.write("\t"+o.getSrcStrand()+"1");

            out.write("\t"+taxonId2);
            out.write("\t"+o.getDestRgdId());
            out.write("\t"+o.getDestGeneSymbol());
            out.write("\t"+o.getDestChromosome());
            out.write("\t"+o.getDestStartPos());
            out.write("\t"+o.getDestStopPos());
            out.write("\t"+o.getDestStrand()+"1");

            out.write("\n");
        }
        out.close();
    }
}
