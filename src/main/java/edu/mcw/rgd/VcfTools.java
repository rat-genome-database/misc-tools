package edu.mcw.rgd;


import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class VcfTools {

    public static void main(String[] args) throws IOException {

        if( args.length<3 ) {
            printUsageAndExit();
        }

        if( !args[0].equals("--ANNsplitter") ) {
            printUsageAndExit();
        }

        String inFileName = args[1];
        String outFileName = args[2];
        splitANNfield(inFileName, outFileName);
    }

    static void printUsageAndExit() {
        System.out.println("VCF tools; currently available modules:");
        System.out.println("  --ANNsplitter <input .vcf or .vcf.gz file> <output .vcf or .vcf.gz file>");
        System.out.println("  analyzes ANN data in INFO field and splits it by ','");
        System.exit(-1);
    }

    static void splitANNfield(String inFileName, String outFileName) throws IOException {

        BufferedReader in = openReader(inFileName);
        BufferedWriter out = openWriter(outFileName);

        int originalLineCount = 0;
        int newLineCount = 0;

        String line;
        while( (line=in.readLine())!=null ) {
            originalLineCount++;

            // copy comments as they are
            if( line.startsWith("#") ) {
                out.write(line);
                out.write("\n");
                newLineCount++;
                continue;
            }

            int infoFieldStart = findInfoField(line);
            int infoFieldEnd = line.indexOf('\t', infoFieldStart+1);
            String infoField = line.substring(infoFieldStart, infoFieldEnd);

            // parse out ANN attribute
            int annStart = infoField.indexOf("ANN=");
            int annEnd = infoField.indexOf(";", annStart);
            String ann = infoField.substring(annStart+4, annEnd);

            // split ann field by ','
            String[] annChunks = ann.split(",");

            int annFieldPos = infoFieldStart + annStart + 4;
            int annFieldAfterPos = infoFieldStart + annEnd;
            for( String annChunk: annChunks ) {

                // how much to write until ann field
                String beforeAnn = line.substring(0, annFieldPos);
                String afterAnn = line.substring(annFieldAfterPos);
                out.write(beforeAnn+annChunk+afterAnn+"\n");
                newLineCount++;
            }

        }
        in.close();
        out.close();

        System.out.println("INPUT FILE: "+inFileName);
        System.out.println("  LINE COUNT: "+originalLineCount);
        System.out.println("OUTPUT FILE: "+outFileName);
        System.out.println("  LINE COUNT: "+newLineCount);
    }

    static int findInfoField(String line) {
        // #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	5217-DS-0001
        // INFO field is the eight field and we parse only this field
        int pos = -1;
        for( int i=0; i<7; i++ ) {
            pos = line.indexOf('\t', pos+1);
        }
        return pos+1;
    }

    static public BufferedReader openReader(String fileName) throws IOException {
        BufferedReader reader;
        if( fileName.endsWith(".gz") ) {
            reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName))));
        } else
            reader = new BufferedReader(new FileReader(fileName));
        return reader;
    }

    static public BufferedWriter openWriter(String fileName) throws IOException {
        BufferedWriter writer;
        if( fileName.endsWith(".gz") ) {
            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fileName))));
        } else
            writer = new BufferedWriter(new FileWriter(fileName));
        return writer;
    }
}
