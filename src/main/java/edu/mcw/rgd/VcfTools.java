package edu.mcw.rgd;


import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class VcfTools {

    public static void main(String[] args) throws IOException {

        if( args.length<3 ) {
            printUsageAndExit();
        }

        for( int i=0; i<args.length; i++ ) {

            if( args[i].equals("--ANNsplitter") ) {
                String inFileName = args[++i];
                String outFileName = args[++i];
                splitANNfield(inFileName, outFileName);
                return;
            }

            if( args[i].equals("--ChrRenamer") ) {
                String inFileName = args[++i];
                String outFileName = args[++i];
                renameChromosomes(inFileName, outFileName);
                return;
            }
        }

        printUsageAndExit();
    }

    static void printUsageAndExit() {
        System.out.println("VCF tools; currently available modules:");
        System.out.println("  --ANNsplitter <input .vcf or .vcf.gz file> <output .vcf or .vcf.gz file>");
        System.out.println("  analyzes ANN data in INFO field and splits it by ','");
        System.out.println("  --ChrRenamer <input .vcf or .vcf.gz file> <output .vcf or .vcf.gz file>");
        System.out.println("  replaces 'chr1, ...' with 'Chr1, ' and removes scaffold lines");
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

    //////

    static void renameChromosomes(String inFile, String outFile) throws IOException {

        BufferedReader in = openReader(inFile);
        BufferedWriter out = openWriter(outFile);

        int linesRead = 0;
        int linesWritten = 0;

        String line;
        while( (line = in.readLine())!=null ) {

            linesRead ++;

            // special header processing
            if( line.startsWith("#") ) {

                if( line.startsWith("##contig=<ID=") ) {
                    if( line.startsWith("##contig=<ID=chr") ) {
                        int commaPos = line.indexOf(',');
                        int chrNameLen = commaPos - "##contig=<ID=chr".length();
                        if( chrNameLen <= 3 ) {
                            out.write("##contig=<ID=Chr");
                            out.write(line.substring("##contig=<ID=chr".length()));
                            out.write("\n");
                            linesWritten++;
                        }
                    }
                    continue;
                }
                out.write(line);
                out.write("\n");
                linesWritten++;
                continue;
            }

            // data line
            if( line.startsWith("chr") ) {
                int tabPos = line.indexOf('\t');
                if( tabPos>0 && tabPos<6 ) {
                    out.write("C");
                    out.write(line.substring(1));
                    out.write("\n");
                    linesWritten++;
                } else {
                    continue; // skip 'chr1_unplaced' etc
                }
            } else {
                continue; // skip any other chromosomes
            }
        }


        in.close();
        out.close();

        System.out.println("lines read: "+linesRead);
        System.out.println("lines written: "+ linesWritten);
    }
}
