package edu.mcw.rgd;

import java.io.IOException;

public class VcfTools1_7 {
/*
    public static void main(String[] args) throws IOException {

        if( args.length<2 ) {
            printUsageAndExit();
        }

        boolean annSplitter = false;
        boolean commonSnps = false;
        boolean gtStats = false;
        
        switch( args[0] ) {
            case "--ANNsplitter": annSplitter = true; break;
            case "--commonSnps": commonSnps = true; break;
            case "--gtStats": gtStats = true; break;
            default: printUsageAndExit();
        }

        String inFileName = args[1];
        if( gtStats ) {
            genotypeStats(inFileName);
            System.exit(0);
        }

        if( args.length<3 ) {
            printUsageAndExit();
        }

        String outFileName = args[2];
        
        if( annSplitter ) {
            splitANNfield(inFileName, outFileName);
        }
        if( commonSnps ) {
            int N = 5;
            int minDP = 0;
            int minGQ = 0;
            boolean homozygous = false;

            if( args.length<3 ) {
                printUsageAndExit();
            }

            for( int i=3; i<args.length; i++ ) {
                String arg = args[i];
                if( arg.startsWith("N=") ) {
                    N = Integer.parseInt(arg.substring(2));
                }
                if( arg.startsWith("minDP=") ) {
                    minDP = Integer.parseInt(arg.substring(6));
                }
                if( arg.startsWith("minGQ=") ) {
                    minGQ = Integer.parseInt(arg.substring(6));
                }
                if( arg.startsWith("homozygous") ) {
                    homozygous=true;
                }
            }
            findCommonSnps(inFileName, outFileName, N, minDP, minGQ, homozygous);
        }
    }

    static void printUsageAndExit() {
        System.out.println("VCF tools; currently available modules:");
        System.out.println("  --ANNsplitter <input .vcf or .vcf.gz file> <output .vcf or .vcf.gz file>");
        System.out.println("    analyzes ANN data in INFO field and splits it by ','");
        System.out.println("  --commonSnps <input .vcf or .vcf.gz file> <output .vcf or .vcf.gz file> N=<n> minDP=5 minGQ=5");
        System.out.println("    n - number of samples with allele calls (genotype different than 0/0) must be N or more");
        System.out.println("    minDP=? - minimum value of DP field for the sample to be considered to be called");
        System.out.println("    minGQ=? - minimum value of GQ field for the sample to be considered to be called");
        System.out.println("    homozygous - (optional) if specified, only lines having at least n homozygous samples are exported");
        System.out.println("  --gtStats <input .vcf or .vcf.gz file>");
        System.exit(-1);
    }

    static void splitANNfield(String inFileName, String outFileName) throws IOException {

        BufferedReader in = openReader(inFileName);
        BufferedWriter out = openWriter(outFileName);

        System.out.println("ANN splitter,  version Feb 19, 2024");

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

            //int origTabCount = countTabs(line);
            
            int infoFieldStart = findInfoField(line);
            int infoFieldEnd = line.indexOf('\t', infoFieldStart+1);
            String infoField = line.substring(infoFieldStart, infoFieldEnd);

            // parse out ANN attribute
            int annStart = infoField.indexOf("ANN=");
            int annEnd = infoField.indexOf(";", annStart);
            String ann;
            if( annEnd<0 ) {
                ann = infoField.substring(annStart+4);
            } else {
                ann = infoField.substring(annStart+4, annEnd);
            }

            // split ann field by ','
            String[] annChunks = ann.split(",");

            int annFieldPos = infoFieldStart + annStart + 4;
            int annFieldAfterPos;
            if( annEnd<0 ) {
                annFieldAfterPos = infoFieldStart + infoField.length();
            } else {
                annFieldAfterPos = infoFieldStart + annEnd;
            }
            for( String annChunk: annChunks ) {

                // how much to write until ann field
                String beforeAnn = line.substring(0, annFieldPos);
                String afterAnn = line.substring(annFieldAfterPos);
                String newLine = beforeAnn+annChunk+afterAnn+"\n";
                
                //int newTabCount = countTabs(newLine);
                //if( origTabCount != newTabCount ) {
                //    System.out.println("tab count problem");
                //}
                
                out.write(newLine);
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

    
    static void findCommonSnps(String inFileName, String outFileName, int N, int minDP, int minGQ, boolean homozygous) throws IOException {

        BufferedReader in = openReader(inFileName);
        BufferedWriter out = openWriter(outFileName);

        System.out.println("Common Snps Finder,  version 1.7, February 19, 2024");
        System.out.println(" N = "+N+"  (every exported line must have at least so many samples with allele call)");
        System.out.println("     samples with ./. and 0/0 genotypes are skipped from processing");
        System.out.println(" minDP = "+minDP+"  (every sample counted as allele call must have DP field at least that much)");
        System.out.println(" minGQ = "+minGQ+"  (every sample counted as allele call must have GQ field at least that much)");
        System.out.println("                   if a sample does not have a GQ field, it counts as it passess the filter");
        if( homozygous ) {
            System.out.println(" homozygous - lines must have at least n="+N+" homozygous samples (1/1, 2/2, 3/3, 4/4)");
        }

        String[] headerCols = new String[100];
        int originalLineCount = 0;
        int newLineCount = 0;

        String line;
        while( (line=in.readLine())!=null ) {

            // copy comments as they are
            if( line.startsWith("##") ) {
                out.write(line);
                out.write("\n");
                newLineCount++;
                continue;
            }

            if( line.startsWith("#C") ) {
                // header line
                headerCols = line.split("[\\t]", -1);
                
                out.write(line);
                out.write("\n");
                newLineCount++;
                continue;
            }
            originalLineCount++;

            String[] fields = line.split("[\\t]", -1);
            
            String format = fields[8];
            int gtIndex = -1;
            int dpIndex = -1;
            int gqIndex = -1;
            int fmtIndex = 0;
            for( String fmtField: format.split(":") ) {
                switch(fmtField) {
                    case "GT": gtIndex = fmtIndex; break;
                    case "DP": dpIndex = fmtIndex; break;
                    case "GQ": gqIndex = fmtIndex; break;
                }
                fmtIndex++;
            }
            
            if( gtIndex<0 || dpIndex<0 ) {
                System.out.println("ERROR: line format does not have mandatory fields GT DP! line skipped");
                System.out.println("   "+line);
                continue;
            }
            
            // samples are in columns 9 and other
            int samplesWithAllele = 0;
            for( int i=9; i<fields.length; i++ ) {
                String sample = fields[i];
                
                // skip any samples './.'
                if( sample.equals("./.") ) {
                    continue;
                }

                String[] subfields = sample.split("[\\:]", -1);

                String genotype = subfields[gtIndex];

                if( genotype.startsWith("0/0") ) { // filter out lines with '0/0' genotype
                    continue;
                }

                // in homozygous mode, skip non-homozygous samples
                if( !(genotype.equals("1/1") || genotype.equals("1|1")
                     || genotype.equals("2/2") || genotype.equals("2|2")
                     || genotype.equals("3/3") || genotype.equals("3|3")
                     || genotype.equals("4/4") || genotype.equals("4|4")
                        ) ) {
                    continue;
                }
                
                if( dpIndex >= subfields.length ) {
                    //System.out.println("problematic line: "+line);
                    continue;
                }
                String dp = subfields[dpIndex];
                int dpVal;
                if( dp.equals(".") ) {
                    dpVal = 0;
                } else {
                    dpVal = Integer.parseInt(dp);
                }
                if( dpVal < minDP ) { // filter out lines with DP less than minDP
                    continue;
                }

                if( gqIndex>=0 && gqIndex<subfields.length ) {
                    String gq = subfields[gqIndex];
                    int gqVal;
                    if( gq.equals(".") ) {
                        gqVal = 0;
                    } else {
                        gqVal = Integer.parseInt(gq);
                    }
                    if( gqVal < minGQ ) { // filter out lines with GQ less than minGQ
                        continue;
                    }
                }
                
                samplesWithAllele++;

                // stop counting when we reached N
                if( samplesWithAllele >= N ) {
                    break;
                }
            }

            // write the line only if it has more N or more columns with allele calls
            if( samplesWithAllele >= N ) {
                out.write(line+"\n");
                newLineCount++;
            }

        }
        in.close();
        out.close();

        System.out.println("INPUT FILE: "+inFileName);
        System.out.println("  DATA LINE COUNT: "+originalLineCount);
        System.out.println("OUTPUT FILE: "+outFileName);
        System.out.println("  DATA LINE COUNT: "+newLineCount);
    }

    static void genotypeStats(String inFileName) throws IOException {

        BufferedReader in = openReader(inFileName);

        System.out.println("Genotype Stats,  version 1.0, February 20, 2024");

        ArrayList<TreeMap<String,Integer> > genotypeCounts = new ArrayList<>();

        String[] headerCols = new String[100];
        int originalLineCount = 0;

        String line;
        while( (line=in.readLine())!=null ) {

            // copy comments as they are
            if( line.startsWith("##") ) {
                continue;
            }

            if( line.startsWith("#C") ) {
                // header line
                headerCols = line.split("[\\t]", -1);
                for( int i=0; i<headerCols.length; i++ ) {
                    genotypeCounts.add(null);
                }
                
                continue;
            }
            originalLineCount++;

            String[] fields = line.split("[\\t]", -1);
            
            String format = fields[8];
            int gtIndex = -1;
            int fmtIndex = 0;
            for( String fmtField: format.split(":") ) {
                switch(fmtField) {
                    case "GT": gtIndex = fmtIndex; break;
                }
                fmtIndex++;
            }
            
            if( gtIndex<0 ) {
                System.out.println("ERROR: line format does not have mandatory field GT! line skipped");
                System.out.println("   "+line);
                continue;
            }
            
            // samples are in columns 9 and other
            for( int i=9; i<fields.length; i++ ) {
                String sample = fields[i];
                
                // skip any samples './.'
                if( sample.equals("./.") ) {
                    incrementGenotypeCount( genotypeCounts, i, "./." );
                    continue;
                }

                String[] subfields = sample.split("[\\:]", -1);

                String genotype = subfields[gtIndex];

                incrementGenotypeCount( genotypeCounts, i, genotype );
            }
        }
        in.close();

        System.out.println("INPUT FILE: "+inFileName);
        System.out.println("  DATA LINE COUNT: "+originalLineCount);

        TreeMap<String, Integer> genotypeCountAllCols = new TreeMap<>();
        System.out.println("\nGENOTYPE COUNTS: ");
        for( int i=0; i<genotypeCounts.size(); i++ ) {
            TreeMap<String, Integer> genotypeCount = genotypeCounts.get(i);
            if( genotypeCount == null ) {
                continue;
            }
            System.out.println("--- COLUMN "+i+" ["+headerCols[i]+"]");
            for (Map.Entry<String, Integer> entry : genotypeCount.entrySet()) {
                System.out.println("   " + entry.getKey() + " : " + entry.getValue());
                
                incrementGenotypeCount(genotypeCountAllCols, entry.getKey(), entry.getValue());
            }
        }
        System.out.println("--- ALL COLUMNS:");
        for (Map.Entry<String, Integer> entry : genotypeCountAllCols.entrySet()) {
            System.out.println("   " + entry.getKey() + " : " + entry.getValue());
        }
    }


    static void incrementGenotypeCount( ArrayList< TreeMap<String,Integer> > genotypeCounts, int sample, String genotype ) {

        TreeMap<String, Integer> genotypeCount = genotypeCounts.get(sample);
        if( genotypeCount == null ) {
            genotypeCount = new TreeMap<>();
            genotypeCounts.set(sample, genotypeCount);
        }

        incrementGenotypeCount(genotypeCount, genotype, 1);
    }

    static void incrementGenotypeCount( TreeMap<String,Integer> genotypeCount, String genotype, int increment ) {

        Integer gtCount = genotypeCount.get(genotype);
        if (gtCount == null) {
            gtCount = increment;
        } else {
            gtCount += increment;
        }
        genotypeCount.put(genotype, gtCount);
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

 */
}
