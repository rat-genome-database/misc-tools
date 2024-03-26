package mtutaj;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
  *
  * @author marek
  */
public class Main {

    public static void main(String[] args) throws Exception {

        int chrCol = 0;
        int posCol = 0;
        int posCol2 = 0;
        int dataCol = 0;
        int dataCol2 = 0;
        int binSize = 0;
        int posMin = 0;
        int posMax = 0;
        String inputFile = null;
        String outputFile = null;
        String chrPar = null;
        boolean dataAsNumbers = true;
        boolean slidingWindow = false;
        boolean reverse = false;
        Double cutoffValue = null;

        boolean join = false;
        boolean join2 = false;
        boolean join2x = false;
        boolean expandStartStop = false;
        List<String> inputFiles = new ArrayList<>();

        for( int i=0; i<args.length; i++ ) {

            String arg = args[i];
            switch(arg) {
                case "--chr":
                    chrPar = args[++i];
                    break;
                case "--chr_col":
                    chrCol = Integer.parseInt(args[++i]);
                    break;
                case "--pos_col":
                    posCol = Integer.parseInt(args[++i]);
                    break;
                case "--pos_col2":
                    posCol2 = Integer.parseInt(args[++i]);
                    break;
                case "--data_col":
                    dataCol = Integer.parseInt(args[++i]);
                    break;
                case "--data_col2":
                    dataCol2 = Integer.parseInt(args[++i]);
                    break;
                case "--bin_size":
                    binSize = Integer.parseInt(args[++i]);
                    break;
                case "--pos_range":
                    posMin = Integer.parseInt(args[++i]);
                    posMax = Integer.parseInt(args[++i]);
                    break;
                case "--input":
                    inputFile = args[++i];
                    inputFiles.add(inputFile);
                    break;
                case "--output":
                    outputFile = args[++i];
                    break;
                case "--data_as_numbers":
                    dataAsNumbers = true;
                    break;
                case "--data_as_text":
                    dataAsNumbers = false;
                    break;
                case "--join":
                    join = true;
                    break;
                case "--join2":
                    join2 = true;
                    break;
                case "--join2x":
                    join2x = true;
                    break;
                case "--reverse":
                    reverse = true;
                    break;
                case "--sliding_window":
                    slidingWindow = true;
                    break;
                case "--cutoff":
                    cutoffValue = Double.parseDouble(args[++i]);
                    break;
                case "--expand_start_stop":
                    expandStartStop = true;
                    break;
            }
        }

        if( expandStartStop ) {
            expandStartStop(inputFile, outputFile);
            return;
        }

        if( join ) {
            joinFiles(inputFiles, outputFile, dataCol, reverse);
            return;
        }
        if( join2 ) {
            joinFiles2(inputFiles, outputFile, dataCol, reverse);
            return;
        }
        if( join2x ) {
            JoinFiles.joinFiles2x(inputFiles, outputFile, dataCol, reverse);
            return;
        }
        if( cutoffValue!=null ) {
            removeCutOffValues(inputFile, outputFile, dataCol, cutoffValue);
            return;
        }

        if( chrCol <= 0 ||
            posCol <= 0 ||
            dataCol <= 0 ||
            binSize <= 0 ||
            posMin <= 0 ||
            posMax <= 0 ||
            chrPar == null ||
            inputFile == null ||
            outputFile == null )
        {
            usage();
            return;
        }

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));

        try {
            if (slidingWindow) {

                if (dataAsNumbers) {
                    if (dataCol2 > 0) {
                        slidingWindowWithNumericData(in, out, chrCol, posCol, dataCol, dataCol2, posMin, posMax, binSize, chrPar);
                    } else {
                        slidingWindowWithNumericData(in, out, chrCol, posCol, dataCol, posMin, posMax, binSize, chrPar);
                    }
                } else {
                    slidingWindowWithTextData(in, out, chrCol, posCol, posCol2, dataCol, posMin, posMax, binSize, chrPar);
                }
            } else {
                if (dataAsNumbers) {
                    if (dataCol2 > 0) {
                        runWithNumericData(in, out, chrCol, posCol, dataCol, dataCol2, posMin, posMax, binSize, chrPar);
                    } else {
                        runWithNumericData(in, out, chrCol, posCol, dataCol, posMin, posMax, binSize, chrPar);
                    }
                } else {
                    runWithTextData(in, out, chrCol, posCol, dataCol, posMin, posMax, binSize, chrPar);
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        in.close();
        out.close();
    }

    static void runWithNumericData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int dataCol,
                                   int posMin, int posMax, int binSize, String chrPar) throws IOException {

        out.write("#chr\tbin_pos_start\tbin_pos_end\tnonzero_lines_in_bin\tsum\tavg\n");

        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }

        Map<Integer, List<Double>> binMap = new HashMap<>();

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            ++lineNr;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }
            String chr = cols[chrCol-1];
            int pos = Integer.parseInt(cols[posCol-1]);
            String dataStr = cols[dataCol-1];
            double dVal = 0.0;
            try {
                dVal = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }
            if( dVal==0.0 ) {
                continue;
            }

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            int bin = getBin(pos, binSize, posMin);

            List<Double> binData = binMap.get(bin);
            if( binData==null ) {
                binData = new ArrayList<>();
                binMap.put(bin, binData);
            }
            binData.add(dVal);
        }

        // display all bins
        for( int pos=posMin; pos<posMax; pos+=binSize ) {
            int bin = getBin(pos, binSize, posMin);
            List<Double> binData = binMap.get(bin);
            if( binData==null ) {
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t0\t\t\n");
            } else {
                Double sum = 0.0;
                int dataLinesInBin = binData.size();
                for( Double d: binData ) {
                    sum += d;
                }
                Double avg = sum / dataLinesInBin;
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t"+dataLinesInBin+"\t"+sum+"\t"+avg+"\n");
            }
        }
    }

    static void runWithNumericData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int dataCol, int dataCol2,
                                   int posMin, int posMax, int binSize, String chrPar) throws IOException {

        out.write("#chr\tbin_pos_start\tbin_pos_end\tdata1_sum\tdata2_sum\tratio (data1_sum/(data1_sum+data2_sum))\n");

        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }
        if( dataCol2>colsNeeded ) {
            colsNeeded = dataCol2;
        }

        Map<Integer, List<Double[]>> binMap = new HashMap<>();

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            ++lineNr;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }
            String chr = cols[chrCol-1];
            int pos = Integer.parseInt(cols[posCol-1]);

            String dataStr = cols[dataCol-1];
            double dVal = 0.0;
            try {
                dVal = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }

            dataStr = cols[dataCol2-1];
            double dVal2 = 0.0;
            try {
                dVal2 = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }

            if( dVal==0.0 && dVal2==0.0) {
                continue;
            }

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            int bin = getBin(pos, binSize, posMin);

            List<Double[]> binData = binMap.get(bin);
            if( binData==null ) {
                binData = new ArrayList<>();
                binMap.put(bin, binData);
            }
            Double[] d2 = new Double[] {dVal, dVal2};
            binData.add(d2);
        }

        // display all bins
        for( int pos=posMin; pos<posMax; pos+=binSize ) {
            int bin = getBin(pos, binSize, posMin);
            List<Double[]> binData = binMap.get(bin);
            if( binData==null ) {
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t0\t0\t0\n");
            } else {
                Double sum = 0.0, sum2 = 0.0;
                for( Double[] d2: binData ) {
                    sum += d2[0];
                    sum2 += d2[1];
                }
                Double ratio = ((sum+sum2)==0.0) ? 0.0 : (sum / (sum+sum2));
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t"+sum+"\t"+sum2+"\t"+ratio+"\n");
            }
        }
    }

    static void runWithTextData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int dataCol,
                                   int posMin, int posMax, int binSize, String chrPar) throws IOException {

        out.write("#chr\tbin_pos_start\tbin_pos_end\tunique_values_in_bin\n");

        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }

        Map<Integer, Set<String>> binMap = new HashMap<>();

        int skippedLines = 0;
        int processedLines = 0;

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }

            int pos = 0;
            try {
                pos = Integer.parseInt(cols[posCol-1]);
            } catch( NumberFormatException e ) {
                skippedLines++;
                continue;
            }
            String chr = cols[chrCol-1];
            String dataStr = cols[dataCol-1];
            processedLines++;

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            int bin = getBin(pos, binSize, posMin);

            Set<String> binData = binMap.get(bin);
            if( binData==null ) {
                binData = new HashSet<>();
                binMap.put(bin, binData);
            }
            binData.add(dataStr);
        }

        // display all bins
        for( int pos=posMin; pos<posMax; pos+=binSize ) {
            int bin = getBin(pos, binSize, posMin);
            Set<String> binData = binMap.get(bin);
            int uniqueValues = binData==null ? 0 : binData.size();
            out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t"+uniqueValues+"\n");
        }

        System.out.println("SKIPPED LINES (invalid position): "+skippedLines);
        System.out.println("PROCESSED LINES: "+processedLines);
    }

    static int getBin(int pos, int binSize, int minPos) {
        return (pos - minPos) / binSize;
    }

    static void usage() {
        System.out.print("BinTool   -- build Mar 26, 2024\n"+
                "\n"+
                "Usage:\n"+
                "java -jar bintool.jar \n"+
                "   --input <input file>\n"+
                "   --output <output file>\n"+
                "   --chr <chromosome>\n"+
                "   --chr_col <chromosome name column>\n"+
                "   --pos_col <position column>\n"+
                "   --pos_col2 <position end column>\n"+
                "   --data_col <data column>\n"+
                "   --data_col2 <data column>  -- if given, sum(data1)/sum(data2) will be computed in output file\n"+
                "   --bin_size <bin size>\n"+
                "   --pos_range <min pos> <max pos>\n"+
                "   --cutoff <cutoff value>  -- if given, numbers lower than cutoff will be skipped\n"+
                "   --data_as_numbers   or   --data_as_text\n"+
                "   --join   (join all files specified in --input into multi column file; first 3 columns uniquely identify the row)\n"+
                "   --join2  (join all files specified in --input into multi column file; first 2 columns uniquely identify the row)\n"+
                "   --join2x (as join2, but works for very large files -- slower)\n"+
                "   --reverse  -- for --join and --join2 options: adds extra column with row numbers in reverse\n"+
                "   --sliding_window   (if specified, use sliding window)\n"+
                "   --expand_start_stop --input INPUT_FILE --output OUTPUT_FILE\n"+
                "");
    }

    static void removeCutOffValues(String inputFile, String outputFile, int dataCol, Double cutoffValue) throws IOException {

        dataCol--; // convert from 1-based to 0-based

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
        String line;

        int linesRead = 0;
        int linesWritten = 0;

        while( (line=in.readLine())!=null ) {

            linesRead++;
            String[] cols = line.split("[\\t]", -1);
            if( dataCol>=cols.length ) {
                out.write(line);
                out.write("\n");
                linesWritten++;
                continue;
            }

            Double val = Double.parseDouble(cols[dataCol]);
            if( val<cutoffValue ) {
                continue; // current value lower than cutoff value -- skip the line
            }
            out.write(line);
            out.write("\n");
            linesWritten++;
        }
        in.close();
        out.close();

        System.out.println("lines read: "+linesRead);
        System.out.println("lines written: "+linesWritten);
    }

    static void joinFiles(List<String> inputFiles, String outputFile, int dataCol, boolean reverse) throws IOException {

        if( dataCol==0 ) {
            dataCol = 3;
        } else {
            dataCol--;
        }

        int fileCount = inputFiles.size();
        Map<String, String[]> results = new TreeMap<>();

        for( int fileIndex=0; fileIndex<fileCount; fileIndex++ ) {

            String inputFile = inputFiles.get(fileIndex);
            BufferedReader in = new BufferedReader(new FileReader(inputFile));
            String line;

            while( (line=in.readLine())!=null ) {

                if( line.startsWith("#") ) {
                    continue;
                }
                String[] cols = line.split("[\\t]", -1);
                String chr = cols[0];
                int binStart = Integer.parseInt(cols[1]);
                String binEnd = cols[2];
                String dataVal = cols[dataCol];
                String token = chr+"\t"+binStart+"\t"+binEnd;

                String[] fileData = results.get(token);
                if( fileData==null ) {
                    fileData = new String[fileCount];
                    results.put(token, fileData);
                }
                fileData[fileIndex] = dataVal;
            }
            in.close();
        }

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
        out.write("#chr\tbin_start_pos\tbin_end_pos");
        for( String fname: inputFiles ) {
            out.write("\t"+fname);
        }
        if( reverse ) {
            out.write("\treverse_row_nr");
        }
        out.write("\n");

        int reverseRowNr = results.size();
        for( Map.Entry<String, String[]> entry: results.entrySet() ) {
            out.write(entry.getKey()); // write 1st 3 columns
            String[] dataCols = entry.getValue();
            for( String dataVal: dataCols ) {
                if( dataVal==null || dataVal.equals("") ) {
                    dataVal = "0";
                }
                out.write("\t"+dataVal);
            }
            if( reverse ) {
                out.write("\t"+reverseRowNr);
                reverseRowNr--;
            }
            out.write("\n");
        }
        out.close();
    }

    static void joinFiles2(List<String> inputFiles, String outputFile, int dataCol, boolean reverse) throws IOException {

        if( dataCol==0 ) {
            dataCol = 2;
        } else {
            dataCol--;
        }

        int fileCount = inputFiles.size();
        Map<String, String[]> results = new TreeMap<>();

        for( int fileIndex=0; fileIndex<fileCount; fileIndex++ ) {

            String inputFile = inputFiles.get(fileIndex);
            BufferedReader in = new BufferedReader(new FileReader(inputFile));
            String line;

            while( (line=in.readLine())!=null ) {

                if( line.startsWith("#") ) {
                    continue;
                }
                String[] cols = line.split("[\\t]", -1);
                String chr = cols[0];
                int pos = Integer.parseInt(cols[1]);
                String dataVal = cols[dataCol];
                String token = chr+"\t"+pos;

                String[] fileData = results.get(token);
                if( fileData==null ) {
                    fileData = new String[fileCount];
                    results.put(token, fileData);
                }
                fileData[fileIndex] = dataVal;
            }
            in.close();
        }

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
        out.write("#chr\tpos");
        for( String fname: inputFiles ) {
            out.write("\t"+fname);
        }
        if( reverse ) {
            out.write("\treverse_row_nr");
        }
        out.write("\n");

        int reverseRowNr = results.size();
        for( Map.Entry<String, String[]> entry: results.entrySet() ) {
            out.write(entry.getKey()); // write 1st 3 columns
            String[] dataCols = entry.getValue();
            for( String dataVal: dataCols ) {
                if( dataVal==null || dataVal.equals("") ) {
                    dataVal = "0";
                }
                out.write("\t"+dataVal);
            }
            if( reverse ) {
                out.write("\t"+reverseRowNr);
                reverseRowNr--;
            }
            out.write("\n");
        }
        out.close();
    }


    static void slidingWindowWithNumericData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int dataCol,
                                   int posMin, int posMax, int binSize, String chrPar) throws Exception {

        out.write("#SLIDING_WINDOW\n");
        out.write("#BIN_SIZE="+binSize+"\n");
        out.write("#POS_MIN="+posMin+"\n");
        out.write("#POS_MAX="+posMax+"\n");
        out.write("#CHR="+chrPar+"\n");
        out.write("#chr\twnd_pos_start\twnd_pos_end\tnonzero_lines_in_window\tsum\tavg\n");

        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }

        Map<Integer, Double> posMap = new HashMap<>();
        int skippedLines = 0;
        int processedLines = 0;
        int duplicatePosLines = 0;

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            ++lineNr;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }

            int pos = 0;
            try {
                pos = Integer.parseInt(cols[posCol-1]);
            } catch( NumberFormatException e ) {
                skippedLines++;
                continue;
            }
            String chr = cols[chrCol-1];
            processedLines++;

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            String dataStr = cols[dataCol-1];
            double dVal = 0.0;
            try {
                dVal = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }
            if( dVal==0.0 ) {
                continue;
            }

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            Double dPrev = posMap.put(pos, dVal);
            if( dPrev!=null ) {
                duplicatePosLines++;
                // sum the values
                posMap.put(pos, dVal+dPrev);
            }
        }

        // compute sliding window data
        //
        for( int pos=posMin; pos<=posMax; pos++ ) {

            Double dSum = 0.0;
            int dataLinesInBin = 0;

            for( int k=pos; k<pos+binSize; k++ ) {
                Double d = posMap.get(k);
                if( d!=null ) {
                    dSum += d;
                    dataLinesInBin++;
                }
            }

            if( dSum==0.0 || dataLinesInBin==0 ) {
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t0\t\t\n");
            } else {
                float sum = dSum.floatValue();
                float avg = (float)(dSum / dataLinesInBin);
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t"+dataLinesInBin+"\t"+sum+"\t"+avg+"\n");
            }
        }

        System.out.println("SKIPPED LINES (invalid position): "+skippedLines);
        System.out.println("PROCESSED LINES: "+processedLines);
        System.out.println("DUPLICATE POS LINES: "+duplicatePosLines);
    }

    static void slidingWindowWithNumericData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int dataCol, int dataCol2,
                                   int posMin, int posMax, int binSize, String chrPar) throws Exception {

        out.write("#sliding window size: "+binSize+"\n");
        out.write("#position range: "+posMin+"-"+posMax+"\n");
        out.write("#chr\twnd_pos_start\twnd_pos_end\tdata1_sum\tdata2_sum\tratio (data1_sum/(data1_sum+data2_sum))\n");


        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }

        Map<Integer, Double[]> posMap = new HashMap<>();
        int skippedLines = 0;
        int processedLines = 0;

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }

            int pos = 0;
            try {
                pos = Integer.parseInt(cols[posCol-1]);
            } catch( NumberFormatException e ) {
                skippedLines++;
                continue;
            }
            String chr = cols[chrCol-1];
            processedLines++;

            // check if out of range
            if( pos<posMin || pos>posMax || !chr.equals(chrPar)) {
                continue;
            }

            String dataStr = cols[dataCol-1];
            double dVal = 0.0;
            try {
                dVal = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }

            dataStr = cols[dataCol2-1];
            double dVal2 = 0.0;
            try {
                dVal2 = Double.parseDouble(dataStr);
            } catch(NumberFormatException e) {
            }

            if( dVal==0.0 && dVal2==0.0) {
                continue;
            }

            Double[] d2 = new Double[] {dVal, dVal2};
            Double[] d2Prev = posMap.put(pos, d2);
            if( d2Prev!=null ) {
                throw new Exception("unexpected: previous data found at pos "+pos);
            }
        }

        // compute sliding window data
        //
        for( int pos=posMin; pos<=posMax; pos++ ) {

            Double sum = 0.0, sum2 = 0.0;
            for( int k=pos; k<pos+binSize; k++ ) {
                Double[] d2 = posMap.get(k);
                if( d2!=null ) {
                    sum += d2[0];
                    sum2 += d2[1];
                }
            }

            if( sum+sum2==0.0 ) {
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t0\t0\t0\n");
            } else {
                double ratio = ((sum+sum2)==0.0) ? 0.0 : (sum / (sum+sum2));
                float ratiof = (float)(ratio);
                out.write(chrPar+"\t"+pos+"\t"+(pos+binSize-1)+"\t"+sum+"\t"+sum2+"\t"+ratiof+"\n");
            }
        }

        System.out.println("SKIPPED LINES (invalid position): "+skippedLines);
        System.out.println("PROCESSED LINES: "+processedLines);
    }

    static void slidingWindowWithTextData(BufferedReader in, BufferedWriter out, int chrCol, int posCol, int posCol2, int dataCol,
                                int posMin, int posMax, int binSize, String chrPar) throws IOException {

        out.write("#sliding window size: "+binSize+"\n");
        out.write("#position range: "+posMin+"-"+posMax+"\n");
        out.write("#chr\twnd_pos\tvalue_sum\n");

        int colsNeeded = chrCol;
        if( posCol>colsNeeded ) {
            colsNeeded = posCol;
        }
        if( posCol2>colsNeeded ) {
            colsNeeded = posCol2;
        }
        if( dataCol>colsNeeded ) {
            colsNeeded = dataCol;
        }
        if( posCol2<=0 ) {
            posCol2 = posCol;
        }

        Map<Integer, Integer> posMap = new HashMap<>(); // frequency map
        int skippedLines = 0;
        int processedLines = 0;

        int lineNr = 0;
        String line;
        while( (line=in.readLine())!=null ) {
            lineNr++;
            String[] cols = line.split("[\\t]", -1);
            if( cols.length<colsNeeded ) {
                System.out.println("line nr "+lineNr+" has only "+cols.length+" columns! skipped");
                continue;
            }

            int pos1 = 0;
            try {
                pos1 = Integer.parseInt(cols[posCol-1]);
            } catch( NumberFormatException e ) {
                skippedLines++;
                continue;
            }
            int pos2 = 0;
            try {
                if( posCol2>0 ) {
                    pos2 = Integer.parseInt(cols[posCol2 - 1]);
                }
            } catch( NumberFormatException e ) {
                skippedLines++;
                continue;
            }
            String chr = cols[chrCol-1];
            //String dataStr = cols[dataCol-1];
            processedLines++;

            // check if out of range
            if( pos2<posMin || pos1>posMax || !chr.equals(chrPar)) {
                continue;
            }

            for( int pos=pos1; pos<=pos2; pos++) {
                Integer count = posMap.get(pos);
                if( count==null ) {
                    count = 1;
                } else {
                    count++;
                }
                posMap.put(pos, count);
            }
        }

        // compute sliding window data
        //
        // compute sum for 1st window
        int sum = 0;
        for( int i=0; i<binSize; i++ ) {
            Integer cnt = posMap.get(i+posMin);
            if( cnt!=null ) {
                sum += cnt;
            }
        }
        //float avg = (float)((double)sum/(double)binSize);
        out.write(chrPar+"\t"+posMin+"\t"+sum+"\n");

        for( int k=posMin+1; k<=posMax; k++ ) {
            // update sum for new sliding window
            Integer cnt = posMap.get(k-1);
            if( cnt!=null ) {
                sum -= cnt;
            }
            cnt = posMap.get(k+binSize-1);
            if( cnt!=null ) {
                sum += cnt;
            }
            //avg = (float)((double)sum/(double)binSize);
            out.write(chrPar+"\t"+k+"\t"+sum+"\n");
        }

        System.out.println("SKIPPED LINES (invalid position): "+skippedLines);
        System.out.println("PROCESSED LINES: "+processedLines);
    }


    static void expandStartStop( String inputFile, String outputFile ) throws IOException {

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        BufferedWriter out;
        if( outputFile.endsWith(".gz") ) {
            out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputFile))));
        } else {
            out = new BufferedWriter(new FileWriter(outputFile));
        }


        //"","seqnames","start","end","width","strand","RMTAL3","RMTAL5","RMTAL9","RPT16","RPT24","RPT25","RPT26"
        //"1","chr1",57496,57896,401,"*",6.43869987425506,7.7478433377694,9.18820781496976,7.11599641983037,4.16389494468535,10.2020874330595,3.83717139524125

        int chrCol = 1;
        int startCol = 2;
        int endCol = 3;
        int dataCol = 6;

        String header = in.readLine();

        String[] headerCols = header.split("[,]", -1);
        int dataColCount = headerCols.length - dataCol;
        String zeroData = "0";
        for( int i=1; i<dataColCount; i++ ) {
            zeroData += ",0";
        }
        out.write("chr,pos");
        for( int i=dataCol; i<dataCol+dataColCount; i++ ) {
            out.write(","+headerCols[i]);
        }
        out.write("\n");


        int prevEndPos = 0;
        String prevChr = "";

        String line;
        while( (line=in.readLine())!=null ) {

            String[] cols = line.split("[,]", -1);
            if( cols.length <= dataCol ) {
                continue;
            }

            String chr = cols[chrCol];
            int start = Integer.parseInt(cols[startCol]);
            int end = Integer.parseInt(cols[endCol]);

            // flush gap lines -- for positions between previous line and this line
            if( chr.equals(prevChr) ) {
                for( int pos = prevEndPos+1; pos < start; pos++ ) {
                    String gapLine = chr+","+pos+","+zeroData+"\n";
                    out.write(gapLine);
                }
            }

            // flush data for the current line
            String thisData = cols[dataCol];
            for( int j=dataCol+1; j<cols.length; j++ ) {
                thisData += ","+cols[j];
            }
            for( int pos = start; pos <= end; pos++ ) {
                String thisLine = chr+","+pos+","+thisData+"\n";
                out.write(thisLine);
            }

            // end processing for this line
            prevChr = chr;
            prevEndPos = end;

            System.out.println(cols[0]+"   "+chr+" "+start+" "+end);

        }

        in.close();
        out.close();

        System.out.println("======= OK =========");
    }
}