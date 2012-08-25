package org.apache.pig.test.utils.datagen;

import java.io.*;
import java.util.Random;

/**
 * Author: malakar
 */
public class LocalRunner {
    private DataGeneratorConf dgConf;
    private Random rand;
    private Writer writer;

    public LocalRunner(DataGeneratorConf dgConf) {
        this.dgConf = dgConf;
        rand = new Random(dgConf.getSeed());
        writer = new Writer(dgConf);
    }

    public void generate() throws IOException {

        PrintWriter out;
        try {
            out = new PrintWriter(dgConf.getOutputFile());
        } catch (FileNotFoundException fnfe) {
            System.err.println("Could not find file " + dgConf.getOutputFile() +
                    ", " + fnfe.getMessage());
            return;
        } catch (SecurityException se) {
            System.err.println("Could not write to file " + dgConf.getOutputFile() +
                    ", " + se.getMessage());
            return;
        }

        BufferedReader in = null;
        if (dgConf.getInFile() != null) {
            try {
                in = new BufferedReader(new FileReader(dgConf.getInFile()));
            } catch (FileNotFoundException fnfe) {
                System.err.println("Unable to find input file " + dgConf.getInFile());
                return;
            }
        }

        if (dgConf.getNumRows() > 0) {
            for (int i = 0; i < dgConf.getNumRows(); i++) {
                writeLine(out);
                out.println();
            }
        } else if (in != null) {
            String line;
            while ((line = in.readLine()) != null) {
                out.print(line);
                writeLine(out);
                out.println();
            }
        }
        out.close();
    }

    protected void writeLine(PrintWriter out) {
        for (int i = 0; i < dgConf.getColSpecs().length; i++) {
            if (i != 0) out.print(dgConf.getSeparator());
            // First, decide if it's going to be null
            if (rand.nextInt(100) < dgConf.getColSpecs()[i].getPercentageNull()) {
                continue;
            }
            writer.writeCol(dgConf.getColSpecs()[i], out);
        }
    }
}
