package com.gmail.benrcarver.distributed.coin;

import com.gmail.benrcarver.distributed.FSOperation;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

import static com.gmail.benrcarver.distributed.coin.FileSizeMultiFaceCoin.round;

/**
 * Originally written by Salman Niazi, the author of HopsFS.
 */
public class InterleavedMultiFaceCoin {

    private final BigDecimal create;
    private final BigDecimal append;
    private final BigDecimal read;
    private final BigDecimal rename;
    private final BigDecimal delete;
    private final BigDecimal lsFile;
    private final BigDecimal lsDir;
    private final BigDecimal chmodFiles;
    private final BigDecimal chmodDirs;
    private final BigDecimal mkdirs;
    private final BigDecimal setReplication;
    private final BigDecimal fileInfo;
    private final BigDecimal dirInfo;
    private final BigDecimal fileChown;
    private final BigDecimal dirChown;
    //private BigDecimal
    private final Random rand;
    private final BigDecimal expansion = new BigDecimal("100.00", new MathContext(4, RoundingMode.HALF_UP));
    //1000 face dice
    ArrayList<FSOperation> dice = new ArrayList<>();

    public InterleavedMultiFaceCoin(BigDecimal create, BigDecimal append, BigDecimal read, BigDecimal rename, BigDecimal delete, BigDecimal lsFile,
                                    BigDecimal lsDir, BigDecimal chmodFiles, BigDecimal chmodDirs, BigDecimal mkdirs,
                                    BigDecimal setReplication, BigDecimal fileInfo, BigDecimal dirInfo,
                                    BigDecimal fileChown, BigDecimal dirChown) {
        this.create = create;
        this.append = append;
        this.read = read;
        this.rename = rename;
        this.delete = delete;
        this.chmodFiles = chmodFiles;
        this.chmodDirs = chmodDirs;
        this.mkdirs = mkdirs;
        this.lsFile = lsFile;
        this.lsDir = lsDir;
        this.setReplication = setReplication;
        this.fileInfo = fileInfo;
        this.dirInfo = dirInfo;
        this.fileChown = fileChown;
        this.dirChown = dirChown;

        this.rand = new Random(System.currentTimeMillis());

        createCoin();
    }

    private void createCoin() {

        // System.out.println("Percentages create: " + create + " append: " + append + " read: " + read + " mkdir: "
        //         + mkdirs + " rename: " + rename + " delete: " + delete + " lsFile: "
        //         + lsFile + " lsDir: " + lsDir + " chmod files: " + chmodFiles + " chmod dirs: " + chmodDirs
        //         + " setReplication: " + setReplication + " fileInfo: " + fileInfo + " dirInfo: " + dirInfo
        //         + " fileChown: "+fileChown+" dirChown: "+dirChown);

        BigDecimal total = create.add(append).add(read).add(rename).add(delete).add(lsFile).add(lsDir)
                .add(chmodFiles).add(chmodDirs).add(mkdirs).add(setReplication).add(fileInfo).
                add(dirInfo).add(fileChown).add(dirChown);

        if (total.compareTo(new BigDecimal(100))!=0) {
            throw new IllegalArgumentException("All probabilities should add to 100. Got: " + total);
        }


        for (int i = 0; i < create.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.CREATE_FILE);
        }

        for (int i = 0; i < append.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        for (int i = 0; i < read.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.READ_FILE);
        }

        for (int i = 0; i < rename.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.RENAME_FILE);
        }

        for (int i = 0; i < delete.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.DELETE_FILE);
        }

        for (int i = 0; i < lsFile.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.LIST_DIR_NO_PRINT);
        }

        for (int i = 0; i < lsDir.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.LIST_DIR_NO_PRINT);
        }

        for (int i = 0; i < chmodFiles.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        for (int i = 0; i < chmodDirs.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        for (int i = 0; i < mkdirs.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.MKDIRS);
        }

        for (int i = 0; i < setReplication.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        for (int i = 0; i < fileInfo.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.FILE_INFO);
        }

        for (int i = 0; i < dirInfo.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.DIR_INFO);
        }

        for (int i = 0; i < fileChown.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        for (int i = 0; i < dirChown.multiply(expansion).intValueExact(); i++) {
            dice.add(FSOperation.NOT_SUPPORTED);
        }

        double expectedSize = expansion.multiply(new BigDecimal(100)).intValueExact();
        if (dice.size() != expectedSize) {
            Map<FSOperation, Integer> counts = new HashMap<>();
            for (FSOperation op : dice) {
                Integer opCount = counts.get(op);
                if (opCount == null) {
                    opCount = 0;
                }
                opCount++;
                counts.put(op, opCount);
            }
            for (FSOperation op : counts.keySet()) {
                double percent = ((double) counts.get(op) / (expectedSize) * 100);

                System.out.println(op + " count " + counts.get(op) + ",  " + round(percent) + "%");
            }
            throw new IllegalStateException("Dice is not properly created. Dice should have  " + expectedSize + " faces. Found " + dice.size());
        }

        Collections.shuffle(dice);
    }

    public FSOperation flip() {
        int choice = rand.nextInt(100 * expansion.intValueExact());
        return dice.get(choice);
    }

    public void testFlip() {
        Map<FSOperation, Integer> counts = new HashMap<>();
        BigDecimal times = new BigDecimal(100000);
        for (int i = 0; i < times.intValueExact(); i++) {
            FSOperation op = flip();
            Integer opCount = counts.get(op);
            if (opCount == null) {
                opCount = 0;
            }
            opCount++;
            counts.put(op, opCount);
        }


        for (FSOperation op : counts.keySet()) {
            double percent = (double) counts.get(op) / ( times.doubleValue()) * (double) 100;
            System.out.println(op + ": count: "+counts.get(op)+"        " + round(percent)+"%");
        }
    }
}