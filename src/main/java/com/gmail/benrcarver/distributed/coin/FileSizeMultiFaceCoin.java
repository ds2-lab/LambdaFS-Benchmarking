package com.gmail.benrcarver.distributed.coin;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

/**
 * Originally written by Salman Niazi, the author of HopsFS.
 */
public class FileSizeMultiFaceCoin {

    private static class Point {
        long size;
        BigDecimal percentage;
        Point(long size, BigDecimal percentage){
            this.size = size;
            this.percentage = percentage;
        }
    }

    //private BigDecimal
    private final Random rand;
    private final BigDecimal expansion = new BigDecimal("100.00", new MathContext(4, RoundingMode.HALF_UP));
    //1000 face dice
    ArrayList<Long> dice = new ArrayList<>();

    public FileSizeMultiFaceCoin(String str) {
        this.rand = new Random(System.currentTimeMillis());
        createCoin(parse(str));
    }

    public static String round(double val){
        return String.format("%5s", String.format("%.2f", val));
    }

    private void createCoin(List<Point> points) {

        BigDecimal total = new BigDecimal(0);
        for(Point point : points){
            total = total.add(point.percentage);
        }

        if (total.compareTo(new BigDecimal("100.0"))!=0) {
            throw new IllegalArgumentException("All probabilities should add to 100. Got: " + total);
        }

        for(Point point : points){
            for (int i = 0; i < point.percentage.multiply(expansion).intValueExact(); i++) {
                dice.add(point.size);
            }
        }

        double expectedSize = expansion.multiply(new BigDecimal(100)).intValueExact();
        if (dice.size() != expectedSize) {
            Map<Long, Integer> counts = new HashMap<>();
            for (Long size : dice) {
                Integer opCount = counts.get(size);
                if (opCount == null) {
                    opCount = 0;
                }
                opCount++;
                counts.put(size, opCount);
            }

            for (Long size : counts.keySet()) {
                double percent = ((double) counts.get(size) / expectedSize * 100);
                System.out.println(size + " count " + counts.get(size) + ",  " + round(percent) + "%");
            }
            throw new IllegalStateException("Dice is not properly created. Dice should have  " + expectedSize + " faces. Found " + dice.size());
        }

        Collections.shuffle(dice);
    }

    public static boolean isTwoDecimalPlace(double val) {
        if (val == 0 || val == ((int) val)) {
            return true;
        } else {
            String valStr = Double.toString(val);
            int i = valStr.lastIndexOf('.');
            return i != -1 && (valStr.substring(i + 1).length() == 1 || valStr.substring(i + 1).length() == 2);
        }
    }

    private List<Point> parse(String str){
        List<Point> points = new ArrayList<>();
        try{
            StringTokenizer strTok = new StringTokenizer(str," ,[]()");
            while(strTok.hasMoreElements()){
                String size = strTok.nextToken();
                String percentage = strTok.nextToken();
                long s = Long.parseLong(size);
                double pd = Double.parseDouble(percentage);
                if(!isTwoDecimalPlace(pd)){
                    throw new IllegalArgumentException("Wrong default Value. Only one decimal place is supported.");
                }
                points.add(new Point(s, new BigDecimal(pd, new MathContext(4, RoundingMode.HALF_UP))));
            }
        }catch (Exception e){
            throw new IllegalArgumentException("Malformed file size parameter. See documentation. Exception caused: "+e);
        }
        return points;
    }

    public Long getFileSize() {
        int choice = rand.nextInt(100 * expansion.intValueExact());
        return dice.get(choice);
    }

    public void testFlip() {
        Map<Long, Integer> counts = new HashMap<>();
        BigDecimal times = new BigDecimal(100000);
        for (int i = 0; i < times.intValueExact(); i++) {
            Long size = getFileSize();
            Integer opCount = counts.get(size);
            if (opCount == null) {
                opCount = 0;
            }
            opCount++;
            counts.put(size, opCount);
        }

        for (Long size : counts.keySet()) {
            double percent = (double) counts.get(size) / ( times.doubleValue()) * (double) 100;
            System.out.println(size + ": count: "+counts.get(size)+"        " + round(percent)+"%");
        }
    }

    public static void main(String [] argv){
        FileSizeMultiFaceCoin coin = new FileSizeMultiFaceCoin("[(1024,10),(2024,20),(3303,70)]");
        coin.testFlip();
    }
}