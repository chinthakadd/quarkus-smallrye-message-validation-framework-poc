package com.chinthakad.poc;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates histogram buckets optimized for 0 to infinity range with approximately 50 buckets.
 * Uses a hybrid approach combining linear and exponential scaling for good distribution.
 */
public class HistogramBucketBinningGenerator {

    /**
     * Generates histogram buckets for 0 to infinity range.
     * Uses a hybrid approach:
     * - Linear buckets for small values (0-1000)
     * - Exponential buckets for larger values (1000+)
     * 
     * @param targetBucketCount approximate number of buckets desired
     * @return list of bucket upper bounds
     */
    public static List<Double> generateBuckets(int targetBucketCount) {
        List<Double> buckets = new ArrayList<>();
        
        // Add 0 bucket
        buckets.add(0.0);
        
        // Linear buckets for small values (0-1000)
        // These provide good granularity for common small values
        double linearStep = 10.0;
        double linearMax = 1000.0;
        for (double bucket = linearStep; bucket <= linearMax; bucket += linearStep) {
            buckets.add(bucket);
        }
        
        // Calculate remaining buckets for exponential scaling
        int remainingBuckets = targetBucketCount - buckets.size();
        
        // Exponential buckets for larger values (1000+)
        // Start from 1000 and use exponential scaling
        double currentBucket = linearMax;
        double multiplier = calculateOptimalMultiplier(currentBucket, remainingBuckets);
        
        for (int i = 0; i < remainingBuckets - 1; i++) {
            currentBucket *= multiplier;
            buckets.add(currentBucket);
        }
        
        // Add infinity bucket
        buckets.add(Double.POSITIVE_INFINITY);
        
        return buckets;
    }
    
    /**
     * Calculates the optimal multiplier for exponential scaling to reach a reasonable max value
     * while maintaining good distribution across the remaining buckets.
     */
    private static double calculateOptimalMultiplier(double startValue, int remainingBuckets) {
        // Target max value around 1M to 10M for practical purposes
        double targetMaxValue = 1_000_000.0;
        return Math.pow(targetMaxValue / startValue, 1.0 / (remainingBuckets - 1));
    }
    
    /**
     * Alternative algorithm using power-of-2 scaling for specific use cases.
     * Provides more granular buckets at lower values.
     */
    public static List<Double> generatePowerOfTwoBuckets(int targetBucketCount) {
        List<Double> buckets = new ArrayList<>();
        
        // Add 0 bucket
        buckets.add(0.0);
        
        // Power-of-2 buckets for better distribution
        double current = 1.0;
        for (int i = 0; i < targetBucketCount - 2; i++) {
            buckets.add(current);
            current *= 2.0;
        }
        
        // Add infinity bucket
        buckets.add(Double.POSITIVE_INFINITY);
        
        return buckets;
    }
    
    /**
     * Logarithmic scaling algorithm for even distribution across orders of magnitude.
     */
    public static List<Double> generateLogarithmicBuckets(int targetBucketCount) {
        return generateLogarithmicBuckets(targetBucketCount, 1.0, 1_000_000.0);
    }
    
    /**
     * Logarithmic scaling algorithm for generating histogram buckets for any given range.
     * @param targetBucketCount Total number of buckets to generate
     * @param minValue Minimum value for the range (exclusive of 0)
     * @param maxValue Maximum value for the range (exclusive of infinity)
     */
    public static List<Double> generateLogarithmicBuckets(int targetBucketCount, double minValue, double maxValue) {
        List<Double> buckets = new ArrayList<>();
        
        // Add 0 bucket
        buckets.add(0.0);
        
        // Calculate logarithmic distribution for the given range
        double logMin = Math.log(minValue);
        double logMax = Math.log(maxValue);
        
        for (int i = 1; i < targetBucketCount - 1; i++) {
            double logValue = logMin + (logMax - logMin) * i / (targetBucketCount - 2);
            double bucket = Math.exp(logValue);
            buckets.add(bucket);
        }
        
        // Add infinity bucket
        buckets.add(Double.POSITIVE_INFINITY);
        
        return buckets;
    }

    public static void main(String[] args) {
        System.out.println("=== Histogram Bucket Binning Generator ===\n");
        
        int targetBuckets = 100;
        
        // Test hybrid algorithm
        System.out.println("1. Hybrid Algorithm (Linear + Exponential):");
        List<Double> hybridBuckets = generateBuckets(targetBuckets);
        printBuckets(hybridBuckets);
        
        System.out.println("\n2. Power-of-2 Algorithm:");
        List<Double> powerOfTwoBuckets = generatePowerOfTwoBuckets(targetBuckets);
        printBuckets(powerOfTwoBuckets);
        
        System.out.println("\n3. Logarithmic Algorithm (1 to 1M):");
        List<Double> logBuckets = generateLogarithmicBuckets(targetBuckets);
        printBuckets(logBuckets);
        
        System.out.println("\n4. Logarithmic Algorithm (0.1 to 100):");
        List<Double> logBucketsSmall = generateLogarithmicBuckets(targetBuckets, 0.1, 100.0);
        printBuckets(logBucketsSmall);
        
        System.out.println("\n5. Logarithmic Algorithm (100 to 10,000):");
        List<Double> logBucketsMedium = generateLogarithmicBuckets(targetBuckets, 100.0, 10_000.0);
        printBuckets(logBucketsMedium);
        
        // Compare bucket distributions
        System.out.println("\n=== Comparison ===");
        System.out.println("Hybrid buckets count: " + hybridBuckets.size());
        System.out.println("Power-of-2 buckets count: " + powerOfTwoBuckets.size());
        System.out.println("Logarithmic (1-1M) buckets count: " + logBuckets.size());
        System.out.println("Logarithmic (0.1-100) buckets count: " + logBucketsSmall.size());
        System.out.println("Logarithmic (10-10K) buckets count: " + logBucketsMedium.size());
        
        // Show range examples
        System.out.println("\n=== Range Examples ===");
        System.out.println("Range 0.1 to 100:");
        System.out.println("  First bucket: " + logBucketsSmall.get(1));
        System.out.println("  Last bucket: " + logBucketsSmall.get(logBucketsSmall.size() - 2));
        System.out.println("Range 10 to 10,000:");
        System.out.println("  First bucket: " + logBucketsMedium.get(1));
        System.out.println("  Last bucket: " + logBucketsMedium.get(logBucketsMedium.size() - 2));
    }
    
    private static void printBuckets(List<Double> buckets) {
        System.out.println("Total buckets: " + buckets.size());
        System.out.println("First 10 buckets:");
        for (int i = 0; i < buckets.size(); i++) {
            System.out.printf("  [%d] %.2f%n", i, buckets.get(i));
        }
    }
}
