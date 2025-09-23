package com.chinthakad.poc;

public enum VerificationResultType {
    SUCCESS,
    FAILED_LATE_ARRIVAL,
    FAILED_DUPLICATE;
    
    boolean verified() {
        return this == SUCCESS;
    }
}
