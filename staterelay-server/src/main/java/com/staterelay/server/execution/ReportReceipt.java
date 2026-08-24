package com.staterelay.server.execution;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Server 接收 Worker 进度或终态后的回执。 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReportReceipt {

    private Status status;
    private String message;

    public static ReportReceipt accepted() {
        return new ReportReceipt(Status.ACCEPTED, null);
    }

    public static ReportReceipt duplicate() {
        return new ReportReceipt(Status.DUPLICATE, null);
    }

    public static ReportReceipt rejected(String message) {
        return new ReportReceipt(Status.REJECTED, message);
    }

    public enum Status {
        ACCEPTED,
        DUPLICATE,
        REJECTED
    }
}
