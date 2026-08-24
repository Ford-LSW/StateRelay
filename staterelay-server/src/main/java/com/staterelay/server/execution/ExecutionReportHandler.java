package com.staterelay.server.execution;

import com.staterelay.contract.protocol.TaskProgressReport;
import com.staterelay.contract.protocol.TaskResultReport;

/** 接收单类上报协议，无需感知 HTTP 路由如何选择处理器。 */
public interface ExecutionReportHandler {

    ReportReceipt progress(TaskProgressReport report);

    ReportReceipt result(TaskResultReport report);
}
