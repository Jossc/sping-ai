package com.queryloop.plan;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "of")
public class ReflectionResult {

    private ReflectionAction action;
    private String reason;
    private String revisedQuery;

    public static ReflectionResult continue_(String reason) {
        return new ReflectionResult(ReflectionAction.CONTINUE, reason, null);
    }

    public static ReflectionResult replan(String reason, String revisedQuery) {
        return new ReflectionResult(ReflectionAction.REPLAN, reason, revisedQuery);
    }

    public static ReflectionResult abort(String reason) {
        return new ReflectionResult(ReflectionAction.ABORT, reason, null);
    }
}
