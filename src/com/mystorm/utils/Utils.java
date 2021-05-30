package com.mystorm.utils;

public class Utils {

    public static String exceptionParser(Throwable e) {
        StringBuilder sb = new StringBuilder();
        parseException(e, sb);
        Throwable cause = e.getCause();
        if (cause != null) {
            while (cause.getCause() != null) {
                parseException(cause, sb);
                cause = cause.getCause();
            }
        }
        return sb.toString();
    }

    private static void parseException(Throwable e, StringBuilder sb) {
        sb.append(" |||||| Exception : " + e.getClass());
        if (e.getMessage() != null) {
            sb.append(" || Message : " + e.getMessage());
        }
        if (e.getCause() != null) {
            sb.append(" || Cause : " + e.getCause());
        }
        if (e.getStackTrace() != null) {
            sb.append(" || StackTrace : ");
            for (StackTraceElement ste : e.getStackTrace()) {
                sb.append(ste.toString());
                sb.append("                 ");
            }
        }
    }
}