package org.yexey.common.util.string;

public class StringUtils {

    public static String join(String[] arr, String delimiter) {
        if(arr == null || arr.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < arr.length; i++) {
            sb.append(arr[i]);
            if(i < arr.length - 1) {
                sb.append(delimiter);
            }
        }
        return sb.toString();
    }

}
