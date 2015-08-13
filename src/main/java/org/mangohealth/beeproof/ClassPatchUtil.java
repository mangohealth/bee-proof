package org.mangohealth.beeproof;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

/**
 * Easy to use wrapper for on-the-fly class patching
 */
public class ClassPatchUtil {

    public enum Method {
        BEFORE,
        REPLACE,
        AFTER
    }

    public static void prependClassMethod(String clazz, String method, String body) {
        modifyClassMethod(clazz, method, body, Method.BEFORE);
    }

    public static void blockClassMethod(String clazz, String method, String body) {
        modifyClassMethod(clazz, method, body, Method.REPLACE);
    }

    public static void followClassMethod(String clazz, String method, String body) {
        modifyClassMethod(clazz, method, body, Method.AFTER);
    }

    public static void modifyClassMethod(String clazz, String method, String body, Method howToInject) {
        try {
            CtClass cc = ClassPool.getDefault().get(clazz);
            CtMethod m = cc.getDeclaredMethod(method);

            switch(howToInject) {
                case BEFORE:
                    m.insertBefore(body);
                    break;

                case REPLACE:
                    m.setBody(body);
                    break;

                case AFTER:
                    m.insertAfter(body);
                    break;

                default:
                    throw new RuntimeException("Unknown patch type:  " + howToInject);
            }

            cc.toClass();
        }
        catch(Exception ex) {
            StringBuilder errMsg = new StringBuilder();
            errMsg.append("Failed to block:  ")
                    .append("class=").append(clazz).append(", ")
                    .append("method=").append(method).append(", ")
                    .append("body=").append(body);
            throw new RuntimeException(errMsg.toString(), ex);
        }
    }

}
