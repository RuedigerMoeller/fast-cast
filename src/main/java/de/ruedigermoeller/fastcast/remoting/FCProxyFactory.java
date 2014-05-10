package de.ruedigermoeller.fastcast.remoting;

import de.ruedigermoeller.fastcast.packeting.PacketSendBuffer;
import de.ruedigermoeller.fastcast.packeting.TopicEntry;
import de.ruedigermoeller.fastcast.util.FCLog;
import de.ruedigermoeller.serialization.FSTObjectInput;
import javassist.*;
import javassist.Modifier;
import javassist.bytecode.AccessFlag;

import java.io.Externalizable;
import java.lang.reflect.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class FCProxyFactory {

    static HashMap<String,Class> generatedProxyClasses = new HashMap<String, Class>();
    static HashMap<String,FCInvoker> generatedCallerClasses = new HashMap<String, FCInvoker>();

    public FCProxyFactory() {
    }

    public FCInvoker getMethod( String serviceClass, int index ) {
        return generatedCallerClasses.get(serviceClass+"#"+index);
    }

    protected <T> T createProxy(Class<T> serviceClz, TopicEntry topic, FastCast fastCast) throws Exception {
        Class proxyClass = createProxyClass(serviceClz, topic.getConf().getName());
        Constructor[] constructors = proxyClass.getConstructors();
        T instance = null;
        try {
            instance = (T) proxyClass.newInstance();
        } catch (Exception e) {
            for (int i = 0; i < constructors.length; i++) {
                Constructor constructor = constructors[i];
                if ( constructor.getParameterTypes().length == 1) {
                    instance = (T) constructor.newInstance((Class)null);
                    break;
                }
            }
            if ( instance == null )
                throw e;
        }
        Field f = instance.getClass().getField("marshaller");
        f.setAccessible(true);
        f.set(instance, fastCast);
        f = instance.getClass().getField("topic");
        f.setAccessible(true);
        f.set(instance, topic);
        f = instance.getClass().getField("sender");
        f.setAccessible(true);
        f.set(instance, topic.getSender());
        return instance;
    }

    protected <T> Class<T> createProxyClass(Class<T> clazz, String topic) throws NotFoundException, CannotCompileException, IllegalAccessException, InstantiationException, NoSuchFieldException, ClassNotFoundException {
        synchronized (generatedProxyClasses) {
            String proxyName = clazz.getName() + "Proxy";
            String key = clazz.getName() + "#" + topic;
            Class ccClz = generatedProxyClasses.get(key);
            if (ccClz == null) {
                ClassPool pool = ClassPool.getDefault();
                CtClass cc = null;
                try {
                    cc = pool.getCtClass(proxyName);
                } catch (NotFoundException ex) {
                    //ignore
                }
                if (cc == null) {
                    cc = pool.makeClass(proxyName);
                    CtClass orig = pool.get(clazz.getName());
                    if (clazz.isInterface()) {
                        cc.setInterfaces(new CtClass[]{orig, pool.get(Externalizable.class.getName()), pool.get(FCRemoteServiceProxy.class.getName())});
                    } else {
                        cc.setSuperclass(orig);
                        cc.setInterfaces(new CtClass[]{pool.get(Externalizable.class.getName()), pool.get(FCRemoteServiceProxy.class.getName())});
                    }

                    defineProxyFields(pool, cc);
                    boolean hasResultMethods = defineProxyMethods(cc, orig, topic, clazz.isInterface());
                    defineFCRemoteObjectMethods(pool, cc, clazz.getName(), hasResultMethods);
                }

                ccClz = loadProxyClass(clazz, pool, cc);
                generatedProxyClasses.put(key, ccClz);
            }
            return ccClz;
        }
    }

    protected <T> Class loadProxyClass(Class clazz, ClassPool pool, final CtClass cc) throws ClassNotFoundException {
        Class ccClz;
        Loader cl = new Loader(clazz.getClassLoader(), pool) {
            protected Class loadClassByDelegation(String name)
                    throws ClassNotFoundException
            {
                if ( name.equals(cc.getName()) )
                    return null;
                return delegateToParent(name);
            }
        };
        ccClz = cl.loadClass(cc.getName());
        return ccClz;
    }

    protected void defineProxyFields(ClassPool pool, CtClass cc) throws CannotCompileException, NotFoundException {
        CtField marsh = new CtField(pool.get(FastCast.class.getName()), "marshaller", cc);
        marsh.setModifiers(AccessFlag.PUBLIC);
        cc.addField(marsh);
        CtField topic = new CtField(pool.get(TopicEntry.class.getName()), "topic", cc);
        topic.setModifiers(AccessFlag.PUBLIC);
        cc.addField(topic);
        CtField sender = new CtField(pool.get(PacketSendBuffer.class.getName()), "sender", cc);
        sender.setModifiers(AccessFlag.PUBLIC);
        cc.addField(sender);
    }

    protected boolean defineProxyMethods(CtClass cc, CtClass orig, String topic, boolean isInterf) throws CannotCompileException, NotFoundException, ClassNotFoundException {
        CtMethod[] methods = getSortedPublicCtMethods(orig,false);
        boolean hasResultCalls = false;
        for (int i = 0; i < methods.length; i++) {
            CtMethod method = methods[i];
            CtMethod originalMethod = method;
            method = new CtMethod(method, cc, null);
            CtClass[] parameterTypes = method.getParameterTypes();
            CtClass returnType = method.getReturnType();
            Object remote = originalMethod.getAnnotation(RemoteMethod.class);
            Object loopback = originalMethod.getAnnotation(Loopback.class);
            Object unreliable = originalMethod.getAnnotation(Unreliable.class);
            Object unordered = originalMethod.getAnnotation(Unordered.class);
            boolean allowed = ((method.getModifiers() & AccessFlag.ABSTRACT) == 0 || isInterf) &&
                    (method.getModifiers() & AccessFlag.NATIVE) == 0 &&
                    (method.getModifiers() & AccessFlag.FINAL) == 0;
            if (allowed) {
                if ((method.getModifiers() & AccessFlag.PUBLIC) == AccessFlag.PUBLIC &&
                        returnType == CtPrimitiveType.voidType && remote != null) {
                    int methodIndex = ((RemoteMethod)originalMethod.getAnnotation(RemoteMethod.class)).value();
                    if ( isRemoteResultCall(method) ) {
                        hasResultCalls = true;
                    }
                    if ( methodIndex == -1 ) // receivebinary
                    {
                        String remotecall = "{ if (sender==null) sender=topic.getSender(); " +
                            "marshaller.sendBinaryContent( topic, sender, $1, $2, $3, "+(loopback != null)+" ); }";
                        method.setBody(remotecall);
                    } else if ( loopback == null && isFastCall(method) ) {
                        String body = "{ if (sender==null) sender=topic.getSender();" +
                                " de.ruedigermoeller.serialization.FSTObjectOutput out = marshaller.prepareFastCall("+methodIndex+", "+parameterTypes.length+");";
                        for (int j = 0; j < parameterTypes.length; j++) {
                            CtClass parameterType = parameterTypes[j];
                            if ( parameterType.isArray() ) {
                                parameterType = parameterType.getComponentType();
                                body += " out.writeFInt( $"+(j+1)+".length );";
                                if ( parameterType == CtClass.booleanType ) {
                                    body+=" out.writeFBooleanArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.byteType ) {
                                    body+=" out.writeFByteArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.charType ) {
                                    body+=" out.writeCCharArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.shortType ) {
                                    body+=" out.writeFShortArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.intType ) {
                                    body+=" out.writeFIntArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.longType ) {
                                    body+=" out.writeFLongArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.floatType ) {
                                    body+=" out.writeFFloatArr( $"+(j+1)+");";
                                } else
                                if ( parameterType == CtClass.doubleType ) {
                                    body+=" out.writeFDoubleArr( $"+(j+1)+");";
                                } else
                                    throw new RuntimeException("this should not happen. in trouble with method:"+ method.getName() );
                            } else
                            if ( parameterType == CtClass.booleanType ) {
                                body+=" out.writeBoolean( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.byteType ) {
                                body+=" out.writeFByte( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.charType ) {
                                body+=" out.writeFChar( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.shortType ) {
                                body+=" out.writeFShort( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.intType ) {
                                body+=" out.writeFInt( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.longType ) {
                                body+=" out.writeFLong( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.floatType ) {
                                body+=" out.writeFFloat( $"+(j+1)+");";
                            } else
                            if ( parameterType == CtClass.doubleType ) {
                                body+=" out.writeFDouble( $"+(j+1)+");";
                            } else
                            if ( parameterType.getName().equals(String.class.getName()) ) {
                                body+=" out.writeStringUTF( $"+(j+1)+");";
                            }
                        }
                        body+=" marshaller.finishFastCall( sender, out ); }";
                        method.setBody(body);
                        generateCallerClass(orig,method,parameterTypes,methodIndex,originalMethod.getDeclaringClass());
                    } else {
                        String remotecall = "{ if (sender==null) sender=topic.getSender(); " +
                                "marshaller.callRemoteMethod( topic, sender," + methodIndex + ", $args, " + (loopback != null) + ", " + (unreliable != null) + " ); }";
                        method.setBody(remotecall);
                    }
                    cc.addMethod(method);
                } else {
                    if ( originalMethod.getAnnotation(RemoteHelper.class) == null && !"toString".equals(originalMethod.getName()) ) {
                        method.setBody("throw new RuntimeException(\"not a remote method, method must be public void method with Annotation 'RemoteMethod' \");");
                        cc.addMethod(method);
                    }
                    if ( remote != null ) {
                        throw new RuntimeException("@RemoteMethods have to be public void(..args). FCFutureResultHandler has to be last argument in case");
                    }
                }
            } else {
                if ( remote != null ) {
                    throw new RuntimeException("@RemoteMethods have to be public void(..args). FCFutureResultHandler has to be last argument in case");
                }
            }
        }
        return hasResultCalls;
    }

    private void generateCallerClass(CtClass callTarget, CtMethod method, CtClass[] parameterTypes, int methodindex, CtClass declaringClass) {
        String key = callTarget.getName() + "#" + methodindex;
        if ( generatedCallerClasses.get(key) != null ) {
            return;
        }
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass cc = null;
            cc = pool.makeClass(callTarget.getName()+"_"+methodindex);
            cc.setInterfaces(new CtClass[]{pool.get(FCInvoker.class.getName())});
            CtClass invoker = pool.get(FCInvoker.class.getName());
            CtMethod[] methods = invoker.getMethods();
            for (int i = 0; i < methods.length; i++) {
                CtMethod ctMethod = methods[i];
                if ( ctMethod.getName().equals("invoke") ) {
                    ctMethod = new CtMethod(ctMethod, cc, null);
                    String body = "{ " +
                            ""+FSTObjectInput.class.getName()+" out = $2;"+
                            "(("+declaringClass.getName()+")$1)."+method.getName()+"(";
                    for (int j = 0; j < parameterTypes.length; j++) {
                        CtClass parameterType = parameterTypes[j];
                        if ( parameterType.isArray() ) {
                            parameterType = parameterType.getComponentType();
                            if ( parameterType == CtClass.booleanType ) {
                                body+=" (boolean[]) out.readFPrimitiveArray( boolean.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.byteType ) {
                                body+=" (byte[]) out.readFPrimitiveArray( byte.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.charType ) {
                                body+=" (char[]) out.readFPrimitiveArray( char.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.shortType ) {
                                body+=" (short[]) out.readFPrimitiveArray( short.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.intType ) {
                                body+=" (int[]) out.readFPrimitiveArray( int.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.longType ) {
                                body+=" (long[]) out.readFPrimitiveArray( long.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.floatType ) {
                                body+=" (float[]) out.readFPrimitiveArray( float.class, out.readFInt()) ";
                            } else
                            if ( parameterType == CtClass.doubleType ) {
                                body+=" (double[]) out.readFPrimitiveArray( double.class, out.readFInt()) ";
                            } else
                                throw new RuntimeException("oops, this is a bug. in trouble with method "+method.getName());
                        } else
                        if ( parameterType == CtClass.booleanType ) {
                            body+=" out.readBoolean()";
                        } else
                        if ( parameterType == CtClass.byteType ) {
                            body+=" out.readFByte()";
                        } else
                        if ( parameterType == CtClass.charType ) {
                            body+=" out.readFChar()";
                        } else
                        if ( parameterType == CtClass.shortType ) {
                            body+=" out.readFShort()";
                        } else
                        if ( parameterType == CtClass.intType ) {
                            body+=" out.readFInt()";
                        } else
                        if ( parameterType == CtClass.longType ) {
                            body+=" out.readFLong()";
                        } else
                        if ( parameterType == CtClass.floatType ) {
                            body+=" out.readFFloat()";
                        } else
                        if ( parameterType == CtClass.doubleType ) {
                            body+=" out.readFDouble()";
                        } else
                        if ( parameterType.getName().equals(String.class.getName()) ) {
                            body+=" out.readStringUTF()";
                        }
                        if ( j != parameterTypes.length-1 ) {
                            body+=",";
                        }
                    }
                    body += "); }";
                    ctMethod.setBody(body);
                    cc.addMethod(ctMethod);
                }
            }

            Class ccClz = loadProxyClass(FCInvoker.class, pool, cc);
            generatedCallerClasses.put(key, (FCInvoker) ccClz.newInstance());
        } catch (Exception e) {
            FCLog.log(e);
        }
    }

    protected boolean isFastCall(CtMethod m) throws NotFoundException {
        CtClass[] parameterTypes = m.getParameterTypes();
        if (parameterTypes==null|| parameterTypes.length==0) {
            return true;
        }
        for (int i = 0; i < parameterTypes.length; i++) {
            CtClass parameterType = parameterTypes[i];
            boolean isPrimArray = parameterType.isArray() && parameterType.getComponentType().isPrimitive();
            if ( !isPrimArray && ! parameterType.isPrimitive() && !parameterType.getName().equals(String.class.getName()) ) {
                return false;
            }
        }
        return true;
    }

    protected boolean isRemoteResultCall(CtMethod m) throws NotFoundException {
        CtClass[] parameterTypes = m.getParameterTypes();
        if (parameterTypes==null|| parameterTypes.length==0) {
            return false;
        }
        String name = parameterTypes[parameterTypes.length - 1].getName();
        String name1 = FCFutureResultHandler.class.getName();
        return name.equals(name1);
    }

    protected void defineFCRemoteObjectMethods(ClassPool pool, CtClass cc, String serviceClazz, boolean hasRemoteResults) throws CannotCompileException, NotFoundException {
        CtClass proxy = pool.getCtClass(FCRemoteServiceProxy.class.getName());
        CtMethod[] methods;// add finalize
        methods = proxy.getMethods();
        for (int i = 0; i < methods.length; i++) {
            CtMethod method = methods[i];
            method = new CtMethod(method, cc, null);
            if (method.getName().equals("getServiceClass")) {
                method.setBody("{ return \"" + serviceClazz + "\"; }");
                cc.addMethod(method);
            }
            if (method.getName().equals("hasCallResultMethods")) {
                method.setBody("{ return " + hasRemoteResults + "; }");
                cc.addMethod(method);
            }
        }
    }

    public String toString(Method m) {
        try {
            StringBuilder sb = new StringBuilder();
            int mod = m.getModifiers() & java.lang.reflect.Modifier.methodModifiers();
            if (mod != 0) {
                sb.append(java.lang.reflect.Modifier.toString(mod)).append(' ');
            }
            sb.append(m.getReturnType()).append(' ');
            sb.append(m.getName()).append('(');
            Class<?>[] params = m.getParameterTypes();
            for (int j = 0; j < params.length; j++) {
                sb.append(params[j].toString());
                if (j < (params.length - 1))
                    sb.append(',');
            }
            sb.append(')');
            return sb.toString();
        } catch (Exception e) {
            return "<" + e + ">";
        }
    }

    public String toString(CtMethod m) {
        try {
            StringBuilder sb = new StringBuilder();
            int mod = m.getModifiers() & java.lang.reflect.Modifier.methodModifiers();
            if (mod != 0) {
                sb.append(java.lang.reflect.Modifier.toString(mod)).append(' ');
            }
            sb.append(m.getReturnType().getName()).append(' ');
            sb.append(m.getName()).append('(');
            CtClass[] params = m.getParameterTypes();
            for (int j = 0; j < params.length; j++) {
                sb.append(params[j].getName());
                if (j < (params.length - 1))
                    sb.append(',');
            }
            sb.append(')');
            return sb.toString();
        } catch (Exception e) {
            return "<" + e + ">";
        }
    }

    public Method[] getSortedPublicMethods(Class orig) {
        int count = 0;
        Method[] methods0 = orig.getMethods();
        HashSet alreadypresent = new HashSet();
        for (int i = 0; i < methods0.length; i++) {
            Method method = methods0[i];
            String str = toString(method);
            if (alreadypresent.contains(str)) {
                methods0[i] = null;
            } else
                alreadypresent.add(str);
        }

        // sanity
        HashSet<Byte> ids = new HashSet<>();
        for (int i = 0; i < methods0.length; i++) {
            Method method = methods0[i];
            RemoteMethod annotation = method.getAnnotation(RemoteMethod.class);
            if ( method != null && annotation != null && (!Modifier.isPublic(method.getModifiers())) )
            {
                throw new RuntimeException("Remote methods must be public:"+method.getName());
            }
            if ( annotation != null ) {
                if ( ids.contains(annotation.value())) {
                    throw new RuntimeException("double remote method id method '"+method.getName()+"' "+annotation.value());
                }
                if ( annotation.value() < 1 && !method.getName().equals("receiveBinary") ) {
                    throw new RuntimeException("remote method id must be > 0"+method);
                }
                ids.add(annotation.value());
            }
        }

        count = 0;
        for (int i = 0; i < methods0.length; i++) {
            Method method = methods0[i];
            if ( method != null && method.getAnnotation(RemoteMethod.class) != null && (Modifier.isPublic(method.getModifiers())) )
            {
                count++;
            }
        }
        Method methods[] = new Method[count];
        count = 0;
        for (int i = 0; i < methods0.length; i++) {
            Method method = methods0[i];
            if ( method != null && method.getAnnotation(RemoteMethod.class) != null && (Modifier.isPublic(method.getModifiers())) ) {
                methods[count++] = method;
            }
        }
        if ( orig.isInterface() ) {
            Method objM[] = Object.class.getMethods();
            Method res[] = new Method[objM.length+methods.length];
            System.arraycopy(methods,0,res,0,methods.length);
            System.arraycopy(objM,0,res,methods.length,objM.length);
            methods = res;
        }
        Arrays.sort(methods, new Comparator<Method>() {
            @Override
            public int compare(Method o1, Method o2) {
                return (o1.getName()+o1.getReturnType()+o1.getParameterTypes().length).compareTo(o2.getName()+o2.getReturnType()+o2.getParameterTypes().length);
            }
        });
        return methods;
    }

    protected CtMethod[] getSortedPublicCtMethods(CtClass orig, boolean onlyRemote) {
        int count = 0;
        CtMethod[] methods0 = orig.getMethods();
        HashSet alreadypresent = new HashSet();
        for (int i = methods0.length-1; i >= 0; i-- ) {
            CtMethod method = methods0[i];
            String str = toString(method);
            if (alreadypresent.contains(str)) {
                methods0[i] = null;
            } else
                alreadypresent.add(str);
        }

        CtMethod methods[] = null;
        if ( onlyRemote ) {
            for (int i = 0; i < methods0.length; i++) {
                try {
                    CtMethod method = methods0[i];
                    if (method != null ) {
                        boolean isRemote = method.getAnnotation(RemoteMethod.class) != null;
                        if ( isRemote && (method.getModifiers() & AccessFlag.PUBLIC) != 0 )
                        {
                            count++;
                        } else if ( isRemote ) {
                            throw new RuntimeException("@RemoteMethod's must be 'public void'");
                        }
                    }
                } catch (ClassNotFoundException e) {
                    FCLog.log(e);
                }
            }
            methods = new CtMethod[count];
            count = 0;
            for (int i = 0; i < methods0.length; i++) {
                try {
                    CtMethod method = methods0[i];
                    boolean isRemote = method.getAnnotation(RemoteMethod.class) != null;
                    if ( isRemote && (method.getModifiers() & AccessFlag.PUBLIC) != 0 ) {
                        methods[count++] = method;
                    }
                } catch (ClassNotFoundException e) {
                    FCLog.log(e);
                }
            }
        } else {
            count = 0;
            for (int i = 0; i < methods0.length; i++) {
                CtMethod method = methods0[i];
                if ( method != null ) {
                    count++;
                }
            }
            methods = new CtMethod[count];
            count = 0;
            for (int i = 0; i < methods0.length; i++) {
                CtMethod method = methods0[i];
                if ( method != null ) {
                    methods[count++] = method;
                }
            }
        }

        Arrays.sort(methods, new Comparator<CtMethod>() {
            @Override
            public int compare(CtMethod o1, CtMethod o2) {
                try {
                    return (o1.getName() + o1.getReturnType() + o1.getParameterTypes().length).compareTo(o2.getName() + o2.getReturnType() + o2.getParameterTypes().length);
                } catch (NotFoundException e) {
                    FCLog.log(e);
                    return 0;
                }
            }
        });
        return methods;
    }
}