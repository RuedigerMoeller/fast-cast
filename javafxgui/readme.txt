the mvn build creates an executable jar in ./target. Because of all the trouble with javafx+classpath, easiest is
to execute the jar with jdk 1.8 .
Note that you have to pass in a full path to a clustere config yaml. see my github fastcast-example/.../ClusterConfig.java
on how to create a file at startup.