package de.ruedigermoeller.fastcast.bigtest.services;

import de.ruedigermoeller.fastcast.remoting.FCFutureResultHandler;
import de.ruedigermoeller.fastcast.remoting.FCTopicService;
import de.ruedigermoeller.fastcast.remoting.RemoteMethod;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ruedi
 * Date: 9/14/13
 * Time: 2:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class RequestServer extends FCTopicService {

    int fi = 0; int numNo = 0;

    public void setFilter(int filter, int numNodes) {
        fi = filter; numNo = numNodes;
        System.out.println("set filter "+fi+" of "+numNodes);
    }

    public static class TestOne implements Serializable {
        String name;
        int age;
        List children;

        public TestOne(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public boolean equals( Object other ) {
            if ( other instanceof TestOne ) {
                TestOne to = (TestOne) other;
                return name.equals(to.name) && age == to.age;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode()^age;
        }

        @Override
        public String toString() {
            return "TestOne{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", children=" + children +
                    '}';
        }
    }

    public static TestOne createTest(int random, int children) {
        TestOne res = new TestOne("Emil"+(random%10)+" Moeller"+(random%10), 7+random%10);
        res.children = new ArrayList();
        for ( int i = 0; i<children;i++ ) {
            res.children.add(createTest(random+1, 0) );
        }
        return res;
    }

    @RemoteMethod(1)
    public void receiveRequest(int random, FCFutureResultHandler res) {
        if ( numNo > 0 ) {
            if ( random%numNo == fi ) {
                TestOne test = createTest(random, random % 5);
                res.sendResult(test);
            }
        }
    }

}
