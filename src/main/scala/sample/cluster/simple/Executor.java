package sample.cluster.simple;

import sample.cluster.simple.DataMonitor;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class Executor
        implements /*Watcher,*/ Runnable, DataMonitor.DataMonitorListener
    {
        private String znode =  "/tables";
        private DataMonitor dm;
        //private ZooKeeper zk;
        private String runningPort;
        private SimpleClusterListener actorRef;

//        public Executor(String hostPort, String znode, int sessiontimeOut) throws KeeperException, IOException {
//
//            System.out.println("+++++++++++++Executor Executor inicio+++++++++++++");
//
//            zk = new ZooKeeper(hostPort, sessiontimeOut, this);
//
//            dm = new DataMonitor(zk, znode, null, this);
//
//            dm.startingUp();
//
//            System.out.println("+++++++++++++Executor Executor fin+++++++++++++");
//        }

//        public Executor(String hostPort, String znode, int sessiontimeOut
//                , String runningPort) throws KeeperException, IOException {
//
//            System.out.println("+++++++++++++Executor Executor inicio+++++++++++++");
//
//            zk = new ZooKeeper(hostPort, sessiontimeOut, this);
//
//            dm = new DataMonitor(zk, znode, null, this, runningPort);
//
//            this.runningPort = runningPort;
//
//            dm.startingUp();
//
//            System.out.println("+++++++++++++Executor Executor fin+++++++++++++");
//        }

        public Executor(String hostPort, String znode, int sessiontimeOut
                , String runningPort , SimpleClusterListener actorRef) throws KeeperException, IOException {

            //System.out.println("+++++++++++++Executor Executor inicio+++++++++++++");
            print("Executor Constructor", runningPort, "beginning");

            this.runningPort = runningPort;
            this.actorRef = actorRef;

            //zk = new ZooKeeper(hostPort, sessiontimeOut, this);

            dm = new DataMonitor(znode, null, this, runningPort, actorRef);

            dm.zookepperTableDeletion(znode);
            dm.startingUp();

            //System.out.println("+++++++++++++Executor Executor fin+++++++++++++");
            print("Executor Constructor", this.runningPort, "end");
        }

//        public void process(WatchedEvent event) {
//            //System.out.println("+++++++++++++Executor process inicio " +  runningPort + " +++++++++++++");
//            print("Executor process", this.runningPort, "beginning");
//            dm.process(event);
//            //System.out.println("+++++++++++++Executor process fin " +  runningPort + " +++++++++++++");
//            print("Executor process", this.runningPort, "end");
//        }

        //Runnable
        @Override
        public void run() {
            //System.out.println("+++++++++++++Executor run inicio  " +  runningPort + " +++++++++++++");
            print("Executor run", this.runningPort, "beginning");
            try {
                synchronized (this) {
                    while (!dm.isDead()) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
            }
            //System.out.println("+++++++++++++Executor run fin " +  runningPort + " +++++++++++++");
            print("Executor run", this.runningPort, "end");
        }

        //DataMonitor.DataMonitorListener
        @Override
        public void closing(int rc) {
            //System.out.println("+++++++++++++Executor closing inicio  " +  runningPort + " +++++++++++++");
            print("Executor closing", this.runningPort, "beginning");
            synchronized (this) {
                notifyAll();
            }
            //System.out.println("+++++++++++++Executor closing fin  " +  runningPort + " +++++++++++++");
            print("Executor closing", this.runningPort, "end");
        }
        //DataMonitor.DataMonitorListener
        @Override
        public void exists(byte[] data) {
            //System.out.println("+++++++++++++Executor exists inicio  " +  runningPort + " +++++++++++++");
            print("Executor exists", this.runningPort, "beginning");

            if (data == null) {

            } else {

            }
            //System.out.println("+++++++++++++Executor exists fin  " +  runningPort + " +++++++++++++");
            print("Executor exists", this.runningPort, "end");
        }

        public DataMonitor getDataMonitor(){
            return dm;
        }

        public List<String> getTablesDictionary(){
            return this.dm.getTablesDictionary();
        }

        public void print(String method, String port, String trace){
            System.out.println("+++++++++++++" + method + " " + port + " " + trace +  "+++++++++++++");
        }
}
