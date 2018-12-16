package sample.cluster.simple;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class Executor implements /*Watcher,*/ Runnable, DataMonitor.DataMonitorListener {
        private String znode =  "/tables";
        private DataMonitor dm;
        private String runningPort;
        private SimpleClusterListener actorRef;

        public Executor(String hostPort, String znode, int sessiontimeOut
                , String runningPort , SimpleClusterListener actorRef) throws KeeperException, IOException {

            print("Executor Constructor", runningPort, "beginning");

            this.runningPort = runningPort;
            this.actorRef = actorRef;

            dm = new DataMonitor(znode, null, this, runningPort, actorRef);

            //dm.zookepperTableDeletion(znode);
            dm.startingUp();

            print("Executor Constructor", this.runningPort, "end");
        }

        //Runnable
        @Override
        public void run() {
            print("Executor run", this.runningPort, "beginning");
            try {
                synchronized (this) {
                    while (!dm.isDead()) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
            }
            print("Executor run", this.runningPort, "end");
        }

        //DataMonitor.DataMonitorListener
        @Override
        public void closing(int rc) {
            print("Executor closing", this.runningPort, "beginning");
            synchronized (this) {
                notifyAll();
            }
            print("Executor closing", this.runningPort, "end");
        }

        //DataMonitor.DataMonitorListener
        @Override
        public void exists(byte[] data) {
            print("Executor exists", this.runningPort, "beginning");

            if (data == null) {

            } else {

            }
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
