package tfm.cluster;

import java.util.List;

public class ZookeeperListener implements Runnable, DataMonitor.DataMonitorListener {

        private DataMonitor dm;
        private String runningPort;

        public ZookeeperListener(String znode, String runningPort , ClusterListener actorRef){

            print("ZookeeperListener Constructor", runningPort, "beginning");

            this.runningPort = runningPort;

            dm = new DataMonitor(znode, null, this, runningPort, actorRef);

            dm.startingUp();

            if(tfm.common.Constants.deleteTablesOnStartup())
                dm.zookepperTableDeletion(znode);

            print("ZookeeperListener Constructor", this.runningPort, "end");
        }

        //Runnable
        @Override
        public void run() {
            print("ZookeeperListener run", this.runningPort, "beginning");
            try {
                synchronized (this) {
                    while (!dm.isDead()) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            print("ZookeeperListener run", this.runningPort, "end");
        }

        //DataMonitor.DataMonitorListener
        @Override
        public void closing(int rc) {
            print("ZookeeperListener closing", this.runningPort, "beginning");
            synchronized (this) {
                notifyAll();
            }
            print("ZookeeperListener closing", this.runningPort, "end");
        }

        @Override
        public void exists(byte[] data) {

        }

        public List<String> getZookeeperTables(){

            return this.dm.getTablesDictionary();

        }

        private void print(String method, String port, String trace){
            System.out.println("+++++++++++++" + method + " " + port + " " + trace +  "+++++++++++++");
        }
}
