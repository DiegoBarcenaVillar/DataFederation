package sample.cluster.simple;

import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class DataMonitor implements Watcher, ZooDefs.Ids {
        private ZooKeeper zk;
        private String znode;
        private Watcher chainedWatcher;
        private boolean dead;
        private DataMonitorListener listener;
        private byte prevData[];
        private List<String> tableDictionary;
        private String runningPort;
        private SimpleClusterListener actorRef;

        private Constants constants  = new Constants();

        public DataMonitor(String znode, Watcher chainedWatcher,
                           DataMonitorListener listener, String runningPort, SimpleClusterListener actorRef) {

            print("DataMonitor Constructor", runningPort, "beginning");

            this.znode = znode;
            this.chainedWatcher = chainedWatcher;
            this.listener = listener;
            this.runningPort = runningPort;
            this.actorRef = actorRef;

            print("DataMonitor Constructor", this.runningPort, "end");
        }

        /**
         * Other classes use the DataMonitor by implementing this method
         */
        public interface DataMonitorListener {
            /**
             * The existence status of the node has changed.
             */
            void exists(byte data[]);

            /**
             * The ZooKeeper session is no longer valid.
             *
             * @param rc
             *                the ZooKeeper reason code
             */
            void closing(int rc);
        }

        public void startingUp(){
            // Get things started by checking if the node exists. We are going
            // to be completely event driven
            print("DataMonitor startingUp", runningPort, "beginning");
            try {
                this.zk = new ZooKeeper(constants.hostPort(), constants.sessionTimeOut(), this);
            }catch(java.io.IOException ioEx){
                ioEx.printStackTrace();
            }
            //zk.exists(znode, true, this, null);
            List<ACL> lstAcl = OPEN_ACL_UNSAFE;

            try{
                if(zk.exists(znode, this)==null)
                    zk.create(znode, null, lstAcl, CreateMode.PERSISTENT);

                tableDictionary = this.zk.getChildren(znode, this);

                print("DataMonitor startingUp", runningPort, "%%List of Children-Tables at Start Up Begin%%%%");
                for(String table : tableDictionary) {
                    print("DataMonitor startingUp", runningPort, "%%Existing at Start UP%%" + table + "%%");
                    print("DataMonitor startingUp", runningPort, "%%Deleted at Start UP%%" + table + "%%");
                    this.zk.exists(znode.concat("/").concat(table), this);
                    this.zk.getData(znode.concat("/").concat(table), this, null);
                }
                print("DataMonitor startingUp", runningPort, "%%List of Children-Tables at Start Up End%%%%");
            }catch (KeeperException kex) {
                kex.printStackTrace();
            } catch (InterruptedException iex) {
                iex.printStackTrace();
            }finally{
                print("DataMonitor startingUp", runningPort, "end");
            }
        }

        public void zookepperTableDeletion(String znode){
            print("DataMonitor zookepperTableDeletion", runningPort, "beginning");

            ZooKeeper zkLocal = null;
            List<String> tableDictionary = null;

            try {
                zkLocal = new ZooKeeper(constants.hostPort(), constants.sessionTimeOut(), this);
                tableDictionary = zkLocal.getChildren(znode, false);

                print("DataMonitor zookepperTableDeletion", runningPort, "Tables to be deleted "  + tableDictionary.size());

                for(String table : tableDictionary) {
                    print("DataMonitor zookepperTableDeletion", runningPort, "%%Existing at Start UP%%" + table + "%%");

                    zkLocal.delete(znode.concat("/").concat(table),-1);

                    print("DataMonitor zookepperTableDeletion", runningPort, "%%Deleted at Start UP%%" + table + "%%");
                }
            }catch (KeeperException kex) {
                    kex.printStackTrace();
            } catch (InterruptedException iex) {
                    iex.printStackTrace();
            }catch(java.io.IOException ioEx){
                    ioEx.printStackTrace();
            }finally{
                print("DataMonitor zookepperTableDeletion", runningPort, "end");
            }
        }

        //Watcher
        @Override
        public void process(WatchedEvent event) {
            print("DataMonitor process", runningPort, "beginning");
            print("DataMonitor process", runningPort, "getType " + event.getType());
            print("DataMonitor process", runningPort, "getState " + event.getState());
            print("DataMonitor process", runningPort, "getPath: " + event.getPath());
            try {
                String path = event.getPath();

                String tableName = null;
                String createString = null;

                if (event.getType() == Event.EventType.None) {
                    // We are are being told that the state of the
                    // connection has changed
                    switch (event.getState()) {
                        case SyncConnected:
                            // In this particular example we don't need to do anything
                            // here - watches are automatically re-registered with
                            // server and any watches triggered while the client was
                            // disconnected will be delivered (in order of course)
                            break;
                        case Expired:
                            // It's all over
                            dead = true;
                            listener.closing(Code.SessionExpired);
                            break;
                    }
                }else if(event.getType() == Event.EventType.NodeChildrenChanged){
                    print("DataMonitor process", runningPort, "event.getPath: " + event.getPath());
                    print("DataMonitor process", runningPort, "event.toString: " + event.toString());
                    print("DataMonitor process", runningPort, "event.getState: " + event.getState());
                    print("DataMonitor process", runningPort, "event.getWrapper().toString(): " + event.getWrapper().getPath());
                    print("DataMonitor process", runningPort, "event.getWrapper().toString(): " + event.getWrapper().getType());
                    print("DataMonitor process", runningPort, "event.getWrapper().toString(): " + event.getWrapper().toString());

                    List<String> newTableDictionary = this.zk.getChildren(znode, true);

                    print("DataMonitor process", runningPort, "newTableDictionary " + newTableDictionary.size()
                        + " tableDictionary " + tableDictionary.size());

                    if (newTableDictionary.size() > tableDictionary.size()) {
                        tableName = newTableDictionary.stream()
                                        .filter(t -> !this.tableDictionary.contains(t)).findFirst().get();

                        print("DataMonitor process", runningPort, "newTable: " + tableName);

                        createString = new String(this.zk.getData(event.getPath().concat("/").concat(tableName)
                                        , this, new Stat()));

                        actorRef.updateSpark(createString, AllowedActions.Create());

                    } else if (newTableDictionary.size() < tableDictionary.size()) {
                        tableName = this.tableDictionary.stream()
                                    .filter(t -> !newTableDictionary.contains(t)).findFirst().get();

                        createString = constants.tableLabelDrop().concat(" ").concat(tableName);

                        actorRef.updateSpark(createString, AllowedActions.Drop());
                    }

                    this.tableDictionary = newTableDictionary;

                    print("DataMonitor process", runningPort, "Operation in Table: " + createString);
                } else if(event.getType() == Event.EventType.NodeDataChanged){
                    String[] nodes = path.split("/");
                    tableName = nodes[nodes.length - 1];

                    createString = new String(this.zk.getData(event.getPath().concat("/").concat(tableName)
                                , this, new Stat()));

                    actorRef.updateSpark(createString, AllowedActions.Create());
                } else {
                    if (path != null && path.equals(znode)) {
                        // Something has changed on the node, let's find out
                        zk.exists(znode, this);
                    }
                }

                if (chainedWatcher != null) {
                    chainedWatcher.process(event);
                }
            }catch(KeeperException kex){
                kex.printStackTrace();
            }catch(InterruptedException iex) {
                iex.printStackTrace();
            }
            print("DataMonitor process", runningPort, "end");
        }

        public boolean isDead() {
            return dead;
        }

        public List<String> getTablesDictionary() {
            return tableDictionary;
        }

        public void print(String method, String port, String trace){
            System.out.println("*************" + method + " " + port + " " + trace +  "*************");
        }
}
