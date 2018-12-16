package sample.cluster.simple;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ZookeeperModifier implements Watcher, Ids {
    private String hostPort;
    private int sessiontimeOut;
    private String rootZnodeName;
    private ZooKeeper zk;
    private String runningPort;
    private Map<String, String> mapTables;
    private SimpleClusterListener actorRef;

    public ZookeeperModifier(String hostPort, String rootZnodeName, int sessiontimeOut
                                    , String runningPort, SimpleClusterListener actorRef){

        print("ZookeeperModifier Constructor", runningPort, "beginning");
        this.hostPort = hostPort;
        this.sessiontimeOut = sessiontimeOut;
        this.rootZnodeName = rootZnodeName;
        this.runningPort = runningPort;
        this.actorRef = actorRef;
        print("ZookeeperModifier Constructor", this.runningPort, "end");
    }

    public void startUp() {
        try {
            print("ZookeeperModifier startUp", runningPort, "beginning");

            this.mapTables = new HashMap<String, String>();

            this.zk = new ZooKeeper(this.hostPort,this.sessiontimeOut, this);
            List<String > lstTables = this.zk.getChildren(rootZnodeName, false);

            lstTables.stream().forEach(t->{
                    try {
                        String creatingString = new String(this.zk.getData(rootZnodeName.concat("/").concat(t)
                                , false, new Stat()));
                        actorRef.updateSpark(creatingString, AllowedActions.Create());
                        mapTables.put(t, creatingString);
                    }catch(KeeperException kex){
                    }catch(InterruptedException iex) {
                    }
                });
        }
        catch(KeeperException kex){
            kex.printStackTrace();
        }
        catch(InterruptedException iex) {
            iex.printStackTrace();
        } catch (java.io.IOException ioex) {
            ioex.printStackTrace();
        } finally{
            print("ZookeeperModifier startUp", runningPort, "end");
        }
    }

    public String createTable(String tableName, String createString){
        print("ZookeeperModifier createTable", runningPort, "beginning");
        print("ZookeeperModifier createTable", runningPort, "Table Name: "  +  tableName);
        print("ZookeeperModifier createTable", runningPort, "Create String: "  +  createString);
        print("ZookeeperModifier createTable", runningPort, "isTableAlreadyCreated(tableName): "
                +  isTableAlreadyCreated(tableName, createString));

        if(isTableAlreadyCreated(tableName, createString))
            return null;

        List<ACL> lstAcl = OPEN_ACL_UNSAFE;
        try {
            this.mapTables.put(tableName, createString);

            String path = this.zk.create(rootZnodeName.concat("/").concat(tableName)
                    , createString.getBytes()
                    , lstAcl, CreateMode.PERSISTENT);
            return path;
        }catch(KeeperException kex){
            if(kex.code()!=KeeperException.Code.NODEEXISTS)
                kex.printStackTrace();
            return "";
        }catch(InterruptedException iex) {
            iex.printStackTrace();
            return "";
        }finally{
            print("ZookeeperModifier createTable", runningPort, "end");
        }
    }

    public void dropTable(String tableName){
        print("ZookeeperModifier dropTable", runningPort, "beginning");
        try{
            this.zk.delete(rootZnodeName.concat("/").concat(tableName), -1);
            this.mapTables.remove(tableName);
        }
        catch(KeeperException kex){
            kex.printStackTrace();
        }catch(InterruptedException iex) {
            iex.printStackTrace();
        }finally{
            print("ZookeeperModifier dropTable", runningPort, "end");
        }
    }

    public void read() {
        print("ZookeeperModifier read", runningPort, "beginning");
        try{
            List<ACL> lstAcl = OPEN_ACL_UNSAFE;

            for (String table : mapTables.keySet()) {
                print("ZookeeperModifier read", runningPort, "@@" + table + "@@ Port: " + runningPort + " @@");
                print("ZookeeperModifier read", runningPort, "@@" + mapTables.get(table) + "@@ Port: " + runningPort + " @@");
            }
        }finally{
            print("ZookeeperModifier read", runningPort, "end");
        }
    }

    private boolean isTableAlreadyCreated(String tableName, String createString){
        return this.mapTables.get(tableName)!=null && this.mapTables.get(tableName).equals(createString);
    }

    public void process(WatchedEvent event) {
        System.out.println("=====================================");
    }

    public void print(String method, String port, String trace){
        System.out.println("&&&&&&&&&&&&&" + method + " " + port + " " + trace +  "&&&&&&&&&&&&&");
    }
}