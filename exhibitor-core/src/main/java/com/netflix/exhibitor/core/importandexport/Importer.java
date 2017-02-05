package com.netflix.exhibitor.core.importandexport;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.rest.UIContext;
import com.sun.jersey.core.util.Base64;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.codehaus.jackson.JsonNode;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.*;

public class Importer {

    private final UIContext context;

    public Importer(UIContext context)
    {
        this.context = context;
    }

    /**
     * Imports all of the supplied nodes, starting at the prescribed base node. Because it isn't possible to set ACLs
     * on set commands, only create, and because we're using a transaction, the flow of this method is as follows.
     *
     * 1. If the node already exists and we're not overwriting, ignore this node and leave it alone.
     * 2. If the node already exists and we are overwriting, add the set command to the transaction and save the ACL
     *    details. Once the transaction has been successfully committed, we then apply all the ACLs that we saved.
     * 3. If the node does not exist, add the create command to the transaction including the ACLs needed.
     *
     * This does leave us open to a couple of possible issues. It might be possible that a node doesn't exist when we
     * add it to the transaction, but it has been created by a 3rd party before we commit.
     *
     * It is also possible that something could go wrong when applying the ACLs to a node. Because the transaction has
     * already been committed at that point, we will be left in a position where the node data has been updated but not
     * the ACL.
     */
    public void doImport(String basePath, boolean overwrite, Iterator<JsonNode> nodesToImport) throws Exception {
        CuratorTransaction transaction = context.getExhibitor().getLocalConnection().inTransaction();
        CuratorTransactionFinal curatorTransactionFinal = null;
        Map<String, List<ACL>> savedACLs = new HashMap<String, List<ACL>>();
        Set<String> toBeCreated = new HashSet<String>();

        while (nodesToImport.hasNext()) {
            JsonNode jsonNode = nodesToImport.next();
            JsonNode pathNode = jsonNode.get("path");
            JsonNode dataNode = jsonNode.get("data");
            JsonNode aclsNode = jsonNode.get("acls");

            if (pathNode == null || dataNode == null) throw new WebApplicationException(Response.Status.BAD_REQUEST);

            String path = ZKPaths.makePath(basePath, pathNode.getTextValue());
            byte[] data = Base64.decode(dataNode.getTextValue());
            List<ACL> aclList = createACLList(aclsNode);

            boolean alreadyExists = nodeAlreadyExists(path);

            if (overwrite || !alreadyExists) {
                if (alreadyExists) {
                    curatorTransactionFinal = transaction.setData().forPath(path, data).and();
                    savedACLs.put(path, aclList);
                } else {
                    createParentsIfNeeded(transaction, path, aclList, toBeCreated);
                    curatorTransactionFinal = transaction.create().withACL(aclList).forPath(path, data).and();
                    toBeCreated.add(path);
                }
            }
        }

        if (curatorTransactionFinal == null) {
            context.getExhibitor().getLog().add(ActivityLog.Type.INFO, "There was nothing to import");
            return;
        }

        curatorTransactionFinal.commit();

        // Finally we apply those ACLs we saved
        for (Map.Entry<String, List<ACL>> entry : savedACLs.entrySet()) {
            context.getExhibitor().getLocalConnection().setACL().withACL(entry.getValue()).forPath(entry.getKey());
        }
    }

    private void createParentsIfNeeded(CuratorTransaction transaction, String path, List<ACL> acls, Set<String> toBeCreated) throws Exception {
        String[] parts = path.substring(1).split("/");
        String builtUpPath = "";

        // We do this to parts.length - 1, because we don't want to create the final path, as that's being done in the
        // calling method.

        for (int i = 0; i < (parts.length - 1); i++) {
            builtUpPath += "/" + parts[i];

            if (!toBeCreated.contains(builtUpPath) && context.getExhibitor().getLocalConnection().checkExists().forPath(builtUpPath) == null) {
                transaction.create().withACL(acls).forPath(builtUpPath, new byte[0]);
                toBeCreated.add(builtUpPath);
            }
        }
   }

    private List<ACL> createACLList(JsonNode aclsNode) {
        List<ACL> aclList = new ArrayList<ACL>();
        if (aclsNode == null) return aclList;

        Iterator<JsonNode> acls = aclsNode.getElements();

        while (acls.hasNext()) {
            JsonNode aclNode = acls.next();

            String scheme = aclNode.get("scheme").getTextValue();
            String id = aclNode.get("id").getTextValue();
            int perms = aclNode.get("perms").getIntValue();

            aclList.add(new ACL(perms, new Id(scheme, id)));
        }

        return aclList;
    }

    private boolean nodeAlreadyExists(String path) throws Exception {
        return (context.getExhibitor().getLocalConnection().checkExists().forPath(path) != null);
    }

}
