package com.netflix.exhibitor.core.importandexport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.rest.UIContext;
import com.sun.jersey.core.util.Base64;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.ACL;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Exporter {
    private final String startPath;
    private final Exhibitor exhibitor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Exporter(UIContext context, String startPath) {
        this.exhibitor = context.getExhibitor();

        if (Strings.isNullOrEmpty(startPath)) {
            this.startPath = "/";
        } else {
            if (startPath.startsWith("/")) {
                this.startPath = startPath;
            } else {
                this.startPath = "/" + startPath;
            }
        }
    }

    public String generate() throws Exception {
        ArrayNode jsonArray = JsonNodeFactory.instance.arrayNode();

        return convertToExportFormat(getChildren(startPath, jsonArray));
    }

    private String convertToExportFormat(ArrayNode jsonArray) throws JsonProcessingException {
        StringBuilder sb = new StringBuilder();
        sb.append("[\r\n");

        Iterator<JsonNode> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            JsonNode jsonNode = iterator.next();

            sb.append(convertToFlatJsonAsString(jsonNode.get("path").getTextValue(), jsonNode.get("data").getTextValue(),
                    jsonNode.get("acls").getElements()));

            if (iterator.hasNext()) {
                sb.append(",\r\n");
            }
        }

        sb.append("\r\n]");

        return sb.toString();
    }

    private String convertToFlatJsonAsString(String path, String data, Iterator<JsonNode> acls) throws JsonProcessingException {
        ExportObject exportObject = new ExportObject()
                .setPath(path)
                .setData(data);

        List<Acl> aclList = new ArrayList<Acl>();
        while (acls.hasNext()) {
            JsonNode aclNode = acls.next();
            aclList.add(new Acl()
                    .setScheme(aclNode.get("scheme").getTextValue())
                    .setId(aclNode.get("id").getTextValue())
                    .setPerms(aclNode.get("perms").getIntValue()));
        }
        exportObject.setAcls(aclList);

        return objectMapper.writeValueAsString(exportObject);
    }


    /**
     * Gets the list of children for a specific path. Then for each result it calls this method again. Eventually
     * we will have traversed the entire tree.
     *
     * @param path
     * @return ArrayNode
     * @throws Exception
     */
    private ArrayNode getChildren(String path, ArrayNode jsonArray) throws Exception {
        List<String> children = exhibitor.getLocalConnection().getChildren().forPath(path);
        jsonArray.add(getNodeDetails(path));

        for (String child : children) {
            getChildren(ZKPaths.makePath(path, child), jsonArray);
        }

        return jsonArray;
    }

    private JsonNode getNodeDetails(String path) throws Exception {
        byte[] data = exhibitor.getLocalConnection().getData().forPath(path);
        if (data == null) data = new byte[0];
        List<ACL> acls = exhibitor.getLocalConnection().getACL().forPath(path);

        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("path", path);
        node.put("data", new String(Base64.encode(data)));

        ArrayNode aclsArray = JsonNodeFactory.instance.arrayNode();
        for (ACL acl : acls) {
            ObjectNode aclNode = JsonNodeFactory.instance.objectNode();

            aclNode.put("scheme", acl.getId().getScheme());
            aclNode.put("id", acl.getId().getId());
            aclNode.put("perms", acl.getPerms());

            aclsArray.add(aclNode);
        }

        node.put("acls", aclsArray);

        return node;
    }

    private class ExportObject {
        private String path;
        private String data;
        private List<Acl> acls;

        public String getPath() {
            return path;
        }

        public ExportObject setPath(String path) {
            this.path = path;
            return this;
        }

        public String getData() {
            return data;
        }

        public ExportObject setData(String data) {
            this.data = data;
            return this;
        }

        public List<Acl> getAcls() {
            return acls;
        }

        public ExportObject setAcls(List<Acl> acls) {
            this.acls = acls;
            return this;
        }
    }

    private class Acl {
        private String scheme;
        private String id;
        private int perms;

        public String getScheme() {
            return scheme;
        }

        public Acl setScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public String getId() {
            return id;
        }

        public Acl setId(String id) {
            this.id = id;
            return this;
        }

        public int getPerms() {
            return perms;
        }

        public Acl setPerms(int perms) {
            this.perms = perms;
            return this;
        }
    }
}
