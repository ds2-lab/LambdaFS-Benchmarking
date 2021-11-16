package com.gmail.benrcarver;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TreeNode {
    final String path;
    final List<TreeNode> children;

    public TreeNode(String path, List<TreeNode> children) {
        this.path = path;
        this.children = children;
    }

    public void addChildren(Collection<TreeNode> nodes) {
        this.children.addAll(nodes);
    }

    public void addChild(TreeNode node) {
        this.children.add(node);
    }

    public String getPath() {
        return this.path;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder(50);
        print(buffer, "", "");
        return buffer.toString();
    }

    private void print(StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append(path);
        buffer.append('\n');
        for (Iterator<TreeNode> it = children.iterator(); it.hasNext();) {
            TreeNode next = it.next();
            if (it.hasNext()) {
                next.print(buffer, childrenPrefix + "├── ", childrenPrefix + "│   ");
            } else {
                next.print(buffer, childrenPrefix + "└── ", childrenPrefix + "    ");
            }
        }
    }
}