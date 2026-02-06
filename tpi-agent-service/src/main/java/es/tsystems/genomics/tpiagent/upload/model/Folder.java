package es.tsystems.genomics.tpiagent.upload.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Model for cataloguing folders containing files.
 */
public class Folder {
    private String name;
    private String url;
    private Source source;
    private List<FileRef> files = new ArrayList<>();
    private List<Folder> folders = new ArrayList<>();

    public Folder() {
    }

    public Folder(String name, String url, Source source) {
        this.name = name;
        this.url = url;
        this.source = source;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public List<FileRef> getFiles() {
        return files;
    }

    public void setFiles(List<FileRef> files) {
        this.files = files;
    }

    public List<Folder> getFolders() {
        return folders;
    }

    public void setFolders(List<Folder> folders) {
        this.folders = folders;
    }
}

