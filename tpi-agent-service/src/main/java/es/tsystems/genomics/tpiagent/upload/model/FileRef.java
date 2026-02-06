package es.tsystems.genomics.tpiagent.upload.model;

/**
 * File metadata for cataloguing folder contents.
 */
public class FileRef {
    private String name;
    private String url;

    public FileRef() {
    }

    public FileRef(String name, String url) {
        this.name = name;
        this.url = url;
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
}

