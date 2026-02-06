package es.tsystems.genomics.tpiagent.upload.model;


public class UploadPart {
    private int partNumber;
    private String etag;

    /**
     * Path relativo del fichero dentro del run (cuando el upload es por carpeta).
     * Para uploads de fichero suelto puede ser null.
     */
    private String itemRelativePath;

    /**
     * URI/identificador del objeto al que corresponde esta parte (normalmente el s3Key).
     */
    private String uri;

    public UploadPart() {
    }

    public UploadPart(int partNumber, String etag) {
        this.partNumber = partNumber;
        this.etag = etag;
    }

    public UploadPart(int partNumber, String etag, String itemRelativePath, String uri) {
        this.partNumber = partNumber;
        this.etag = etag;
        this.itemRelativePath = itemRelativePath;
        this.uri = uri;
    }

    public int getPartNumber() { return partNumber; }
    public void setPartNumber(int partNumber) { this.partNumber = partNumber; }

    public String getEtag() { return etag; }
    public void setEtag(String etag) { this.etag = etag; }

    public String getItemRelativePath() { return itemRelativePath; }
    public void setItemRelativePath(String itemRelativePath) { this.itemRelativePath = itemRelativePath; }

    public String getUri() { return uri; }
    public void setUri(String uri) { this.uri = uri; }
}


