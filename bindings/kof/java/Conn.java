import java.util.ArrayList;

/**
 * JVM face of libglacier. Default package so Kof {@code class Conn} matches.
 * No overloads (Kof resolves the wrong one).
 */
public final class Conn {
    private long db;

    private Conn(long db) {
        this.db = db;
    }

    public Conn() {
        this.db = 0;
    }

    static {
        String jni = System.getenv("GLACIER_KOF_JNI");
        if (jni != null && !jni.isEmpty()) {
            System.load(jni);
        } else {
            System.loadLibrary("glacier_kof");
        }
    }

    public static String version() {
        return nativeVersion();
    }

    public static int apiVersion() {
        return nativeApiVersion();
    }

    public static Conn connectEmpty() {
        return new Conn(nativeOpenEmpty());
    }

    public static Conn connectPath(String path) {
        return new Conn(nativeOpenPath(path));
    }

    public ArrayList execute(String sql) {
        return nativeQuery(db, sql);
    }

    public int firstInt(String sql) {
        ArrayList rows = execute(sql);
        if (rows == null || rows.isEmpty()) {
            throw new RuntimeException("no rows");
        }
        ArrayList row = (ArrayList) rows.get(0);
        Object v = row.get(0);
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        throw new RuntimeException("first cell is not a number");
    }

    public String firstText(String sql) {
        ArrayList rows = execute(sql);
        if (rows == null || rows.isEmpty()) {
            throw new RuntimeException("no rows");
        }
        ArrayList row = (ArrayList) rows.get(0);
        Object v = row.get(0);
        return v == null ? "" : v.toString();
    }

    public int rowCount(String sql) {
        ArrayList rows = execute(sql);
        return rows == null ? 0 : rows.size();
    }

    public void close() {
        if (db != 0) {
            nativeClose(db);
            db = 0;
        }
    }

    private static native String nativeVersion();

    private static native int nativeApiVersion();

    private static native long nativeOpenEmpty();

    private static native long nativeOpenPath(String path);

    private static native void nativeClose(long db);

    private static native ArrayList nativeQuery(long db, String sql);
}
