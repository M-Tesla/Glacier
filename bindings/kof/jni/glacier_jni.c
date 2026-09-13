/* JNI glue for bindings/kof. Speaks glacier.h. No query engine here. */

#include "glacier.h"

#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static void throw_msg(JNIEnv *env, const char *msg) {
    jclass ex = (*env)->FindClass(env, "java/lang/RuntimeException");
    if (ex) (*env)->ThrowNew(env, ex, msg ? msg : "glacier error");
}

static int bit_at(const uint8_t *buf, int64_t i) {
    return (buf[i >> 3] >> (i & 7)) & 1;
}

static int valid_at(const struct ArrowArray *a, int64_t i) {
    if (a->null_count == 0 || a->n_buffers < 1 || !a->buffers || !a->buffers[0]) return 1;
    return bit_at((const uint8_t *)a->buffers[0], i);
}

static jobject box_bool(JNIEnv *env, int v) {
    jclass c = (*env)->FindClass(env, "java/lang/Boolean");
    jmethodID m = (*env)->GetStaticMethodID(env, c, "valueOf", "(Z)Ljava/lang/Boolean;");
    return (*env)->CallStaticObjectMethod(env, c, m, (jboolean)(v != 0));
}

static jobject box_int(JNIEnv *env, jint v) {
    jclass c = (*env)->FindClass(env, "java/lang/Integer");
    jmethodID m = (*env)->GetStaticMethodID(env, c, "valueOf", "(I)Ljava/lang/Integer;");
    return (*env)->CallStaticObjectMethod(env, c, m, v);
}

static jobject box_long(JNIEnv *env, jlong v) {
    jclass c = (*env)->FindClass(env, "java/lang/Long");
    jmethodID m = (*env)->GetStaticMethodID(env, c, "valueOf", "(J)Ljava/lang/Long;");
    return (*env)->CallStaticObjectMethod(env, c, m, v);
}

static jobject box_double(JNIEnv *env, jdouble v) {
    jclass c = (*env)->FindClass(env, "java/lang/Double");
    jmethodID m = (*env)->GetStaticMethodID(env, c, "valueOf", "(D)Ljava/lang/Double;");
    return (*env)->CallStaticObjectMethod(env, c, m, v);
}

static jobject cell_at(JNIEnv *env, const struct ArrowSchema *s, const struct ArrowArray *a, int64_t i) {
    if (!valid_at(a, i)) return NULL;
    const char *fmt = s->format ? s->format : "";
    if (fmt[0] == 'b') {
        const uint8_t *bits = (const uint8_t *)a->buffers[1];
        return box_bool(env, bit_at(bits, i));
    }
    if (fmt[0] == 'i') {
        const int32_t *d = (const int32_t *)a->buffers[1];
        return box_int(env, d[i]);
    }
    if (fmt[0] == 'l') {
        const int64_t *d = (const int64_t *)a->buffers[1];
        return box_long(env, (jlong)d[i]);
    }
    if (fmt[0] == 'f') {
        const float *d = (const float *)a->buffers[1];
        return box_double(env, (jdouble)d[i]);
    }
    if (fmt[0] == 'g') {
        const double *d = (const double *)a->buffers[1];
        return box_double(env, d[i]);
    }
    if (fmt[0] == 'u') {
        const int32_t *off = (const int32_t *)a->buffers[1];
        const char *bytes = (const char *)a->buffers[2];
        int32_t start = off[i], end = off[i + 1];
        int32_t n = end - start;
        char *tmp = (char *)malloc((size_t)n + 1);
        if (!tmp) return NULL;
        memcpy(tmp, bytes + start, (size_t)n);
        tmp[n] = 0;
        jobject js = (*env)->NewStringUTF(env, tmp);
        free(tmp);
        return js;
    }
    return NULL;
}

static jobject rows_from_result(JNIEnv *env, GlacierResult *result) {
    struct ArrowArray array;
    struct ArrowSchema schema;
    memset(&array, 0, sizeof(array));
    memset(&schema, 0, sizeof(schema));
    if (glacier_result_arrow(result, &array, &schema) != 0) {
        throw_msg(env, "failed to export Arrow");
        return NULL;
    }
    if (!schema.format || strcmp(schema.format, "+s") != 0) {
        if (array.release) array.release(&array);
        if (schema.release) schema.release(&schema);
        throw_msg(env, "expected struct Arrow batch");
        return NULL;
    }

    jclass alc = (*env)->FindClass(env, "java/util/ArrayList");
    jmethodID init = (*env)->GetMethodID(env, alc, "<init>", "()V");
    jmethodID add = (*env)->GetMethodID(env, alc, "add", "(Ljava/lang/Object;)Z");
    jobject rows = (*env)->NewObject(env, alc, init);

    int64_t n_cols = schema.n_children;
    int64_t r, c;
    for (r = 0; r < array.length; r++) {
        jobject row = (*env)->NewObject(env, alc, init);
        for (c = 0; c < n_cols; c++) {
            jobject cell = cell_at(env, schema.children[c], array.children[c], r);
            (*env)->CallBooleanMethod(env, row, add, cell);
            if (cell) (*env)->DeleteLocalRef(env, cell);
        }
        (*env)->CallBooleanMethod(env, rows, add, row);
        (*env)->DeleteLocalRef(env, row);
    }
    if (array.release) array.release(&array);
    if (schema.release) schema.release(&schema);
    return rows;
}

JNIEXPORT jstring JNICALL Java_Conn_nativeVersion(JNIEnv *env, jclass cls) {
    (void)cls;
    return (*env)->NewStringUTF(env, glacier_version());
}

JNIEXPORT jint JNICALL Java_Conn_nativeApiVersion(JNIEnv *env, jclass cls) {
    (void)env;
    (void)cls;
    return glacier_api_version();
}

struct JniHandle {
    GlacierDatabase *db;
    GlacierConn *conn;
};

static jlong open_db(JNIEnv *env, const char *path) {
    char *err = NULL;
    GlacierDatabase *db = glacier_open(path, &err);
    if (!db) {
        throw_msg(env, err ? err : "open failed");
        if (err) glacier_free(err);
        return 0;
    }
    GlacierConn *conn = glacier_connect(db, &err);
    if (!conn) {
        throw_msg(env, err ? err : "connect failed");
        if (err) glacier_free(err);
        glacier_close(db);
        return 0;
    }
    struct JniHandle *h = (struct JniHandle *)malloc(sizeof(struct JniHandle));
    if (!h) {
        glacier_disconnect(conn);
        glacier_close(db);
        throw_msg(env, "out of memory");
        return 0;
    }
    h->db = db;
    h->conn = conn;
    return (jlong)(uintptr_t)h;
}

JNIEXPORT jlong JNICALL Java_Conn_nativeOpenEmpty(JNIEnv *env, jclass cls) {
    (void)cls;
    return open_db(env, NULL);
}

JNIEXPORT jlong JNICALL Java_Conn_nativeOpenPath(JNIEnv *env, jclass cls, jstring path) {
    (void)cls;
    if (!path) {
        throw_msg(env, "path is required");
        return 0;
    }
    const char *p = (*env)->GetStringUTFChars(env, path, NULL);
    jlong db = open_db(env, p);
    (*env)->ReleaseStringUTFChars(env, path, p);
    return db;
}

JNIEXPORT void JNICALL Java_Conn_nativeClose(JNIEnv *env, jclass cls, jlong db) {
    (void)env;
    (void)cls;
    struct JniHandle *h = (struct JniHandle *)(uintptr_t)db;
    if (!h) return;
    glacier_disconnect(h->conn);
    glacier_close(h->db);
    free(h);
}

JNIEXPORT jobject JNICALL Java_Conn_nativeQuery(JNIEnv *env, jclass cls, jlong db, jstring sql) {
    (void)cls;
    struct JniHandle *h = (struct JniHandle *)(uintptr_t)db;
    if (!h || !h->conn) {
        throw_msg(env, "connection is closed");
        return NULL;
    }
    if (!sql) {
        throw_msg(env, "sql is required");
        return NULL;
    }
    const char *q = (*env)->GetStringUTFChars(env, sql, NULL);
    char *err = NULL;
    GlacierResult *result = glacier_query(h->conn, q, &err);
    (*env)->ReleaseStringUTFChars(env, sql, q);
    if (!result) {
        throw_msg(env, err ? err : "query failed");
        if (err) glacier_free(err);
        return NULL;
    }
    const char *qerr = glacier_result_error(result);
    if (qerr) {
        throw_msg(env, qerr);
        glacier_result_destroy(result);
        return NULL;
    }
    jobject rows = rows_from_result(env, result);
    glacier_result_destroy(result);
    return rows;
}
