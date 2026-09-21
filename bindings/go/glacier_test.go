package glacier_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	glacier "glacier"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func TestSelect1(t *testing.T) {
	if glacier.Version() != "0.6.0" {
		t.Fatalf("version %s", glacier.Version())
	}
	if glacier.APIVersion() != 1 {
		t.Fatalf("api %d", glacier.APIVersion())
	}
	con, err := glacier.Connect("")
	if err != nil {
		t.Fatal(err)
	}
	defer con.Close()
	rows, err := con.Execute("select 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != int64(1) {
		t.Fatalf("got %#v", rows)
	}
}

func TestParquetAndJoin(t *testing.T) {
	sales := filepath.Join(repoRoot(t), "tests/formats/sales.parquet")
	con, err := glacier.Connect(sales)
	if err != nil {
		t.Fatal(err)
	}
	defer con.Close()
	rows, err := con.Execute("SELECT COUNT(*)")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != int64(10) {
		t.Fatalf("count %#v", rows)
	}
	buf, err := os.ReadFile(sales)
	if err != nil {
		t.Fatal(err)
	}
	all, err := con.ReadParquet(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 10 {
		t.Fatalf("rows %d", len(all))
	}
	empty, err := glacier.Connect("")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	_, err = empty.Execute("SELECT * FROM a JOIN b")
	if err == nil || err.Error() != "JOIN is not supported" {
		t.Fatalf("join err %#v", err)
	}
}
