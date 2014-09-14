package grouter

import (
	"bytes"
	"sync"
	"testing"
	"encoding/binary"
	"fmt"
)

var testDB *LevelDBEngine
var onlyOnce sync.Once

func createDB(name string) *LevelDBEngine  {
	f := func() {
		var err error
		testDB, err = Open(name)
		if err != nil  {
			println(err.Error())
			panic(err)
		}
	}
	onlyOnce.Do(f)
	return testDB
}

func TestSimple(t *testing.T)  {
	db := createDB("/tmp/testdb")

	key := []byte("hi")
	value := []byte("hello world")

	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if (!bytes.Equal(v, value)) {
		t.Fatal("get value not equal")
	}

	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal("after delete, key should not exist any more")
	}

}

func ComposeKey(vbid uint16, key []byte) []byte {
   	data := make([]byte, 2 + len(key))
   	pos := 0
  	binary.BigEndian.PutUint16(data[pos:pos+2], vbid)
   	pos += 2
	copy(data[pos:pos+len(key)], key)
	return data
}

func Pack(cas uint64, extra, body []byte) []byte {
	packBuf := make([]byte, 13+len(body)+len(extra))

	pos := 0
	binary.BigEndian.PutUint64(packBuf[pos:pos+8], cas)
	pos += 8
	packBuf[pos] = byte(len(extra))
	pos++
	copy(packBuf[pos:pos+len(extra)], extra)
	pos += len(extra)
	packBuf[pos] = byte(len(body))
    binary.BigEndian.PutUint32(packBuf[pos:pos+4], uint32(len(body)))
	pos+=4

	copy(packBuf[pos:pos+len(body)], body)

	return packBuf
}

func Unpack(v []byte) (uint64, []byte, []byte) {
	pos := 0
	cas := uint64(binary.BigEndian.Uint64(v[pos:pos+8]))

	pos += 8
	elen := int(v[pos])
	pos++
	extra := make([]byte, elen)
	copy(extra, v[pos:pos+elen])

	pos += elen

	blen := int(binary.BigEndian.Uint32(v[pos:pos+4]))
	pos += 4
	
	body := make([]byte, blen)
	copy(body, v[pos:pos+blen])

	return cas, extra, body
}

func TestPackUnpack(t *testing.T) {
	db := createDB("/tmp/testdb")

	cas := uint64(938424885)
	key := []byte("somekey")
	body := []byte("somevalue")
	extra := []byte("someextra")

	// packing
	packBuf := Pack(cas, extra, body)

	if err := db.Put(key, packBuf); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if (!bytes.Equal(v, packBuf)) {
		t.Fatal("get value not equal")
	} else {
		//unpacking
		cas1, extra1, body1 := Unpack(v)
		if cas != cas1 {
			t.Fatal("cas not equal")
		}
		if bytes.Compare(extra, extra1) != 0 {
			t.Fatal("extra not equal")
		}
		if bytes.Compare(body, body1) != 0 {
			t.Fatal("body not equal")
		}
	}

}

func TestEnumVbucket(t *testing.T) {
	db := createDB("/tmp/testdb1")

	k1 := []byte("aaa")
	k2 := []byte("bbb")
	k3 := []byte("ccc")
	k4 := []byte("a323")
	value := []byte("test value")

	var vbid = uint16(1)

	if err := db.Put(ComposeKey(vbid, k1), value); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ComposeKey(vbid, k2), value); err != nil {
		t.Fatal(err)
	}
	vbid = uint16(3)
	if err := db.Put(ComposeKey(vbid, k3), value); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(ComposeKey(vbid, k4), value); err != nil {
		t.Fatal(err)
	}

	db.ro.SetFillCache(false)
	it := db.db.NewIterator(db.ro)
	defer it.Close()

    vbid = uint16(1)
	start_key := make([]byte, 2)
	binary.BigEndian.PutUint16(start_key, vbid)

	end_key := make([]byte, 2)
	binary.BigEndian.PutUint16(end_key, vbid+1)

	it.Seek(start_key)
	count := 0
	for it = it; it.Valid(); it.Next() {
		if bytes.Compare(it.Key(), end_key) < 0 {
			fmt.Println("key:%s, value:%s", it.Key(), it.Value())
			count++
		}
	}
}	
