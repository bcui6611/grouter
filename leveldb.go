package grouter

import (
	"github.com/jmhodges/levigo"
)

type LevelDBEngine struct {
	name    string
	db      *levigo.DB
	options *levigo.Options
	ro      *levigo.ReadOptions
	wo      *levigo.WriteOptions
	ito     *levigo.ReadOptions

	filter  *levigo.FilterPolicy
	cache   *levigo.Cache
	wbatch  *levigo.WriteBatch
}

func Open(name string) (*LevelDBEngine, error) {
	dbEngine := new(LevelDBEngine)
	dbEngine.name = name
	dbEngine.options = dbEngine.InitOptions()

	dbEngine.ro = levigo.NewReadOptions()
	dbEngine.wo = levigo.NewWriteOptions()

	var err error
	dbEngine.db, err = levigo.Open(name, dbEngine.options)
	return dbEngine, err
}

func (db *LevelDBEngine) InitOptions() *levigo.Options {
	opts := levigo.NewOptions()
	db.cache = levigo.NewLRUCache(3 <<10)
	opts.SetCache(db.cache)
	opts.SetCreateIfMissing(true)
	db.filter = levigo.NewBloomFilter(10)
	opts.SetFilterPolicy(db.filter)
	opts.SetCompression(levigo.NoCompression)
	opts.SetBlockSize(1024)
	opts.SetWriteBufferSize(1024*1024)

	return opts
}

func (db *LevelDBEngine) Close() {
	db.cache.Close()
	db.filter.Close()
	db.options.Close()
	db.ro.Close()
	db.wo.Close()
	if db.wbatch != nil {
		db.wbatch.Close()
	}
   	db.db.Close()
   	db.db = nil
}

func (db *LevelDBEngine) Destroy() {
	db.Close()

	opts := levigo.NewOptions()
	defer opts.Close()

	levigo.DestroyDatabase(db.name, opts)
}

func (db *LevelDBEngine) Put(key, value []byte) error {
	return db.db.Put(db.wo, key, value)
}

func (db *LevelDBEngine) BatchPut(key, value []byte) {
	if db.wbatch == nil  {
		db.NewWriteBatch()
	}
	db.wbatch.Put(key, value)
}

func (db *LevelDBEngine) Get(key []byte) ([]byte, error) {
	return db.db.Get(db.ro, key)
}

func (db *LevelDBEngine) Delete(key []byte) error {
	return db.db.Delete(db.wo, key)
}

func (db *LevelDBEngine) BatchDelete(key []byte) {
	if db.wbatch == nil  {
		db.NewWriteBatch()
	}
	db.wbatch.Delete(key)	
}

func (db *LevelDBEngine) NewWriteBatch()  {
	if (db.wbatch != nil) {
		db.wbatch = nil
	}
	db.wbatch = levigo.NewWriteBatch()
}

func (db *LevelDBEngine) BatchCommit() error {
	if db.wbatch == nil {
		//throw error
	}
	err := db.db.Write(db.wo, db.wbatch)
	db.wbatch.Close()
	return err
}

func (db *LevelDBEngine) Rollback() {
	if (db.wbatch != nil) {
		db.wbatch.Clear()
		db.wbatch.Close()
	}
}

func (db *LevelDBEngine) NewIterator() *levigo.Iterator {
	return db.db.NewIterator(db.ro)
}

//writeBatch
//Iterator
//snapshot





