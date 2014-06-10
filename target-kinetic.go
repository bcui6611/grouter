package grouter

import (
    "bytes"
	"encoding/binary"
    "sync"
    "strconv"
	"github.com/couchbase/gomemcached"
	"github.com/bcui6611/kinetic"
	"fmt"
)

type KineticStorage struct {
	cas      uint64
    db      *kinetic.LevelDBEngine
	incoming chan []Request
}

type KineticStorageHandler struct {
	Opcode  gomemcached.CommandCode
	Handler func(s *KineticStorage, req Request)
}

var KineticStorageHandlers = map[gomemcached.CommandCode] *KineticStorageHandler{
	gomemcached.GET: 	&KineticStorageHandler{gomemcached.GET, KineticGetHandler},
	gomemcached.GETK:	&KineticStorageHandler{gomemcached.GETK, KineticGetHandler},
	gomemcached.GETQ:   &KineticStorageHandler{gomemcached.GETQ, KineticGetQuietHandler},
	gomemcached.GETKQ:  &KineticStorageHandler{gomemcached.GETQ, KineticGetQuietHandler},
	gomemcached.SET: 	&KineticStorageHandler{gomemcached.SET, KineticSetHandler},
	gomemcached.ADD:    &KineticStorageHandler{gomemcached.ADD, KineticSetHandler},
	gomemcached.DELETE: &KineticStorageHandler{
				gomemcached.DELETE, 
				func(s *KineticStorage, req Request) {
					ret := &gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Opaque: req.Req.Opaque,
						Key:    req.Req.Key,
			            Status: gomemcached.SUCCESS,
					}
			        if err := s.db.Delete(ComposeKey(req.Req.VBucket, req.Req.Key)); err != nil {
						ret.Status = gomemcached.KEY_ENOENT
					}
					req.Res <- ret
				},
			},
}

func ComposeKey(vbid uint16, key []byte) []byte {
	keylen := len(key)
   	data := make([]byte, 4 + keylen)
   	pos := 0
  	binary.BigEndian.PutUint16(data[pos:pos+2], vbid)
   	pos += 2
   	binary.BigEndian.PutUint16(data[pos:pos+2], uint16(keylen))
   	pos += 2
	copy(data[pos:pos+len(key)], key)
	return data
}

func DecomposeKey(body []byte) (uint16, int, []byte) {
	pos := 0
	vbid := uint16(binary.BigEndian.Uint16(body[pos:pos+2]))
	pos += 2
	keylen := int(binary.BigEndian.Uint16(body[pos:pos+2]))
	pos += 2
	key := make([]byte, keylen+1)
	copy(key, body[pos:pos+keylen])
	return vbid, keylen, key
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

func KineticGetHandler(s *KineticStorage, req Request) {
	ret := &gomemcached.MCResponse{
		Opcode: req.Req.Opcode,
		Opaque: req.Req.Opaque,
		Key:    req.Req.Key,
		Status: gomemcached.SUCCESS,
	}
    if v, err := s.db.Get(ComposeKey(req.Req.VBucket, req.Req.Key)); err != nil {
        ret.Status = gomemcached.KEY_ENOENT
        fmt.Println("Fail to get key:%s", string(req.Req.Key))
    } else {
		ret.Extras = make([]byte, 4)
		binary.BigEndian.PutUint32(ret.Extras, 0)
		ret.Cas = 0
		ret.Body = v
	}
	req.Res <- ret
}

func KineticGetQuietHandler(s *KineticStorage, req Request) {
	
	it := s.db.NewIterator()
	defer it.Close()
	//extract vbucket id
	vbid := req.Req.VBucket
	fmt.Println("vbid:", vbid)
	start_key := make([]byte, 2)
	binary.BigEndian.PutUint16(start_key, vbid)
	end_key := make([]byte, 2)
	binary.BigEndian.PutUint16(end_key, vbid+1)
	
	key := req.Req.Key
	if string(key) == "keys_only" {
		value := make([]byte, 1)
		for it.Seek(start_key); it.Valid(); it.Next() {
			if bytes.Compare(it.Key(), end_key) < 0 {
				ret := &gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
					Opaque: req.Req.Opaque,
					Key:    req.Req.Key,
					Status: gomemcached.SUCCESS,
					Body:   value,
				}
				ret.Extras = make([]byte, 4)
				binary.BigEndian.PutUint32(ret.Extras, 0)
				_, _, key := DecomposeKey(it.Key())
				ret.Key = key
				ret.Cas = 0
				req.Res <- ret
			}
		}
	} else if string(key) == "items_count" {
		total := 0
		for it.Seek(start_key); it.Valid(); it.Next() {
			if bytes.Compare(it.Key(), end_key) < 0 {
				total++
			}
		}
		ret := &gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key:    req.Req.Key,
			Status: gomemcached.SUCCESS,
		}
		fmt.Println("total:", total)
		ret.Extras = make([]byte, 4)
		ret.Cas = 0
		ret.Body = []byte(strconv.Itoa(total))
		req.Res <- ret
	} else {
		for it.Seek(start_key); it.Valid(); it.Next() {
			if bytes.Compare(it.Key(), end_key) < 0 {
				ret := &gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
					Opaque: req.Req.Opaque,
					Key:    req.Req.Key,
					Status: gomemcached.SUCCESS,
				}
				ret.Extras = make([]byte, 4)
				binary.BigEndian.PutUint32(ret.Extras, 0)
				_, _, key := DecomposeKey(it.Key())
				ret.Key = key
				ret.Cas = 0
				ret.Body = it.Value()
				//fmt.Println("Get vid:%d, key:%d, value:%s", vbid, string(ret.Key), string(ret.Body[:]))
				req.Res <- ret
			}
		}
	}
	
	ret := &gomemcached.MCResponse{
		Opcode: gomemcached.NOOP,
		Opaque: req.Req.Opaque,
		Key:    req.Req.Key,
		Status: gomemcached.NOT_MY_VBUCKET,
	}
	req.Res <- ret
}

func KineticSetHandler(s *KineticStorage, req Request) {
	ret := &gomemcached.MCResponse {
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key:    req.Req.Key,
            Status: gomemcached.SUCCESS,
    }
	s.cas += 1
    if err := s.db.Put(ComposeKey(req.Req.VBucket, req.Req.Key), req.Req.Body); err != nil {
        ret.Status = gomemcached.KEY_ENOENT
        fmt.Println("fail to set key with error:%d", err)
    } else {
    	fmt.Println("set key %s with value successfully", string(req.Req.Key))
    }
	//flags := binary.BigEndian.Uint32(req.Req.Extras)
    //exp := binary.BigEndian.Uint32(req.Req.Extras[4:])
    //cas := s.cas

	req.Res <- ret
}

func (s KineticStorage) PickChannel(clientNum uint32, bucket string) chan []Request {
	return s.incoming
}

func KineticStorageStart(spec string, params Params, statsChan chan Stats) Target {
	s := KineticStorage{
		incoming: make(chan []Request, params.TargetChanSize),
	}
    
    f := func() {
        var err error
        s.db, err = kinetic.Open("/tmp/testdb")
        if err != nil {
            println(err.Error())
            panic(err)
        }
    }

    var onlyOnce sync.Once
    onlyOnce.Do(f)

	go func() {
		for reqs := range s.incoming {
			for _, req := range reqs {
				if h, ok := KineticStorageHandlers[req.Req.Opcode]; ok {
					h.Handler(&s, req)
				} else {
					req.Res <- &gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Status: gomemcached.UNKNOWN_COMMAND,
						Opaque: req.Req.Opaque,
					}
				}
			}
		}
	}()

	return s
}
