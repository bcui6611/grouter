package grouter

import (
	"bufio"
	"io"
	"log"
	"time"

	"github.com/couchbase/gomemcached"
)

type KineticSource struct {
	// A source that handles kinetic requests.
}

func (self KineticSource) Run(s io.ReadWriter,
                              clientNum uint32,
                              target Target,
                              statsChan chan Stats) {
	tot_source_kinetic_ops_nsecs := int64(0)
	tot_source_kinetic_ops := 0

	br := bufio.NewReader(s)
	bw := bufio.NewWriter(s)
	res := make(chan *gomemcached.MCResponse)

	for {
        req := gomemcached.MCRequest{}
        
        _, err := req.Receive(br, nil)
        if err != nil {
            log.Printf("KineticSource receiving error: %v", err)
            return
        }

		reqs_start := time.Now()
		if cmd, ok := KineticCmds[req.Opcode]; ok {
			if !cmd.Handler(&self, target, res, cmd, &req, br, bw, clientNum) {
				return
			}
		} else {
			KineticClientError(bw, "unknown command - "+req.Opcode.String()+"\r\n")
		}
		reqs_end := time.Now()

		tot_source_kinetic_ops_nsecs += reqs_end.Sub(reqs_start).Nanoseconds()
		tot_source_kinetic_ops += 1

		if tot_source_kinetic_ops%100 == 0 {
			statsChan <- Stats{
				Keys: []string{
					"tot-source-kinetic-ops",
					"tot-source-kinetic-ops-usecs",
				},
				Vals: []int64{
					int64(tot_source_kinetic_ops),
					int64(tot_source_kinetic_ops_nsecs / 1000),
				},
			}
			tot_source_kinetic_ops_nsecs = 0
			tot_source_kinetic_ops = 0
		}
	}
}

type KineticCmd struct {
	Opcode  gomemcached.CommandCode
	Handler func(source *KineticSource,
		target Target, res chan *gomemcached.MCResponse,
		cmd *KineticCmd, req *gomemcached.MCRequest, br *bufio.Reader, bw *bufio.Writer,
		clientNum uint32) bool
}

var KineticCmds = map[gomemcached.CommandCode]*KineticCmd{
	gomemcached.QUIT: &KineticCmd{
		gomemcached.QUIT,
		func(source *KineticSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *KineticCmd, req *gomemcached.MCRequest, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			response := gomemcached.MCResponse {
				Status: gomemcached.SUCCESS,
				Opcode: req.Opcode,
				Opaque: req.Opaque,
			}
			response.Transmit(bw)
			bw.Flush()
			return false
		},
	},
	gomemcached.VERSION: &KineticCmd{
		gomemcached.VERSION,
		func(source *KineticSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *KineticCmd, req *gomemcached.MCRequest, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {

			response := gomemcached.MCResponse{
			    Status: gomemcached.SUCCESS,
				Opcode: req.Opcode,
				Opaque: req.Opaque,
				Key:    req.Key,
				Body:   []byte(version),
	        }
			response.Transmit(bw)
			bw.Flush()
			return true
		},
	},

	gomemcached.DELETE: &KineticCmd{
		gomemcached.DELETE,
		func(source *KineticSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *KineticCmd, req *gomemcached.MCRequest, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			reqs := make([]Request, 1)
			reqs[0] = Request{
				"default",
				req,
				res,
				clientNum,
			}
			targetChan := target.PickChannel(clientNum, "default")
			targetChan <- reqs
			response := <-res
			response.Transmit(bw)
			bw.Flush()
			return true
		},
	},
	gomemcached.GET:  	 &KineticCmd{gomemcached.GET, KineticCmdGet},
	gomemcached.GETK: 	 &KineticCmd{gomemcached.GETK, KineticCmdGet},
	gomemcached.GETQ: 	 &KineticCmd{gomemcached.GETQ, KineticCmdGetQuiet},
	gomemcached.GETKQ:	 &KineticCmd{gomemcached.GETKQ, KineticCmdGetQuiet},
	gomemcached.SET:     &KineticCmd{gomemcached.SET, KineticCmdMutation},
	gomemcached.ADD:     &KineticCmd{gomemcached.ADD, KineticCmdMutation},
}

func KineticCmdGet(source *KineticSource,
			       target Target, 
			       res chan *gomemcached.MCResponse,
			       cmd *KineticCmd, 
			       req *gomemcached.MCRequest,
			       br *bufio.Reader,
			       bw *bufio.Writer,
			       clientNum uint32) bool {

	reqs := make([]Request, 1)
	reqs[0] = Request{
				"default",
				req,
				res,
				clientNum,
			  }
	targetChan := target.PickChannel(clientNum, "default")
	targetChan <- reqs
	response := <-res
	response.Transmit(bw)
	bw.Flush()
	return true
}

func KineticCmdGetQuiet(source *KineticSource,
			       target Target, 
			       res chan *gomemcached.MCResponse,
			       cmd *KineticCmd, 
			       req *gomemcached.MCRequest,
			       br *bufio.Reader,
			       bw *bufio.Writer,
			       clientNum uint32) bool {

	reqs := make([]Request, 1)
	reqs[0] = Request{
				"default",
				req,
				res,
				clientNum,
			  }

	targetChan := target.PickChannel(clientNum, "default")
	targetChan <- reqs
	
	for {
		response := <-res
		response.Transmit(bw)
		bw.Flush()
		if response.Status != gomemcached.SUCCESS {
			break
		}
	}
	return true
}

func KineticCmdMutation(source *KineticSource,
					   target Target, 
					   res chan *gomemcached.MCResponse,
					   cmd *KineticCmd, 
					   req *gomemcached.MCRequest, 
					   br *bufio.Reader, 
					   bw *bufio.Writer,
					   clientNum uint32) bool {
	reqs := make([]Request, 1)
	reqs[0] = Request{
		"default",
		req,
		res,
		clientNum,
	}
	targetChan := target.PickChannel(clientNum, "default")
	targetChan <- reqs
	response := <-res
	response.Transmit(bw)
	bw.Flush()
	return true
}

func KineticClientError(bw *bufio.Writer, msg string) bool {
	bw.Write([]byte("CLIENT_ERROR "))
	bw.Write([]byte(msg))
	bw.Flush()
	return true
}
