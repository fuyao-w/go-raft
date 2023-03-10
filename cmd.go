package go_raft

import (
	"time"
)

type (
	Processor interface {
		Do(cmdType, interface{}) (interface{}, error)
		SetFastPath(cb fastPath)
	}
	// ProcessorProxy 服务器接口 handler 代理，提供将序列化数据，解析成接口 struct 指针的功能
	ProcessorProxy struct {
		Processor
	}
	// ServerProcessor 服务器接口 handler ，提供具体的接口处理逻辑
	ServerProcessor struct {
		cmdChan  chan *CMD
		fastPath fastPath
	}
)

func (d *ProcessorProxy) SetFastPath(cb fastPath) {
	d.Processor.SetFastPath(cb)
}
func (d *ServerProcessor) SetFastPath(cb fastPath) {
	d.fastPath = cb
}

// Do ServerProcessor 不关心上层协议，所以不用处理第一个参数（cmdType）
func (d *ServerProcessor) Do(_ cmdType, req interface{}) (resp interface{}, err error) {
	resCh := make(chan any, 1)
	cmd := &CMD{
		Request:  req,
		Response: resCh,
	}
	if d.fastPath != nil && d.fastPath(cmd) {
		return <-resCh, nil
	}
	d.cmdChan <- cmd
	return <-resCh, nil
}

type processorOption struct {
	Processor
	CmdConvert
}

func withProcessor(p Processor) func(opt *processorOption) {
	return func(opt *processorOption) {
		opt.Processor = p
	}
}
func withCmdConvert(c CmdConvert) func(opt *processorOption) {
	return func(opt *processorOption) {
		opt.CmdConvert = c
	}
}
func newProcessorProxy(cmdCh chan *CMD, options ...func(opt *processorOption)) Processor {
	proxy := &ProcessorProxy{
		Processor: &ServerProcessor{
			cmdChan: cmdCh,
		},
	}
	var opt processorOption
	for _, do := range options {
		do(&opt)
	}
	if opt.Processor != nil {
		proxy.Processor = opt.Processor
	}
	//if opt.CmdConvert != nil {
	//	proxy.CmdConvert = opt.CmdConvert
	//}
	return proxy
}

func (p *ProcessorProxy) Do(cmdType cmdType, reqBytes interface{}) (respBytes interface{}, err error) {
	date := reqBytes.([]byte)
	var req interface{}

	switch cmdType {
	case CmdVoteRequest:
		req = new(VoteRequest)
	case CmdAppendEntry:
		req = new(AppendEntryRequest)
	case CmdAppendEntryPipeline:
		req = new(AppendEntryRequest)
	case CmdInstallSnapshot:
		req = new(InstallSnapshotRequest)
	}
	err = defaultCmdConverter.Deserialization(date, req)
	if err != nil {
		return
	}
	resp, err := p.Processor.Do(cmdType, req)
	if err != nil {
		return nil, err
	}
	return defaultCmdConverter.Serialization(resp)
}

func doWithTimeout(timeout time.Duration, do func()) bool {
	wrapper := func() chan struct{} {
		done := make(chan struct{})
		go do()
		return done
	}
	select {
	case <-time.After(timeout):
		return false
	case <-wrapper():
		return true
	}
}
