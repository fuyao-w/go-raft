package go_raft

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

/*

	协议：${魔数 1byte 0x3} ${ 请求类型 1 byte}  ${包体长度 不固定} \n 包体
*/
const (
	delim = '\n'
	magic = 0x3
)

type DefaultPackageParser struct {
}

var defaultPackageParser = new(DefaultPackageParser)

func (d *DefaultPackageParser) Encode(writer *bufio.Writer, cmdType cmdType, data []byte) (err error) {
	onceErr := func(e error) {
		if e != nil && err == nil {
			err = e
		}
	}
	_ = onceErr
	writer.WriteByte(magic)                     // magic
	writer.WriteByte(byte(cmdType))             // 命令类型
	writer.WriteString(strconv.Itoa(len(data))) // 包体长度
	writer.WriteByte(delim)                     // 分割符
	writer.Write(data)                          // 包体
	return err
}

func (d *DefaultPackageParser) Decode(reader *bufio.Reader) (cmdType, []byte, error) {

	_magic, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	if _magic != magic {
		return 0, nil, errors.New("unrecognized request")
	}

	// 获取命令类型
	ct, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	// 获取包体长度
	pkgLength, err := reader.ReadString(delim)
	if err != nil {
		return 0, nil, err
	}

	// 获取包体
	length, err := strconv.Atoi(strings.TrimRight(pkgLength, string(delim)))
	if err != nil {
		return 0, nil, err
	}

	buf := make([]byte, length)
	_, err = reader.Read(buf)
	return cmdType(ct), buf, err
}
