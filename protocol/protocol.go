package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

type PacketType byte

const (
	TypePing PacketType = 1
	TypeData PacketType = 2
	TypeAck  PacketType = 3
)

type Packet struct {
	Type      PacketType
	ID        uint32
	Timestamp int64
	DataLen   uint32
	Data      []byte
	Hash      [32]byte
}

type Ack struct {
	Type PacketType
	ID   uint32
	OK   bool
}

func SerializePacket(p Packet) ([]byte, error) {
	if p.Type != TypeData {
		return nil, fmt.Errorf("only TypeData supported")
	}

	buf := new(bytes.Buffer)

	// type
	buf.WriteByte(byte(p.Type))

	// id
	_ = binary.Write(buf, binary.BigEndian, p.ID)

	// timestamp
	_ = binary.Write(buf, binary.BigEndian, p.Timestamp)

	//data length
	p.DataLen = uint32(len(p.Data))
	_ = binary.Write(buf, binary.BigEndian, p.DataLen)

	// data
	buf.Write(p.Data)

	// hash
	buf.Write(p.Hash[:])

	return buf.Bytes(), nil
}

func DeserializePacket(b []byte) (Packet, error) {
	var p Packet

	minSize := 1 + 4 + 8 + 4 + 32
	if len(b) < minSize {
		return p, fmt.Errorf("packet too small")
	}

	reader := bytes.NewReader(b)

	t, err := reader.ReadByte()
	if err != nil {
		return p, err
	}
	p.Type = PacketType(t)

	if p.Type != TypeData {
		return p, fmt.Errorf("not data packet")
	}

	// id
	if err := binary.Read(reader, binary.BigEndian, &p.ID); err != nil {
		return p, err
	}

	// timestamp
	if err := binary.Read(reader, binary.BigEndian, &p.Timestamp); err != nil {
		return p, err
	}

	if err := binary.Read(reader, binary.BigEndian, &p.DataLen); err != nil {
		return p, err
	}

	// data
	p.Data = make([]byte, p.DataLen)
	if _, err := reader.Read(p.Data); err != nil {
		return p, err
	}

	// hash
	if _, err := reader.Read(p.Hash[:]); err != nil {
		return p, err
	}

	return p, nil
}

func CalculateHash(id uint32, ts int64, data []byte) [32]byte {
	buf := new(bytes.Buffer)

	_ = binary.Write(buf, binary.BigEndian, id)
	_ = binary.Write(buf, binary.BigEndian, ts)
	buf.Write(data)

	return sha256.Sum256(buf.Bytes())
}

func SerializeAck(id uint32, ok bool) []byte {
	buf := new(bytes.Buffer)

	buf.WriteByte(byte(TypeAck))

	if ok {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)

	buf.Write(idBytes)

	return buf.Bytes()
}

// ping
func SerializePing() []byte {
	return []byte{byte(TypePing)}
}
