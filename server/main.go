package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"time"
)

const (
	addr        = ":9000"
	workerCount = 0 // 0 = runtime.NumCPU()
)

const (
	TypePing byte = 1
	TypeData byte = 2
	TypeAck  byte = 3
)

type PacketJob struct {
	Data []byte
	Addr *net.UDPAddr
}

type Packet struct {
	ID        uint32
	Timestamp int64
	DataLen   uint32
	Data      []byte
	Hash      [32]byte
}

var (
	incoming = make(chan PacketJob, 10000)
)

func main() {
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	fmt.Printf("[SERVER] started on %s\n", addr)

	workers := workerCount
	if workers == 0 {
		workers = runtime.NumCPU()
	}

	fmt.Printf("[SERVER] starting %d workers\n", workers)

	for i := 0; i < workers; i++ {
		go worker(conn)
	}

	fmt.Println("[SERVER] workers started, waiting for packets...")

	go func() {
		buf := make([]byte, 65535)

		for {
			n, addr, err := conn.ReadFromUDP(buf)

			if err != nil {
				continue
			}

			data := make([]byte, n)
			copy(data, buf[:n])

			incoming <- PacketJob{
				Data: data,
				Addr: addr,
			}
		}
	}()

	fmt.Println("[SERVER] UDP listener active, ready to receive data")
	select {}
}

// worker function
func worker(conn *net.UDPConn) {
	for job := range incoming {

		start := time.Now()
		p, err := deserialize(job.Data)

		if err != nil {
			continue
		}

		ok := validate(p)
		ack := buildAck(p.ID, ok)

		fmt.Printf(
			"packet=%d created=%s received=%s integrity=%v size=%d\n",
			p.ID,
			time.Unix(0, p.Timestamp).Format(time.RFC3339Nano),
			start.Format(time.RFC3339Nano),
			ok,
			p.DataLen,
		)
		os.Stdout.Sync()

		_, _ = conn.WriteToUDP(ack, job.Addr)
	}
}

func deserialize(data []byte) (*Packet, error) {
	reader := bytes.NewReader(data)

	var ptype byte
	if err := binary.Read(reader, binary.BigEndian, &ptype); err != nil {
		return nil, err
	}

	var id uint32
	var ts int64
	var dataLen uint32

	if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.BigEndian, &ts); err != nil {
		return nil, err
	}

	if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}

	payload := make([]byte, dataLen)
	if _, err := reader.Read(payload); err != nil {
		return nil, err
	}

	var hash [32]byte
	if err := binary.Read(reader, binary.BigEndian, &hash); err != nil {
		return nil, err
	}

	return &Packet{
		ID:        id,
		Timestamp: ts,
		DataLen:   dataLen,
		Data:      payload,
		Hash:      hash,
	}, nil
}

func validate(p *Packet) bool {

	h := sha256.New()
	binary.Write(h, binary.BigEndian, p.ID)
	binary.Write(h, binary.BigEndian, p.Timestamp)
	h.Write(p.Data)

	sum := h.Sum(nil)

	return bytes.Equal(sum, p.Hash[:])
}

func buildAck(id uint32, ok bool) []byte {
	buf := make([]byte, 0, 6)

	buf = append(buf, TypeAck)

	if ok {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}

	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, id)

	buf = append(buf, idBytes...)
	return buf
}
