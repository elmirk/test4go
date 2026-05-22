package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"udp-test/protocol"
)

//quality counters

var (
	totalSent uint32
	totalAcked uint32
	totalFailed uint32
)


const (
	serverAddr   = "127.0.0.1:9000"
//	totalPackets = 10000
	totalPackets = 10000
        workersCount = 10

	retryTimeout = 500 * time.Millisecond
	maxRetries   = 5
)

const MaxUDPPayload = 1200

type PacketState struct {
	Packet   protocol.Packet
	SentAt   time.Time
	Acked    bool
	Failed   bool
	Retries  int
	LastSend time.Time
}

type Result struct {
	ID uint32
	OK bool
}

var (
	packetCounter uint32

	packetStates = make(map[uint32]*PacketState)
	statesMu     sync.RWMutex
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
        fmt.Println("sending to:", addr)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	sendQueue := make(chan protocol.Packet, 10000)
	resultsChan := make(chan Result, 10000)

	go ackListener(conn, resultsChan)

	go orderedPrinter(resultsChan)

	go sender(conn, sendQueue)

	go retryManager(sendQueue)

	var wg sync.WaitGroup

	// 10 producers
	for i := 0; i < workersCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				id := atomic.AddUint32(&packetCounter, 1)

				if id > totalPackets {
					return
				}

				packet := generatePacket(id)

				statesMu.Lock()
				packetStates[id] = &PacketState{
					Packet: packet,
				}
				statesMu.Unlock()

				select {
		case sendQueue <- packet:
		case <-time.After(10 * time.Millisecond):
			
		}
			}
		}()
	}

	wg.Wait()

	// wait until all ACKs received
	for {
		time.Sleep(10 * time.Second)

		statesMu.RLock()

		done := true

		for _, st := range packetStates {
			if !st.Acked && !st.Failed {
				done = false
				break
			}
		}

		statesMu.RUnlock()

		if done {
			break
		}
	}

acked_ratio := float64(totalAcked) / float64(totalPackets) * 100

fmt.Printf("\n--- STATS ---\n")
fmt.Printf("total packets: %d\n", totalPackets)
fmt.Printf("sent: %d\n", totalSent)
fmt.Printf("acked: %d\n", totalAcked)
fmt.Printf("acked_ratio: %.2f%%\n", acked_ratio)

}

func generatePacket(id uint32) protocol.Packet {

	minSize := int(id)
	maxSize := int(id * 2)

	if minSize < 1 {
		minSize = 1
	}

	if maxSize < minSize {
		maxSize = minSize
	}

	r, err := rand.Int(rand.Reader, big.NewInt(int64(maxSize-minSize+1)))
	if err != nil {
		log.Fatal(err)
	}

	logicalSize := minSize + int(r.Int64())

	data := make([]byte, logicalSize)
	_, err = rand.Read(data)
	if err != nil {
		log.Fatal(err)
	}

	ts := time.Now().UnixNano()

	hash := protocol.CalculateHash(id, ts, data)

	return protocol.Packet{
		Type:      protocol.TypeData,
		ID:        id,
		Timestamp: ts,
		Data:      data,
		Hash:      hash,
	}
}


func sender(conn *net.UDPConn, sendQueue <-chan protocol.Packet) {
	for packet := range sendQueue {
		raw, err := protocol.SerializePacket(packet)
//                fmt.Println("sender: client send data:", raw[:20])

		if err != nil {
			continue
		}

		_, err = conn.Write(raw)
                //fmt.Println("sender: sent:", n, "err:", err)
		if err != nil {
			continue
		}

                atomic.AddUint32(&totalSent, 1)

		statesMu.Lock()

		if st, ok := packetStates[packet.ID]; ok {
			st.SentAt = time.Now()
			st.LastSend = time.Now()
		}

		statesMu.Unlock()
	}
}

func retryManager(sendQueue chan<- protocol.Packet) {
	ticker := time.NewTicker(100 * time.Millisecond)

	for range ticker.C {
		statesMu.Lock()

		for _, st := range packetStates {
			if st.Acked || st.Failed {
				continue
			}

			if st.LastSend.IsZero() {
				continue
			}

			if time.Since(st.LastSend) > retryTimeout {
				if st.Retries >= maxRetries {
					st.Failed = true
					continue
				}

				st.Retries++
				st.LastSend = time.Now()

				sendQueue <- st.Packet
			}
		}

		statesMu.Unlock()
	}
}

func ackListener(conn *net.UDPConn, results chan<- Result) {
	buf := make([]byte, 1024)

	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))

		n, err := conn.Read(buf)
		if err != nil {
			continue
		}

		if n < 6 {
			continue
		}

		reader := bytes.NewReader(buf[:n])

		var packetType byte
		var okByte byte
		var id uint32

		if err := binary.Read(reader, binary.BigEndian, &packetType); err != nil {
			continue
		}

		if packetType != byte(protocol.TypeAck) {
			continue
		}

		if err := binary.Read(reader, binary.BigEndian, &okByte); err != nil {
			continue
		}

		if err := binary.Read(reader, binary.BigEndian, &id); err != nil {
			continue
		}

		ok := okByte == 1

		statesMu.Lock()

		st, exists := packetStates[id]
		if exists {
			if ok && !st.Acked {
				st.Acked = true
				atomic.AddUint32(&totalAcked, 1)
			}
		}

		statesMu.Unlock()

		select {
		case results <- Result{ID: id, OK: ok}:
		default:
		}
	}
}


func waitServer(conn *net.UDPConn) error {
	testPacket := []byte("PING")

	deadline := time.Now().Add(5 * time.Second)

	buf := make([]byte, 1024)

	for {
		_, err := conn.Write(testPacket)
		if err != nil {
			return err
		}

		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		n, err := conn.Read(buf)

		if err == nil && n > 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting server")
		}

		time.Sleep(300 * time.Millisecond)
	}
}

func orderedPrinter(results <-chan Result) {
	buffer := make(map[uint32]bool)

	nextExpected := uint32(1)

	for result := range results {
		buffer[result.ID] = result.OK

		for {
			ok, exists := buffer[nextExpected]
			if !exists {
				break
			}

			fmt.Printf(
				"packet=%d delivered=%v\n",
				nextExpected,
				ok,
			)

			delete(buffer, nextExpected)

			nextExpected++
		}
	}
}
