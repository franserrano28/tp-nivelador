package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/lottery"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType byte

const (
	MsgBatch    MessageType = 0x01
	MsgFinished MessageType = 0x02
	MsgWinners  MessageType = 0x03
	MsgBatchAck MessageType = 0x04
)

const lengthPrefixSize = 4

func SendBatch(conn net.Conn, agencyId string, bets []lottery.Bet) error {
	lines := make([]string, 0, len(bets)+1)
	lines = append(lines, agencyId)
	for _, bet := range bets {
		fields := []string{bet.FirstName, bet.LastName, bet.Document, bet.Birthdate, bet.Number}
		lines = append(lines, strings.Join(fields, "|"))
	}
	payload := []byte(strings.Join(lines, "\n"))
	
	if err := sendMessage(conn, MsgBatch, payload); err != nil {
		return err
	}

	ok, err := recvBatchAck(conn)

	if err != nil {
		return err
	}

	if !ok {
		return errors.New("protocol: batch rejected")
	}

	return nil
}

func recvBatchAck(conn io.Reader) (bool, error) {
	msgType, payload, err := recvMessage(conn)
	if err != nil {
		return false, err
	}
	if msgType != MsgBatchAck {
		return false, fmt.Errorf("protocol: expected BATCH_ACK message, got type %v", msgType)
	}
	if len(payload) == 0 {
		return false, errors.New("protocol: empty BATCH_ACK payload")
	}
	return payload[0] == 1, nil
}

func SendFinished(conn io.Writer, agencyId string) error {
	return sendMessage(conn, MsgFinished, []byte(agencyId))
}

func RecvWinners(conn io.Reader) ([]string, error) {
    msgType, payload, err := recvMessage(conn)

    if err != nil {
        return nil, err
    }

    if msgType != MsgWinners {
        return nil, fmt.Errorf("protocol: expected WINNERS message, got type %v", msgType)
    }

    if len(payload) == 0 {
        return []string{}, nil
    }

    raw := strings.TrimSpace(string(payload))

    if raw == "" {
        return []string{}, nil
    }

    return strings.Split(raw, "\n"), nil
}

func sendMessage(conn io.Writer, msgType MessageType, payload []byte) error {
	body := make([]byte, 0, len(payload)+1)
	body = append(body, byte(msgType))
	body = append(body, payload...)

	frame := make([]byte, 0, lengthPrefixSize+len(body))
	lengthBuf := make([]byte, lengthPrefixSize)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(body)))
	frame = append(frame, lengthBuf...)
	frame = append(frame, body...)

	return safe_socket.SendAll(conn, frame)
}

func recvMessage(conn io.Reader) (MessageType, []byte, error) {
	lengthBuf, err := safe_socket.RecvAll(conn, lengthPrefixSize)

	if err != nil {
		return 0, nil, err
	}

	if len(lengthBuf) < lengthPrefixSize {
		return 0, nil, errors.New("Protocol: connection closed while reading length prefix")
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	body, err := safe_socket.RecvAll(conn, int(length))
	if err != nil {
		return 0, nil, err
	}
	if len(body) == 0 {
		return 0, nil, errors.New("Protocol: connection closed while reading message body")
	}

	return MessageType(body[0]), body[1:], nil
}