package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	Input      string
	OutputDir  string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	const mainAction = "process-bets"
	defer client.conn.Close()

	inFile, err := os.Open(client.config.Input)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "error", err)
		return err
	}
	defer inFile.Close()

	betsSent, err := client.sendBets(inFile)
	if err != nil {
		return err
	}

	if err := protocol.SendFinished(client.conn, client.config.AgencyId); err != nil {
		logger.Error("send-finished", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	winners, err := protocol.RecvWinners(client.conn)
	if err != nil {
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	if err := client.persistWinners(winners); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success,
		"agency-id", client.config.AgencyId,
		"bets-sent", betsSent,
		"winners", len(winners))

	return nil
}

func (client *Client) sendBets(inFile *os.File) (int, error) {
	scanner := bufio.NewScanner(inFile)
	batch := make([]protocol.Bet, 0, client.config.BatchSize)
	betsSent := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := protocol.SendBatch(client.conn, client.config.AgencyId, batch); err != nil {
			logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "batch-size", len(batch))
			return err
		}
		ok, err := protocol.RecvBatchAck(client.conn)
		if err != nil {
			logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}
		if !ok {
			logger.Error("batch-rejected", logger.Fail, "agency-id", client.config.AgencyId, "batch-size", len(batch))
			return fmt.Errorf("server rejected batch of %d bets", len(batch))
		}
		betsSent += len(batch)
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if line == "" {
			continue
		}

		fields := strings.Split(line, ",")
		if len(fields) != 5 {
			logger.Error("parse-bet", logger.Fail, "agency-id", client.config.AgencyId, "line", line)
			continue
		}

		batch = append(batch, protocol.Bet{
			FirstName: fields[0],
			LastName:  fields[1],
			Document:  fields[2],
			Birthdate: fields[3],
			Number:    fields[4],
		})

		if len(batch) == client.config.BatchSize {
			if err := flush(); err != nil {
				return betsSent, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "error", err)
		return betsSent, err
	}

	if err := flush(); err != nil {
		return betsSent, err
	}

	return betsSent, nil
}

func (client *Client) persistWinners(winners []string) error {
	outputPath := fmt.Sprintf("%s/output-%s.txt", client.config.OutputDir, client.config.AgencyId)

	outFile, err := os.Create(outputPath)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "error", err)
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	for _, winnerDoc := range winners {
		if _, err := writer.WriteString(winnerDoc + "\n"); err != nil {
			logger.Error("write-output", logger.Fail, "error", err)
			return err
		}
	}

	return nil
}