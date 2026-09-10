package client

import (
	"bufio"
	"net"
	"os"
	"strings"
	"time"
	"sync/atomic"

	lottery "github.com/7574-sistemas-distribuidos/tp-nivelador/src/lottery"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId string
	InputFile string
	OutputFile string
	BatchSize int
}

type Client struct {
	conn net.Conn
	config ClientConfig
	running atomic.Bool
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)

	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	client := &Client{config: config, conn: conn}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)

	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)

		if err == nil {
			logger.Info(action, logger.Success)
			return conn, nil
		}

		logger.Warn(action, logger.Fail, "attempt", i)
		time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
	}

	return nil, err
}

func (client *Client) Run() error {
	const mainAction = "process-bets"

	defer client.conn.Close()

	inFile, err := os.Open(client.config.InputFile)
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
		return err
	}

	winners, err := protocol.RecvWinners(client.conn)
	if err != nil {
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
	batch := []lottery.Bet{}
	betsSent := 0

	for scanner.Scan() {
	
		line := strings.TrimRight(scanner.Text(), "\r\n")
		
		if line == "" {
			continue
		}

		bet, err := lottery.ParseToBet(line)
		if err != nil {
			logger.Error("parse-bet", logger.Fail, "agency-id", client.config.AgencyId, "line", line)
			continue
		}

		batch = append(batch, bet)

		if len(batch) == client.config.BatchSize {
			if err := protocol.SendBatch(client.conn, client.config.AgencyId, batch); err != nil {
				logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "batch-size", len(batch))
				return betsSent, err
			}
			betsSent += len(batch)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := protocol.SendBatch(client.conn, client.config.AgencyId, batch); err != nil {
			logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "batch-size", len(batch))
			return betsSent, err
		}
		betsSent += len(batch)
	}

	return betsSent, nil
}

func (client *Client) persistWinners(winners []string) error {
	outFile, err := os.Create(client.config.OutputFile)
	
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "error", err)
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)

	for _, winnerDoc := range winners {
		if _, err := writer.WriteString(winnerDoc + "\n"); err != nil {
			logger.Error("write-output", logger.Fail, "error", err)
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}