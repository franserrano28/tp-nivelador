package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	return &Client{config: config}, nil
}

func connectToServer(ctx context.Context, host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var dialer net.Dialer
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := 0; i < CONNECTION_ATTEMPTS_MAX; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
		if err == nil {
			logger.Info(action, logger.Success)
			return conn, nil
		}

		logger.Warn(action, logger.Fail, "attempt", i)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond):
		}
	}

	return nil, err
}

func (client *Client) Run() error {
	const mainAction = "process-bets"

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	conn, err := connectToServer(ctx, client.config.ServerHost, client.config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return err
	}
	client.conn = conn
	defer client.conn.Close()

	go func() {
		<-ctx.Done()
		if client.conn != nil {
			client.conn.Close()
		}
	}()

	inFile, err := os.Open(client.config.Input)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "error", err)
		return err
	}
	defer inFile.Close()

	betsSent, err := client.sendBets(ctx, inFile)
	if err != nil {
		if ctx.Err() != nil {
			logger.Info(mainAction, logger.Success, "action", "graceful-shutdown-during-send")
			return nil
		}
		return err
	}

	if err := protocol.SendFinished(client.conn, client.config.AgencyId); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		logger.Error("send-finished", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	winners, err := protocol.RecvWinners(client.conn)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
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

func (client *Client) sendBets(ctx context.Context, inFile *os.File) (int, error) {
	scanner := bufio.NewScanner(inFile)
	batch := make([]protocol.Bet, 0, client.config.BatchSize)
	betsSent := 0

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
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
		select {
		case <-ctx.Done():
			return betsSent, ctx.Err()
		default:
		}

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
	outFile, err := os.Create(client.config.OutputFile)
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