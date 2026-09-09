package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	totalSent := 0
	for totalSent < len(bytes) {
		n, err := socket.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	totalRead := 0
	for totalRead < size {
		n, err := socket.Read(buff[totalRead:])
		totalRead += n
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return buff[:totalRead], nil
}