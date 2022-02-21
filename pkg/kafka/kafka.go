package kafka

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"helloTigerGraph/pkg/model"
)

var (
	gAddrs = `106.75.106.139:9092`
)

func Insert(topic string, smas ...*model.Schema) error {
	log.Info().Msgf(`KafKaInsert %v len=%+v from=%+v to=%+v `,
		topic, len(smas), smas[0], smas[len(smas)-1])
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", gAddrs, topic, partition)
	if err != nil {
		return err
	}
	ins := make([]kafka.Message, len(smas))
	for i, v := range smas {
		ins[i] = kafka.Message{Value: []byte(v.String())}
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(ins...)
	if err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}

func Read(topic string) (chan *model.Schema, error) {
	// to consume messages

	partition := 0
	addrs := `106.75.106.139:9092`

	conn, err := kafka.DialLeader(context.Background(), "tcp", addrs, topic, partition)
	if err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	batch := conn.ReadBatch(100, 1e6*50)
	// 遍历
	c := make(chan *model.Schema, 10000)

	go func() {
		for {
			n, err := batch.ReadMessage()
			if err != nil {
				close(c)
				break
			}
			scm, err := model.NewSchema(string(n.Value))
			if err != nil {
				close(c)
				break
			}
			c <- scm
		}
		if err := batch.Close(); err != nil {
			log.Error().Err(err).Send()
		}
		if err := conn.Close(); err != nil {
			log.Error().Err(err).Send()
		}
	}()

	return c, nil
}
