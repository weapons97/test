package kafka

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"

	"helloTigerGraph/pkg/model"
)

var (
	// gAddrs = `106.75.106.139:9092`
	gAddrs = `127.0.0.1:9092`
	gConns = map[string]*kafka.Conn{}
	times  = 0
)

func GetConn(topic string) *kafka.Conn {
	conn, ok := gConns[topic]
	var err error
	if !ok || times > 5 {
		conn, err = kafka.DialLeader(context.Background(), "tcp", gAddrs, topic, 0)
		if err != nil {
			log.Panic().Err(err).Send()
		}
		gConns[topic] = conn
		times = 0
	}
	times++
	return conn
}
func Insert(topic string, smas ...*model.Schema) error {
	log.Info().Msgf(`KafKaInsert %v len=%+v from=%+v to=%+v `,
		topic, len(smas), smas[0], smas[len(smas)-1])

	conn := GetConn(topic)
	ins := make([]kafka.Message, len(smas))
	for i, v := range smas {
		ins[i] = kafka.Message{Value: []byte(v.String())}
	}
	// conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.WriteMessages(ins...)
	if err != nil {
		log.Panic().Err(err).Send()
		return err
	}

	return nil
}

func Read(topic string) (chan *model.Schema, error) {
	// to consume messages

	conn := GetConn(topic)

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

	}()

	return c, nil
}
