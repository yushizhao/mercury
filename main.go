package main

import (
	"log"

	"github.com/yushizhao/mercury/messenger"
)

func main() {
	hermes, err := messenger.NewMessenger()
	if err != nil {
		log.Fatal(err)
	}
	defer hermes.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case Message, ok := <-hermes.Messages:
				if !ok {
					return
				}
				log.Println("msg:", string(Message.Msg))
				log.Println("modified file:", Message.Event.Name)
			case err, ok := <-hermes.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	<-done

}
