package messenger

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/fsnotify/fsnotify"
)

const (
	CONFIG = "mercury.json"
	DB     = "mercury.bolt"
)

var (
	BUCKET = []byte("keeper")
)

type config struct {
	FolderMapFileList map[string][]string
}

type Message struct {
	Event fsnotify.Event
	Msg   []byte
}

type Messenger struct {
	Messages chan Message
	Errors   chan error
	watcher  *fsnotify.Watcher
	keeper   *bolt.DB
	done     chan struct{} // Channel for sending a "quit message" to the reader goroutine
	// doneResp chan struct{} // Channel to respond to Close
}

func NewMessenger() (*Messenger, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	k, err := bolt.Open(DB, 0666, nil)
	if err != nil {
		return nil, err
	}

	err = k.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(BUCKET)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	m := &Messenger{
		Messages: make(chan Message),
		Errors:   make(chan error),
		watcher:  w,
		keeper:   k,
		done:     make(chan struct{}),
		// doneResp: make(chan struct{}),
	}

	// load config
	conf := config{}
	data, err := ioutil.ReadFile(CONFIG)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return nil, err
	}

	for k, v := range conf.FolderMapFileList {
		err := m.addFileList(k, v)
		if err != nil {
			return nil, err
		}
	}

	go m.writeMessages()
	return m, nil
}

func (m *Messenger) isClosed() bool {
	select {
	case <-m.done:
		return true
	default:
		return false
	}
}

func (m *Messenger) Close() error {
	if m.isClosed() {
		return nil
	}

	close(m.done)

	err := m.watcher.Close()
	err = m.keeper.Close()

	return err
}

func (m *Messenger) writeMessages() {
	// defer close(m.doneResp)
	defer close(m.Errors)
	defer close(m.Messages)
	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			if event.Op == fsnotify.Create {
				msg, err := m.processCreate(event.Name)
				if msg != nil {
					m.Messages <- Message{Event: event, Msg: msg}
				}
				if err != nil {
					m.Errors <- err
				}
			}
			if event.Op == fsnotify.Write {
				msg, err := m.processWrite(event.Name)
				if msg != nil {
					m.Messages <- Message{Event: event, Msg: msg}
				}
				if err != nil {
					m.Errors <- err
				}
			}
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			m.Errors <- err
		}
	}
}

func (m *Messenger) processCreate(name string) ([]byte, error) {
	_, err := m.getFileSize(name)
	// ignore files not in keeper
	if err != nil {
		return nil, nil
	}

	err = m.putFileSize(name, 0)
	return []byte(name + "created."), err
}

func (m *Messenger) processWrite(name string) ([]byte, error) {
	size, err := m.getFileSize(name)
	// ignore files not in keeper
	if err != nil {
		return nil, nil
	}

	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	newSize := fileInfo.Size()
	// edge case when save trigger two writes
	if newSize == 0 {
		return nil, nil
	}

	bufferSize := newSize - size
	// If file is not just appended
	if bufferSize < 0 {
		// reset keeper
		err = m.putFileSize(name, newSize)
		return []byte("File size decreased."), err
	}

	_, err = file.Seek(size, 0)
	if err != nil {
		return nil, err
	}

	msg := make([]byte, bufferSize)
	_, err = file.Read(msg)
	if err != nil {
		return nil, err
	}

	err = m.putFileSize(name, newSize)
	return msg, err
}

func (m *Messenger) addFileList(folder string, FileList []string) error {

	folder = filepath.Clean(folder)
	// store origin file size to keeper
	for _, f := range FileList {
		f = filepath.Join(folder, f)
		fInfo, err := os.Stat(f)
		if err != nil {
			return err
		}
		err = m.putFileSize(f, fInfo.Size())
		if err != nil {
			return err
		}
	}
	// add folder to watcher

	err := m.watcher.Add(folder)
	if err != nil {
		return err
	}
	return nil
}

func (m *Messenger) putFileSize(file string, size int64) error {
	file = filepath.Clean(file)
	return m.keeper.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(BUCKET)
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(size))
		err := b.Put([]byte(file), bytes)
		return err
	})
}

func (m *Messenger) getFileSize(file string) (size int64, err error) {
	file = filepath.Clean(file)
	err = m.keeper.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(BUCKET)
		bytes := b.Get([]byte(file))
		if bytes == nil {
			// size = -1
			return fmt.Errorf("%s not found in keeper!", file)
		}
		size = int64(binary.LittleEndian.Uint64(bytes))
		return nil
	})
	return size, err
}
