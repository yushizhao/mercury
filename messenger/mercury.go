package messenger

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

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
	Lag               int // seconds
}

type Message struct {
	Event fsnotify.Event
	Msg   []byte
}

type Messenger struct {
	Messages     chan Message
	Errors       chan error
	watcher      *fsnotify.Watcher
	keeper       *bolt.DB
	keeperNote   map[string]note
	keeperTicker *time.Ticker
	done         chan struct{} // Channel for sending a "quit message" to the reader goroutine
	// doneResp chan struct{} // Channel to respond to Close
}

type note int

const (
	idle note = iota
	clean
	dirty
)

func NewMessenger() (*Messenger, error) {
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

	t := time.NewTicker(time.Duration(conf.Lag) * time.Second)

	m := &Messenger{
		Messages:     make(chan Message),
		Errors:       make(chan error),
		watcher:      w,
		keeper:       k,
		keeperNote:   make(map[string]note),
		keeperTicker: t,
		done:         make(chan struct{}),
		// doneResp: make(chan struct{}),
	}

	// actually set watcher as well
	err = m.resetKeeper(conf.FolderMapFileList)
	if err != nil {
		return nil, err
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
	m.keeperTicker.Stop()

	return err
}

func (m *Messenger) resetKeeper(FolderMapFileList map[string][]string) error {
	folders := []string{}
	names := make(map[string]bool)

	for k, v := range FolderMapFileList {
		folders = append(folders, k)
		for _, e := range v {
			names[filepath.Join(k, e)] = false
			m.keeperNote[filepath.Join(k, e)] = idle
		}
	}

	// remove the unwanted in keeper
	// mark the found in names
	err := m.keeper.Update(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(BUCKET)

		return b.ForEach(func(k, _ []byte) error {
			_, ok := names[string(k)]
			if ok {
				// mark the found
				names[string(k)] = true
			} else {
				err := b.Delete(k)
				if err != nil {
					return err
				}
			}
			return nil
		})
	})

	if err != nil {
		return err
	}

	// add the not found to keeper
	for k, v := range names {
		if !v {
			fInfo, err := os.Stat(k)
			var fSize int64
			if err != nil { // no such file yet
				fSize = 0
			} else {
				fSize = fInfo.Size()
			}
			err = m.putFileSize(k, fSize)
			if err != nil {
				return err
			}
		}
	}

	// add folders to watcher
	for _, folder := range folders {
		err := m.watcher.Add(folder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Messenger) writeMessages() {
	// defer close(m.doneResp)
	defer close(m.Errors)
	defer close(m.Messages)
	for {
		select {
		case <-m.done:
			return
		case <-m.keeperTicker.C:
			for k, v := range m.keeperNote {
				if v == dirty {
					msg, err := m.processWrite(k)
					if msg != nil {
						makeup := fsnotify.Event{Op: fsnotify.Write, Name: k}
						m.Messages <- Message{Event: makeup, Msg: msg}
					}
					if err != nil {
						m.Errors <- err
					}
				}
				m.keeperNote[k] = idle // may set to clean for dirty ones
			}
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
				if m.checkNote(event.Name) {
					msg, err := m.processWrite(event.Name)
					if msg != nil {
						m.Messages <- Message{Event: event, Msg: msg}
					}
					if err != nil {
						m.Errors <- err
					}
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
	return []byte(name + " created."), err
}

func (m *Messenger) checkNote(name string) bool {
	// check keeper note
	name = filepath.Clean(name)
	val, ok := m.keeperNote[name]

	if !ok {
		// ignore files not in keeper note
		return false
	}

	var result bool
	switch val {
	case idle:
		m.keeperNote[name] = clean
		result = true
	case clean:
		m.keeperNote[name] = dirty
		result = true
	case dirty:
		result = false
	}

	return result
}

func (m *Messenger) processWrite(name string) ([]byte, error) {
	size, err := m.getFileSize(name)
	if err != nil {
		// keeper should keep every file within keeper note
		return nil, fmt.Errorf("keeper missing %s", name)
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

	bufferSize := newSize - size
	// If file is not just appended
	if bufferSize <= 0 {
		// treat as error
		return nil, fmt.Errorf("%s size not increase.", name)
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
