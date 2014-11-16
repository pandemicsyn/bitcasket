// Package bitcasket implements a bitcask inspired on disk storage thingy.
// Its not complete, has no tests, is reasonably fast, written by someone just learning Go, but mostly works.
package bitcasket

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/spaolacci/murmur3"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	/* header consists of: [crc + tstamp + len key data + len value] */
	HeaderSize int32 = 16
)

// CasketFile represents an open bitcasket LOG file
// This shouldn't really be exported
type CasketFile struct {
	file *os.File
	// our current spot in the file
	cpos int32
	sync.RWMutex
}

// Caskets represents
type Caskets struct {
	// Our active keys
	keys map[string]*CasketEntry
	// The channel used by the hashing workers
	writechan chan *kventry
	sync.RWMutex
	// The current active file
	ActiveFile *CasketFile
	// The path to the current active file
	ActiveFilePath string
	bufw           *bufio.Writer
	bufr           *bufio.Reader
}

// CasketEntry represents an invididual db entry
type CasketEntry struct {
	// which fh this is stored at
	cfile         *CasketFile
	valuesize     int32
	valueposition int32
	timestamp     int32
}

// NewCasketFile opens and returns a new CasketFile
func NewCasketFile(path string) (casketfile *CasketFile, err error) {
	tf, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	graveyard := &CasketFile{file: tf, cpos: 0}
	return graveyard, err
}

// ExistingCasketFile opens and returns an existing CasketFile
func ExistingCasketFile(path string) (casketfile *CasketFile, err error) {
	tf, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0766)
	graveyard := &CasketFile{file: tf, cpos: 0}
	return graveyard, err
}

// NewCasketDB sets up a new bitcasket db.
// It handles creation of the initial bitcasket LOG file
// and spin's up a given number of hashing workers.
func NewCasketDB(dbpath string, channelLimit int) *Caskets {
	var err error
	caskets := new(Caskets)
	caskets.keys = make(map[string]*CasketEntry)
	caskets.ActiveFilePath = filepath.Join(dbpath, fmt.Sprintf("%v-casket.LOG", time.Now().Unix()))
	caskets.ActiveFile, err = NewCasketFile(caskets.ActiveFilePath)
	caskets.bufw = bufio.NewWriterSize(caskets.ActiveFile.file, 1048576)
	if err != nil {
		log.Panicf("Error attempting to open db file '%s': %v", caskets.ActiveFilePath, err)
	}
	//log.Println("Starting write worker")
	caskets.writechan = make(chan *kventry, 1024)
	go caskets.writer(3)
	return caskets
}

// GenerateHintFile writes out a hint file from an on disk casket file so keys can be loaded faster on startup
func GenerateHintFile(dbpath string) {
	log.Println("Starting hint file generation")
	hintfile := make(map[string]string)
	var err error
	caskets := new(Caskets)
	caskets.keys = make(map[string]*CasketEntry)
	caskets.ActiveFilePath = dbpath
	caskets.ActiveFile, err = ExistingCasketFile(caskets.ActiveFilePath)
	caskets.bufw = bufio.NewWriterSize(caskets.ActiveFile.file, 1048576)
	caskets.bufr = bufio.NewReaderSize(caskets.ActiveFile.file, 1048576)
	if err != nil {
		log.Panicf("Error attempting to open db file '%s': %v", caskets.ActiveFilePath, err)
	}
	caskets.writechan = make(chan *kventry, 1024)
	counter := 0
	for {
		crc, ts, _, valuesize, valueposition, key, err := caskets.GetHeader()
		if crc == 0 {
			log.Println(string(key))
			log.Println(fmt.Sprintf("%d:%d:%d", ts, valuesize, valueposition))
			break
		}
		if err != nil {
			log.Panicf("Error trying to generate hint file")
		}
		//hintfile[string(key)] = "Wat1" //fmt.Sprintf("%d:%d:%d", ts, valuesize, valueposition)
		counter++
		//log.Println(counter)
	}
	log.Println("Loaded", counter, "entries.")
	//log.Println(hintfile)
	log.Println(len(hintfile))
}

// LoadCasketDB reads a casket file into our  in memory db structure (also known as a map)
func LoadCasketDB(dbpath string, channelLimit int) *Caskets {
	var err error
	caskets := new(Caskets)
	caskets.keys = make(map[string]*CasketEntry)
	caskets.ActiveFilePath = dbpath
	caskets.ActiveFile, err = ExistingCasketFile(caskets.ActiveFilePath)
	caskets.bufw = bufio.NewWriterSize(caskets.ActiveFile.file, 1048576)
	if err != nil {
		log.Panicf("Error attempting to open db file '%s': %v", caskets.ActiveFilePath, err)
	}
	log.Println("Starting write worker")
	caskets.writechan = make(chan *kventry, 1024)
	go caskets.writer(3)
	return caskets
}

type kventry struct {
	keysize       int32
	checksumBytes *[]byte
	buff          *bytes.Buffer
	// What channel we should return our result on
	writeSuccess chan *writeResult
}

type writeResult struct {
	valuePosition int32
	casketFile    *CasketFile
	err           error
}

// ForceFlush flush's our write buffer to disk
func (c *Caskets) ForceFlush() error {
	return c.bufw.Flush()
}

func (c *Caskets) writer(interval int) {
	for entry := range c.writechan {
		valueposition := int32(c.ActiveFile.cpos + HeaderSize + entry.keysize)
		csize, err := c.bufw.Write(*entry.checksumBytes)
		if err != nil {
			log.Fatalln("Error while getting size during crc put.", err)
		}
		bsize, err := c.bufw.Write(entry.buff.Bytes())
		if err != nil {
			log.Fatalln("Error while getting size during payload put.", err)
		}
		c.ActiveFile.cpos += int32(csize + bsize)
		entry.writeSuccess <- &writeResult{valueposition, c.ActiveFile, nil}
	}
}

// Put a key/value/timestamp entry into our DB
// On disk an entry looks like:
// crc int32 | tstamp int32 | key length int32 | value length int32 | key []byte | value []byte
func (c *Caskets) Put(key string, value []byte, ts int32) error {
	resultChannel := make(chan *writeResult)

	entry := new(CasketEntry)
	entry.timestamp = ts
	entry.valuesize = int32(len(value))

	keysize := int32(len(key))

	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, entry.timestamp)
	binary.Write(buff, binary.BigEndian, keysize)
	binary.Write(buff, binary.BigEndian, entry.valuesize)
	buff.Write([]byte(key))
	buff.Write(value)
	checksumBytes := make([]byte, 4)

	binary.BigEndian.PutUint32(checksumBytes, uint32(murmur3.Sum32(buff.Bytes())))
	c.writechan <- &kventry{keysize, &checksumBytes, buff, resultChannel}
	result := <-resultChannel
	if result.err != nil {
		return result.err
	}

	entry.valueposition = result.valuePosition
	entry.cfile = result.casketFile

	c.Lock()
	c.keys[key] = entry
	c.Unlock()
	return nil
}

// GetHeader returns "all the things!" for the current position
func (c *Caskets) GetHeader() (crc, tstamp, keysize, valuesize, valueposition int32, key []byte, err error) {
	header := make([]byte, HeaderSize)
	size, err := io.ReadFull(c.bufr, header)
	//size, err := c.bufr.Read(header)
	if err != nil {
		if err == io.EOF {
			return 0, 0, 0, 0, 0, []byte(""), nil
		} else {
			log.Println(err)
			log.Fatalln("Failed to read header")
		}
	}
	if int32(size) != HeaderSize {
		log.Fatalln("short header read ?")
	}
	buff := bufio.NewReader(bytes.NewBuffer(header))
	binary.Read(buff, binary.BigEndian, &crc)
	binary.Read(buff, binary.BigEndian, &tstamp)
	binary.Read(buff, binary.BigEndian, &keysize)
	binary.Read(buff, binary.BigEndian, &valuesize)
	key = make([]byte, keysize)
	size, err = io.ReadFull(c.bufr, key)
	//_, err = c.bufr.Peek(int(keysize))
	//size, err = c.bufr.Read(key)
	if err != nil {
		log.Println(err)
		log.Fatalln("Failed to read key")
	}

	if int32(size) != keysize {
		log.Println("Only got", size, "expected", keysize, "have", c.bufr.Buffered(), "left in buffer")
		peek := make([]byte, keysize)
		peek, err = c.bufr.Peek(int(keysize))
		log.Println(err)
		log.Println(len(peek))
		log.Println("buffered now", c.bufr.Buffered())
		log.Fatalln("Short read on key ?")
	}

	value := make([]byte, valuesize)
	size, err = io.ReadFull(c.bufr, value)
	//c.bufr.Read(value)
	if int32(size) != valuesize {
		log.Fatalln("Short value read")
	}
	if err != nil {
		log.Println(err)
		log.Fatalln("Failed to read value")
	}

	valueposition = c.ActiveFile.cpos + HeaderSize + keysize
	/* move to next position in file */
	c.ActiveFile.cpos += int32(HeaderSize + keysize + valuesize)
	return
}

// Get a key. That is all.
func (c *Caskets) Get(key string) (value []byte) {
	var read int
	c.RLock()
	value = make([]byte, c.keys[key].valuesize)
	read, err := c.keys[key].cfile.file.ReadAt(value, int64(c.keys[key].valueposition))
	if err != nil {
		log.Printf("Error reading %s from file\n", key)
		log.Panic(err)
	}
	if int32(read) != c.keys[key].valuesize {
		log.Panicf("Expected %d bytes got %d\n", c.keys[key].valuesize, read)
	}
	c.RUnlock()
	return
}
