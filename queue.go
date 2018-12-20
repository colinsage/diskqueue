package diskqueue

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (

	LogExtension   = "log"
	IndexExtension = "idx"

	FilePrefix = "_"
)

var (
	bytesPool = NewLimitedBytes(256, 4*1024*1024)

	ErrQueueOpenFailed = fmt.Errorf("queue open failed")
	ErrQueueClosed = fmt.Errorf("queue closed")
	ErrQueueCorrupt = fmt.Errorf("corrupted cons Queue entry")
	ErrReadDone  = fmt.Errorf("already read after write")
	ErrSegmentNotFound  = fmt.Errorf("segment not found")
)

type Message interface {
	Body()   []byte
	Timestamp() int64
}

type DefaultMessage struct {
	body []byte
	timestamp  int64
}

func NewDefaultMessage(b []byte, ts int64) *DefaultMessage{
	return &DefaultMessage{
		body: b,
		timestamp: ts,
	}
}

func (dm *DefaultMessage) Body() []byte{
	return dm.body
}

func (dm *DefaultMessage) Timestamp() int64{
	return dm.timestamp
}

type Segment struct {
	segFile  string
	segIndex string
}

type Queue struct {
	path  	string
	SegmentSize  int32

	syncDelay time.Duration
	syncCount   uint32
	syncWaiters chan chan error
	mu            sync.RWMutex

	depth 	int32
	currentSegId   int32

	currentSegWriter *SegmentWriter

	Segments map[int32]*Segment
	consumers map[string]*consumer

	logger  *zap.Logger
	closing chan struct{}
}

func NewQueue(path string, segSize int32) *Queue {
	logger := zap.NewNop()
	return &Queue{
		path: path,

		SegmentSize: segSize,
		closing:     make(chan struct{}),
		syncWaiters: make(chan chan error, 1024),
		logger:      logger,
		Segments:    make(map[int32]*Segment),
	}
}

func (q *Queue) Open() error{
	q.mu.Lock()
	defer q.mu.Unlock()
	q.logger.Info("open queue", zap.String("path", q.path))

	if err := os.MkdirAll(q.path, 0777); err != nil {
		return err
	}

	segments, err := segFileNames(q.path)
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		// open last seg writer
		lastSegment := segments[len(segments)-1]
		id, err := idFromFileName(lastSegment)
		if err != nil {
			return err
		}

		q.currentSegId = id
		stat, err := os.Stat(lastSegment)
		if err != nil {
			return err
		}

		lastSegFileSize := int32(stat.Size())
		if lastSegFileSize == 0 || lastSegFileSize < 13{
			os.Remove(lastSegment)
			segments = segments[:len(segments)-1]
			q.currentSegId--
			q.newSegmentFile()
		} else {
			lastSegIndex := strings.Replace(lastSegment, LogExtension, IndexExtension, 1)
			ifd, err := os.OpenFile(lastSegIndex, os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			if _, err := ifd.Seek(0, io.SeekEnd); err != nil {
				return err
			}

			fd, err := os.OpenFile(lastSegment, os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			if _, err := fd.Seek(0, io.SeekEnd); err != nil {
				return err
			}

			lastTimestamp, totalMsgSize, err := recoverySegStatus(fd, ifd, lastSegFileSize)
			if err != nil {
				return err
			}
			// recovery last timestamp & size
			q.currentSegWriter = NewSegmentWriter(fd, ifd, q.currentSegId)
			q.currentSegWriter.logger = q.logger
			q.currentSegWriter.lastMsgTimestamp = lastTimestamp
			q.currentSegWriter.totalMsgSize = totalMsgSize
		}

	}else {
		q.newSegmentFile()

	}
	q.closing = make(chan struct{})
	return nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Close, but don't set to nil so future goroutines can still be signaled
	close(q.closing)

	if q.currentSegWriter != nil {
		q.sync()
		q.currentSegWriter.close()
		q.currentSegWriter = nil
	}

	q.logger.Info("queue closed", zap.Int32("seg_id", q.currentSegId))
	return nil
}

func recoverySegStatus(fd *os.File, ifd *os.File, lastSegSize int32) (int64, int32, error){
   //read foot
	// recover foot eof entry
	buf := make([]byte, 22)
	if _, err := fd.Seek(-22, io.SeekEnd); err != nil {
		return 0,0, err
	}
	fd.Read(buf)
	if isFoot(buf){
		lastTimestamp := int64(binary.BigEndian.Uint64(buf[10:18]))
		totalMsgSize := int32(binary.BigEndian.Uint32(buf[18:22]))
		fd.Seek(-22, io.SeekEnd)
		return lastTimestamp, totalMsgSize, nil
	}else{
		//no foot, will find last msg
		headBuf := make([]byte, 12)
		ifd.Seek(-12, io.SeekEnd)
		ifd.Read(headBuf)
		lastTimestampAtIndex := int64(binary.BigEndian.Uint64(buf[0:8]))
		totalMsgSizeAtIndex := int32(binary.BigEndian.Uint32(headBuf[8:12]))

		fd.Seek(int64(totalMsgSizeAtIndex), io.SeekStart)

		totalMsgSize := totalMsgSizeAtIndex
		lastTimestamp := lastTimestampAtIndex
		for {
			fd.Read(headBuf)
			length := int32(binary.BigEndian.Uint32(headBuf[0:4]))
			lastTimestamp = int64(binary.BigEndian.Uint64(headBuf[4:12]))

			fd.Seek(int64(length), io.SeekCurrent)
			totalMsgSize = totalMsgSize + 12 + length

			if totalMsgSize >= lastSegSize {
				return lastTimestamp, totalMsgSize, nil
			}
		}
	}

	return 0, 0, ErrQueueOpenFailed
}

func (q *Queue) Write (msg Message) (int32, error){
	msgLen := len(msg.Body())

	encBuf := bytesPool.Get(snappy.MaxEncodedLen(msgLen))
	compressed := snappy.Encode(encBuf, msg.Body())

	syncErr := make(chan error)

	segID, err := func() (int32, error) {
		q.mu.Lock()
		defer q.mu.Unlock()

		// Make sure the log has not been closed
		select {
		case <-q.closing:
			return -1, ErrQueueClosed
		default:
		}

		// roll the segment file if needed
		if err := q.rollSegment(); err != nil {
			return -1, fmt.Errorf("error rolling cons segment: %v", err)
		}

		// write and sync
		if err := q.currentSegWriter.Write(compressed, msg.Timestamp()); err != nil {
			return -1, fmt.Errorf("error writing cons entry: %v", err)
		}

		select {
		case q.syncWaiters <- syncErr:
		default:
			return -1, fmt.Errorf("error syncing wal")
		}
		q.scheduleSync()

		q.pumpConsumer(msg)

		atomic.AddInt32(&q.depth, 1)
		return q.currentSegId, nil

	}()

	bytesPool.Put(encBuf)

	if err != nil {
		return segID, err
	}

	// schedule an fsync and wait for it to complete
	return segID, <-syncErr
}

func (q *Queue) rollSegment() error {
	if q.currentSegWriter == nil || q.currentSegWriter.totalMsgSize > q.SegmentSize {
		if err := q.newSegmentFile(); err != nil {
			// A drop database or RP call could trigger this error if writes were in-flight
			// when the drop statement executes.
			return fmt.Errorf("error opening new segment file for wal (2): %v", err)
		}
		return nil
	}

	return nil
}

func (q *Queue) newSegmentFile() error {
	q.currentSegId++
	if q.currentSegWriter != nil {
		//q.sync()
		if err := q.currentSegWriter.close(); err != nil {
			return err
		}
	}

	fileName := filepath.Join(q.path, fmt.Sprintf("%s%05d.%s", FilePrefix, q.currentSegId, LogExtension))
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	indexName := filepath.Join(q.path, fmt.Sprintf("%s%05d.%s", FilePrefix, q.currentSegId, IndexExtension))
	ifd, err := os.OpenFile(indexName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	q.currentSegWriter = NewSegmentWriter(fd, ifd, q.currentSegId)
	q.currentSegWriter.logger = q.logger

	seg := &Segment{
		segFile:  fileName,
		segIndex: indexName,
	}
	q.Segments[q.currentSegId] = seg

	return nil
}

func (q *Queue) scheduleSync() {
	// If we're not the first to sync, then another goroutine is fsyncing the wal for us.
	if !atomic.CompareAndSwapUint32(&q.syncCount, 0, 1) {
		return
	}

	// Fsync the wal and notify all pending waiters
	go func() {
		var timerCh <-chan time.Time

		// time.NewTicker requires a > 0 delay, since 0 indicates no delay, use a closed
		// channel which will always be ready to read from.
		if q.syncDelay == 0 {
			// Create a RW chan and close it
			timerChrw := make(chan time.Time)
			close(timerChrw)
			// Convert it to a read-only
			timerCh = timerChrw
		} else {
			t := time.NewTicker(q.syncDelay)
			defer t.Stop()
			timerCh = t.C
		}
		for {
			select {
			case <-timerCh:
				q.mu.Lock()
				if len(q.syncWaiters) == 0 {
					atomic.StoreUint32(&q.syncCount, 0)
					q.mu.Unlock()
					return
				}

				q.sync()
				q.mu.Unlock()
			case <-q.closing:
				atomic.StoreUint32(&q.syncCount, 0)
				return
			}
		}
	}()
}

func (q *Queue) sync() {
	err := q.currentSegWriter.sync()
	for len(q.syncWaiters) > 0 {
		errC := <-q.syncWaiters
		errC <- err
	}
}
type SegmentWriter struct {
	id      		int32
	queue  			*Queue

	bw      *bufio.Writer
	w       io.WriteCloser
	iw      io.WriteCloser

	bufMsgSize 				int32
	totalMsgSize    		int32

	lastMsgTimestamp  		int64
	lastMsgSize 			int32

	logger       	*zap.Logger
}

func NewSegmentWriter(w, iw io.WriteCloser, segmentId int32) *SegmentWriter {
	sw := &SegmentWriter{
		bw: bufio.NewWriterSize(w, 16*1024),
		w:  w,
		iw: iw,
		id: segmentId,
	}
	return sw
}

func (w *SegmentWriter) path() string {
	if f, ok := w.w.(*os.File); ok {
		return f.Name()
	}
	return ""
}

// Write writes entryType and the buffer containing compressed entry data.
func (w *SegmentWriter) Write(compressedMsg []byte, timestamp int64) error {
	var buf [12]byte
	length := uint32(len(compressedMsg))
	binary.BigEndian.PutUint32(buf[0:4], length)
	if length > 10*1024*1024 {
		w.logger.Error( "too_big_message", zap.Uint32("length", length))
	}

	binary.BigEndian.PutUint64(buf[4:12], uint64(timestamp))

	if _, err := w.bw.Write(buf[:]); err != nil {
		return err
	}

	if _, err := w.bw.Write(compressedMsg); err != nil {
		return err
	}

	msgSize := int32(12 + len(compressedMsg))

	w.bufMsgSize += msgSize
	if w.totalMsgSize== 0 || w.bufMsgSize > 512*1024 {
		binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
		binary.BigEndian.PutUint32(buf[8:12], uint32(w.totalMsgSize))
		w.iw.Write(buf[0:12])
		w.bufMsgSize = 0
	}

	w.totalMsgSize += msgSize

	w.lastMsgSize = msgSize
	w.lastMsgTimestamp = timestamp
	return nil
}

// Sync flushes the file systems in-memory copy of recently written data to disk,
// if w is writing to an os.File.
func (w *SegmentWriter) sync() error {
	if err := w.iSync(); err != nil {
		return err
	}
	if err := w.wSync(); err != nil {
		return err
	}
	return nil
}

func (w *SegmentWriter) wSync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}

	if f, ok := w.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (w *SegmentWriter) iSync() error {
	if f, ok := w.iw.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (w *SegmentWriter) Flush() error {
	return w.bw.Flush()
}

func isFoot(buf []byte) bool {
	if  len(buf)== 22 &&
		buf[0] == byte(0x00) &&
		buf[1] == byte('D') &&
		buf[2] == byte('I') &&
		buf[3] == byte('S') &&
		buf[4] == byte('K') &&
		buf[5] == byte('Q') &&
		buf[6] == byte('U') &&
		buf[7] == byte('E') &&
		buf[8] == byte('U') &&
		buf[9] == byte('E') {
		return true
	}

	return false
}
func (w *SegmentWriter) writeFoot() {
	var buf [22]byte
	buf[0] = byte(0x00)
	buf[1] = byte('D')
	buf[2] = byte('I')
	buf[3] = byte('S')
	buf[4] = byte('K')
	buf[5] = byte('Q')
	buf[6] = byte('U')
	buf[7] = byte('E')
	buf[8] = byte('U')
	buf[9] = byte('E')
	binary.BigEndian.PutUint64(buf[10:18], uint64(w.lastMsgTimestamp))
	binary.BigEndian.PutUint32(buf[18:22], uint32(w.totalMsgSize))

	w.bw.Write(buf[0:22])

	//write last msg index
	binary.BigEndian.PutUint64(buf[0:8], uint64(w.lastMsgTimestamp))
	binary.BigEndian.PutUint32(buf[8:12], uint32(w.totalMsgSize - w.lastMsgSize))
	w.iw.Write(buf[0:12])
	w.logger.Info("write segment foot done.", zap.Int32("segment", w.id ))
}

func (w *SegmentWriter) close() error {
	w.writeFoot()
	var err error
	if err = w.sync(); err != nil {
		return err
	}

	if err = w.iw.Close(); err != nil {
		return err
	}

	if err = w.w.Close(); err != nil {
		return err
	}
	w.logger.Info("current segment writer closed.", zap.Int32("seg_id", w.id))
	return err
}

func idFromFileName(name string) (int32, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s has wrong name format to have an id", name)
	}

	id, err := strconv.ParseUint(parts[0][1:], 10, 32)

	return int32(id), err
}

func segFileNames(dir string) ([]string, error) {
	names, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%s*.%s", FilePrefix, LogExtension)))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

type SendMsg func(msg []byte) error

type memChan struct {
	cap   int32
	channel  chan Message
	start   int64
	end     int64
	size    int32
	mux     sync.RWMutex
}

func NewMemChan(c int32) *memChan{
	return &memChan{
		cap: c,
		channel: make(chan Message, c) ,
	}
}

func (mc *memChan) push(msg Message){
	mc.mux.Lock()
	defer mc.mux.Unlock()
	if mc.size == 0 {
		mc.start = msg.Timestamp()
	}
	mc.size++
	if mc.size >= mc.cap {
		head := <-mc.channel
		mc.start = head.Timestamp()
		mc.size--
	}
	mc.channel <- msg
	mc.end = msg.Timestamp()
}

func (mc *memChan) poll() Message {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	if mc.size > 0 {
		msg := <- mc.channel
		mc.start = msg.Timestamp()
		mc.size--
		return msg
	}
	return nil
}

func (mc *memChan) contain(ts int64) bool{
	mc.mux.Lock()
	defer mc.mux.Unlock()
	if mc.start < ts && mc.end > ts {
		return true
	}
	return false
}

type consumer struct {
	sendFn  SendMsg
	id 		string

	mc      *memChan

	logger  *zap.Logger
	closing chan struct{}
}

func (c *consumer) pump(msg Message){
	c.mc.push(msg)
}

func (q *Queue) pumpConsumer(msg Message){
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, c := range q.consumers {
		c.pump(msg)
	}
}

func (q *Queue) AddConsumer(sendFn SendMsg, id string, start int64){
	q.mu.Lock()
	defer q.mu.Unlock()
	c := &consumer{
		sendFn: sendFn,
		id : id,
		mc : NewMemChan(20),
	}
	q.consumers[id] = c

	go q.doConsumer(c, start)
}

func (q *Queue) RemoveConsumer(id string){
	q.mu.Lock()
	defer q.mu.Unlock()

	c, ok := q.consumers[id]
	if ok {
		delete(q.consumers, id)
		close(c.closing)
	}
}

func (q *Queue) doConsumer(consumer *consumer, start int64){

}