package rs

import (
	"ds_inter_test/lib/objectstream"
	"ds_inter_test/lib/utils"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"io"
	"log"
	"net/http"
)

const (
	DATA_SHARDS = 4
	PATITY_SHARDS = 2
	ALL_SHARDS = DATA_SHARDS + PATITY_SHARDS
	BLOCK_PER_SHARD = 8000
	BLOCK_SIZE = BLOCK_PER_SHARD * DATA_SHARDS
)

type RSPutStream struct {
	*encoder
}

type resumableToken struct {
	Name string
	Size int64
	Hash string
	Servers []string
	Uuids []string
}

type RSResumablePutStream struct {
	*RSPutStream
	*resumableToken
}

func NewRSResumablePutStream(dataServers []string, name, hash string, size int64)(*RSResumablePutStream, error){
	putStream, e := NewRSPutStream(dataServers, hash, size)
	if e != nil {
		return nil, e
	}
	uuids := make([]string, ALL_SHARDS)
	for i := range uuids {
		uuids[i] = putStream.writers[i].(*objectstream.TempPutStream).Uuid
	}
	token := &resumableToken{name, size, hash, dataServers, uuids}
	return &RSResumablePutStream{putStream, token}, nil
}

func (s *RSResumablePutStream) ToToken() string {
	b, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(b)
}

func NewRSPutStream(dataServers []string, hash string, size int64) (*RSPutStream, error){
	if len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	writers := make([]io.Writer, ALL_SHARDS)
	fmt.Println("size:", perShard)
	var e error
	for i := range writers{
		writers[i], e = objectstream.NewTempPutStream(dataServers[i], fmt.Sprintf("%s.%d",hash,i), perShard)
		if e != nil {
			return nil, e
		}
	}
	enc := NewEncoder(writers)
	return &RSPutStream{enc}, nil
}

type encoder struct {
	writers []io.Writer
	enc reedsolomon.Encoder
	cache []byte
}

func NewEncoder(writers []io.Writer) *encoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PATITY_SHARDS)
	return &encoder{writers, enc, nil}
}

func (e *encoder) Write(p []byte) (n int, err error) {
	length := len(p)
	current := 0
	for length != 0{
		next := BLOCK_SIZE - len(e.cache)
		if next > length {
			next = length
		}
		e.cache = append(e.cache, p[current: current+next]...)
		if len(e.cache) == BLOCK_SIZE {
			e.Flush()
		}
		current += next
		length -= next
	}
	return len(p), nil
}

func (e *encoder) Flush(){
	if len(e.cache) == 0{
		return
	}
	shards, _ := e.enc.Split(e.cache)
	e.enc.Encode(shards)
	for i := range shards {
		e.writers[i].Write(shards[i])
	}
	e.cache = []byte{}
}

func (s *RSPutStream) Commit(success bool){
	s.Flush()
	for i := range s.writers{
		s.writers[i].(*objectstream.TempPutStream).Commit(success)
	}
}

type RSGetStream struct {
	*decoder
}

func NewRSGetStream(locateInfo map[int]string, dataServers []string, hash string, size int64)(*RSGetStream, error){
	if len(locateInfo) + len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}
	readers := make([]io.Reader, ALL_SHARDS)
	for i:=0;i< ALL_SHARDS;i++{
		server := locateInfo[i]
		if server == "" {
			locateInfo[i] = dataServers[0]
			dataServers = dataServers[1:]
			continue
		}
		reader, e := objectstream.NewGetStream(server, fmt.Sprintf("%s.%d", hash, i))
		if e == nil {
			readers[i] = reader
		}
	}
	writers := make([]io.Writer, ALL_SHARDS)
	perSard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	var e error
	for i := range readers {
		if readers[i] == nil {
			writers[i], e = objectstream.NewTempPutStream(locateInfo[i],
				fmt.Sprintf("%s.%d", hash, i), perSard)
			if e != nil {
				return nil, e
			}
		}
	}
	dec := NewDecoder(readers, writers, size)
	return &RSGetStream{dec}, nil
}

type decoder struct {
	readers []io.Reader
	writers []io.Writer
	enc reedsolomon.Encoder
	size int64
	cache []byte
	cacheSize int
	total int64
}

func NewDecoder(readers []io.Reader, writers []io.Writer, size int64)*decoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PATITY_SHARDS)
	return &decoder{readers, writers, enc, size, nil, 0, 0}
}

func (d *decoder) Read(p []byte)(n int, err error){
	if d.cacheSize == 0{
		e := d.getData()
		if e != nil {
			return 0, e
		}
	}
	length := len(p)
	if d.cacheSize < length {
		length = d.cacheSize
	}
	d.cacheSize -= length
	copy(p, d.cache[:length])
	d.cache = d.cache[length:]
	return length, nil
}

func (d *decoder) getData() error {
	if d.total == d.size {
		return io.EOF
	}
	shards := make([][]byte, ALL_SHARDS)
	repairIds := make([]int, 0)
	for i := range shards {
		if d.readers[i] == nil {
			repairIds = append(repairIds, i)
		} else {
			shards[i] = make([]byte, BLOCK_PER_SHARD)
			n, e := io.ReadFull(d.readers[i], shards[i])
			if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
				shards[i] = nil
			} else if n != BLOCK_PER_SHARD {
				shards[i] = shards[i][:n]
			}
		}
	}
	e := d.enc.Reconstruct(shards)
	if e != nil {
		return e
	}
	for i := range repairIds {
		id := repairIds[i]
		d.writers[id].Write(shards[id])
	}
	for i:=0;i< DATA_SHARDS;i++ {
		shardSize := int64(len(shards[i]))
		if d.total+shardSize > d.size {
			shardSize -= d.total + shardSize - d.size
		}
		d.cache = append(d.cache, shards[i][:shardSize]...)
		d.cacheSize += int(shardSize)
		d.total += shardSize
	}
	return nil
}

func (s *RSGetStream) Close(){
	for i := range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*objectstream.TempPutStream).Commit(true)
		}
	}
}

func (s *RSGetStream) Seek(offset int64, whence int)(int64, error){
	if whence != io.SeekCurrent{
		panic("only support SeekCurrent")
	}
	if offset < 0 {
		panic("only support forward seek")
	}
	for offset != 0 {
		length := int64(BLOCK_SIZE)
		if offset < length {
			length = offset
		}
		buf := make([]byte, length)
		io.ReadFull(s, buf)
		offset -= length
	}
	return offset, nil
}

func NewRSResumablePutStreamFromToken(token string)(*RSResumablePutStream, error){
	b, e := base64.StdEncoding.DecodeString(token)
	if e != nil {
		return nil, e
	}
	var t resumableToken
	e = json.Unmarshal(b, &t)
	if e != nil {
		return nil, e
	}
	writers := make([]io.Writer, ALL_SHARDS)
	for i := range writers {
		writers[i] = &objectstream.TempPutStream{t.Servers[i], t.Uuids[i]}
	}
	enc := NewEncoder(writers)
	return &RSResumablePutStream{&RSPutStream{enc}, &t}, nil
}

func (s *RSResumablePutStream) CurrentSize() int64 {
	r, e := http.Head(fmt.Sprintf("http://%s/temp/%s", s.Servers[0], s.Uuids[0]))
	if e != nil {
		log.Println(e)
		return -1
	}
	if r.StatusCode != http.StatusOK {
		log.Println(r.StatusCode)
		return -1
	}
	size := utils.GetSizeFromHeader(r.Header) * DATA_SHARDS
	if size > s.Size {
		size = s.Size
	}
	return size
}

type RSResumableGetStream struct {
	*decoder
}

func NewRSResumableGetStream(dataServers, uuids []string, size int64) (*RSResumableGetStream, error) {
	readers := make([]io.Reader, ALL_SHARDS)
	var e error
	for i:=0; i < ALL_SHARDS; i++ {
		readers[i], e = objectstream.NewTempGetStream(dataServers[i], uuids[i])
		if e != nil {
			return nil, e
		}
	}
	writers := make([]io.Writer, ALL_SHARDS)
	dec := NewDecoder(readers, writers, size)
	return &RSResumableGetStream{dec}, nil
}
