package objects

import (
	"ds_inter_test/lib/es"
	"ds_inter_test/lib/heartbeat"
	"ds_inter_test/lib/rs"
	"ds_inter_test/lib/utils"
	"ds_inter_test/locate"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request){
	m := r.Method
	if m == http.MethodGet{
		get(w, r)
		return
	}
	if m == http.MethodPut{
		put(w, r)
		return
	}
	if m == http.MethodDelete{
		del(w, r)
		return
	}
	if m == http.MethodPost{
		post(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// 分布式接口服务put方法，不包含元数据服务
//func put(w http.ResponseWriter, r *http.Request){
//	object := strings.Split(r.URL.EscapedPath(), "/")[2]
//	c, err := storeObject(r.Body, object)
//	if err != nil{
//		log.Println(err)
//		return
//	}
//	w.WriteHeader(c)
//}

// 填入元数据服务put方法
//func put(w http.ResponseWriter, r *http.Request){
//	hash := utils.GetHashFromHeader(r.Header)
//	if hash == ""{
//		log.Println("missing object hash in digest header")
//		w.WriteHeader(http.StatusBadRequest)
//		return
//	}
//	c, e := storeObject(r.Body, url.PathEscape(hash))
//	if e != nil{
//		log.Println(e)
//		w.WriteHeader(c)
//		return
//	}
//	if c != http.StatusOK{
//		w.WriteHeader(c)
//		return
//	}
//	name := strings.Split(r.URL.EscapedPath(), "/")[2]
//	size := utils.GetSizeFromHeader(r.Header)
//	e = es.AddVersion(name, hash, size)
//	if e != nil{
//		log.Println(e)
//		w.WriteHeader(http.StatusInternalServerError)
//	}
//}


func put(w http.ResponseWriter, r *http.Request){
	hash := utils.GetHashFromHeader(r.Header)
	if hash == ""{
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	size := utils.GetSizeFromHeader(r.Header)
	c, e := storeObject(r.Body, hash, size)
	if e != nil{
		log.Println(e)
		w.WriteHeader(c)
		return
	}
	if c != http.StatusOK{
		w.WriteHeader(c)
		return
	}
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	e = es.AddVersion(name, hash, size)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
	}
}


// 元数据服务
//func storeObject(r io.Reader, object string) (int, error){
//	stream, err := putStream(object)
//	if err != nil{
//		return http.StatusServiceUnavailable, err
//	}
//	io.Copy(stream, r)
//	err = stream.Close()
//	if err != nil{
//		return http.StatusInternalServerError, err
//	}
//	return http.StatusOK, nil
//}
func storeObject(r io.Reader, hash string, size int64)(int, error){
	if locate.Exist(url.PathEscape(hash)){
		return http.StatusOK, nil
	}
	stream ,e := putStream(url.PathEscape(hash), size)
	if e != nil{
		return http.StatusInternalServerError, e
	}
	reader := io.TeeReader(r, stream)
	d := utils.CalculateHash(reader)

	if d != hash {
		stream.Commit(false)
		return http.StatusBadRequest, fmt.Errorf("object hash mismatch, calculated=%s, requested=%s,",
			d, hash)
	}
	stream.Commit(true)
	return http.StatusOK, nil
}

//元数据服务
//func putStream(object string)(*objectstream.PutStream, error){
//	server := heartbeat.ChooseRandomDataServer()
//	if server == ""{
//		return nil, fmt.Errorf("cannot find any dataServer")
//	}
//	return objectstream.NewPutStream(server, object), nil
//}

//数据去重版本
//func putStream(hash string, size int64)(*objectstream.TempPutStream, error){
//	server := heartbeat.ChooseRandomDataServer()
//	if server == ""{
//		return nil, fmt.Errorf("cannot find any dataServer")
//	}
//	return objectstream.NewTempPutStream(server, hash, size)
//}
func putStream(hash string, size int64)(*rs.RSPutStream, error){
	servers := heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS, nil)
	if len(servers) != rs.ALL_SHARDS {
		return nil, fmt.Errorf("cannot find enough dataServer")
	}
	return rs.NewRSPutStream(servers, hash, size)
}


// 分布式接口服务get方法，不包含元数据服务
//func get(w http.ResponseWriter, r *http.Request){
//	object := strings.Split(r.URL.EscapedPath(), "/")[2]
//	stream, e := getStream(object)
//	if e != nil{
//		log.Println(e)
//		w.WriteHeader(http.StatusNotFound)
//		return
//	}
//	io.Copy(w, stream)
//}

// 数据去重版本
//func get(w http.ResponseWriter, r *http.Request){
//	name := strings.Split(r.URL.EscapedPath(), "/")[2]
//	versionId := r.URL.Query()["version"]
//	version := 0
//	var e error
//	if len(versionId) != 0{
//		version, e = strconv.Atoi(versionId[0])
//		if e != nil {
//			log.Println(e)
//			w.WriteHeader(http.StatusBadRequest)
//			return
//		}
//	}
//	meta, e := es.GetMetadata(name, version)
//	if e != nil{
//		log.Println(e)
//		w.WriteHeader(http.StatusInternalServerError)
//		return
//	}
//	if meta.Hash == ""{
//		w.WriteHeader(http.StatusNotFound)
//		return
//	}
//	object := url.PathEscape(meta.Hash)
//	stream, e := getStream(object)
//	if e != nil {
//		log.Println(e)
//		w.WriteHeader(http.StatusNotFound)
//		return
//	}
//	io.Copy(w, stream)
//}

func get(w http.ResponseWriter, r *http.Request){
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	versionId := r.URL.Query()["version"]
	version := 0
	var e error
	if len(versionId) != 0 {
		version, e = strconv.Atoi(versionId[0])
		if e != nil {
			log.Println(e)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	meta, e := es.GetMetadata(name, version)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if meta.Hash == ""{
		w.WriteHeader(http.StatusNotFound)
		return
	}
	hash := url.PathEscape(meta.Hash)
	stream, e := GetStream(hash, meta.Size)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	offset := utils.GetOffsetFromHeader(r.Header)
	if offset != 0{
		stream.Seek(offset, io.SeekCurrent)
		w.Header().Set("content-range", fmt.Sprintf("bytes %d-%d/%d",
			offset, meta.Size-1, meta.Size))
		w.WriteHeader(http.StatusPartialContent)
	}
	// 数据冗余及恢复版本
	//_, e = io.Copy(w, stream)
	//if e != nil {
	//	log.Println(e)
	//	w.WriteHeader(http.StatusNotFound)
	//	return
	//}
	io.Copy(w, stream)
	stream.Close()
}

// 数据去重版本
//func getStream(object string)(io.Reader, error){
//	server := locate.Locate(object)
//	if server == ""{
//		return nil, fmt.Errorf("object %s locate fail", object)
//	}
//	return objectstream.NewGetStream(server, object)
//}
func GetStream(hash string, size int64)(*rs.RSGetStream, error){
	locateInfo := locate.Locate(hash)
	if len(locateInfo) < rs.DATA_SHARDS {
		return nil, fmt.Errorf("object %s locate fail, result%v", hash, locateInfo)
	}
	dataServers := make([]string, 0)
	if len(locateInfo) != rs.ALL_SHARDS {
		dataServers = heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS- len(locateInfo), locateInfo)
	}
	return rs.NewRSGetStream(locateInfo, dataServers, hash, size)
}

func del(w http.ResponseWriter, r *http.Request){
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	version, e := es.SearchLatestVersion(name)
	if e != nil{
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	e = es.PutMetadata(name, version.Version+1, 0, "")
	if e != nil{
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func post(w http.ResponseWriter, r *http.Request) {
	name := strings.Split(r.URL.EscapedPath(), "/")[2]
	size, e := strconv.ParseInt(r.Header.Get("size"), 0, 64)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	hash := utils.GetHashFromHeader(r.Header)
	if hash == ""{
		log.Println("missing object hash in digest header")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if locate.Exist(url.PathEscape(hash)){
		e = es.AddVersion(name, hash, size)
		if e != nil {
			log.Println(e)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		return
	}
	ds := heartbeat.ChooseRandomDataServers(rs.ALL_SHARDS, nil)
	if len(ds) != rs.ALL_SHARDS {
		log.Println("cannot find enough dataServer")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	stream, e := rs.NewRSResumablePutStream(ds, name, url.PathEscape(hash), size)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("location", "/temp/" + url.PathEscape(stream.ToToken()))
	w.WriteHeader(http.StatusCreated)
}

