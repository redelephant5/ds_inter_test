package objectstream

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type PutStream struct {
	writer *io.PipeWriter
	c chan error
}

type GetStream struct {
	reader io.Reader
}

type TempPutStream struct {
	Server string
	Uuid string
}

func NewTempGetStream(server, uuid string) (*GetStream, error) {
	return newGetStream("http://" + server + "/temp/" + uuid)
}

func NewPutStream(server, object string) *PutStream {
	reader, writer := io.Pipe()
	c := make(chan error)
	go func() {
		request, _ := http.NewRequest("PUT", "http://"+ server + "/objects/" + object, reader)
		client := http.Client{}
		r, err := client.Do(request)
		if err == nil && r.StatusCode != http.StatusOK {
			err = fmt.Errorf("dataServer return http code%d", r.StatusCode)
		}
		c <- err
	}()
	return &PutStream{writer, c}
}

func (w *PutStream) Write(p []byte)(n int, err error) {
	return w.writer.Write(p)
}

func (w *PutStream) Close() error {
	w.writer.Close()
	return <- w.c
}

func newGetStream(url string)(*GetStream, error){
	r, e := http.Get(url)
	if e != nil {
		return nil, e
	}
	if r.StatusCode != http.StatusOK{
		return nil, fmt.Errorf("dataServer reeturn http code %d", r.StatusCode)
	}
	return &GetStream{r.Body}, nil
}

func NewGetStream(server, object string)(*GetStream, error){
	if server == "" || object == ""{
		return nil, fmt.Errorf("invalid server %s object %s", server, object)
	}
	return newGetStream("http://" + server + "/objects/" + object)
}

func (r *GetStream) Read(p []byte)(n int, err error){
	return r.reader.Read(p)
}

func NewTempPutStream(server, hash string, size int64) (*TempPutStream, error){
	request , e := http.NewRequest("POST", "http://"+server+"/temp/"+hash, nil)
	if e != nil{
		return nil, e
	}
	request.Header.Set("size", fmt.Sprintf("%d", size))
	client := http.Client{}
	response, e := client.Do(request)
	if e != nil{
		return nil, e
	}
	if response.StatusCode != http.StatusOK{
		return nil, fmt.Errorf(response.Status)
	}
	uuid, e := ioutil.ReadAll(response.Body)
	if e != nil {
		return nil, e
	}
	return &TempPutStream{server, string(uuid)}, nil
}

func (w *TempPutStream) Write(p []byte)(n int, err error){
	request, e := http.NewRequest("PATCH", "http://"+w.Server+"/temp/"+w.Uuid, strings.NewReader(string(p)))
	if e != nil {
		return 0, e
	}
	client := http.Client{}
	r, e := client.Do(request)
	if e != nil {
		return 0, e
	}
	if r.StatusCode != http.StatusOK{
		return 0, fmt.Errorf("dataServer return http code: %d",r.StatusCode)
	}
	return len(p), nil
}

func (w *TempPutStream) Commit(good bool){
	method := "DELETE"
	if good{
		method = "PUT"
	}
	request, _ := http.NewRequest(method, "http://"+w.Server+"/temp/"+w.Uuid, nil)
	client := http.Client{}
	client.Do(request)
}
