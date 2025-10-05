package lokiclient

import (
	"bytes"
	"io"
	"net/http"
)

const LogEntriesChanSize = 5000

type Client interface {
	Logf(logLine string, meta map[string]string)
	Shutdown()
}

type httpClient struct {
	parent http.Client
}

func (client *httpClient) sendJsonReq(method, url, ctype string, reqBody []byte) (resp *http.Response, resBody []byte, err error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", ctype)

	resp, err = client.parent.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	resBody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return
}
