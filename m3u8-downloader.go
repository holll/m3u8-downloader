// @author:llychao<lychao_vip@163.com>
// @contributor: Junyi<me@junyi.pw>
// @date:2020-02-18
// @功能:golang m3u8 video Downloader
package main

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/levigross/grequests"
	"golang.org/x/net/websocket"
)

const (
	// HEAD_TIMEOUT 请求头超时时间
	HEAD_TIMEOUT = 5 * time.Second
	// PROGRESS_WIDTH 进度条长度
	PROGRESS_WIDTH = 20
	// TS_NAME_TEMPLATE ts视频片段命名规则
	TS_NAME_TEMPLATE = "%05d.ts"
)

var (
	// 命令行参数
	urlFlag       = flag.String("u", "", "m3u8下载地址(http(s)://url/xx/xx/index.m3u8)")
	nFlag         = flag.Int("n", 24, "num:下载线程数(默认24)")
	jFlag         = flag.Int("j", 1, "jobNum:并行下载任务数(默认1, 仅API模式生效)")
	htFlag        = flag.String("ht", "v1", "hostType:设置getHost的方式(v1: `http(s):// + url.Host + filepath.Dir(url.Path)`; v2: `http(s)://+ u.Host`")
	oFlag         = flag.String("o", "movie", "movieName:自定义文件名(默认为movie)不带后缀")
	cFlag         = flag.String("c", "", "cookie:自定义请求cookie")
	rFlag         = flag.Bool("r", true, "autoClear:是否自动清除ts文件")
	sFlag         = flag.Int("s", 0, "InsecureSkipVerify:是否允许不安全的请求(默认0)")
	spFlag        = flag.String("sp", "", "savePath:文件保存的绝对路径(默认为当前路径,建议默认值)")
	apiFlag       = flag.String("api-listen", "", "apiListen:aria2风格JSON-RPC地址(例如 :6800)")
	rpcSecretFlag = flag.String("rpc-secret", "", "rpcSecret:aria2 rpc鉴权密钥(API模式必填)")

	logger *log.Logger
	ro     = grequests.RequestOptions{
		UserAgent:      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
		RequestTimeout: HEAD_TIMEOUT,
		Headers: map[string]string{
			"Connection":      "keep-alive",
			"Accept":          "*/*",
			"Accept-Encoding": "*",
			"Accept-Language": "zh-CN,zh;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
		},
	}
)

type DownloadJob struct {
	M3U8URL       string
	MaxGoroutines int
	HostType      string
	MovieName     string
	OutputName    string
	Cookie        string
	AutoClear     bool
	Insecure      int
	SavePath      string
}

type TaskStatus struct {
	GID               string `json:"gid"`
	Status            string `json:"status"`
	Result            string `json:"result,omitempty"`
	Error             string `json:"error,omitempty"`
	TotalSegments     int    `json:"totalSegments,omitempty"`
	CompletedSegments int    `json:"completedSegments,omitempty"`
}

type DownloadManager struct {
	limiter   chan struct{}
	mu        sync.RWMutex
	tasks     map[string]*TaskStatus
	rpcSecret string
}

// TsInfo 用于保存 ts 文件的下载地址和文件名
type TsInfo struct {
	Name string
	Url  string
}

type ProgressFunc func(done, total int)

func init() {
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	Run()
}

func Run() {
	msgTpl := "[功能]:多线程下载直播流m3u8视屏\n[提醒]:下载失败，请使用 -ht=v2 \n[提醒]:下载失败，m3u8 地址可能存在嵌套\n[提醒]:进度条中途下载失败，可重复执行"
	fmt.Println(msgTpl)
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if *apiFlag != "" {
		if *rpcSecretFlag == "" {
			fmt.Println("[Failed] API模式下必须设置 -rpc-secret")
			return
		}
		runAPIServer(*apiFlag, *jFlag, *rpcSecretFlag)
		return
	}
	job := DownloadJob{
		M3U8URL:       *urlFlag,
		MaxGoroutines: *nFlag,
		HostType:      *htFlag,
		MovieName:     *oFlag,
		OutputName:    *oFlag + ".mp4",
		AutoClear:     *rFlag,
		Cookie:        *cFlag,
		Insecure:      *sFlag,
		SavePath:      *spFlag,
	}
	mv, err := runDownload(job, nil)
	if err != nil {
		fmt.Printf("\n[Failed] %v\n", err)
		return
	}
	DrawProgressBar("Merging", float32(1), PROGRESS_WIDTH, mv)
}

func runDownload(job DownloadJob, onProgress ProgressFunc) (string, error) {
	now := time.Now()
	if !strings.HasPrefix(job.M3U8URL, "http") || job.M3U8URL == "" {
		return "", fmt.Errorf("invalid m3u8 url")
	}
	if job.MaxGoroutines <= 0 {
		job.MaxGoroutines = 1
	}
	if job.HostType == "" {
		job.HostType = "v1"
	}
	if job.MovieName == "" && job.OutputName != "" {
		job.MovieName = strings.TrimSuffix(job.OutputName, filepath.Ext(job.OutputName))
	}
	if job.MovieName == "" {
		job.MovieName = "movie"
	}
	if job.OutputName == "" {
		job.OutputName = job.MovieName + ".mp4"
	}
	pwd, _ := os.Getwd()
	if job.SavePath != "" {
		pwd = job.SavePath
	}
	outputPath := filepath.Join(pwd, job.OutputName)
	tmpDir := filepath.Join(pwd, job.MovieName+".parts")
	if isExist, _ := pathExists(tmpDir); !isExist {
		_ = os.MkdirAll(tmpDir, os.ModePerm)
	}

	ro := ro
	ro.Headers = cloneHeaders(ro.Headers)
	ro.Headers["Referer"] = getHost(job.M3U8URL, "v2")
	if job.Insecure != 0 {
		ro.InsecureSkipVerify = true
	}
	if job.Cookie != "" {
		ro.Headers["Cookie"] = job.Cookie
	}

	m3u8Host := getHost(job.M3U8URL, job.HostType)
	m3u8Body := getM3u8Body(job.M3U8URL, &ro)
	tsKey := getM3u8Key(m3u8Host, m3u8Body, &ro)
	if tsKey != "" {
		fmt.Printf("待解密 ts 文件 key : %s \n", tsKey)
	}
	tsList := getTsList(m3u8Host, m3u8Body)
	if len(tsList) == 0 {
		return "", fmt.Errorf("m3u8中未解析到ts分片")
	}
	fmt.Println("待下载 ts 文件数量:", len(tsList))
	if onProgress != nil {
		onProgress(0, len(tsList))
	}

	okCount := downloader(tsList, job.MaxGoroutines, tmpDir, tsKey, &ro, onProgress)
	if okCount != len(tsList) {
		return "", fmt.Errorf("ts下载不完整: %d/%d", okCount, len(tsList))
	}
	mv, err := mergeTs(tmpDir, outputPath, tsList)
	if err != nil {
		return "", err
	}
	if job.AutoClear {
		_ = os.RemoveAll(tmpDir)
	}
	fmt.Printf("\n[Success] 下载保存路径：%s | 共耗时: %6.2fs\n", mv, time.Now().Sub(now).Seconds())
	return mv, nil
}

func cloneHeaders(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func runAPIServer(addr string, maxJobs int, rpcSecret string) {
	if maxJobs <= 0 {
		maxJobs = 1
	}
	manager := &DownloadManager{
		limiter:   make(chan struct{}, maxJobs),
		tasks:     map[string]*TaskStatus{},
		rpcSecret: rpcSecret,
	}
	http.HandleFunc("/jsonrpc", manager.handleJSONRPC)
	http.HandleFunc("/rpc", manager.handleJSONRPC)
	fmt.Printf("[API] json-rpc listening on %s, max parallel jobs: %d\n", addr, maxJobs)
	checkErr(http.ListenAndServe(addr, nil))
}

func (m *DownloadManager) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if isWebSocketRequest(r) {
		websocket.Handler(m.handleJSONRPCWebSocket).ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": nil, "error": map[string]interface{}{"code": -32700, "message": "invalid body"}}))
		return
	}
	resp := m.processRPCBody(body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(resp)
}

func (m *DownloadManager) handleJSONRPCWebSocket(conn *websocket.Conn) {
	defer conn.Close()
	for {
		var message []byte
		if err := websocket.Message.Receive(conn, &message); err != nil {
			return
		}
		resp := m.processRPCBody(message)
		if err := websocket.Message.Send(conn, string(resp)); err != nil {
			return
		}
	}
}

func isWebSocketRequest(r *http.Request) bool {
	upgrade := strings.ToLower(r.Header.Get("Upgrade"))
	connection := strings.ToLower(r.Header.Get("Connection"))
	return upgrade == "websocket" && strings.Contains(connection, "upgrade")
}

func (m *DownloadManager) processRPCBody(body []byte) []byte {
	var req struct {
		JSONRPC string            `json:"jsonrpc"`
		ID      interface{}       `json:"id"`
		Method  string            `json:"method"`
		Params  []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": nil, "error": map[string]interface{}{"code": -32700, "message": "parse error"}})
	}
	params, rpcErr := m.authorize(req.Method, req.Params)
	if rpcErr != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "error": map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
	}
	result, rpcErr := m.dispatch(req.Method, params)
	if rpcErr != nil {
		return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "error": map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
	}
	return marshalRPCResponse(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func (m *DownloadManager) authorize(method string, params []json.RawMessage) ([]json.RawMessage, *RPCError) {
	if m.rpcSecret == "" {
		return params, nil
	}
	if len(params) == 0 {
		return nil, &RPCError{Code: 1, Message: "Unauthorized"}
	}
	var token string
	if err := json.Unmarshal(params[0], &token); err == nil && token == "token:"+m.rpcSecret {
		return params[1:], nil
	}
	return nil, &RPCError{Code: 1, Message: "Unauthorized"}
}

func marshalRPCResponse(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		fallback := []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error"}}`)
		return fallback
	}
	return b
}

type RPCError struct {
	Code    int
	Message string
}

func (m *DownloadManager) dispatch(method string, params []json.RawMessage) (interface{}, *RPCError) {
	switch method {
	case "aria2.addUri":
		return m.rpcAddURI(params)
	case "aria2.tellStatus":
		return m.rpcTellStatus(params)
	case "aria2.tellActive":
		return m.rpcTellActive(), nil
	case "aria2.tellWaiting":
		return m.rpcTellByStatus(params, "waiting")
	case "aria2.tellStopped":
		return m.rpcTellByStatus(params, "stopped")
	case "aria2.getGlobalStat":
		return m.rpcGlobalStat(), nil
	case "aria2.getGlobalOption":
		return map[string]string{"max-concurrent-downloads": strconv.Itoa(cap(m.limiter))}, nil
	case "aria2.getVersion":
		return map[string]interface{}{"version": "1.0.0", "enabledFeatures": []string{"JSON-RPC", "m3u8"}}, nil
	case "system.multicall":
		return m.rpcMultiCall(params)
	default:
		return nil, &RPCError{Code: -32601, Message: "method not found"}
	}
}

func (m *DownloadManager) rpcAddURI(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var uris []string
	if err := json.Unmarshal(params[0], &uris); err != nil || len(uris) == 0 {
		return nil, &RPCError{Code: -32602, Message: "invalid uri list"}
	}
	job := DownloadJob{M3U8URL: uris[0], MaxGoroutines: *nFlag, HostType: *htFlag, MovieName: *oFlag, AutoClear: *rFlag, Cookie: *cFlag, Insecure: *sFlag, SavePath: *spFlag}
	if len(params) > 1 {
		var opts map[string]interface{}
		if err := json.Unmarshal(params[1], &opts); err == nil {
			applyJobOptions(&job, opts)
		}
	}
	gid := newGID()
	m.mu.Lock()
	m.tasks[gid] = &TaskStatus{GID: gid, Status: "waiting", Result: expectedOutputPath(job), Error: ""}
	m.mu.Unlock()
	go m.runTask(gid, job)
	return gid, nil
}

func (m *DownloadManager) rpcTellStatus(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var gid string
	if err := json.Unmarshal(params[0], &gid); err != nil || gid == "" {
		return nil, &RPCError{Code: -32602, Message: "invalid gid"}
	}
	m.mu.RLock()
	task, ok := m.tasks[gid]
	m.mu.RUnlock()
	if !ok {
		return nil, &RPCError{Code: 1, Message: "gid not found"}
	}
	return toAria2Status(task), nil
}

func (m *DownloadManager) rpcTellActive() []map[string]interface{} {
	return m.collectByStatus("active")
}

func (m *DownloadManager) rpcTellByStatus(params []json.RawMessage, target string) (interface{}, *RPCError) {
	if len(params) < 2 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	return m.collectByStatus(target), nil
}

func (m *DownloadManager) collectByStatus(target string) []map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]map[string]interface{}, 0)
	for _, t := range m.tasks {
		if target == "stopped" {
			if t.Status == "complete" || t.Status == "error" {
				res = append(res, toAria2Status(t))
			}
			continue
		}
		if t.Status == target {
			res = append(res, toAria2Status(t))
		}
	}
	return res
}

func (m *DownloadManager) rpcGlobalStat() map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var active, waiting, stopped int
	for _, t := range m.tasks {
		switch t.Status {
		case "active":
			active++
		case "waiting":
			waiting++
		case "complete", "error":
			stopped++
		}
	}
	return map[string]string{
		"downloadSpeed": "0",
		"uploadSpeed":   "0",
		"numActive":     strconv.Itoa(active),
		"numWaiting":    strconv.Itoa(waiting),
		"numStopped":    strconv.Itoa(stopped),
	}
}

func (m *DownloadManager) rpcMultiCall(params []json.RawMessage) (interface{}, *RPCError) {
	if len(params) < 1 {
		return nil, &RPCError{Code: -32602, Message: "invalid params"}
	}
	var calls []struct {
		MethodName string            `json:"methodName"`
		Params     []json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(params[0], &calls); err != nil {
		return nil, &RPCError{Code: -32602, Message: "invalid multicall params"}
	}
	results := make([][]interface{}, 0, len(calls))
	for _, c := range calls {
		ret, rpcErr := m.dispatch(c.MethodName, c.Params)
		if rpcErr != nil {
			results = append(results, []interface{}{map[string]interface{}{"code": rpcErr.Code, "message": rpcErr.Message}})
			continue
		}
		results = append(results, []interface{}{ret})
	}
	return results, nil
}

func (m *DownloadManager) runTask(gid string, job DownloadJob) {
	m.limiter <- struct{}{}
	m.updateTask(gid, "active", "", "")
	result, err := runDownload(job, func(done, total int) {
		m.updateTaskProgress(gid, done, total)
	})
	if err != nil {
		m.updateTask(gid, "error", "", err.Error())
		<-m.limiter
		return
	}
	m.updateTask(gid, "complete", result, "")
	<-m.limiter
}

func (m *DownloadManager) updateTask(gid, status, result, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[gid]; ok {
		task.Status = status
		task.Result = result
		task.Error = errMsg
	}
}

func (m *DownloadManager) updateTaskProgress(gid string, done, total int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[gid]; ok {
		if total >= 0 {
			task.TotalSegments = total
		}
		if done >= 0 {
			task.CompletedSegments = done
		}
	}
}

func toAria2Status(task *TaskStatus) map[string]interface{} {
	status := task.Status
	if status == "complete" || status == "error" {
		status = "complete"
		if task.Error != "" {
			status = "error"
		}
	}
	filePath := task.Result
	if filePath == "" {
		filePath = task.GID + ".mp4"
	}
	errorCode := ""
	if task.Error != "" {
		errorCode = "1"
	}
	return map[string]interface{}{
		"gid":             task.GID,
		"status":          status,
		"totalLength":     strconv.Itoa(task.TotalSegments),
		"completedLength": strconv.Itoa(task.CompletedSegments),
		"downloadSpeed":   "0",
		"errorCode":       errorCode,
		"errorMessage":    task.Error,
		"files": []map[string]interface{}{
			{"index": "1", "path": filePath, "uris": []map[string]string{}},
		},
	}
}

func applyJobOptions(job *DownloadJob, options map[string]interface{}) {
	if out, ok := options["out"].(string); ok && out != "" {
		job.OutputName = out
		job.MovieName = strings.TrimSuffix(out, filepath.Ext(out))
	}
	if dir, ok := options["dir"].(string); ok {
		job.SavePath = dir
	}
	if header, ok := options["header"].(string); ok {
		job.Cookie = header
	}
	if split, ok := options["split"].(float64); ok && int(split) > 0 {
		job.MaxGoroutines = int(split)
	}
	if split, ok := options["split"].(string); ok {
		if n, err := strconv.Atoi(split); err == nil && n > 0 {
			job.MaxGoroutines = n
		}
	}
}

func expectedOutputPath(job DownloadJob) string {
	pwd, _ := os.Getwd()
	if job.SavePath != "" {
		pwd = job.SavePath
	}
	out := job.OutputName
	if out == "" {
		name := job.MovieName
		if name == "" {
			name = "movie"
		}
		out = name + ".mp4"
	}
	return filepath.Join(pwd, out)
}

func newGID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano()+rand.Int63())
}

// 获取m3u8地址的host
func getHost(Url, ht string) (host string) {
	u, err := url.Parse(Url)
	checkErr(err)
	switch ht {
	case "v1":
		host = u.Scheme + "://" + u.Host + filepath.Dir(u.EscapedPath())
	case "v2":
		host = u.Scheme + "://" + u.Host
	}
	return
}

// 获取m3u8地址的内容体
func getM3u8Body(Url string, ro *grequests.RequestOptions) string {
	r, err := grequests.Get(Url, ro)
	checkErr(err)
	return r.String()
}

// 获取m3u8加密的密钥
func getM3u8Key(host, html string, ro *grequests.RequestOptions) (key string) {
	lines := strings.Split(html, "\n")
	key = ""
	for _, line := range lines {
		if strings.Contains(line, "#EXT-X-KEY") {
			if !strings.Contains(line, "URI") {
				continue
			}
			fmt.Println("[debug] line_key:", line)
			uri_pos := strings.Index(line, "URI")
			quotation_mark_pos := strings.LastIndex(line, "\"")
			key_url := strings.Split(line[uri_pos:quotation_mark_pos], "\"")[1]
			if !strings.Contains(line, "http") {
				key_url = fmt.Sprintf("%s/%s", host, key_url)
			}
			res, err := grequests.Get(key_url, ro)
			checkErr(err)
			if res.StatusCode == 200 {
				key = res.String()
				break
			}
		}
	}
	fmt.Println("[debug] m3u8Host:", host, "m3u8Key:", key)
	return
}

func getTsList(host, body string) (tsList []TsInfo) {
	lines := strings.Split(body, "\n")
	index := 0
	var ts TsInfo
	for _, line := range lines {
		if !strings.HasPrefix(line, "#") && line != "" {
			//有可能出现的二级嵌套格式的m3u8,请自行转换！
			index++
			if strings.HasPrefix(line, "http") {
				ts = TsInfo{
					Name: fmt.Sprintf(TS_NAME_TEMPLATE, index),
					Url:  line,
				}
				tsList = append(tsList, ts)
			} else {
				line = strings.TrimPrefix(line, "/")
				ts = TsInfo{
					Name: fmt.Sprintf(TS_NAME_TEMPLATE, index),
					Url:  fmt.Sprintf("%s/%s", host, line),
				}
				tsList = append(tsList, ts)
			}
		}
	}
	return
}

func getFromFile() string {
	data, _ := ioutil.ReadFile("./ts.txt")
	return string(data)
}

// 下载ts文件
// @modify: 2020-08-13 修复ts格式SyncByte合并不能播放问题
func downloadTsFile(ts TsInfo, download_dir, key string, retries int, ro *grequests.RequestOptions) bool {
	if retries <= 0 {
		return false
	}
	curr_path_file := fmt.Sprintf("%s/%s", download_dir, ts.Name)
	if isExist, _ := pathExists(curr_path_file); isExist {
		//logger.Println("[warn] File: " + ts.Name + "already exist")
		return true
	}
	res, err := grequests.Get(ts.Url, ro)
	if err != nil || !res.Ok {
		if retries > 0 {
			return downloadTsFile(ts, download_dir, key, retries-1, ro)
		} else {
			//logger.Printf("[warn] File :%s", ts.Url)
			return false
		}
	}
	// 校验长度是否合法
	var origData []byte
	origData = res.Bytes()
	contentLen := 0
	contentLenStr := res.Header.Get("Content-Length")
	if contentLenStr != "" {
		contentLen, _ = strconv.Atoi(contentLenStr)
	}
	if len(origData) == 0 || (contentLen > 0 && len(origData) < contentLen) || res.Error != nil {
		//logger.Println("[warn] File: " + ts.Name + "res origData invalid or err：", res.Error)
		return downloadTsFile(ts, download_dir, key, retries-1, ro)
	}
	// 解密出视频 ts 源文件
	if key != "" {
		//解密 ts 文件，算法：aes 128 cbc pack5
		origData, err = AesDecrypt(origData, []byte(key))
		if err != nil {
			return downloadTsFile(ts, download_dir, key, retries-1, ro)
		}
	}
	// https://en.wikipedia.org/wiki/MPEG_transport_stream
	// Some TS files do not start with SyncByte 0x47, they can not be played after merging,
	// Need to remove the bytes before the SyncByte 0x47(71).
	syncByte := uint8(71) //0x47
	bLen := len(origData)
	for j := 0; j < bLen; j++ {
		if origData[j] == syncByte {
			origData = origData[j:]
			break
		}
	}
	if err := ioutil.WriteFile(curr_path_file, origData, 0666); err != nil {
		return false
	}
	return true
}

// downloader m3u8 下载器
func downloader(tsList []TsInfo, maxGoroutines int, downloadDir string, key string, ro *grequests.RequestOptions, onProgress ProgressFunc) int {
	retry := 5 //单个ts 下载重试次数
	var wg sync.WaitGroup
	limiter := make(chan struct{}, maxGoroutines) //chan struct 内存占用 0 bool 占用 1
	tsLen := len(tsList)
	if tsLen == 0 {
		return 0
	}
	var downloadCount int64
	for _, ts := range tsList {
		wg.Add(1)
		limiter <- struct{}{}
		go func(ts TsInfo, downloadDir, key string, retryies int) {
			defer func() {
				wg.Done()
				<-limiter
			}()
			if downloadTsFile(ts, downloadDir, key, retryies, ro) {
				count := atomic.AddInt64(&downloadCount, 1)
				DrawProgressBar("Downloading", float32(count)/float32(tsLen), PROGRESS_WIDTH, ts.Name)
				if onProgress != nil {
					onProgress(int(count), tsLen)
				}
			}
		}(ts, downloadDir, key, retry)
	}
	wg.Wait()
	return int(downloadCount)
}

// 合并ts文件
func mergeTs(downloadDir, outputPath string, tsList []TsInfo) (string, error) {
	outMv, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer outMv.Close()
	writer := bufio.NewWriter(outMv)
	for _, ts := range tsList {
		path := filepath.Join(downloadDir, ts.Name)
		bytes, err := ioutil.ReadFile(path)
		if err != nil {
			return "", err
		}
		if _, err = writer.Write(bytes); err != nil {
			return "", err
		}
	}
	if err = writer.Flush(); err != nil {
		return "", err
	}
	return outputPath, nil
}

// 进度条
func DrawProgressBar(prefix string, proportion float32, width int, suffix ...string) {
	pos := int(proportion * float32(width))
	s := fmt.Sprintf("[%s] %s%*s %6.2f%% \t%s",
		prefix, strings.Repeat("■", pos), width-pos, "", proportion*100, strings.Join(suffix, ""))
	fmt.Print("\r" + s)
}

// ============================== shell相关 ==============================
// 判断文件是否存在
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 执行 shell
func execUnixShell(s string) {
	cmd := exec.Command("bash", "-c", s)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", out.String())
}

func execWinShell(s string) error {
	cmd := exec.Command("cmd", "/C", s)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return err
	}
	fmt.Printf("%s", out.String())
	return nil
}

// windows 合并文件
func win_merge_file(path string) {
	pwd, _ := os.Getwd()
	os.Chdir(path)
	execWinShell("copy /b *.ts merge.tmp")
	execWinShell("del /Q *.ts")
	os.Rename("merge.tmp", "merge.mp4")
	os.Chdir(pwd)
}

// unix 合并文件
func unix_merge_file(path string) {
	pwd, _ := os.Getwd()
	os.Chdir(path)
	//cmd := `ls  *.ts |sort -t "\." -k 1 -n |awk '{print $0}' |xargs -n 1 -I {} bash -c "cat {} >> new.tmp"`
	cmd := `cat *.ts >> merge.tmp`
	execUnixShell(cmd)
	execUnixShell("rm -rf *.ts")
	os.Rename("merge.tmp", "merge.mp4")
	os.Chdir(pwd)
}

// ============================== 加解密相关 ==============================

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AesEncrypt(origData, key []byte, ivs ...[]byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	var iv []byte
	if len(ivs) == 0 {
		iv = key
	} else {
		iv = ivs[0]
	}
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, iv[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return crypted, nil
}

func AesDecrypt(crypted, key []byte, ivs ...[]byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	var iv []byte
	if len(ivs) == 0 {
		iv = key
	} else {
		iv = ivs[0]
	}
	blockMode := cipher.NewCBCDecrypter(block, iv[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return origData, nil
}

func checkErr(e error) {
	if e != nil {
		logger.Panic(e)
	}
}
