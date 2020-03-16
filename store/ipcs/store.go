package store_ipcs

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	. "github.com/arkgo/asset"

	"github.com/arkgo/ark"
	ipfs "github.com/ipfs/go-ipfs-api"
)

//-------------------- ipcsStoreBase begin -------------------------

type (
	ipcsStoreDriver  struct{}
	ipcsStoreConnect struct {
		mutex   sync.RWMutex
		actives int64

		name    string
		config  ark.StoreConfig
		setting ipcsStoreSetting

		client *ipcsClient
		shell  *ipfs.Shell
	}
	ipcsStoreSetting struct {
		Server, Cluster, Gateway string
		RFMin, RFMax             int
	}
)

//连接
func (driver *ipcsStoreDriver) Connect(name string, config ark.StoreConfig) (ark.StoreConnect, error) {

	setting := ipcsStoreSetting{
		Server:  "http://127.0.0.1:9095",
		Cluster: "http://127.0.0.1:9094",
		Gateway: "http://127.0.0.1:8080",
		RFMin:   -1, RFMax: -1,
	}

	if vv, ok := config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if false == strings.HasPrefix(setting.Server, "http") {
		setting.Server = "http://" + setting.Server
	}
	if vv, ok := config.Setting["cluster"].(string); ok && vv != "" {
		setting.Cluster = vv
	}
	if false == strings.HasPrefix(setting.Cluster, "http") {
		setting.Cluster = "http://" + setting.Cluster
	}
	if vv, ok := config.Setting["gateway"].(string); ok && vv != "" {
		setting.Gateway = vv
	}
	if false == strings.HasPrefix(setting.Gateway, "http") {
		setting.Gateway = "http://" + setting.Gateway
	}

	if vv, ok := config.Setting["rfmin"].(int); ok {
		setting.RFMin = vv
	}
	if vv, ok := config.Setting["rfmin"].(int64); ok {
		setting.RFMin = int(vv)
	}
	if vv, ok := config.Setting["rfmin"].(float64); ok {
		setting.RFMin = int(vv)
	}

	if vv, ok := config.Setting["rfmax"].(int); ok {
		setting.RFMax = vv
	}
	if vv, ok := config.Setting["rfmax"].(int64); ok {
		setting.RFMax = int(vv)
	}
	if vv, ok := config.Setting["rfmax"].(float64); ok {
		setting.RFMax = int(vv)
	}

	if config.Cache == "" {
		config.Cache = os.TempDir()
	} else {
		if _, err := os.Stat(config.Cache); err != nil {
			os.MkdirAll(config.Cache, 0777)
		}
	}

	return &ipcsStoreConnect{
		actives: int64(0),
		name:    name, config: config, setting: setting,
	}, nil

}

//打开连接
func (connect *ipcsStoreConnect) Open() error {
	connect.client = &ipcsClient{connect.setting.Cluster}
	connect.shell = ipfs.NewShell(connect.setting.Server)
	return nil
}
func (connect *ipcsStoreConnect) Health() (ark.StoreHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.StoreHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *ipcsStoreConnect) Close() error {
	return nil
}

func (connect *ipcsStoreConnect) Upload(target string, metadata Map) (ark.File, ark.Files, error) {
	stat, err := os.Stat(target)
	if err != nil {
		return nil, nil, err
	}

	//是目录，就整个目录上传
	if stat.IsDir() {

		cid, err := connect.shell.AddDir(target)
		if err != nil {
			return nil, nil, err
		}

		obj, err := connect.shell.ObjectGet(cid)
		if err != nil {
			return nil, nil, err
		}

		//pin住目录
		connect.client.Pin(cid, &ipcsPinOpt{
			RFMin: connect.setting.RFMin, RFMax: connect.setting.RFMax,
			Name: stat.Name(), Metadata: metadata,
		})

		//目录
		dir := ark.Filing(connect.name, cid, stat.Name(), stat.Size())

		files := ark.Files{}
		for _, link := range obj.Links {
			files = append(files, ark.Filing(connect.name, link.Hash, link.Name, int64(link.Size)))

			//pin住文件
			connect.client.Pin(link.Hash, &ipcsPinOpt{
				RFMin: connect.setting.RFMin, RFMax: connect.setting.RFMax,
				Name: link.Name, Metadata: metadata,
			})
		}

		return dir, files, nil

		////目录
		//dirCode := ark.Encoding(connect.name, "", cid, 0)
		//fileCodes := []string{}
		//
		//for _,link := range obj.Links {
		//	fileCode := ark.Encoding(connect.name, link.Name, link.Hash, int64(link.Size))
		//	fileCodes = append(fileCodes, fileCode)
		//
		//	//pin住文件
		//	connect.client.Pin(link.Hash, &ipcsPinOpt{
		//		RFMin: connect.setting.RFMin, RFMax: connect.setting.RFMax,
		//		Name: link.Name, Metadata: metadata,
		//	})
		//}

		//return dirCode, fileCodes

	} else {

		file, err := os.Open(target)
		if err != nil {
			return nil, nil, err
		}
		defer file.Close()

		//stat,err := file.Stat()
		//if err != nil {
		//	connect.lastError = err
		//	return nil, nil
		//}

		hash, err := connect.shell.Add(file)
		if err != nil {
			return nil, nil, err
		}

		ffff := ark.Filing(connect.name, hash, stat.Name(), stat.Size())

		return ffff, nil, nil
	}
}

func (connect *ipcsStoreConnect) Download(file ark.File) (string, error) {
	target := path.Join(connect.config.Cache, file.Hash())
	if file.Type() != "" {
		target += "." + file.Type()
	}

	_, err := os.Stat(target)
	if err == nil {
		return target, nil //无错误，文件已经存在，直接返回
	}

	err = connect.shell.Get(file.Hash(), target)
	if err != nil {
		return "", err
	}

	return target, nil
}

func (connect *ipcsStoreConnect) Remove(file ark.File) error {
	_, err := connect.client.Unpin(file.Hash())
	return err
}

func (connect *ipcsStoreConnect) Browse(file ark.File, name string, expiries ...time.Duration) (string, error) {
	return fmt.Sprintf("%s/ipcs/%s", connect.setting.Gateway, file.Hash()), nil
}

func (connect *ipcsStoreConnect) Preview(file ark.File, w, h, t int64, expiries ...time.Duration) (string, error) {
	return fmt.Sprintf("%s/ipcs/%s", connect.setting.Gateway, file.Hash()), nil
}

//-------------------- ipcsStoreBase end -------------------------
