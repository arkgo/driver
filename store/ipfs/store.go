package store_ipfs

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	. "github.com/arkgo/base"

	"github.com/arkgo/ark"
	ipfs "github.com/ipfs/go-ipfs-api"
)

//-------------------- ipfsStoreBase begin -------------------------

type (
	ipfsStoreDriver  struct{}
	ipfsStoreConnect struct {
		mutex   sync.RWMutex
		actives int64

		name    string
		config  ark.StoreConfig
		setting ipfsStoreSetting

		shell *ipfs.Shell
	}
	ipfsStoreSetting struct {
		Server  string
		Gateway string
	}
)

//连接
func (driver *ipfsStoreDriver) Connect(name string, config ark.StoreConfig) (ark.StoreConnect, error) {

	setting := ipfsStoreSetting{
		Server: "http://localhost:5001", Gateway: "http://127.0.0.1:8080",
	}
	if vv, ok := config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := config.Setting["gateway"].(string); ok && vv != "" {
		setting.Gateway = vv
	}

	if false == strings.HasPrefix(setting.Server, "http") {
		setting.Server = "http://" + setting.Server
	}
	if false == strings.HasPrefix(setting.Gateway, "http") {
		setting.Gateway = "http://" + setting.Gateway
	}

	if config.Cache == "" {
		config.Cache = os.TempDir()
	} else {
		if _, err := os.Stat(config.Cache); err != nil {
			os.MkdirAll(config.Cache, 0777)
		}
	}

	return &ipfsStoreConnect{
		actives: int64(0),
		name:    name, config: config, setting: setting,
	}, nil

}

//打开连接
func (connect *ipfsStoreConnect) Open() error {
	connect.shell = ipfs.NewShell(connect.setting.Server)
	return nil
}

func (connect *ipfsStoreConnect) Health() (ark.StoreHealth, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return ark.StoreHealth{Workload: connect.actives}, nil
}

//关闭连接
func (connect *ipfsStoreConnect) Close() error {
	return nil
}

func (connect *ipfsStoreConnect) Upload(target string, metadata Map) (ark.File, ark.Files, error) {
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

		//目录
		dir := ark.Filing(connect.name, stat.Name(), cid, stat.Size())
		//file := ark.NewFile(dirCode, stat.Name(), stat.Size())

		files := ark.Files{}
		for _, link := range obj.Links {
			files = append(files, ark.Filing(connect.name, link.Name, link.Hash, int64(link.Size)))
		}

		return dir, files, nil

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

		//kJ1qlvpChiYHgqjzlq5JIQIUIHCiG4GVIhYxmBSWhZfzEB16MaE6mRGvKhIXhJATjpvZKfpWEPdWEXhv
		//QmSJf8rnWVUmU2VDDoUMtxzqSt84pztczztpeEsQqkbL7ek$J1qlvpC2H4ydX4ZEH0=

		//name := path.Base(target)
		//size := stat.Size()
		//code := ark.Encoding(connect.name, name, hash, size)
		//file := ark.NewFile(code, name, stat.Size())

		//code := ark.Encoding(connect.name, "ad.mp4", "", stat.Size())
		//ark.Debug("short", hash, code)

		ffff := ark.Filing(connect.name, path.Base(target), hash, stat.Size())

		return ffff, nil, nil
	}
}

func (connect *ipfsStoreConnect) Download(file ark.File) (string, error) {
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

func (connect *ipfsStoreConnect) Remove(file ark.File) error {
	return connect.shell.Unpin(file.Hash())
}

func (connect *ipfsStoreConnect) Browse(file ark.File, name string, expiries ...time.Duration) (string, error) {
	return fmt.Sprintf("%s/ipfs/%s", connect.setting.Gateway, file.Hash()), nil
}

func (connect *ipfsStoreConnect) Preview(file ark.File, w, h, t int64, expiries ...time.Duration) (string, error) {
	return fmt.Sprintf("%s/ipfs/%s", connect.setting.Gateway, file.Hash()), nil
}

//-------------------- ipfsStoreBase end -------------------------
