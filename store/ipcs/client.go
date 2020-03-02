package store_ipcs

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/arkgo/asset/util"
	. "github.com/arkgo/base"
)

type (
	ipcsClient struct {
		Cluster string
	}
	ipcsPinOpt struct {
		RFMin, RFMax int
		Name         string
		Metadata     Map
	}
)

func (opt *ipcsPinOpt) Query() string {
	qs := url.Values{}
	if opt.RFMin > 0 && opt.RFMax > 0 {
		qs.Set("replication-min", fmt.Sprintf("%d", opt.RFMin))
		qs.Set("replication-max", fmt.Sprintf("%d", opt.RFMax))
	}
	if opt.Name != "" {
		qs.Set("name", opt.Name)
	}

	//qs.Set("shard-size", fmt.Sprintf("%d", po.ShardSize))
	//qs.Set("user-allocations", strings.Join(PeersToStrings(po.UserAllocations), ","))
	//if !po.ExpireAt.IsZero() {
	//	v, err := po.ExpireAt.MarshalText()
	//	if err != nil {
	//		return "", err
	//	}
	//	q.Set("expire-at", string(v))
	//}

	for k, v := range opt.Metadata {
		if k == "" {
			continue
		}
		qs.Set(fmt.Sprintf("meta-%s", k), fmt.Sprintf("%s", v))
	}
	//if po.PinUpdate != cid.Undef {
	//	q.Set("pin-update", po.PinUpdate.String())
	//}

	return qs.Encode()
}

func (client *ipcsClient) Pin(hash string, opt *ipcsPinOpt) (Map, error) {

	url := fmt.Sprintf("%s/pins/%s", client.Cluster, hash)
	query := ""
	if opt != nil {
		query = opt.Query()
	}

	res := util.HttpPostJson(url+"?"+query, "application/x-www-form-urlencoded", nil)
	if res == nil {
		return nil, errors.New("http error")
	}

	return res, nil
}

func (client *ipcsClient) Unpin(hash string) (Map, error) {
	url := fmt.Sprintf("%s/pins/%s", client.Cluster, hash)
	res := util.HttpDeleteJson(url)
	if res == nil {
		return nil, errors.New("http error")
	}

	return res, nil
}
