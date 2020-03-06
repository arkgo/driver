package data_postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/arkgo/ark"
	. "github.com/arkgo/base"

	"strconv"
	"strings"
	"time"
)

type (
	postgresTrigger struct {
		Name  string
		Value Map
	}
	PostgresBase struct {
		connect *PostgresConnect

		name   string
		schema string

		tx *sql.Tx
		// cache ark.CacheBase

		//是否手动提交事务，否则为自动
		//当调用begin时， 自动变成手动提交事务
		//triggers保存待提交的触发器，手动下有效
		manual   bool
		triggers []postgresTrigger

		lastError error
	}
)

//记录触发器
func (base *PostgresBase) trigger(name string, values ...Map) {
	if base.manual {
		//手动时保存触发器
		value := Map{}
		if len(values) > 0 {
			value = values[0]
		}
		base.triggers = append(base.triggers, postgresTrigger{Name: name, Value: value})
	} else {
		//自动时，直接触发
		ark.Trigger(name, values...)
	}

}

//查询表，支持多个KEY遍历
func (base *PostgresBase) tableConfig(name string) Map {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := ark.Table(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *PostgresBase) viewConfig(name string) Map {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := ark.View(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *PostgresBase) modelConfig(name string) Map {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := ark.Model(key); cfg != nil {
			return cfg
		}
	}

	return nil
}

func (base *PostgresBase) errorHandler(key string, err error, args ...Any) {
	if err != nil {
		//出错自动取消事务
		base.Cancel()

		errors := []Any{key, err}
		errors = append(errors, args...)

		base.lastError = err
		ark.Warning(errors...)
	}
}

//关闭数据库
func (base *PostgresBase) Close() error {
	base.connect.mutex.Lock()
	base.connect.actives--
	base.connect.mutex.Unlock()

	//好像目前不需要关闭什么东西
	if base.tx != nil {
		//关闭时候,一定要提交一次事务
		//如果手动提交了, 这里会失败, 问题不大
		//如果没有提交的话, 连接不会交回连接池. 会一直占用
		base.Cancel()
	}

	// if base.cache != nil {
	// 	base.cache.Close()
	// }

	return nil
}
func (base *PostgresBase) Erred() error {
	err := base.lastError
	base.lastError = nil
	return err
}

//ID生成器
func (base *PostgresBase) Serial(key string, start, step int64) int64 {

	exec, err := base.beginTx()
	if err != nil {
		base.errorHandler("data.serial", err, key)
		return 0
	}

	serial := "serial"
	if base.connect.config.Serial != "" {
		serial = base.connect.config.Serial
	} else if vv, ok := base.connect.config.Setting["serial"].(string); ok && vv != "" {
		serial = vv
	}

	if step == 0 {
		step = 1
	}

	//`INSERT INTO %v(key,seq) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET seq=%v.seq+excluded.seq RETURNING seq;`,
	sql := fmt.Sprintf(
		`INSERT INTO %v(key,seq) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET seq=%v.seq+$3 RETURNING seq;`,
		serial, serial,
	)
	args := []Any{key, start, step}
	row := exec.QueryRow(sql, args...)

	seq := int64(0)

	err = row.Scan(&seq)
	if err != nil {
		base.errorHandler("data.serial", err, key)
		return 0
	}

	return seq
}

//获取表对象
func (base *PostgresBase) Table(name string) ark.DataTable {
	if config := base.tableConfig(name); config != nil {
		//模式，表名
		schema, table, key, fields := base.schema, name, "id", Map{}
		if n, ok := config["schema"].(string); ok {
			schema = n
		}
		if n, ok := config["table"].(string); ok {
			table = n
		}
		if n, ok := config["key"].(string); ok {
			key = n
		}
		if n, ok := config["fields"].(Map); ok {
			fields = n
		}

		fff := Map{
			"$count": Map{"type": "int", "must": nil, "name": "统计", "text": "统计"},
		}
		for k, v := range fields {
			fff[k] = v
		}

		table = strings.Replace(table, ".", "_", -1)
		return &PostgresTable{
			PostgresView{base, name, schema, table, key, fff},
		}
	} else {
		panic("[数据]表不存在")
	}
}

//获取模型对象
func (base *PostgresBase) View(name string) ark.DataView {
	if config := base.viewConfig(name); config != nil {

		//模式，表名
		schema, view, key, fields := base.schema, name, "id", Map{}
		if n, ok := config["schema"].(string); ok {
			schema = n
		}
		if n, ok := config["view"].(string); ok {
			view = n
		}
		if n, ok := config["key"].(string); ok {
			key = n
		}
		if n, ok := config["fields"].(Map); ok {
			fields = n
		}

		fff := Map{
			"$count": Map{"type": "int", "must": nil, "name": "统计", "text": "统计"},
		}
		for k, v := range fields {
			fff[k] = v
		}

		view = strings.Replace(view, ".", "_", -1)
		return &PostgresView{
			base, name, schema, view, key, fff,
		}
	} else {
		panic("[数据]视图不存在")
	}
}

//获取模型对象
func (base *PostgresBase) Model(name string) ark.DataModel {
	if config := base.modelConfig(name); config != nil {

		//模式，表名
		schema, model, key, fields := base.schema, name, "id", Map{}
		if n, ok := config["schema"].(string); ok {
			schema = n
		}
		if n, ok := config["model"].(string); ok {
			model = n
		}
		if n, ok := config["key"].(string); ok {
			key = n
		}
		if n, ok := config["fields"].(Map); ok {
			fields = n
		}

		fff := Map{
			"$count": Map{"type": "int", "must": nil, "name": "统计", "text": "统计"},
		}
		for k, v := range fields {
			fff[k] = v
		}

		model = strings.Replace(model, ".", "_", -1)
		return &PostgresModel{
			base, name, schema, model, key, fff,
		}
	} else {
		panic("[数据]模型不存在")
	}
}

//是否开启缓存
// func (base *PostgresBase) Cache(use bool) (DataBase) {
// 	base.caching = use
// 	return base
// }

//开启手动模式
func (base *PostgresBase) Begin() (*sql.Tx, error) {
	base.lastError = nil

	if _, err := base.beginTx(); err != nil {
		return nil, err
	}

	base.manual = true
	return base.tx, nil
}

//注意，此方法为实际开始事务
func (base *PostgresBase) beginTx() (PostgresExecutor, error) {
	if base.manual {
		if base.tx == nil {
			tx, err := base.connect.db.Begin()
			if err != nil {
				return nil, err
			}
			base.tx = tx
		}
		return base.tx, nil
	} else {
		return base.connect.db, nil
	}
}

//此为取消事务
func (base *PostgresBase) endTx() error {
	base.tx = nil
	base.manual = false
	base.triggers = []postgresTrigger{}
	return nil
}

//提交事务
func (base *PostgresBase) Submit() error {
	//不管成功失败，都结束事务
	defer base.endTx()

	if base.tx == nil {
		return errors.New("[数据]无效事务")
	}

	err := base.tx.Commit()
	if err != nil {
		return err
	}

	//提交事务后,要把触发器都发掉
	for _, trigger := range base.triggers {
		ark.Trigger(trigger.Name, trigger.Value)
	}

	return nil
}

//取消事务
func (base *PostgresBase) Cancel() error {
	if base.tx == nil {
		return errors.New("[数据]无效事务")
	}

	err := base.tx.Rollback()
	if err != nil {
		return err
	}

	//提交后,要清掉事务
	base.endTx()

	return nil
}

//创建的时候,也需要对值来处理,
//数组要转成{a,b,c}格式,要不然不支持
//json可能要转成字串才支持
func (base *PostgresBase) packing(value Map) Map {

	newValue := Map{}

	for k, v := range value {
		switch t := v.(type) {
		case []string:
			{
				newValue[k] = fmt.Sprintf(`{%s}`, strings.Join(t, `,`))
			}
		case []bool:
			{
				arr := []string{}
				for _, v := range t {
					if v {
						arr = append(arr, "TRUE")
					} else {
						arr = append(arr, "FALSE")
					}
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, strconv.Itoa(v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int8:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int16:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int32:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int64:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []float32:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []float64:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case Map:
			{
				b, e := json.Marshal(t)
				if e == nil {
					newValue[k] = string(b)
				} else {
					newValue[k] = "{}"
				}
			}
		case []Map:
			{
				//ms := []string{}
				//for _,v := range t {
				//	ms = append(ms, util.ToString(v))
				//}
				//
				//newValue[k] = fmt.Sprintf("{%s}", strings.Join(ms, ","))

				b, e := json.Marshal(t)
				if e == nil {
					newValue[k] = string(b)
				} else {
					newValue[k] = "[]"
				}
			}
		default:
			newValue[k] = t
		}
	}
	return newValue
}

//楼上写入前要打包处理值
//这里当然 读取后也要解包处理
func (base *PostgresBase) unpacking(keys []string, vals []interface{}) Map {

	m := Map{}

	for i, n := range keys {
		switch v := vals[i].(type) {
		case time.Time:
			m[n] = v.Local()
		case string:
			{
				m[n] = v
			}
		case []byte:
			{
				m[n] = string(v)
			}
		default:
			m[n] = v
		}
	}

	return m
}

//把MAP编译成sql查询条件
func (base *PostgresBase) parsing(i int, args ...Any) (string, []interface{}, string, error) {

	sql, val, odr, err := ark.Parse(args...)

	if err != nil {
		return "", nil, "", err
	}

	//结果要处理一下，字段包裹、参数处理
	sql = strings.Replace(sql, DELIMS, `"`, -1)
	odr = strings.Replace(odr, DELIMS, `"`, -1)
	odr = strings.Replace(odr, RANDBY, `RANDOM()`, -1)
	for range val {
		sql = strings.Replace(sql, "?", fmt.Sprintf("$%d", i), 1)
		i++
	}

	return sql, val, odr, nil
}

// //获取relate定义的parents
// func (base *PostgresBase) parents(name string) (Map) {
// 	values := Map{}

// 	if config,ok := base.tables(name); ok {
// 		if fields,ok := config["fields"].(Map); ok {
// 			base.parent(name, fields, []string{}, values)
// 		}
// 	}

// 	return values;
// }

// //获取relate定义的parents
// func (base *PostgresBase) parent(table string, fields Map, tree []string, values Map) {
// 	for k,v := range fields {
// 		config := v.(Map)
// 		trees := append(tree, k)

// 		if config["relate"] != nil {

// 			relates := []Map{}

// 			switch ttts := config["relate"].(type) {
// 			case Map:
// 				relates = append(relates, ttts)
// 			case []Map:
// 				for _,ttt := range ttts {
// 					relates = append(relates, ttt)
// 				}
// 			}

// 			for i,relating := range relates {

// 				//relating := config["relate"].(Map)
// 				parent := relating["parent"].(string)

// 				//要从模型定义中,把所有父表的 schema, table 要拿过来
// 				if tableConfig,ok := base.tables(parent); ok {

// 					schema,table := SCHEMA,parent
// 					if tableConfig["schema"] != nil && tableConfig["schema"] != "" {
// 						schema = tableConfig["schema"].(string)
// 					}
// 					if tableConfig["table"] != nil && tableConfig["table"] != "" {
// 						table = tableConfig["table"].(string)
// 					}

// 					//加入列表，带上i是可能有多个字段引用同一个表？还是引用多个表？
// 					values[fmt.Sprintf("%v:%v", strings.Join(trees, "."), i)] = Map{
// 						"schema": schema, "table": table,
// 						"field": strings.Join(trees, "."), "relate": relating,
// 					}
// 				}
// 			}

// 		} else {
// 			if json,ok := config["json"].(Map); ok {
// 				base.parent(table, json, trees, values)
// 			}
// 		}
// 	}
// }

// //获取relate定义的childs
// func (base *PostgresBase) childs(model string) (Map) {
// 	values := Map{}

// 	for modelName,modelConfig := range base.bonder.tables {

// 		schema,table := SCHEMA,modelName
// 		if modelConfig["schema"] != nil && modelConfig["schema"] != "" {
// 			schema = modelConfig["schema"].(string)
// 		}
// 		if modelConfig["table"] != nil && modelConfig["table"] != "" {
// 			table = modelConfig["table"].(string)
// 		}

// 		if fields,ok := modelConfig["field"].(Map); ok {
// 			base.child(model, modelName, schema, table, fields, []string{ }, values)
// 		}
// 	}

// 	return values;
// }

// //获取relate定义的child
// func (base *PostgresBase) child(parent,model,schema,table string, configs Map, tree []string, values Map) {
// 	for k,v := range configs {
// 		config := v.(Map)
// 		trees := append(tree, k)

// 		if config["relate"] != nil {

// 			relates := []Map{}

// 			switch ttts := config["relate"].(type) {
// 			case Map:
// 				relates = append(relates, ttts)
// 			case []Map:
// 				for _,ttt := range ttts {
// 					relates = append(relates, ttt)
// 				}
// 			}

// 			for i,relating := range relates {

// 				//relating := config["relate"].(Map)

// 				if relating["parent"] == parent {
// 					values[fmt.Sprintf("%v:%v:%v", model, strings.Join(trees, "."), i)] = Map{
// 						"schema": schema, "table": table,
// 						"field": strings.Join(trees, "."), "relate": relating,
// 					}
// 				}
// 			}

// 		} else {
// 			if json,ok := config["json"].(Map); ok {
// 				base.child(parent,model,schema,table,json, trees, values)
// 			}
// 		}
// 	}
// }