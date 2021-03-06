package conn

import (
	"fmt"
	"reflect"
	"time"

	json "github.com/intel-go/fastjson"
	nats "github.com/nats-io/go-nats"
	. "github.com/sencydai/utils/log"
)

//SysMessage 系统消息
type SysMessage struct {
	MsgID int
	Data  []byte
}

//ResponseMessage rpc消息
type ResponseMessage struct {
	Subject string
	Data    interface{}
}

type emptyResponseData struct {
}

var emptyRespData = &emptyResponseData{}

//MsgHandle 系统消息处理接口
type MsgHandle func(*SysMessage)

//ResponseHandle rpc处理接口
// func(in ...interface{}) interface{} | void
type ResponseHandle interface{}

//NatsServer NatsServer服务器
type NatsServer struct {
	Name        string
	Conn        *nats.EncodedConn
	MsgHandles  map[int]MsgHandle
	RespHandles map[string]ResponseHandle
}

//RegMsgHandle 注册系统消息处理函数
func (ns *NatsServer) RegMsgHandle(msgID int, handle MsgHandle) {
	ns.MsgHandles[msgID] = handle
}

//RegResponseHandle 注册rpc处理函数
func (ns *NatsServer) RegResponseHandle(subject string, handle ResponseHandle) {
	t := reflect.TypeOf(handle) // 获得对象类型,从而知道有多少个参数
	if t.Kind() != reflect.Func {
		log.Errorf("RegResponseHandle: subject(%s) reg not func type", subject)
		return
	}
	ns.RespHandles[subject] = handle
}

func disconnectHandler(conn *nats.Conn) {
	log.Warnf("conn disconnect %v ...", conn.Servers())
}

func reconnectHandler(conn *nats.Conn) {
	log.Infof("conn reconnect %v %d ...", conn.Servers(), conn.Reconnects)
}

var (
	log   = DefaultLogger
	conns = make(map[string]*nats.EncodedConn)
)

func SetLog(l ILogger) {
	log = l
}

//RegConnection 新的连接
func RegConnection(typeName, url string) bool {
	conn, err := nats.Connect(url,
		nats.Name(typeName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Microsecond*500),
		nats.DisconnectHandler(disconnectHandler),
		nats.ReconnectHandler(reconnectHandler))
	if err != nil {
		return false
	}

	econn, _ := nats.NewEncodedConn(conn, nats.JSON_ENCODER)
	conns[typeName] = econn

	log.Infof("RegConnection %s %s success", typeName, url)

	return true
}

//GetConnection 返回连接
func GetConnection(typeName string) (*nats.EncodedConn, bool) {
	conn, ok := conns[typeName]
	return conn, ok
}

//Close 关闭所有连接
func Close() {
	for _, conn := range conns {
		conn.FlushTimeout(time.Second * 5)
		conn.Close()
	}
}

//InitNatsServer 初始化NatsServer，绑定Subscribe
func InitNatsServer(server *NatsServer, typeName string) bool {
	if server.Conn != nil {
		return false
	}
	econn, ok := conns[typeName]
	if !ok {
		return false
	}

	server.Conn = econn

	//处理事件
	msgChan := make(chan *SysMessage, 100)
	server.Conn.BindRecvChan(fmt.Sprintf("%s.msg", server.Name), msgChan)
	go func(ch chan *SysMessage) {
		handle := func(fun MsgHandle, msg *SysMessage) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("%s handle msg %d error: %v", server.Name, msg.MsgID, err)
				}
			}()
			fun(msg)
		}

		for msg := range ch {
			if fun, ok := server.MsgHandles[msg.MsgID]; ok {
				go handle(fun, msg)
			}
		}
	}(msgChan)

	//处理回调
	server.Conn.Subscribe(fmt.Sprintf("%s.response", server.Name), func(_, reply string, msg *ResponseMessage) {
		go func(msg *ResponseMessage) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("%s handle response %s error: %v", server.Name, msg.Subject, err)
				}
			}()

			cb, ok := server.RespHandles[msg.Subject]
			if !ok {
				return
			}
			cbType := reflect.TypeOf(cb)
			cbValue := reflect.ValueOf(cb)

			var oV []reflect.Value
			if cbType.NumIn() > 0 {
				data := msg.Data.([]interface{})[0].([]interface{})
				oV = make([]reflect.Value, len(data))
				var argType reflect.Type
				for i, v := range data {
					argType = cbType.In(i)
					var oPtr reflect.Value
					if argType.Kind() != reflect.Ptr {
						oPtr = reflect.New(argType)
					} else {
						oPtr = reflect.New(argType.Elem())
					}
					d, _ := json.Marshal(v)
					json.Unmarshal(d, oPtr.Interface())
					if argType.Kind() != reflect.Ptr {
						oPtr = reflect.Indirect(oPtr)
					}
					oV[i] = oPtr
				}
			}
			ret := cbValue.Call(oV)
			if reply != "" {
				if len(ret) == 0 {
					server.Conn.Publish(reply, nil)
				} else {
					value := ret[0]
					server.Conn.Publish(reply, value.Interface())
				}
			}
		}(msg)
	})

	log.Infof("register nats server %s %s success", typeName, server.Name)

	return true
}

//Publish Publish系统消息
func Publish(typeName, serverName string, msgID int, data []byte) {
	con, ok := GetConnection(typeName)
	if !ok {
		log.Warnf("nats %s not connect", typeName)
		return
	}
	msg := &SysMessage{MsgID: msgID, Data: data}
	con.Publish(fmt.Sprintf("%s.msg", serverName), msg)
}

//Call 同步调用rpc
func Call(typeName, serverName string, subject string, out interface{}, data ...interface{}) bool {
	con, ok := GetConnection(typeName)
	if !ok {
		log.Warnf("nats %s not connect", typeName)
		return false
	}
	if out == nil {
		out = emptyRespData
	}
	err := con.Request(fmt.Sprintf("%s.response", serverName), &ResponseMessage{Subject: subject, Data: data}, out, time.Second*10)
	if err != nil {
		log.Errorf("call %s subject error: %s", subject, err.Error())
		return false
	}
	return true
}

//Cast 异步调用rpc
func Cast(typeName, serverName string, subject string, data ...interface{}) {
	con, ok := GetConnection(typeName)
	if !ok {
		log.Warnf("nats %s not connect", typeName)
		return
	}
	con.Publish(fmt.Sprintf("%s.response", serverName), &ResponseMessage{Subject: subject, Data: data})
}
