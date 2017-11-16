package conn

import (
	"fmt"
	"reflect"
	"time"

	json "github.com/intel-go/fastjson"
	nats "github.com/nats-io/go-nats"
)

//SysMessage 系统消息
type SysMessage struct {
	MsgID int
	Data  []byte
}

//ResponseMessage rpc消息
type ResponseMessage struct {
	Subject string
	Data    []byte
}

type emptyResponseData struct {
}

var emptyRespData = &emptyResponseData{}

//MsgHandle 系统消息处理接口
type MsgHandle func(*SysMessage)

//ResponseHandle rpc处理接口
// func(in object) object 支持0或1个输入参数 和 0或一个返回值 参数可以为指针类型
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
	ns.RespHandles[subject] = handle
}

func disconnectHandler(conn *nats.Conn) {
	fmt.Printf("conn disconnect %v ...\n", conn.Servers())
}

func reconnectHandler(conn *nats.Conn) {
	fmt.Printf("conn reconnect %v %d ...\n", conn.Servers(), conn.Reconnects)
}

func closedHandler(conn *nats.Conn) {
	fmt.Printf("conn close %v ...\n", conn.Servers())
}

var (
	conns = make(map[string]*nats.EncodedConn)
)

//RegConnection 新的连接
func RegConnection(typeName, url string) bool {
	conn, err := nats.Connect(url,
		nats.Name(typeName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Microsecond*500),
		nats.DisconnectHandler(disconnectHandler),
		nats.ReconnectHandler(reconnectHandler),
		nats.ClosedHandler(closedHandler))
	if err != nil {
		return false
	}

	econn, _ := nats.NewEncodedConn(conn, nats.JSON_ENCODER)
	conns[typeName] = econn

	fmt.Printf("RegConnection %s %s success\n", typeName, url)

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

// Dissect the cb Handler's signature
func argInfo(cb ResponseHandle) (reflect.Type, int) {
	cbType := reflect.TypeOf(cb)
	if cbType.Kind() != reflect.Func {
		panic("conn: Handler needs to be a func")
	}
	numArgs := cbType.NumIn()
	if numArgs == 0 {
		return nil, numArgs
	}
	return cbType.In(numArgs - 1), numArgs
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
	msgChan := make(chan *SysMessage, 1000)
	server.Conn.BindRecvChan(fmt.Sprintf("%s.msg", server.Name), msgChan)
	go func(ch chan *SysMessage) {
		handle := func(fun MsgHandle, msg *SysMessage) {
			defer func() {
				if err := recover(); err != nil {
					fmt.Printf("%s handle msg %d error: %v\n", server.Name, msg.MsgID, err)
				}
			}()
			fun(msg)
		}

		for {
			msg := <-ch
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
					fmt.Printf("%s handle response %s error: %v\n", server.Name, msg.Subject, err)
				}
			}()

			cb, ok := server.RespHandles[msg.Subject]
			if !ok {
				return
			}
			argType, numArgs := argInfo(cb)
			cbValue := reflect.ValueOf(cb)

			var oV []reflect.Value
			if numArgs == 0 {
				oV = nil
			} else {
				var oPtr reflect.Value
				if argType.Kind() != reflect.Ptr {
					oPtr = reflect.New(argType)
				} else {
					oPtr = reflect.New(argType.Elem())
				}

				json.Unmarshal(msg.Data, oPtr.Interface())

				if argType.Kind() != reflect.Ptr {
					oPtr = reflect.Indirect(oPtr)
				}

				oV = []reflect.Value{oPtr}
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

	fmt.Printf("register nats server %s %s success\n", typeName, server.Name)

	return true
}

//Publish Publish系统消息
func Publish(typeName, serverName string, msgID int, data []byte) {
	con, ok := GetConnection(typeName)
	if !ok {
		fmt.Printf("nats %s not connect\n", typeName)
		return
	}
	msg := &SysMessage{MsgID: msgID, Data: data}
	con.Publish(fmt.Sprintf("%s.msg", serverName), msg)
}

//Call 同步调用rpc
func Call(typeName, serverName string, subject string, data interface{}, out interface{}) bool {
	con, ok := GetConnection(typeName)
	if !ok {
		fmt.Printf("nats %s not connect\n", typeName)
		return false
	}
	if out == nil {
		out = emptyRespData
	}
	msg := &ResponseMessage{Subject: subject}
	if data != nil {
		msg.Data, _ = json.Marshal(data)
	}
	err := con.Request(fmt.Sprintf("%s.response", serverName), msg, out, time.Second*5)
	return err == nil
}

//Cast 异步调用rpc
func Cast(typeName, serverName string, subject string, data interface{}) {
	con, ok := GetConnection(typeName)
	if !ok {
		fmt.Printf("nats %s not connect\n", typeName)
		return
	}
	msg := &ResponseMessage{Subject: subject}
	if data != nil {
		msg.Data, _ = json.Marshal(data)
	}
	con.Publish(fmt.Sprintf("%s.response", serverName), msg)
}
