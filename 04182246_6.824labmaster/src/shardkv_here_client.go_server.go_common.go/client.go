package shardkv

/*
用于与分片键值服务通信的客户端代码。

客户端首先与 shardctrler 通信，以了解分片（键）的分配情况，
然后与持有该键所在分片的组通信。
*/

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

/*
一个键位于哪个分片？
请使用此函数，并请勿更改它。
*/
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	id  int64
	seq uint64
}

/*
测试程序会调用MakeClerk函数。
ctrlers[] 数组用于调用 shardctrler.MakeClerk() 函数。
make_end(servername) 函数将 Config.Groups[gid][i] 中的服务器名称转换为可以发送 RPC 请求的 labrpc.ClientEnd 对象。
*/
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

/*
获取指定键的当前值。
如果该键不存在，则返回 ""。
在遇到其他所有错误时，函数将一直尝试。
You will have to modify this function.
*/
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.id
	args.ClientSeq = ck.seq
	ck.seq++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard(尝试连接负责该分片的每个服务器)
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				Debug(dCLGet, "S%d Send Get Request to Server:%s. args: %v", ck.id, servers[si], args)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				Debug(dCLGet, "S%d Receive Get Reply from Server: %s. args: %v, reply: %v", ck.id, servers[si], args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		Debug(dCLConfig, "S%d Reset Cofnig to :%v", ck.id, ck.config)
	}

	return ""
}

/* 
Put 和 Append 共用的代码。
You will have to modify this function.
*/
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.id
	args.ClientSeq = ck.seq
	ck.seq++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				Debug(dCLPutAppend, "S%d Send PutAppend Request to Server: %s, args: %v", ck.id, servers[si], args)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				Debug(dCLPutAppend, "S%d Receive PutAppend Reply from Server: %s. args: %v, reply: %v", ck.id, servers[si], args, reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		Debug(dCLConfig, "S%d Reset Cofnig to :%v", ck.id, ck.config)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
