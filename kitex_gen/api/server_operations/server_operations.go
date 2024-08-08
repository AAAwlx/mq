// Code generated by Kitex v0.10.3. DO NOT EDIT.

package server_operations

import (
	"context"
	"errors"
	client "github.com/cloudwego/kitex/client"
	kitex "github.com/cloudwego/kitex/pkg/serviceinfo"
	api "mq/kitex_gen/api"
)

var errInvalidMessageType = errors.New("invalid message type for service method handler")

var serviceMethods = map[string]kitex.MethodInfo{
	"push": kitex.NewMethodInfo(
		pushHandler,
		newServer_OperationsPushArgs,
		newServer_OperationsPushResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"pull": kitex.NewMethodInfo(
		pullHandler,
		newServer_OperationsPullArgs,
		newServer_OperationsPullResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"ConInfo": kitex.NewMethodInfo(
		conInfoHandler,
		newServer_OperationsConInfoArgs,
		newServer_OperationsConInfoResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"StarttoGet": kitex.NewMethodInfo(
		starttoGetHandler,
		newServer_OperationsStarttoGetArgs,
		newServer_OperationsStarttoGetResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"PrepareAccept": kitex.NewMethodInfo(
		prepareAcceptHandler,
		newServer_OperationsPrepareAcceptArgs,
		newServer_OperationsPrepareAcceptResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
	"PrepareSend": kitex.NewMethodInfo(
		prepareSendHandler,
		newServer_OperationsPrepareSendArgs,
		newServer_OperationsPrepareSendResult,
		false,
		kitex.WithStreamingMode(kitex.StreamingNone),
	),
}

var (
	server_OperationsServiceInfo                = NewServiceInfo()
	server_OperationsServiceInfoForClient       = NewServiceInfoForClient()
	server_OperationsServiceInfoForStreamClient = NewServiceInfoForStreamClient()
)

// for server
func serviceInfo() *kitex.ServiceInfo {
	return server_OperationsServiceInfo
}

// for stream client
func serviceInfoForStreamClient() *kitex.ServiceInfo {
	return server_OperationsServiceInfoForStreamClient
}

// for client
func serviceInfoForClient() *kitex.ServiceInfo {
	return server_OperationsServiceInfoForClient
}

// NewServiceInfo creates a new ServiceInfo containing all methods
func NewServiceInfo() *kitex.ServiceInfo {
	return newServiceInfo(false, true, true)
}

// NewServiceInfo creates a new ServiceInfo containing non-streaming methods
func NewServiceInfoForClient() *kitex.ServiceInfo {
	return newServiceInfo(false, false, true)
}
func NewServiceInfoForStreamClient() *kitex.ServiceInfo {
	return newServiceInfo(true, true, false)
}

func newServiceInfo(hasStreaming bool, keepStreamingMethods bool, keepNonStreamingMethods bool) *kitex.ServiceInfo {
	serviceName := "Server_Operations"
	handlerType := (*api.Server_Operations)(nil)
	methods := map[string]kitex.MethodInfo{}
	for name, m := range serviceMethods {
		if m.IsStreaming() && !keepStreamingMethods {
			continue
		}
		if !m.IsStreaming() && !keepNonStreamingMethods {
			continue
		}
		methods[name] = m
	}
	extra := map[string]interface{}{
		"PackageName": "api",
	}
	if hasStreaming {
		extra["streaming"] = hasStreaming
	}
	svcInfo := &kitex.ServiceInfo{
		ServiceName:     serviceName,
		HandlerType:     handlerType,
		Methods:         methods,
		PayloadCodec:    kitex.Thrift,
		KiteXGenVersion: "v0.10.3",
		Extra:           extra,
	}
	return svcInfo
}

func pushHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPushArgs)
	realResult := result.(*api.Server_OperationsPushResult)
	success, err := handler.(api.Server_Operations).Push(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPushArgs() interface{} {
	return api.NewServer_OperationsPushArgs()
}

func newServer_OperationsPushResult() interface{} {
	return api.NewServer_OperationsPushResult()
}

func pullHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPullArgs)
	realResult := result.(*api.Server_OperationsPullResult)
	success, err := handler.(api.Server_Operations).Pull(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPullArgs() interface{} {
	return api.NewServer_OperationsPullArgs()
}

func newServer_OperationsPullResult() interface{} {
	return api.NewServer_OperationsPullResult()
}

func conInfoHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsConInfoArgs)
	realResult := result.(*api.Server_OperationsConInfoResult)
	success, err := handler.(api.Server_Operations).ConInfo(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsConInfoArgs() interface{} {
	return api.NewServer_OperationsConInfoArgs()
}

func newServer_OperationsConInfoResult() interface{} {
	return api.NewServer_OperationsConInfoResult()
}

func starttoGetHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsStarttoGetArgs)
	realResult := result.(*api.Server_OperationsStarttoGetResult)
	success, err := handler.(api.Server_Operations).StarttoGet(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsStarttoGetArgs() interface{} {
	return api.NewServer_OperationsStarttoGetArgs()
}

func newServer_OperationsStarttoGetResult() interface{} {
	return api.NewServer_OperationsStarttoGetResult()
}

func prepareAcceptHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPrepareAcceptArgs)
	realResult := result.(*api.Server_OperationsPrepareAcceptResult)
	success, err := handler.(api.Server_Operations).PrepareAccept(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPrepareAcceptArgs() interface{} {
	return api.NewServer_OperationsPrepareAcceptArgs()
}

func newServer_OperationsPrepareAcceptResult() interface{} {
	return api.NewServer_OperationsPrepareAcceptResult()
}

func prepareSendHandler(ctx context.Context, handler interface{}, arg, result interface{}) error {
	realArg := arg.(*api.Server_OperationsPrepareSendArgs)
	realResult := result.(*api.Server_OperationsPrepareSendResult)
	success, err := handler.(api.Server_Operations).PrepareSend(ctx, realArg.Req)
	if err != nil {
		return err
	}
	realResult.Success = success
	return nil
}
func newServer_OperationsPrepareSendArgs() interface{} {
	return api.NewServer_OperationsPrepareSendArgs()
}

func newServer_OperationsPrepareSendResult() interface{} {
	return api.NewServer_OperationsPrepareSendResult()
}

type kClient struct {
	c client.Client
}

func newServiceClient(c client.Client) *kClient {
	return &kClient{
		c: c,
	}
}

func (p *kClient) Push(ctx context.Context, req *api.PushRequest) (r *api.PushResponse, err error) {
	var _args api.Server_OperationsPushArgs
	_args.Req = req
	var _result api.Server_OperationsPushResult
	if err = p.c.Call(ctx, "push", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) Pull(ctx context.Context, req *api.PullRequest) (r *api.PullResponse, err error) {
	var _args api.Server_OperationsPullArgs
	_args.Req = req
	var _result api.Server_OperationsPullResult
	if err = p.c.Call(ctx, "pull", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) ConInfo(ctx context.Context, req *api.InfoRequest) (r *api.InfoResponse, err error) {
	var _args api.Server_OperationsConInfoArgs
	_args.Req = req
	var _result api.Server_OperationsConInfoResult
	if err = p.c.Call(ctx, "ConInfo", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) StarttoGet(ctx context.Context, req *api.InfoGetRequest) (r *api.InfoGetResponse, err error) {
	var _args api.Server_OperationsStarttoGetArgs
	_args.Req = req
	var _result api.Server_OperationsStarttoGetResult
	if err = p.c.Call(ctx, "StarttoGet", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) PrepareAccept(ctx context.Context, req *api.PrepareAcceptRequest) (r *api.PrepareAcceptResponse, err error) {
	var _args api.Server_OperationsPrepareAcceptArgs
	_args.Req = req
	var _result api.Server_OperationsPrepareAcceptResult
	if err = p.c.Call(ctx, "PrepareAccept", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}

func (p *kClient) PrepareSend(ctx context.Context, req *api.PrepareSendRequest) (r *api.PrepareSendResponse, err error) {
	var _args api.Server_OperationsPrepareSendArgs
	_args.Req = req
	var _result api.Server_OperationsPrepareSendResult
	if err = p.c.Call(ctx, "PrepareSend", &_args, &_result); err != nil {
		return
	}
	return _result.GetSuccess(), nil
}
