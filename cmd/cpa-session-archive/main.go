//go:build cshared

package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct { uint32_t abi_version; void* host_ctx; void* call; void* free_buffer; } cliproxy_host_api;
typedef struct { uint32_t abi_version; cliproxy_plugin_call_fn call; cliproxy_plugin_free_fn free_buffer; cliproxy_plugin_shutdown_fn shutdown; } cliproxy_plugin_api;
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"
import (
	"cpa-session-archive/internal/archive"
	"unsafe"
)

var app = archive.NewPlugin()

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(_ *C.cliproxy_host_api, api *C.cliproxy_plugin_api) C.int {
	if api == nil {
		return 1
	}
	api.abi_version = 1
	api.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	api.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	api.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, req *C.uint8_t, n C.size_t, out *C.cliproxy_buffer) C.int {
	if out != nil {
		out.ptr = nil
		out.len = 0
	}
	var b []byte
	if req != nil && n > 0 {
		b = C.GoBytes(unsafe.Pointer(req), C.int(n))
	}
	r, e := app.Handle(C.GoString(method), b)
	if e != nil {
		return 1
	}
	if len(r) > 0 && out != nil {
		p := C.CBytes(r)
		out.ptr = p
		out.len = C.size_t(len(r))
	}
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(p unsafe.Pointer, _ C.size_t) {
	if p != nil {
		C.free(p)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() { app.Shutdown() }
