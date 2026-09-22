package engine

import (
	"errors"

	"github.com/lechandonga/preview-env-orchestrator-go/internal/domain"
	"github.com/lechandonga/preview-env-orchestrator-go/internal/sandbox"
)

// errorsIs 封装 errors.Is，供各文件统一使用。
func errorsIs(err, target error) bool { return errors.Is(err, target) }

// sandboxStartReq 组装沙箱启动请求。注意：密钥值只进入请求的 SecretEnv，
// 由沙箱层注入进程环境变量，不会落盘或出现在日志里。
func sandboxStartReq(envName string, svc domain.ServiceSpec, secretValues map[string]string) sandbox.StartRequest {
	env := map[string]string{}
	for k, v := range svc.Env {
		env[k] = v
	}
	// Image 名以约定 "fail" 开头或带 fail=true 标记模拟启动失败（仅用于本地演示/测试）。
	fail := svc.Image == "fail"
	return sandbox.StartRequest{
		EnvName:   envName,
		Service:   svc.Name,
		Image:     svc.Image,
		Env:       env,
		SecretEnv: secretValues,
		Fail:      fail,
	}
}
