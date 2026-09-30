package orchestrator

import (
	"time"

	"github.com/lechandonga/preview-env-orchestrator-go/spec"
)

// 落盘记录只包含密钥引用（key + version + 注入变量名），绝不包含密钥值。

type secretBindingRec struct {
	Env     string `json:"env"`
	Key     string `json:"key"`
	Version int    `json:"version"`
}

type procRec struct {
	PID        int   `json:"pid"`
	StartTicks int64 `json:"start_ticks"`
}

type svcRecord struct {
	Name    string             `json:"name"`
	SvcKey  string             `json:"svc_key"`
	Ports   map[string]int     `json:"ports"`
	Secrets []secretBindingRec `json:"secrets"`
	Proc    *procRec           `json:"proc,omitempty"`
	Blocked bool               `json:"blocked"`
	Failed  bool               `json:"failed"`
}

type envRecord struct {
	Name       string                `json:"name"`
	Spec       *spec.EnvSpec         `json:"spec"`
	SpecKey    string                `json:"spec_key"`
	Status     Status                `json:"status"`
	Services   map[string]*svcRecord `json:"services"`
	CreatedAt  time.Time             `json:"created_at"`
	ExpiresAt  *time.Time            `json:"expires_at,omitempty"`
	Tombstoned bool                  `json:"tombstoned"`
	SchemaV    int                   `json:"schema_v"`
}

const stateSchemaV = 1

func envRel(name string) string { return "envs/" + name + ".json" }

func (o *Orchestrator) persistEnvLocked(st *envState) {
	rec := &envRecord{
		Name:       st.env.Name,
		Spec:       st.spec,
		SpecKey:    st.specKey,
		Status:     st.env.Status,
		Services:   map[string]*svcRecord{},
		CreatedAt:  st.env.CreatedAt,
		Tombstoned: st.tombstoned,
		SchemaV:    stateSchemaV,
	}
	if !st.env.ExpiresAt.IsZero() {
		exp := st.env.ExpiresAt
		rec.ExpiresAt = &exp
	}
	for name, rt := range st.svcs {
		sr := &svcRecord{
			Name:    name,
			SvcKey:  rt.svcKey,
			Ports:   map[string]int{},
			Blocked: rt.blocked,
			Failed:  rt.failed,
		}
		for pn, p := range rt.ports {
			sr.Ports[pn] = p
		}
		for envName, b := range rt.secrets {
			sr.Secrets = append(sr.Secrets, secretBindingRec{
				Env: envName, Key: b.key, Version: b.version,
			})
		}
		if rt.proc != nil && rt.proc.Alive() {
			if ticks, err := rt.proc.StartTicks(); err == nil {
				sr.Proc = &procRec{PID: rt.proc.PID(), StartTicks: ticks}
			}
		}
		rec.Services[name] = sr
	}
	if err := o.store.WriteJSON(envRel(st.env.Name), rec); err != nil {
		// 状态目录写失败 = 无法保证可恢复性：显式打失败流水，调用方可观测。
		o.auditLocked(Event{Kind: "persist-failed", Env: st.env.Name, Detail: err.Error()})
	}
}

func (o *Orchestrator) removeEnvLocked(name string) {
	if err := o.store.Remove(envRel(name)); err != nil {
		o.auditLocked(Event{Kind: "persist-failed", Env: name, Detail: err.Error()})
	}
}
